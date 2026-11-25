"""Unit tests for PID file locking and Unix IPC framing protocol."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from p2p.daemon.ipc_client import send_ipc_request
from p2p.daemon.ipc_protocol import pack_ipc_message, read_ipc_message
from p2p.daemon.ipc_server import IpcServer
from p2p.daemon.pidfile import PidFileLock, is_process_alive, read_pid
from p2p.exceptions import (
    DaemonAlreadyRunningError,
    DaemonError,
    DaemonNotRunningError,
    FramingError,
    PathValidationError,
    PeerNotFoundError,
    ResourceLimitError,
)


@pytest.mark.unit
def test_pidfile_exclusive_lock_and_cleanup(tmp_path: Path) -> None:
    pid_path = tmp_path / "daemon.pid"
    lock1 = PidFileLock(pid_path)
    lock2 = PidFileLock(pid_path)

    pid = lock1.acquire()
    assert pid == os.getpid()
    assert read_pid(pid_path) == pid
    assert is_process_alive(pid) is True

    with pytest.raises(DaemonAlreadyRunningError):
        lock2.acquire()

    lock1.release()
    assert not pid_path.exists()
    # Second release is safe & idempotent
    lock1.release()

    # After release, another instance can acquire cleanly
    assert lock2.acquire() == os.getpid()
    lock2.release()


@pytest.mark.unit
def test_pidfile_rejects_symlink(tmp_path: Path) -> None:
    victim = tmp_path / "victim.txt"
    victim.write_text("do-not-clobber", encoding="utf-8")
    link = tmp_path / "daemon.pid"
    link.symlink_to(victim)

    with pytest.raises(PathValidationError):
        PidFileLock(link).acquire()
    assert victim.read_text(encoding="utf-8") == "do-not-clobber"


@pytest.mark.unit
async def test_ipc_framing_validation_and_unix_server_round_trip() -> None:
    raw = pack_ipc_message({"action": "status", "params": {"ok": True}})
    reader = asyncio.StreamReader()
    reader.feed_data(raw[:4])
    reader.feed_data(raw[4:])
    reader.feed_eof()
    decoded = await read_ipc_message(reader)
    assert decoded == {"action": "status", "params": {"ok": True}}

    # Bad magic rejected
    bad_reader = asyncio.StreamReader()
    bad_reader.feed_data(b"XX\x01\x00\x00\x00\x02{}")
    bad_reader.feed_eof()
    with pytest.raises(FramingError):
        await read_ipc_message(bad_reader)

    # Oversized payload rejected prior to reading body
    big_reader = asyncio.StreamReader()
    big_reader.feed_data(b"PI\x01\x10\x00\x00\x00")
    big_reader.feed_eof()
    with pytest.raises(ResourceLimitError):
        await read_ipc_message(big_reader, max_bytes=1024)

    # Overlong macOS socket path rejected upfront
    with pytest.raises(DaemonError, match="exceeds"):
        IpcServer(
            Path("/var/run/" + ("a" * 120) + ".sock"),
            handler=lambda a, p: asyncio.sleep(0),
        )


@pytest.mark.integration
async def test_unix_ipc_server_permissions_error_mapping_and_round_trip() -> None:
    def _make_short_sock(base: str) -> Path:
        return Path(base) / "d.sock"

    def _socket_mode(path: Path) -> int:
        return path.stat().st_mode & 0o777

    with tempfile.TemporaryDirectory(prefix="p2pipc-") as short_dir:
        sock_path = _make_short_sock(short_dir)

        # Client against non-existent socket raises DaemonNotRunningError cleanly
        with pytest.raises(DaemonNotRunningError):
            await send_ipc_request(sock_path, "status")

        async def _handler(action: str, params: dict[str, Any]) -> dict[str, Any]:
            if action == "echo":
                return {"echo": params}
            if action == "missing":
                raise PeerNotFoundError("peer missing")
            return {}

        server = IpcServer(sock_path, _handler)
        await server.start()
        try:
            res = await send_ipc_request(sock_path, "echo", {"x": 42})
            assert res == {"echo": {"x": 42}}
            assert await asyncio.to_thread(_socket_mode, sock_path) == 0o600

            with pytest.raises(PeerNotFoundError, match="peer missing"):
                await send_ipc_request(sock_path, "missing")
        finally:
            await server.stop()
            assert not await asyncio.to_thread(sock_path.exists)
