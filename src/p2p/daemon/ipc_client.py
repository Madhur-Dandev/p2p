"""Client utilities for communicating with the local P2P daemon over Unix socket IPC."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from p2p.constants import MAX_FRAME_PAYLOAD_BYTES
from p2p.daemon.ipc_protocol import error_dict_to_exception, read_ipc_message, write_ipc_message
from p2p.exceptions import DaemonError, DaemonNotRunningError, FramingError, ResourceLimitError


def _resolve_socket_sync(socket_path: Path) -> str:
    return str(Path(socket_path).expanduser().resolve(strict=False))


async def send_ipc_request(
    socket_path: Path,
    action: str,
    params: dict[str, Any] | None = None,
    *,
    timeout_sec: float | None = 15.0,
    max_bytes: int = MAX_FRAME_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Connect to the daemon Unix domain socket, run action, and return result dictionary."""
    resolved = _resolve_socket_sync(socket_path)
    connect_timeout = 5.0 if timeout_sec is None else min(timeout_sec, 5.0)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(resolved),
            timeout=connect_timeout,
        )
    except (FileNotFoundError, ConnectionRefusedError, PermissionError, OSError) as exc:
        raise DaemonNotRunningError(
            f"P2P daemon is not running (cannot connect to {resolved}: {exc}). "
            "Start with: p2p start"
        ) from exc

    try:
        await write_ipc_message(
            writer,
            {"action": action, "params": params or {}},
            max_bytes=max_bytes,
            timeout_sec=timeout_sec,
        )
        reply = await read_ipc_message(
            reader,
            max_bytes=max_bytes,
            timeout_sec=timeout_sec,
        )
        if reply is None:
            raise DaemonError("Daemon closed IPC connection without returning a response")
        if not reply.get("ok"):
            err = reply.get("error")
            if isinstance(err, dict):
                raise error_dict_to_exception(err)
            raise DaemonError("Daemon returned an error response")
        result = reply.get("result", {})
        if not isinstance(result, dict):
            raise FramingError("Daemon IPC result payload must be a JSON object")
        return result
    except (FramingError, ResourceLimitError, TimeoutError) as exc:
        raise DaemonError(f"IPC communication failed for action {action!r}: {exc}") from exc
    finally:
        try:
            if not writer.is_closing():
                writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
        except (OSError, TimeoutError, ConnectionError, asyncio.CancelledError):
            pass


def call_ipc(
    socket_path: Path,
    action: str,
    params: dict[str, Any] | None = None,
    *,
    timeout_sec: float = 15.0,
) -> dict[str, Any]:
    """Synchronous CLI wrapper around send_ipc_request."""
    return asyncio.run(send_ipc_request(socket_path, action, params, timeout_sec=timeout_sec))


def is_daemon_running(socket_path: Path) -> bool:
    """Return True if a live daemon responds to a quick IPC status probe."""
    try:
        call_ipc(socket_path, "status", timeout_sec=1.0)
        return True
    except DaemonError:
        return False
