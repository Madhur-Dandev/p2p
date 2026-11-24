"""Integration tests for framed TCP server/client communication and request correlation."""

from __future__ import annotations

import asyncio
import os
import re
import signal
import struct
import subprocess
import sys
from pathlib import Path

import pytest

from p2p.constants import HEADER_FORMAT, PROTOCOL_MAGIC, PROTOCOL_VERSION
from p2p.exceptions import FramingError
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import (
    MessageType,
    PingMessage,
    PongMessage,
    SearchRequestMessage,
    TransferErrorMessage,
)
from p2p.network.serialization import encode_message
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress, TcpServer
from p2p.utils.identifiers import generate_peer_id, generate_request_id


@pytest.mark.integration
async def test_tcp_ping_pong_and_persistent_connection() -> None:
    server_id = "00000000-0000-4000-8000-000000000010"
    client_id = "00000000-0000-4000-8000-000000000020"
    server = TcpServer(
        host="127.0.0.1",
        port=0,
        peer_id=server_id,
        display_name="server-peer",
    )
    client = TcpClient(peer_id=client_id)

    async with server:
        assert server.is_running
        port = server.bound_port
        assert port > 0

        pong = await client.ping("127.0.0.1", port, sent_at_ms=4242)
        assert isinstance(pong, PongMessage)
        assert pong.peer_id == server_id
        assert pong.display_name == "server-peer"
        assert pong.tcp_port == port
        assert pong.status == "ok"
        assert pong.sent_at_ms == 4242

        # Reuse one persistent TCP connection for multiple correlated messages
        async with await client.connect("127.0.0.1", port) as conn:
            d1 = await conn.request_message(PingMessage(peer_id=client_id, sent_at_ms=101))
            d2 = await conn.request_message(SearchRequestMessage(peer_id=client_id, query="notes"))

        assert isinstance(d1.payload, PongMessage) and d1.payload.sent_at_ms == 101
        assert isinstance(d2.payload, TransferErrorMessage)
        assert d2.payload.code == "UNSUPPORTED_OPERATION"
        assert server.stats.total_messages == 3


@pytest.mark.integration
async def test_client_discards_mismatched_request_id_frames() -> None:
    server_id = generate_peer_id()
    client_id = generate_peer_id()
    bogus_req_id = generate_request_id()

    async def spoofed_prefix_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        _ = peer
        frame = await read_frame(reader)
        assert frame is not None
        # First send a stray PONG with an unrelated request_id
        bogus = encode_message(
            PongMessage(peer_id=server_id, tcp_port=9000, status="stale"),
            request_id=bogus_req_id,
        )
        await write_frame(writer, bogus)
        # Then send the genuine reply echoing the exact request_id
        real = encode_message(
            PongMessage(peer_id=server_id, tcp_port=9000, status="ok"),
            request_id=frame.header.request_id,
        )
        await write_frame(writer, real)

    server = TcpServer(host="127.0.0.1", port=0, peer_id=server_id, handler=spoofed_prefix_handler)
    client = TcpClient(peer_id=client_id)
    async with server:
        port = server.bound_port
        async with await client.connect("127.0.0.1", port) as conn:
            reply = await conn.request_message(PingMessage(peer_id=client_id))
            assert isinstance(reply.payload, PongMessage)
            assert reply.payload.status == "ok"
            assert conn.ignored_unmatched_frames == 1


@pytest.mark.integration
async def test_concurrent_clients_succeed() -> None:
    server = TcpServer(host="127.0.0.1", port=0, max_connections=16)
    client = TcpClient(peer_id="00000000-0000-4000-8000-000000000021")

    async with server:

        async def _worker(idx: int) -> int | None:
            pong = await client.ping("127.0.0.1", server.bound_port, sent_at_ms=idx)
            return pong.sent_at_ms

        replies = await asyncio.gather(*(_worker(i) for i in range(12)))
        assert replies == list(range(12))
        assert server.stats.total_accepted == 12
        assert server.stats.total_errors == 0


@pytest.mark.integration
async def test_connection_limit_rejects_excess_clients() -> None:
    gate = asyncio.Event()
    held_started = asyncio.Event()
    held_count = 0

    async def slow_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        nonlocal held_count
        _ = reader, writer, peer
        held_count += 1
        if held_count == 2:
            held_started.set()
        await gate.wait()

    server = TcpServer(host="127.0.0.1", port=0, max_connections=2, handler=slow_handler)
    client = TcpClient()

    async with server:
        conn1 = await client.connect("127.0.0.1", server.bound_port)
        conn2 = await client.connect("127.0.0.1", server.bound_port)
        try:
            await asyncio.wait_for(held_started.wait(), timeout=2.0)

            with pytest.raises((FramingError, ConnectionError, EOFError)):
                await client.ping("127.0.0.1", server.bound_port)

            assert server.stats.rejected_over_limit >= 1
        finally:
            gate.set()
            await conn1.close()
            await conn2.close()


@pytest.mark.integration
async def test_abrupt_disconnect_and_malformed_frames_do_not_crash_server() -> None:
    server = TcpServer(host="127.0.0.1", port=0, max_payload_bytes=1024)
    client = TcpClient()

    async with server:
        port = server.bound_port

        # 1. Raw client sends truncated header and closes abruptly
        _, raw_writer1 = await asyncio.open_connection("127.0.0.1", port)
        raw_writer1.write(b"P2\x01\x10")
        await raw_writer1.drain()
        raw_writer1.close()
        await raw_writer1.wait_closed()

        # 2. Raw client declares oversized 5 MiB frame in 26-byte header
        raw_reader2, raw_writer2 = await asyncio.open_connection("127.0.0.1", port)
        oversized_hdr = struct.pack(
            HEADER_FORMAT,
            PROTOCOL_MAGIC,
            PROTOCOL_VERSION,
            int(MessageType.PING),
            0,
            generate_request_id(),
            5 * 1024 * 1024,
        )
        raw_writer2.write(oversized_hdr)
        await raw_writer2.drain()
        eof = await asyncio.wait_for(raw_reader2.read(16), timeout=2.0)
        assert eof == b""
        raw_writer2.close()
        await raw_writer2.wait_closed()

        # 3. Healthy client still succeeds afterwards
        pong = await client.ping("127.0.0.1", port)
        assert pong.status == "ok"
        assert server.stats.total_errors >= 2


@pytest.mark.integration
async def test_unreachable_port_and_read_timeout() -> None:
    probe = TcpServer(host="127.0.0.1", port=0)
    await probe.start()
    closed_port = probe.bound_port
    await probe.stop()

    client = TcpClient(connect_timeout_sec=1.0, control_timeout_sec=0.2)
    with pytest.raises(OSError):
        await client.ping("127.0.0.1", closed_port)

    async def silent_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        _ = reader, writer, peer
        await asyncio.sleep(2.0)

    silent_server = TcpServer(host="127.0.0.1", port=0, handler=silent_handler)
    async with silent_server:
        with pytest.raises(TimeoutError):
            await client.ping("127.0.0.1", silent_server.bound_port, timeout_sec=0.15)


@pytest.mark.integration
async def test_graceful_shutdown_closes_active_connections() -> None:
    connected = asyncio.Event()

    async def blocking_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        _ = reader, writer, peer
        connected.set()
        await asyncio.sleep(30.0)

    server = TcpServer(
        host="127.0.0.1",
        port=0,
        shutdown_timeout_sec=0.2,
        handler=blocking_handler,
    )
    await server.start()
    client = TcpClient()
    conn = await client.connect("127.0.0.1", server.bound_port)
    await asyncio.wait_for(connected.wait(), timeout=2.0)
    assert server.stats.active_connections == 1

    await server.stop()
    assert not server.is_running
    assert server.stats.active_connections == 0
    await conn.close()


@pytest.mark.integration
def test_dev_tcp_server_and_ping_across_subprocesses(tmp_path: Path) -> None:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{src_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else src_dir
    )

    cfg_path = tmp_path / "dev-peer.toml"
    cfg_path.write_text(
        "[node]\n"
        'peer_id = "00000000-0000-4000-8000-000000000077"\n'
        'display_name = "subproc-node"\n'
        "[logging]\n"
        f'file_path = "{tmp_path / "dev.log"}"\n'
        "console_enabled = false\n"
        "[daemon]\n"
        'socket_path = "/var/run/p2p-dev-subproc.sock"\n',
        encoding="utf-8",
    )

    server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "p2p",
            "--config",
            str(cfg_path),
            "dev",
            "tcp-server",
            "--host",
            "127.0.0.1",
            "--port",
            "0",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        assert server_proc.stdout is not None
        banner = server_proc.stdout.readline()
        match = re.search(r"listening on 127\.0\.0\.1:(\d+)", banner)
        assert match is not None, f"Unexpected banner: {banner!r}"
        bound_port = int(match.group(1))

        ping_proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "p2p",
                "--config",
                str(cfg_path),
                "dev",
                "tcp-ping",
                "127.0.0.1",
                str(bound_port),
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=5.0,
            check=False,
        )
        assert ping_proc.returncode == 0, ping_proc.stderr
        assert "PONG from 127.0.0.1" in ping_proc.stdout
        assert '"status": "ok"' in ping_proc.stdout
        assert '"peer_id": "00000000-0000-4000-8000-000000000077"' in ping_proc.stdout
    finally:
        server_proc.send_signal(signal.SIGINT)
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
