"""Integration tests for Milestone 10 security hardening, rate limiting, and shutdown."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from p2p.config import (
    AppConfig,
    DaemonConfig,
    DiscoveryConfig,
    NetworkConfig,
    NodeConfig,
    StorageConfig,
)
from p2p.daemon.service import DaemonRuntimeOptions, P2PDaemon
from p2p.exceptions import ResourceLimitError
from p2p.indexing.file_index import FileIndexer
from p2p.network.protocol import GetFileMessage, SearchRequestMessage, TransferErrorMessage
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import TcpServer
from p2p.search.local_search import LocalSearchService
from p2p.security.limits import SlidingWindowRateLimiter
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository
from p2p.transfer.download import DownloadService
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id


@pytest.mark.integration
async def test_upload_rate_limiter_and_slowloris_and_disk_quota(tmp_path: Path) -> None:
    share_dir = tmp_path / "shared"
    share_dir.mkdir()
    (share_dir / "payload.bin").write_bytes(b"Z" * 16_384)
    dl_dir = tmp_path / "dl"

    peer_a = generate_peer_id()
    peer_b = generate_peer_id()

    with Database(tmp_path / "a.db") as db_a, Database(tmp_path / "b.db") as db_b:
        await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
        fid = FileIndexRepository(db_a).list_all()[0].file_id

        tight_limiter = SlidingWindowRateLimiter(max_requests=2, window_sec=30.0)
        upload = UploadService(
            db_a,
            peer_id=peer_a,
            shared_directories=[share_dir],
            search_service=LocalSearchService(db_a, peer_id=peer_a),
            control_timeout_sec=0.25,
            rate_limiter=tight_limiter,
        )
        server = TcpServer(
            host="127.0.0.1", port=0, peer_id=peer_a, handler=upload.serve_connection
        )
        client = TcpClient(peer_id=peer_b)

        async with server:
            # 1. Slowloris partial-header client stalls and gets closed cleanly
            raw_reader, raw_writer = await asyncio.open_connection("127.0.0.1", server.bound_port)
            raw_writer.write(b"P2\x01\x10\x00\x00\x00\x00")
            await raw_writer.drain()
            eof = await asyncio.wait_for(raw_reader.read(64), timeout=2.0)
            assert eof == b""
            raw_writer.close()
            await raw_writer.wait_closed()

            # 2. First two requests pass rate limit; third hits RATE_LIMITED cleanly
            async with await client.connect("127.0.0.1", server.bound_port) as conn:
                r1 = await conn.request_message(
                    SearchRequestMessage(peer_id=peer_b, query="payload")
                )
                r2 = await conn.request_message(
                    SearchRequestMessage(peer_id=peer_b, query="payload")
                )
                r3 = await conn.request_message(GetFileMessage(peer_id=peer_b, file_id=fid))
                assert not isinstance(r1.payload, TransferErrorMessage)
                assert not isinstance(r2.payload, TransferErrorMessage)
                assert isinstance(r3.payload, TransferErrorMessage)
                assert r3.payload.code == "RATE_LIMITED"
                assert r3.payload.retryable is True
                assert upload.stats.rate_limited == 1

            # 3. Tight download_dir quota blocks incoming 16 KiB download before writing .part
            tight_limiter.reset()
            quota_downloader = DownloadService(
                db_b,
                client,
                local_peer_id=peer_b,
                download_dir=dl_dir,
                max_download_dir_bytes=1024,
            )
            with pytest.raises(ResourceLimitError, match="quota exceeded"):
                await quota_downloader.download_file(
                    host="127.0.0.1",
                    port=server.bound_port,
                    file_id=fid,
                    file_name="payload.bin",
                    source_peer_id=peer_a,
                )
            assert not (dl_dir / "payload.bin").exists()


@pytest.mark.integration
async def test_daemon_graceful_shutdown_leaves_no_stale_files_or_tasks(tmp_path: Path) -> None:
    import tempfile

    with tempfile.TemporaryDirectory(prefix="p2psh-") as short_dir:
        sock_path = Path(short_dir) / "daemon.sock"
        pid_path = tmp_path / "daemon.pid"
        cfg = AppConfig(
            node=NodeConfig(peer_id=generate_peer_id(), display_name="shutdown-node"),
            network=NetworkConfig(bind_host="127.0.0.1", tcp_port=0),
            discovery=DiscoveryConfig(enabled=False, udp_port=19991),
            storage=StorageConfig(
                data_dir=tmp_path,
                state_dir=tmp_path,
                database_path=tmp_path / "d.db",
                download_dir=tmp_path / "dl",
            ),
            daemon=DaemonConfig(socket_path=sock_path, pid_path=pid_path),
        )
        daemon = P2PDaemon(
            cfg,
            options=DaemonRuntimeOptions(
                bind_host="127.0.0.1",
                tcp_port=0,
                udp_port=0,
                broadcast_enabled=False,
            ),
        )
        await daemon.start()
        assert daemon.is_running is True
        assert daemon.tcp_port > 0
        assert daemon.udp_port > 0

        await daemon.stop()
        assert daemon.is_running is False
        assert not pid_path.exists()
        assert not sock_path.exists()
