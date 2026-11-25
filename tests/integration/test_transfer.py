"""Integration tests for Milestone 8 direct TCP streaming file transfers & integrity."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import selectors
import signal
import subprocess
import sys
from pathlib import Path
from typing import IO

import pytest

from p2p.config import AppConfig, LimitsConfig, NodeConfig, StorageConfig, TransferConfig
from p2p.exceptions import IntegrityError, TransferError
from p2p.indexing.file_index import FileIndexer
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import (
    FileDataMessage,
    FileInfoMessage,
    GetFileMessage,
    SearchRequestMessage,
    SearchResponseMessage,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress, TcpServer
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRepository, FileIndexRepository
from p2p.transfer.download import DownloadService
from p2p.transfer.progress import TransferProgress, TransferState
from p2p.transfer.transfer_manager import TransferManager
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id
from p2p.utils.time_utils import monotonic_now


def _read_until_match(stream: IO[bytes], pattern: str, *, max_wait_sec: float = 5.0) -> str:
    fd = stream.fileno()
    selector = selectors.DefaultSelector()
    selector.register(fd, selectors.EVENT_READ)
    deadline = monotonic_now() + max_wait_sec
    pending = bytearray()
    seen: list[str] = []
    try:
        while True:
            while b"\n" in pending:
                raw_line, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                line = raw_line.decode("utf-8", errors="replace")
                seen.append(line)
                if re.search(pattern, line):
                    return line
            remaining = deadline - monotonic_now()
            if remaining <= 0 or not selector.select(timeout=remaining):
                raise AssertionError(f"Timeout waiting for {pattern!r}; seen={seen!r}")
            chunk = os.read(fd, 4096)
            if not chunk:
                raise AssertionError(f"EOF waiting for {pattern!r}; seen={seen!r}")
            pending.extend(chunk)
    finally:
        selector.close()


@pytest.mark.integration
async def test_index_search_stream_download_and_verify_sha256(tmp_path: Path) -> None:
    share_dir = tmp_path / "peer_a_shared"
    share_dir.mkdir()
    dl_dir = tmp_path / "peer_b_downloads"

    # Large multi-chunk payload (~2.5 MiB) + empty 0-byte file
    large_payload = os.urandom(2_500_000)
    large_file = share_dir / "dataset archive.bin"
    large_file.write_bytes(large_payload)
    empty_file = share_dir / "empty.txt"
    empty_file.write_bytes(b"")

    peer_a_id = generate_peer_id()
    peer_b_id = generate_peer_id()

    with Database(tmp_path / "peer_a.db") as db_a, Database(tmp_path / "peer_b.db") as db_b:
        indexer_a = FileIndexer(db_a, shared_directories=[share_dir])
        await indexer_a.rebuild_async()
        search_a = LocalSearchService(db_a, peer_id=peer_a_id)
        upload_a = UploadService(
            db_a,
            peer_id=peer_a_id,
            shared_directories=[share_dir],
            search_service=search_a,
            default_chunk_size=16_384,
        )
        server_a = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=peer_a_id,
            handler=upload_a.serve_connection,
        )

        client_b = TcpClient(peer_id=peer_b_id)
        downloader_b = DownloadService(
            db_b,
            client_b,
            local_peer_id=peer_b_id,
            download_dir=dl_dir,
            chunk_size_bytes=16_384,
        )

        async with server_a:
            port_a = server_a.bound_port
            upload_a.set_tcp_port(port_a)

            # Peer B searches Peer A for dataset
            resp = await client_b.request(
                "127.0.0.1",
                port_a,
                SearchRequestMessage(peer_id=peer_b_id, query="dataset"),
            )
            assert isinstance(resp.payload, SearchResponseMessage)
            assert len(resp.payload.results) == 1
            hit = resp.payload.results[0]

            events: list[TransferProgress] = []
            result = await downloader_b.download_file(
                host="127.0.0.1",
                port=port_a,
                file_id=hit.file_id,
                file_name=hit.file_name,
                source_peer_id=peer_a_id,
                expected_sha256=hit.sha256,
                expected_size=hit.file_size,
                on_progress=events.append,
            )

            assert result.state == TransferState.COMPLETED
            assert result.destination_path == dl_dir.resolve() / "dataset archive.bin"
            assert result.destination_path.read_bytes() == large_payload
            assert result.actual_sha256 == hashlib.sha256(large_payload).hexdigest()
            assert any(p.state == TransferState.TRANSFERRING for p in events)
            assert events[-1].state == TransferState.COMPLETED
            assert events[-1].percent == 100.0
            assert list(dl_dir.glob("*.part")) == []

            # Verify empty 0-byte file transfer also completes cleanly
            empty_hit = next(
                r for r in FileIndexRepository(db_a).list_all() if r.file_name == "empty.txt"
            )
            empty_res = await downloader_b.download_file(
                host="127.0.0.1",
                port=port_a,
                file_id=empty_hit.file_id,
                file_name=empty_hit.file_name,
                source_peer_id=peer_a_id,
            )
            assert empty_res.bytes_downloaded == 0
            assert empty_res.destination_path.read_bytes() == b""

            # Refuses silent overwrite when destination already exists
            with pytest.raises(TransferError, match="already exists"):
                await downloader_b.download_file(
                    host="127.0.0.1",
                    port=port_a,
                    file_id=empty_hit.file_id,
                    file_name=empty_hit.file_name,
                    source_peer_id=peer_a_id,
                )

            history = DownloadRepository(db_b).list_downloads()
            assert len(history) == 2
            assert all(h.status == str(TransferState.COMPLETED) for h in history)


@pytest.mark.integration
async def test_missing_file_and_post_index_symlink_attack_rejected(tmp_path: Path) -> None:
    share_dir = tmp_path / "shared"
    share_dir.mkdir()
    victim_file = share_dir / "legit.txt"
    victim_file.write_bytes(b"safe-data")

    outside_secret = tmp_path / "secret.txt"
    outside_secret.write_bytes(b"PRIVATE-KEY-MATERIAL")

    peer_a_id = generate_peer_id()
    peer_b_id = generate_peer_id()

    with Database(tmp_path / "a.db") as db_a, Database(tmp_path / "b.db") as db_b:
        await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
        indexed = FileIndexRepository(db_a).list_all()[0]

        upload_a = UploadService(db_a, peer_id=peer_a_id, shared_directories=[share_dir])
        server_a = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=peer_a_id,
            handler=upload_a.serve_connection,
        )
        downloader_b = DownloadService(
            db_b,
            TcpClient(peer_id=peer_b_id),
            local_peer_id=peer_b_id,
            download_dir=tmp_path / "dl",
        )

        async with server_a:
            port_a = server_a.bound_port

            # 1. Missing file_id produces actionable FILE_NOT_FOUND error
            with pytest.raises(TransferError, match="FILE_NOT_FOUND"):
                await downloader_b.download_file(
                    host="127.0.0.1",
                    port=port_a,
                    file_id="missing_id_000",
                    file_name="missing.txt",
                    source_peer_id=peer_a_id,
                )

            # 2. Replace indexed regular file with symlink to outside_secret (TOCTOU test)
            victim_file.unlink()
            victim_file.symlink_to(outside_secret)

            with pytest.raises(TransferError, match="ACCESS_DENIED"):
                await downloader_b.download_file(
                    host="127.0.0.1",
                    port=port_a,
                    file_id=indexed.file_id,
                    file_name=indexed.file_name,
                    source_peer_id=peer_a_id,
                )

            assert not (tmp_path / "dl" / "legit.txt").exists()


@pytest.mark.integration
async def test_corrupted_transfer_fails_integrity_and_abrupt_eof_saves_checkpoint(
    tmp_path: Path,
) -> None:
    dl_dir = tmp_path / "dl"
    peer_b_id = generate_peer_id()
    good_bytes = b"A" * 4096
    good_sha256 = hashlib.sha256(good_bytes).hexdigest()

    async def corrupt_server_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        _ = peer
        frame = await read_frame(reader)
        assert frame is not None
        decoded = decode_message(frame)
        assert isinstance(decoded.payload, GetFileMessage)
        req_id = frame.header.request_id

        if decoded.payload.file_id == "corrupt_file":
            await write_frame(
                writer,
                encode_message(
                    FileInfoMessage(
                        file_id="corrupt_file",
                        file_name="corrupt.bin",
                        file_size=len(good_bytes),
                        sha256=good_sha256,
                        offset=0,
                        chunk_size=2048,
                    ),
                    request_id=req_id,
                ),
            )
            # Flip 1 byte in transit
            tampered = b"B" + good_bytes[1:]
            await write_frame(
                writer,
                encode_message(
                    FileDataMessage(data=tampered, end_of_stream=True),
                    request_id=req_id,
                ),
            )
        elif decoded.payload.file_id == "drop_file":
            await write_frame(
                writer,
                encode_message(
                    FileInfoMessage(
                        file_id="drop_file",
                        file_name="partial.bin",
                        file_size=4096,
                        sha256=good_sha256,
                        offset=0,
                        chunk_size=1024,
                    ),
                    request_id=req_id,
                ),
            )
            # Send only first 1024 bytes without end_of_stream, then close abruptly
            await write_frame(
                writer,
                encode_message(
                    FileDataMessage(data=good_bytes[:1024], end_of_stream=False),
                    request_id=req_id,
                ),
            )
            writer.close()
            await writer.wait_closed()

    server = TcpServer(host="127.0.0.1", port=0, handler=corrupt_server_handler)
    with Database(tmp_path / "b.db") as db_b:
        downloader = DownloadService(
            db_b,
            TcpClient(peer_id=peer_b_id),
            local_peer_id=peer_b_id,
            download_dir=dl_dir,
        )

        async with server:
            # 1. Corrupted bytes must fail SHA-256 verification and never mark completed
            with pytest.raises(IntegrityError, match="SHA-256 mismatch"):
                await downloader.download_file(
                    host="127.0.0.1",
                    port=server.bound_port,
                    file_id="corrupt_file",
                    file_name="corrupt.bin",
                )
            assert not (dl_dir / "corrupt.bin").exists()

            # 2. Abrupt EOF must mark TRANSFER_INTERRUPTED and retain .part + checkpoint
            with pytest.raises(TransferError, match="prematurely"):
                await downloader.download_file(
                    host="127.0.0.1",
                    port=server.bound_port,
                    file_id="drop_file",
                    file_name="partial.bin",
                )
            assert not (dl_dir / "partial.bin").exists()

            records = DownloadRepository(db_b).list_downloads()
            by_file = {r.file_id: r for r in records}
            assert by_file["corrupt_file"].status == str(TransferState.INTEGRITY_FAILED)
            interrupted = by_file["drop_file"]
            assert interrupted.status == str(TransferState.TRANSFER_INTERRUPTED)
            assert interrupted.bytes_downloaded == 1024

            def _read_part_size(path_str: str) -> int:
                part = Path(path_str)
                return part.stat().st_size if part.exists() else -1

            assert await asyncio.to_thread(_read_part_size, interrupted.part_path) == 1024
            cp = DownloadRepository(db_b).get_checkpoint(interrupted.transfer_id)
            assert cp is not None and cp.resume_offset == 1024


@pytest.mark.integration
async def test_concurrent_downloads_succeed_via_transfer_manager(tmp_path: Path) -> None:
    share_dir = tmp_path / "shared"
    share_dir.mkdir()
    dl_dir = tmp_path / "downloads"

    files_data = {f"blob_{i}.bin": os.urandom(120_000 + i * 137) for i in range(5)}
    for name, content in files_data.items():
        (share_dir / name).write_bytes(content)

    peer_a_id = generate_peer_id()
    peer_b_id = generate_peer_id()

    with Database(tmp_path / "a.db") as db_a, Database(tmp_path / "b.db") as db_b:
        await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
        indexed_rows = FileIndexRepository(db_a).list_all()

        cfg_a = AppConfig(
            node=NodeConfig(peer_id=peer_a_id, display_name="host"),
            storage=StorageConfig(
                data_dir=tmp_path,
                state_dir=tmp_path,
                database_path=tmp_path / "a.db",
                download_dir=tmp_path / "unused",
            ),
            transfer=TransferConfig(chunk_size_bytes=8192),
            limits=LimitsConfig(max_concurrent_transfers=3),
        )
        cfg_a.sharing.directories = [share_dir.resolve()]
        mgr_a = TransferManager(cfg_a, db_a)

        cfg_b = AppConfig(
            node=NodeConfig(peer_id=peer_b_id, display_name="client"),
            storage=StorageConfig(
                data_dir=tmp_path,
                state_dir=tmp_path,
                database_path=tmp_path / "b.db",
                download_dir=dl_dir,
            ),
            transfer=TransferConfig(chunk_size_bytes=8192),
            limits=LimitsConfig(max_concurrent_transfers=3),
        )
        mgr_b = TransferManager(cfg_b, db_b)

        server = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=peer_a_id,
            handler=mgr_a.serve_connection,
        )
        async with server:
            mgr_a.upload_service.set_tcp_port(server.bound_port)
            results = await asyncio.gather(
                *(
                    mgr_b.download_file(
                        host="127.0.0.1",
                        port=server.bound_port,
                        file_id=row.file_id,
                        file_name=row.file_name,
                        source_peer_id=peer_a_id,
                    )
                    for row in indexed_rows
                )
            )

        assert len(results) == 5
        for res in results:
            assert res.state == TransferState.COMPLETED
            assert res.destination_path.read_bytes() == files_data[res.file_name]
        assert mgr_b.active_download_count == 0


@pytest.mark.integration
def test_cli_end_to_end_index_search_serve_download_and_history(tmp_path: Path) -> None:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{src_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else src_dir
    )

    host_share = tmp_path / "host_share"
    host_share.mkdir()
    sample_file = host_share / "project specification.txt"
    sample_bytes = b"Decentralized P2P verification payload!" * 1000
    sample_file.write_bytes(sample_bytes)

    host_cfg = tmp_path / "host.toml"
    client_cfg = tmp_path / "client.toml"
    client_dl = tmp_path / "client_dl"

    host_cfg.write_text(
        "[node]\n"
        'peer_id = "00000000-0000-4000-8000-000000000201"\n'
        'display_name = "host-cli"\n'
        "[storage]\n"
        f'data_dir = "{tmp_path / "host_data"}"\n'
        f'state_dir = "{tmp_path / "host_state"}"\n'
        f'database_path = "{tmp_path / "host.db"}"\n'
        f'download_dir = "{tmp_path / "host_dl"}"\n'
        "[sharing]\n"
        f'directories = ["{host_share.resolve()}"]\n'
        "[logging]\n"
        f'file_path = "{tmp_path / "host.log"}"\n'
        "console_enabled = false\n"
        "[daemon]\n"
        'socket_path = "/var/run/p2p-host-cli.sock"\n',
        encoding="utf-8",
    )
    client_cfg.write_text(
        "[node]\n"
        'peer_id = "00000000-0000-4000-8000-000000000202"\n'
        'display_name = "client-cli"\n'
        "[storage]\n"
        f'data_dir = "{tmp_path / "client_data"}"\n'
        f'state_dir = "{tmp_path / "client_state"}"\n'
        f'database_path = "{tmp_path / "client.db"}"\n'
        f'download_dir = "{client_dl}"\n'
        "[logging]\n"
        f'file_path = "{tmp_path / "client.log"}"\n'
        "console_enabled = false\n"
        "[daemon]\n"
        'socket_path = "/var/run/p2p-client-cli.sock"\n',
        encoding="utf-8",
    )

    # 1. Host rebuilds index & searches locally to get file_id
    idx_proc = subprocess.run(
        [sys.executable, "-m", "p2p", "--config", str(host_cfg), "index", "rebuild"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert idx_proc.returncode == 0, idx_proc.stderr
    assert "added=1" in idx_proc.stdout

    srch_proc = subprocess.run(
        [sys.executable, "-m", "p2p", "--config", str(host_cfg), "search", "specification"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert srch_proc.returncode == 0, srch_proc.stderr
    fid_match = re.search(r"file_id=([A-Za-z0-9_-]+)", srch_proc.stdout)
    assert fid_match is not None, srch_proc.stdout
    file_id = fid_match.group(1)

    # 2. Host starts foreground peer-node
    server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "p2p",
            "--config",
            str(host_cfg),
            "dev",
            "peer-node",
            "--host",
            "127.0.0.1",
            "--tcp-port",
            "0",
            "--udp-port",
            "0",
            "--no-broadcast",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        env=env,
    )
    try:
        assert server_proc.stdout is not None
        banner = _read_until_match(server_proc.stdout, r"Peer node running")
        port_match = re.search(r"tcp=127\.0\.0\.1:(\d+)", banner)
        assert port_match is not None, banner
        tcp_port = int(port_match.group(1))

        # 3. Client downloads directly via CLI & checks downloads history
        dl_proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "p2p",
                "--config",
                str(client_cfg),
                "download",
                file_id,
                "--peer",
                f"127.0.0.1:{tcp_port}",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=10.0,
            check=False,
        )
        assert dl_proc.returncode == 0, dl_proc.stderr
        assert "Download completed:" in dl_proc.stdout
        assert (client_dl / "project specification.txt").read_bytes() == sample_bytes

        hist_proc = subprocess.run(
            [sys.executable, "-m", "p2p", "--config", str(client_cfg), "downloads"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert hist_proc.returncode == 0, hist_proc.stderr
        assert "project specification.txt" in hist_proc.stdout
        assert "status=completed" in hist_proc.stdout
    finally:
        server_proc.send_signal(signal.SIGINT)
        try:
            server_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait(timeout=5.0)
