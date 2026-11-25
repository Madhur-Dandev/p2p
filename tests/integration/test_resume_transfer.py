"""Integration tests for Milestone 9 resumable .part downloads and change recovery."""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path

import pytest

from p2p.exceptions import TransferCancelledError, TransferError
from p2p.indexing.file_index import FileIndexer
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import FileDataMessage, FileInfoMessage, GetFileMessage
from p2p.network.serialization import decode_message, encode_message
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress, TcpServer
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRepository, FileIndexRepository
from p2p.transfer.download import DownloadService
from p2p.transfer.progress import TransferState
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id


@pytest.mark.integration
async def test_interrupted_download_resumes_from_offset_and_verifies_full_sha256(
    tmp_path: Path,
) -> None:
    dl_dir = tmp_path / "dl"
    full_bytes = os.urandom(300_000)
    full_sha256 = hashlib.sha256(full_bytes).hexdigest()
    split_offset = 100_000
    attempt_counter = [0]
    requested_offsets: list[int] = []

    async def flaky_upload_handler(
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
        offset = decoded.payload.offset
        requested_offsets.append(offset)
        attempt_counter[0] += 1

        await write_frame(
            writer,
            encode_message(
                FileInfoMessage(
                    file_id="resume_file_1",
                    file_name="archive.bin",
                    file_size=len(full_bytes),
                    sha256=full_sha256,
                    offset=offset,
                    chunk_size=32_768,
                ),
                request_id=req_id,
            ),
        )

        if attempt_counter[0] == 1:
            # Drop TCP connection cleanly mid-stream right after sending split_offset bytes
            await write_frame(
                writer,
                encode_message(
                    FileDataMessage(data=full_bytes[:split_offset], end_of_stream=False),
                    request_id=req_id,
                ),
            )
            writer.close()
            await writer.wait_closed()
            return

        # Second attempt sends ONLY remaining bytes starting from requested offset!
        remaining = full_bytes[offset:]
        pos = 0
        while pos < len(remaining):
            chunk = remaining[pos : pos + 32_768]
            pos += len(chunk)
            await write_frame(
                writer,
                encode_message(
                    FileDataMessage(data=chunk, end_of_stream=(pos >= len(remaining))),
                    request_id=req_id,
                ),
            )

    peer_b_id = generate_peer_id()
    server = TcpServer(host="127.0.0.1", port=0, handler=flaky_upload_handler)

    with Database(tmp_path / "client.db") as db_b:
        downloader = DownloadService(
            db_b,
            TcpClient(peer_id=peer_b_id),
            local_peer_id=peer_b_id,
            download_dir=dl_dir,
            chunk_size_bytes=32_768,
        )
        repo_b = DownloadRepository(db_b)

        async with server:
            with pytest.raises(TransferError, match="prematurely"):
                await downloader.download_file(
                    host="127.0.0.1",
                    port=server.bound_port,
                    file_id="resume_file_1",
                    file_name="archive.bin",
                )

            history = repo_b.list_downloads()
            assert len(history) == 1
            rec = history[0]
            assert rec.status == str(TransferState.TRANSFER_INTERRUPTED)
            assert rec.bytes_downloaded == split_offset
            cp = repo_b.get_checkpoint(rec.transfer_id)
            assert cp is not None and cp.resume_offset == split_offset

            # Resume same transfer cleanly from split_offset
            resumed = await downloader.resume_download(rec.transfer_id)
            assert resumed.resumed is True
            assert resumed.resume_offset == split_offset
            assert resumed.bytes_downloaded == len(full_bytes)
            assert resumed.actual_sha256 == full_sha256
            assert resumed.state == TransferState.COMPLETED
            assert (dl_dir / "archive.bin").read_bytes() == full_bytes
            assert requested_offsets == [0, split_offset]
            assert repo_b.get_checkpoint(rec.transfer_id) is None
            assert list(dl_dir.glob("*.part")) == []


@pytest.mark.integration
async def test_resume_detects_remote_file_change_and_can_restart_fresh(tmp_path: Path) -> None:
    share_dir = tmp_path / "shared"
    dl_dir = tmp_path / "dl"
    share_dir.mkdir()
    src_file = share_dir / "mutable.bin"
    original = b"FIRST-VERSION-" * 8000
    updated = b"SECOND-VERSION-" * 8000
    src_file.write_bytes(original)

    peer_a = generate_peer_id()
    peer_b = generate_peer_id()

    with Database(tmp_path / "a.db") as db_a, Database(tmp_path / "b.db") as db_b:
        indexer_a = FileIndexer(db_a, shared_directories=[share_dir])
        await indexer_a.rebuild_async()
        fid = FileIndexRepository(db_a).list_all()[0].file_id

        upload_a = UploadService(
            db_a,
            peer_id=peer_a,
            shared_directories=[share_dir],
            default_chunk_size=16_384,
        )
        server_a = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=peer_a,
            handler=upload_a.serve_connection,
        )

        async with server_a:
            downloader = DownloadService(
                db_b,
                TcpClient(peer_id=peer_b),
                local_peer_id=peer_b,
                download_dir=dl_dir,
                chunk_size_bytes=16_384,
            )

            # Start download and cancel mid-flight after partial chunk write -> saves checkpoint!
            cancel_event = asyncio.Event()

            def _cancel_after_first_chunk(prog: object) -> None:
                from p2p.transfer.progress import TransferProgress

                if isinstance(prog, TransferProgress) and prog.bytes_transferred > 0:
                    cancel_event.set()

            with pytest.raises(TransferCancelledError):
                await downloader.download_file(
                    host="127.0.0.1",
                    port=server_a.bound_port,
                    file_id=fid,
                    file_name="mutable.bin",
                    source_peer_id=peer_a,
                    on_progress=_cancel_after_first_chunk,
                    cancel_event=cancel_event,
                )

            rec = DownloadRepository(db_b).list_downloads()[0]
            assert rec.status == str(TransferState.CANCELLED)
            assert DownloadRepository(db_b).get_checkpoint(rec.transfer_id) is not None

            # Mutate remote source file before resuming
            await asyncio.to_thread(src_file.write_bytes, updated)
            await indexer_a.rebuild_async(force_rehash=True)

            # Without restart_if_changed, resume must reject stale .part prefix cleanly
            with pytest.raises(TransferError, match="FILE_CHANGED"):
                await downloader.resume_download(rec.transfer_id, restart_if_changed=False)

            # Create a second interrupted transfer, mutate source again, and verify fresh restart
            cancel_event2 = asyncio.Event()

            def _cancel_once(prog: object) -> None:
                from p2p.transfer.progress import TransferProgress

                if isinstance(prog, TransferProgress) and prog.bytes_transferred > 0:
                    cancel_event2.set()

            with pytest.raises(TransferCancelledError):
                await downloader.download_file(
                    host="127.0.0.1",
                    port=server_a.bound_port,
                    file_id=fid,
                    file_name="mutable2.bin",
                    source_peer_id=peer_a,
                    on_progress=_cancel_once,
                    cancel_event=cancel_event2,
                )
            rec2 = DownloadRepository(db_b).list_downloads()[0]

            # Modify file AGAIN while rec2 is interrupted, then restart_if_changed=True succeeds!
            third = b"THIRD-VERSION-" * 8500
            await asyncio.to_thread(src_file.write_bytes, third)
            await indexer_a.rebuild_async(force_rehash=True)

            restarted = await downloader.resume_download(rec2.transfer_id, restart_if_changed=True)
            assert restarted.state == TransferState.COMPLETED
            assert (dl_dir / "mutable2.bin").read_bytes() == third
