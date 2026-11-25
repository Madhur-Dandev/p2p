"""Direct TCP file downloader with streaming disk writes, resume, and SHA-256 verification."""

from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from p2p.constants import (
    DEFAULT_CONTROL_TIMEOUT_SEC,
    DEFAULT_TRANSFER_CHUNK_BYTES,
    DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_FILE_SIZE_BYTES,
    MAX_FILENAME_LENGTH,
)
from p2p.exceptions import (
    FramingError,
    IntegrityError,
    MalformedMessageError,
    ProtocolError,
    ResourceLimitError,
    TransferCancelledError,
    TransferError,
    TransferTimeoutError,
    UnsupportedVersionError,
)
from p2p.indexing.hashing import compute_sha256_file_async
from p2p.logging_config import get_context_logger
from p2p.network.framing import read_frame
from p2p.network.protocol import (
    FileDataMessage,
    FileInfoMessage,
    GetFileMessage,
    TransferErrorMessage,
    validate_file_id,
)
from p2p.network.serialization import decode_message
from p2p.network.tcp_client import TcpClient, TcpConnection
from p2p.security.limits import check_download_disk_space
from p2p.security.path_validation import (
    open_part_file_safe,
    resolve_download_destination,
    validate_resume_part_file,
)
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRecord, DownloadRepository, TransferCheckpoint
from p2p.transfer.progress import (
    ProgressCallback,
    TransferProgress,
    TransferState,
    TransferStateMachine,
)
from p2p.utils.identifiers import generate_transfer_id
from p2p.utils.time_utils import monotonic_now, utc_now_iso

_CHECKPOINT_FLUSH_INTERVAL_BYTES = 1_048_576  # Periodically save checkpoint every 1 MiB


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Summary returned upon successful verified file download."""

    transfer_id: str
    file_id: str
    file_name: str
    destination_path: Path
    bytes_downloaded: int
    expected_sha256: str
    actual_sha256: str
    state: TransferState
    duration_sec: float
    throughput_mbps: float
    resumed: bool = False
    resume_offset: int = 0


def _write_sync(handle: BinaryIO, data: bytes) -> None:
    handle.write(data)


def _flush_and_close_sync(handle: BinaryIO) -> None:
    try:
        handle.flush()
        os.fsync(handle.fileno())
    finally:
        handle.close()


def _unlink_sync(path: Path) -> None:
    path.unlink(missing_ok=True)


def _path_from_str_sync(raw: str) -> Path:
    return Path(raw)


def _part_disk_size_sync(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _promote_part_sync(part_path: Path, final_path: Path, *, overwrite: bool) -> None:
    if final_path.is_symlink():
        raise TransferError(f"Refusing symlink destination: {final_path}")
    if final_path.exists() and not overwrite:
        raise TransferError(f"Destination file already exists: {final_path}")
    os.replace(part_path, final_path)


class DownloadService:
    """Executes fresh and resumable TCP downloads into .part files with SHA-256 verification."""

    def __init__(
        self,
        db: Database,
        tcp_client: TcpClient,
        *,
        local_peer_id: str,
        download_dir: Path,
        chunk_size_bytes: int = DEFAULT_TRANSFER_CHUNK_BYTES,
        max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES,
        max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
        max_filename_length: int = MAX_FILENAME_LENGTH,
        max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
        control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
        idle_timeout_sec: float = DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC,
        overwrite_existing: bool = False,
        max_download_dir_bytes: int = 0,
    ) -> None:
        self._db = db
        self._repo = DownloadRepository(db)
        self._tcp_client = tcp_client
        self._local_peer_id = local_peer_id
        self._download_dir = Path(download_dir).expanduser().resolve(strict=False)
        self._chunk_size_bytes = chunk_size_bytes
        self._max_data_chunk_bytes = max_data_chunk_bytes
        self._max_control_bytes = max_control_bytes
        self._max_filename_length = max_filename_length
        self._max_file_size_bytes = max_file_size_bytes
        self._control_timeout_sec = control_timeout_sec
        self._idle_timeout_sec = idle_timeout_sec
        self._overwrite_existing = overwrite_existing
        self._max_download_dir_bytes = max_download_dir_bytes

    async def download_file(
        self,
        *,
        host: str,
        port: int,
        file_id: str,
        file_name: str | None = None,
        source_peer_id: str = "",
        expected_sha256: str = "",
        expected_size: int = 0,
        download_dir: Path | None = None,
        on_progress: ProgressCallback | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> DownloadResult:
        """Download file_id from (host, port) starting at offset 0 and verify SHA-256."""
        valid_file_id = validate_file_id(file_id)
        transfer_id = generate_transfer_id()
        now_iso = utc_now_iso()
        target_dir = self._download_dir if download_dir is None else download_dir

        final_path: Path | None = None
        part_path: Path | None = None
        display_name = valid_file_id
        if file_name is not None:
            final_path, part_path = await asyncio.to_thread(
                resolve_download_destination,
                target_dir,
                file_name,
                transfer_id=transfer_id,
                max_filename_length=self._max_filename_length,
                overwrite=self._overwrite_existing,
            )
            display_name = final_path.name

        await self._db.run(
            lambda: self._repo.create_download(
                DownloadRecord(
                    transfer_id=transfer_id,
                    file_id=valid_file_id,
                    file_name=display_name,
                    source_peer_id=source_peer_id or "unknown",
                    source_ip=host,
                    source_tcp_port=port,
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    bytes_downloaded=0,
                    destination_path=str(final_path) if final_path else "",
                    part_path=str(part_path) if part_path else "",
                    status=str(TransferState.QUEUED),
                    error_message=None,
                    started_at=now_iso,
                    updated_at=now_iso,
                    completed_at=None,
                )
            )
        )

        return await self._execute_transfer(
            transfer_id=transfer_id,
            host=host,
            port=port,
            file_id=valid_file_id,
            source_peer_id=source_peer_id,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            resume_offset=0,
            download_dir=target_dir,
            final_path=final_path,
            part_path=part_path,
            display_name=display_name,
            initial_state=TransferState.QUEUED,
            on_progress=on_progress,
            cancel_event=cancel_event,
            restart_if_changed=False,
        )

    async def resume_download(
        self,
        identifier: str,
        *,
        host: str | None = None,
        port: int | None = None,
        download_dir: Path | None = None,
        restart_if_changed: bool = False,
        on_progress: ProgressCallback | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> DownloadResult:
        """Resume an interrupted or cancelled .part download from its persisted offset."""
        found = await self._db.run(lambda: self._repo.find_resumable(identifier))
        if found is None:
            raise TransferError(f"No resumable transfer checkpoint found for {identifier!r}")

        record, checkpoint = found
        target_host = host if host is not None else record.source_ip
        target_port = port if port is not None else record.source_tcp_port
        target_dir = self._download_dir if download_dir is None else download_dir

        if record.status == str(TransferState.SOURCE_UNAVAILABLE) and restart_if_changed:
            if checkpoint.part_path:
                await asyncio.to_thread(_unlink_sync, _path_from_str_sync(checkpoint.part_path))
            await self._db.run(lambda: self._repo.delete_checkpoint(record.transfer_id))
            return await self.download_file(
                host=target_host,
                port=target_port,
                file_id=record.file_id,
                file_name=record.file_name if record.file_name != record.file_id else None,
                source_peer_id=record.source_peer_id,
                download_dir=target_dir,
                on_progress=on_progress,
                cancel_event=cancel_event,
            )

        if record.status not in {
            str(TransferState.TRANSFER_INTERRUPTED),
            str(TransferState.CANCELLED),
        }:
            raise TransferError(
                f"Transfer {record.transfer_id} is in state '{record.status}' and cannot be resumed"
            )

        final_path, part_path = await asyncio.to_thread(
            validate_resume_part_file,
            target_dir,
            checkpoint.part_path,
            record.destination_path,
            overwrite=self._overwrite_existing,
        )
        disk_size = await asyncio.to_thread(_part_disk_size_sync, part_path)
        safe_offset = min(checkpoint.resume_offset, disk_size, checkpoint.expected_size)

        return await self._execute_transfer(
            transfer_id=record.transfer_id,
            host=target_host,
            port=target_port,
            file_id=record.file_id,
            source_peer_id=record.source_peer_id,
            expected_sha256=checkpoint.expected_sha256,
            expected_size=checkpoint.expected_size,
            resume_offset=safe_offset,
            download_dir=target_dir,
            final_path=final_path,
            part_path=part_path,
            display_name=final_path.name,
            initial_state=TransferState(record.status),
            on_progress=on_progress,
            cancel_event=cancel_event,
            restart_if_changed=restart_if_changed,
        )

    async def _execute_transfer(
        self,
        *,
        transfer_id: str,
        host: str,
        port: int,
        file_id: str,
        source_peer_id: str,
        expected_sha256: str,
        expected_size: int,
        resume_offset: int,
        download_dir: Path,
        final_path: Path | None,
        part_path: Path | None,
        display_name: str,
        initial_state: TransferState,
        on_progress: ProgressCallback | None,
        cancel_event: asyncio.Event | None,
        restart_if_changed: bool,
    ) -> DownloadResult:
        is_resume = resume_offset > 0 or initial_state != TransferState.QUEUED
        sm = TransferStateMachine(initial_state)
        started_mono = monotonic_now()
        logger = get_context_logger(
            "transfer.download",
            peer_id=source_peer_id or self._local_peer_id,
            transfer_id=transfer_id,
        )

        def _emit(bytes_done: int, total_bytes: int) -> None:
            if on_progress is None:
                return
            elapsed = max(0.0, monotonic_now() - started_mono)
            transferred_this_run = max(0, bytes_done - resume_offset)
            mbps = (transferred_this_run * 8.0 / elapsed / 1_000_000.0) if elapsed > 0 else 0.0
            on_progress(
                TransferProgress(
                    transfer_id=transfer_id,
                    file_id=file_id,
                    file_name=display_name,
                    state=sm.state,
                    bytes_transferred=bytes_done,
                    total_bytes=total_bytes,
                    elapsed_sec=elapsed,
                    throughput_mbps=mbps,
                )
            )

        async def _update_db(
            state: TransferState,
            *,
            bytes_done: int | None = None,
            err_msg: str | None = None,
            sha256: str | None = None,
            size: int | None = None,
            completed_at: str | None = None,
        ) -> None:
            await self._db.run(
                lambda: self._repo.update_state(
                    transfer_id,
                    status=str(state),
                    file_name=display_name,
                    destination_path=str(final_path) if final_path else None,
                    part_path=str(part_path) if part_path else None,
                    bytes_downloaded=bytes_done,
                    expected_sha256=sha256,
                    expected_size=size,
                    error_message=err_msg,
                    completed_at=completed_at,
                )
            )

        async def _save_checkpoint(bytes_done: int, sha256_hex: str, size: int) -> None:
            if part_path is None:
                return
            await self._db.run(
                lambda: self._repo.upsert_checkpoint(
                    TransferCheckpoint(
                        transfer_id=transfer_id,
                        resume_offset=bytes_done,
                        expected_sha256=sha256_hex,
                        expected_size=size,
                        part_path=str(part_path),
                        updated_at=utc_now_iso(),
                    )
                )
            )

        def _raise_if_cancelled() -> None:
            if cancel_event is not None and cancel_event.is_set():
                raise TransferCancelledError(f"Download {transfer_id} was cancelled")

        conn: TcpConnection | None = None
        part_handle: BinaryIO | None = None
        bytes_received = resume_offset
        total_expected = expected_size
        active_sha256 = expected_sha256

        try:
            _raise_if_cancelled()
            sm.transition(TransferState.CONNECTING)
            await _update_db(sm.state, bytes_done=bytes_received)
            _emit(bytes_received, total_expected)

            try:
                conn = await self._tcp_client.connect(host, port)
            except (OSError, TimeoutError) as exc:
                sm.transition(TransferState.CONNECTION_FAILED)
                await _update_db(
                    sm.state,
                    bytes_done=bytes_received,
                    err_msg=f"Connection failed: {exc}",
                )
                raise TransferError(f"Failed to connect to {host}:{port}: {exc}") from exc

            async with conn:
                _raise_if_cancelled()
                sm.transition(TransferState.REQUESTING)
                await _update_db(sm.state, bytes_done=bytes_received)

                req_id = await conn.send_message(
                    GetFileMessage(
                        peer_id=self._local_peer_id,
                        file_id=file_id,
                        offset=resume_offset,
                        expected_sha256=(
                            expected_sha256 if (resume_offset > 0 and expected_sha256) else None
                        ),
                        chunk_size=self._chunk_size_bytes,
                    ),
                    timeout_sec=self._control_timeout_sec,
                )

                info = await self._await_file_info(conn, req_id)
                if isinstance(info, TransferErrorMessage):
                    if (
                        is_resume
                        and restart_if_changed
                        and info.code in {"FILE_CHANGED", "INVALID_OFFSET"}
                    ):
                        logger.warning(
                            "Source changed (%s) while resuming %s; restarting fresh from offset 0",
                            info.code,
                            display_name,
                        )
                        if part_path is not None:
                            await asyncio.to_thread(_unlink_sync, part_path)
                        await self._db.run(lambda: self._repo.delete_checkpoint(transfer_id))
                        await _update_db(
                            TransferState.SOURCE_UNAVAILABLE,
                            bytes_done=bytes_received,
                            err_msg=f"Superseded by fresh restart ({info.code})",
                        )
                        return await self.download_file(
                            host=host,
                            port=port,
                            file_id=file_id,
                            file_name=display_name if display_name != file_id else None,
                            source_peer_id=source_peer_id,
                            download_dir=download_dir,
                            on_progress=on_progress,
                            cancel_event=cancel_event,
                        )

                    sm.transition(TransferState.SOURCE_UNAVAILABLE)
                    err_text = f"{info.code}: {info.message}"
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    raise TransferError(err_text)

                if expected_sha256 and info.sha256 != expected_sha256:
                    sm.transition(TransferState.SOURCE_UNAVAILABLE)
                    err_text = "Remote file SHA-256 does not match expected metadata"
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    raise IntegrityError(err_text)

                if self._max_file_size_bytes > 0 and info.file_size > self._max_file_size_bytes:
                    sm.transition(TransferState.SOURCE_UNAVAILABLE)
                    err_text = (
                        f"File size {info.file_size} exceeds limit {self._max_file_size_bytes}"
                    )
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    raise ResourceLimitError(err_text)

                if info.offset != resume_offset:
                    sm.transition(TransferState.TRANSFER_INTERRUPTED)
                    err_text = (
                        f"Unexpected server start offset {info.offset} (requested {resume_offset})"
                    )
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    raise ProtocolError(err_text)

                if final_path is None or part_path is None:
                    final_path, part_path = await asyncio.to_thread(
                        resolve_download_destination,
                        download_dir,
                        info.file_name,
                        transfer_id=transfer_id,
                        max_filename_length=self._max_filename_length,
                        overwrite=self._overwrite_existing,
                    )
                display_name = final_path.name
                total_expected = info.file_size
                active_sha256 = info.sha256

                remaining_bytes = max(0, info.file_size - resume_offset)
                await asyncio.to_thread(
                    check_download_disk_space,
                    download_dir,
                    remaining_bytes,
                    max_dir_bytes=self._max_download_dir_bytes,
                )

                sm.transition(TransferState.TRANSFERRING)
                await _update_db(
                    sm.state,
                    sha256=info.sha256,
                    size=info.file_size,
                    bytes_done=bytes_received,
                )
                _emit(bytes_received, total_expected)

                last_checkpoint_bytes = bytes_received
                try:
                    part_handle = await asyncio.to_thread(
                        open_part_file_safe, part_path, offset=resume_offset
                    )
                    while True:
                        _raise_if_cancelled()
                        frame = await read_frame(
                            conn.reader,
                            max_control_bytes=self._max_control_bytes,
                            max_data_bytes=self._max_data_chunk_bytes,
                            timeout_sec=self._idle_timeout_sec,
                        )
                        if frame is None:
                            raise TransferError(
                                "Connection closed prematurely at "
                                f"{bytes_received}/{info.file_size} bytes"
                            )
                        if frame.header.request_id != req_id:
                            continue

                        decoded = decode_message(
                            frame,
                            max_filename_length=self._max_filename_length,
                            max_file_size_bytes=self._max_file_size_bytes,
                        )
                        if isinstance(decoded.payload, TransferErrorMessage):
                            raise TransferError(
                                f"{decoded.payload.code}: {decoded.payload.message}"
                            )
                        if not isinstance(decoded.payload, FileDataMessage):
                            raise MalformedMessageError(
                                f"Expected FILE_DATA frame, got {decoded.msg_type.name}"
                            )

                        chunk = decoded.payload.data
                        if bytes_received + len(chunk) > info.file_size:
                            raise ProtocolError(
                                "Peer sent excess bytes "
                                f"({bytes_received + len(chunk)} > {info.file_size})"
                            )
                        if chunk:
                            await asyncio.to_thread(_write_sync, part_handle, chunk)
                            bytes_received += len(chunk)
                            _emit(bytes_received, info.file_size)
                            if (
                                bytes_received - last_checkpoint_bytes
                                >= _CHECKPOINT_FLUSH_INTERVAL_BYTES
                            ):
                                await _save_checkpoint(bytes_received, info.sha256, info.file_size)
                                await _update_db(sm.state, bytes_done=bytes_received)
                                last_checkpoint_bytes = bytes_received

                        if decoded.payload.end_of_stream:
                            break
                except (
                    TimeoutError,
                    OSError,
                    ConnectionError,
                    FramingError,
                    UnsupportedVersionError,
                    MalformedMessageError,
                    ProtocolError,
                    ResourceLimitError,
                    TransferError,
                ) as exc:
                    if isinstance(exc, TransferCancelledError):
                        raise
                    if part_handle is not None:
                        await asyncio.to_thread(_flush_and_close_sync, part_handle)
                        part_handle = None
                    sm.transition(TransferState.TRANSFER_INTERRUPTED)
                    err_text = str(exc) or type(exc).__name__
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    await _save_checkpoint(bytes_received, info.sha256, info.file_size)
                    if isinstance(exc, TimeoutError):
                        raise TransferTimeoutError(
                            f"Download timed out after {self._idle_timeout_sec}s"
                        ) from exc
                    if isinstance(exc, TransferError):
                        raise
                    raise TransferError(f"Download interrupted: {exc}") from exc

                await asyncio.to_thread(_flush_and_close_sync, part_handle)
                part_handle = None

                if bytes_received != info.file_size:
                    sm.transition(TransferState.TRANSFER_INTERRUPTED)
                    err_text = (
                        f"Incomplete transfer: received {bytes_received} of {info.file_size} bytes"
                    )
                    await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                    await _save_checkpoint(bytes_received, info.sha256, info.file_size)
                    raise TransferError(err_text)

            # Verify full-file SHA-256 of persisted .part file (bytes 0..file_size) before promoting
            _raise_if_cancelled()
            sm.transition(TransferState.VERIFYING)
            await _update_db(sm.state, bytes_done=bytes_received)
            _emit(bytes_received, info.file_size)

            actual_sha256 = await compute_sha256_file_async(
                part_path, chunk_size=self._chunk_size_bytes
            )
            if actual_sha256 != info.sha256:
                sm.transition(TransferState.INTEGRITY_FAILED)
                err_text = (
                    f"SHA-256 mismatch: expected={info.sha256[:12]}... "
                    f"actual={actual_sha256[:12]}..."
                )
                await _update_db(sm.state, bytes_done=bytes_received, err_msg=err_text)
                await asyncio.to_thread(_unlink_sync, part_path)
                await self._db.run(lambda: self._repo.delete_checkpoint(transfer_id))
                logger.error("Integrity verification failed for %s: %s", display_name, err_text)
                raise IntegrityError(err_text)

            try:
                await asyncio.to_thread(
                    _promote_part_sync,
                    part_path,
                    final_path,
                    overwrite=self._overwrite_existing,
                )
            except TransferError as exc:
                sm.transition(TransferState.CANCELLED)
                await _update_db(sm.state, bytes_done=bytes_received, err_msg=str(exc))
                raise

            sm.transition(TransferState.COMPLETED)
            completed_iso = utc_now_iso()
            await _update_db(
                sm.state,
                bytes_done=bytes_received,
                completed_at=completed_iso,
            )
            await self._db.run(lambda: self._repo.delete_checkpoint(transfer_id))

            duration = max(1e-6, monotonic_now() - started_mono)
            transferred_session = max(0, bytes_received - resume_offset)
            mbps = transferred_session * 8.0 / duration / 1_000_000.0
            _emit(bytes_received, info.file_size)
            logger.info(
                "Completed %s %s (%d/%d bytes, %.2f Mbps, sha256=%s)",
                "resumed download" if resume_offset > 0 else "download",
                final_path.name,
                bytes_received,
                info.file_size,
                mbps,
                actual_sha256[:12],
            )
            return DownloadResult(
                transfer_id=transfer_id,
                file_id=file_id,
                file_name=final_path.name,
                destination_path=final_path,
                bytes_downloaded=bytes_received,
                expected_sha256=info.sha256,
                actual_sha256=actual_sha256,
                state=sm.state,
                duration_sec=duration,
                throughput_mbps=mbps,
                resumed=(resume_offset > 0),
                resume_offset=resume_offset,
            )

        except (TransferCancelledError, asyncio.CancelledError) as exc:
            if part_handle is not None:
                await asyncio.to_thread(_flush_and_close_sync, part_handle)
                part_handle = None
            if sm.state not in {TransferState.COMPLETED, TransferState.CANCELLED}:
                with contextlib.suppress(TransferError):
                    sm.transition(TransferState.CANCELLED)
            await _update_db(
                TransferState.CANCELLED,
                bytes_done=bytes_received,
                err_msg="Transfer cancelled",
            )
            if (
                part_path is not None
                and bytes_received > 0
                and active_sha256
                and total_expected > 0
            ):
                await _save_checkpoint(bytes_received, active_sha256, total_expected)
            if isinstance(exc, TransferCancelledError):
                raise
            raise TransferCancelledError("Transfer task cancelled") from exc
        finally:
            if part_handle is not None:
                part_handle.close()

    async def _await_file_info(
        self,
        conn: TcpConnection,
        req_id: bytes,
    ) -> FileInfoMessage | TransferErrorMessage:
        deadline = monotonic_now() + self._control_timeout_sec
        while True:
            remaining = deadline - monotonic_now()
            if remaining <= 0:
                raise TransferTimeoutError("Timed out waiting for FILE_INFO response")
            frame = await read_frame(
                conn.reader,
                max_control_bytes=self._max_control_bytes,
                max_data_bytes=self._max_data_chunk_bytes,
                timeout_sec=remaining,
            )
            if frame is None:
                raise TransferError("Peer closed connection before sending FILE_INFO")
            if frame.header.request_id != req_id:
                continue
            decoded = decode_message(
                frame,
                max_filename_length=self._max_filename_length,
                max_file_size_bytes=self._max_file_size_bytes,
            )
            if isinstance(decoded.payload, (FileInfoMessage, TransferErrorMessage)):
                return decoded.payload
            raise MalformedMessageError(
                f"Expected FILE_INFO or TRANSFER_ERROR, got {decoded.msg_type.name}"
            )
