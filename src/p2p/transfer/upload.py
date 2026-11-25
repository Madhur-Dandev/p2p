"""Direct TCP file upload handler with path containment and bounded streaming."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from p2p.constants import (
    DEFAULT_CONTROL_TIMEOUT_SEC,
    DEFAULT_TCP_PORT,
    DEFAULT_TRANSFER_CHUNK_BYTES,
    DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC,
    MAX_CONCURRENT_TRANSFERS,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_FILE_SIZE_BYTES,
)
from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    PathValidationError,
    ResourceLimitError,
    StorageError,
    UnsupportedVersionError,
)
from p2p.indexing.file_metadata import IndexedFile
from p2p.indexing.hashing import compute_sha256_stream
from p2p.logging_config import get_context_logger
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import (
    FileDataMessage,
    FileInfoMessage,
    GetFileMessage,
    PingMessage,
    PongMessage,
    ProtocolPayload,
    SearchRequestMessage,
    TransferErrorMessage,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.network.tcp_server import PeerAddress
from p2p.search.local_search import LocalSearchService
from p2p.security.path_validation import open_shared_file_readonly
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository
from p2p.utils.identifiers import request_id_to_str
from p2p.utils.time_utils import utc_now_iso


def _resolve_root_sync(path_str: str) -> Path:
    return Path(path_str).expanduser().resolve(strict=False)


@dataclass(slots=True)
class UploadStats:
    """Counters tracking upload activity and rejected requests."""

    uploads_started: int = 0
    uploads_completed: int = 0
    uploads_failed: int = 0
    bytes_sent: int = 0


class UploadService:
    """Serves authorized files and control/search queries over accepted TCP streams."""

    def __init__(
        self,
        db: Database,
        *,
        peer_id: str,
        display_name: str = "p2p-node",
        tcp_port: int = DEFAULT_TCP_PORT,
        shared_directories: Sequence[Path] = (),
        search_service: LocalSearchService | None = None,
        max_concurrent_uploads: int = MAX_CONCURRENT_TRANSFERS,
        default_chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
        max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES,
        max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
        max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
        control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
        idle_timeout_sec: float = DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC,
    ) -> None:
        self._db = db
        self._file_repo = FileIndexRepository(db)
        self._peer_id = peer_id
        self._display_name = display_name
        self._tcp_port = tcp_port
        self._allowed_roots: frozenset[Path] | None = (
            frozenset(Path(d).expanduser().resolve(strict=False) for d in shared_directories)
            if shared_directories
            else None
        )
        self._search_service = search_service
        self._upload_semaphore = asyncio.Semaphore(max(1, max_concurrent_uploads))
        self._default_chunk_size = default_chunk_size
        self._max_data_chunk_bytes = max_data_chunk_bytes
        self._max_control_bytes = max_control_bytes
        self._max_file_size_bytes = max_file_size_bytes
        self._control_timeout_sec = control_timeout_sec
        self._idle_timeout_sec = idle_timeout_sec
        self.stats = UploadStats()
        self._logger = get_context_logger("transfer.upload", peer_id=peer_id)

    def set_tcp_port(self, port: int) -> None:
        """Update advertised TCP port after ephemeral TcpServer startup."""
        self._tcp_port = port

    async def serve_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_addr: PeerAddress,
    ) -> None:
        """Serve framed control, search, and GET_FILE requests on one TCP connection."""
        while True:
            try:
                frame = await read_frame(
                    reader,
                    max_control_bytes=self._max_control_bytes,
                    max_data_bytes=self._max_data_chunk_bytes,
                    timeout_sec=self._control_timeout_sec,
                )
            except (
                FramingError,
                UnsupportedVersionError,
                MalformedMessageError,
                ResourceLimitError,
            ) as exc:
                self._logger.warning(
                    "Closing TCP stream from %s:%d after framing error: %s",
                    peer_addr[0],
                    peer_addr[1],
                    exc,
                )
                return
            except TimeoutError:
                return

            if frame is None:
                return

            req_id = frame.header.request_id
            req_id_str = request_id_to_str(req_id)

            try:
                decoded = decode_message(frame)
            except (MalformedMessageError, ResourceLimitError) as exc:
                await self._send_error(
                    writer,
                    req_id,
                    code="INVALID_REQUEST",
                    message=str(exc),
                )
                return

            payload = decoded.payload
            if isinstance(payload, PingMessage):
                await self._send_payload(
                    writer,
                    PongMessage(
                        peer_id=self._peer_id,
                        tcp_port=self._tcp_port,
                        status="ok",
                        display_name=self._display_name,
                        sent_at_ms=payload.sent_at_ms,
                    ),
                    request_id=req_id,
                )
                continue

            if isinstance(payload, SearchRequestMessage):
                if self._search_service is None:
                    await self._send_error(
                        writer,
                        req_id,
                        code="UNSUPPORTED_OPERATION",
                        message="Search service is not enabled on this endpoint",
                    )
                    continue
                response = await self._search_service.handle_search_request(payload)
                await self._send_payload(writer, response, request_id=req_id)
                continue

            if isinstance(payload, GetFileMessage):
                await self._handle_get_file(writer, req_id, req_id_str, payload, peer_addr)
                continue

            await self._send_error(
                writer,
                req_id,
                code="UNSUPPORTED_OPERATION",
                message=f"Unsupported server request: {decoded.msg_type.name}",
            )

    async def _send_payload(
        self,
        writer: asyncio.StreamWriter,
        payload: ProtocolPayload,
        *,
        request_id: bytes,
        timeout_sec: float | None = None,
    ) -> None:
        encoded = encode_message(
            payload,
            request_id=request_id,
            max_control_bytes=self._max_control_bytes,
            max_data_bytes=self._max_data_chunk_bytes,
        )
        await write_frame(
            writer,
            encoded,
            timeout_sec=self._control_timeout_sec if timeout_sec is None else timeout_sec,
        )

    async def _send_error(
        self,
        writer: asyncio.StreamWriter,
        request_id: bytes,
        *,
        code: str,
        message: str,
        retryable: bool = False,
    ) -> None:
        with contextlib.suppress(OSError, ConnectionError, TimeoutError):
            await self._send_payload(
                writer,
                TransferErrorMessage(code=code, message=message, retryable=retryable),
                request_id=request_id,
            )

    async def _handle_get_file(
        self,
        writer: asyncio.StreamWriter,
        req_id: bytes,
        req_id_str: str,
        req: GetFileMessage,
        peer_addr: PeerAddress,
    ) -> None:
        req_logger = get_context_logger(
            "transfer.upload",
            peer_id=req.peer_id,
            request_id=req_id_str,
        )
        try:
            await asyncio.wait_for(self._upload_semaphore.acquire(), timeout=0.5)
        except TimeoutError:
            self.stats.uploads_failed += 1
            req_logger.warning(
                "Rejecting upload from %s:%d: server busy", peer_addr[0], peer_addr[1]
            )
            await self._send_error(
                writer,
                req_id,
                code="BUSY",
                message="Maximum concurrent uploads reached; retry later",
                retryable=True,
            )
            return

        self.stats.uploads_started += 1
        handle = None
        try:
            try:
                record = await self._db.run(lambda: self._file_repo.get_by_file_id(req.file_id))
            except StorageError as exc:
                self.stats.uploads_failed += 1
                await self._send_error(writer, req_id, code="INTERNAL_ERROR", message=str(exc))
                return

            if record is None:
                self.stats.uploads_failed += 1
                req_logger.info("Requested unknown file_id=%s", req.file_id)
                await self._send_error(
                    writer,
                    req_id,
                    code="FILE_NOT_FOUND",
                    message=f"File ID not found: {req.file_id}",
                )
                return

            share_root = await asyncio.to_thread(_resolve_root_sync, record.share_root)
            if self._allowed_roots is not None and share_root not in self._allowed_roots:
                self.stats.uploads_failed += 1
                req_logger.warning(
                    "Blocked upload for file_id=%s outside active shared directories",
                    req.file_id,
                )
                await self._send_error(
                    writer,
                    req_id,
                    code="ACCESS_DENIED",
                    message="Requested file is no longer inside an authorized shared directory",
                )
                return

            try:
                handle = await asyncio.to_thread(
                    open_shared_file_readonly,
                    share_root,
                    record.relative_path,
                )
            except PathValidationError as exc:
                self.stats.uploads_failed += 1
                req_logger.warning("Path validation rejected file_id=%s: %s", req.file_id, exc)
                await self._send_error(
                    writer,
                    req_id,
                    code="FILE_NOT_FOUND" if "does not exist" in str(exc) else "ACCESS_DENIED",
                    message=str(exc),
                )
                return

            st = os.fstat(handle.fileno())
            actual_size = st.st_size
            actual_mtime_ns = st.st_mtime_ns

            if self._max_file_size_bytes > 0 and actual_size > self._max_file_size_bytes:
                self.stats.uploads_failed += 1
                await self._send_error(
                    writer,
                    req_id,
                    code="FILE_TOO_LARGE",
                    message=f"File size {actual_size} exceeds maximum {self._max_file_size_bytes}",
                )
                return

            if req.offset > actual_size:
                self.stats.uploads_failed += 1
                await self._send_error(
                    writer,
                    req_id,
                    code="INVALID_OFFSET",
                    message=f"Requested offset {req.offset} exceeds file size {actual_size}",
                )
                return

            chunk_size = min(
                req.chunk_size if req.chunk_size is not None else self._default_chunk_size,
                self._max_data_chunk_bytes,
            )
            chunk_size = max(1024, chunk_size)

            actual_sha256 = record.sha256
            if actual_size != record.file_size or actual_mtime_ns != record.mtime_ns:
                actual_sha256 = await asyncio.to_thread(
                    compute_sha256_stream,
                    handle,
                    chunk_size=chunk_size,
                )
                refreshed = IndexedFile(
                    file_id=record.file_id,
                    share_root=record.share_root,
                    relative_path=record.relative_path,
                    file_name=record.file_name,
                    file_size=actual_size,
                    mtime_ns=actual_mtime_ns,
                    sha256=actual_sha256,
                    indexed_at=utc_now_iso(),
                )
                await self._db.run(lambda: self._file_repo.upsert_files([refreshed]))

            if req.expected_sha256 is not None and req.expected_sha256 != actual_sha256:
                self.stats.uploads_failed += 1
                await self._send_error(
                    writer,
                    req_id,
                    code="FILE_CHANGED",
                    message="Source file content changed; resume offset is no longer valid",
                )
                return

            await self._send_payload(
                writer,
                FileInfoMessage(
                    file_id=record.file_id,
                    file_name=record.file_name,
                    file_size=actual_size,
                    sha256=actual_sha256,
                    offset=req.offset,
                    chunk_size=chunk_size,
                ),
                request_id=req_id,
            )

            await asyncio.to_thread(handle.seek, req.offset)
            remaining = actual_size - req.offset
            if remaining == 0:
                await self._send_payload(
                    writer,
                    FileDataMessage(data=b"", end_of_stream=True),
                    request_id=req_id,
                    timeout_sec=self._idle_timeout_sec,
                )
            else:
                while remaining > 0:
                    chunk = await asyncio.to_thread(handle.read, min(chunk_size, remaining))
                    if not chunk:
                        self.stats.uploads_failed += 1
                        await self._send_error(
                            writer,
                            req_id,
                            code="FILE_CHANGED",
                            message="Source file truncated during transfer",
                        )
                        return
                    remaining -= len(chunk)
                    await self._send_payload(
                        writer,
                        FileDataMessage(data=chunk, end_of_stream=(remaining == 0)),
                        request_id=req_id,
                        timeout_sec=self._idle_timeout_sec,
                    )
                    self.stats.bytes_sent += len(chunk)

            self.stats.uploads_completed += 1
            req_logger.info(
                "Uploaded file_id=%s (%s, %d bytes from offset=%d) to %s:%d",
                record.file_id,
                record.file_name,
                actual_size - req.offset,
                req.offset,
                peer_addr[0],
                peer_addr[1],
            )
        except (OSError, ConnectionError, TimeoutError) as exc:
            self.stats.uploads_failed += 1
            req_logger.warning("Upload interrupted for file_id=%s: %s", req.file_id, exc)
        finally:
            if handle is not None:
                handle.close()
            self._upload_semaphore.release()
