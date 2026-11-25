"""Coordinates bounded concurrent downloads, resumes, uploads, and graceful cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from pathlib import Path

from p2p.config import AppConfig
from p2p.exceptions import TransferCancelledError, TransferError
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress
from p2p.search.local_search import LocalSearchService
from p2p.search.result_merger import PeerSearchResult
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRecord, DownloadRepository, TransferCheckpoint
from p2p.transfer.download import DownloadResult, DownloadService
from p2p.transfer.progress import ProgressCallback
from p2p.transfer.upload import UploadService


class TransferManager:
    """Facade managing concurrent peer-to-peer uploads, downloads, resumes, and history."""

    def __init__(
        self,
        config: AppConfig,
        db: Database,
        *,
        search_service: LocalSearchService | None = None,
    ) -> None:
        self._config = config
        self._db = db
        self._download_repo = DownloadRepository(db)
        self._download_semaphore = asyncio.Semaphore(config.limits.max_concurrent_transfers)
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._anonymous_cancel_events: set[asyncio.Event] = set()

        self.tcp_client = TcpClient(
            peer_id=config.node.peer_id,
            connect_timeout_sec=config.network.connect_timeout_sec,
            control_timeout_sec=config.network.control_timeout_sec,
            max_payload_bytes=config.limits.max_control_payload_bytes,
            max_data_chunk_bytes=config.limits.max_data_chunk_bytes,
        )
        self.upload_service = UploadService(
            db,
            peer_id=config.node.peer_id,
            display_name=config.node.display_name,
            tcp_port=config.network.tcp_port,
            shared_directories=config.sharing.directories,
            search_service=search_service,
            max_concurrent_uploads=config.limits.max_concurrent_transfers,
            default_chunk_size=config.transfer.chunk_size_bytes,
            max_data_chunk_bytes=config.limits.max_data_chunk_bytes,
            max_control_bytes=config.limits.max_control_payload_bytes,
            max_file_size_bytes=config.limits.max_file_size_bytes,
            control_timeout_sec=config.network.control_timeout_sec,
            idle_timeout_sec=config.transfer.idle_timeout_sec,
        )
        self.download_service = DownloadService(
            db,
            self.tcp_client,
            local_peer_id=config.node.peer_id,
            download_dir=config.storage.download_dir,
            chunk_size_bytes=config.transfer.chunk_size_bytes,
            max_data_chunk_bytes=config.limits.max_data_chunk_bytes,
            max_control_bytes=config.limits.max_control_payload_bytes,
            max_filename_length=config.limits.max_filename_length,
            max_file_size_bytes=config.limits.max_file_size_bytes,
            control_timeout_sec=config.network.control_timeout_sec,
            idle_timeout_sec=config.transfer.idle_timeout_sec,
            overwrite_existing=config.transfer.overwrite_existing,
        )

    @property
    def active_download_count(self) -> int:
        """Return the number of currently running download tasks."""
        return len(self._anonymous_cancel_events)

    def update_shared_directories(self, directories: Sequence[Path]) -> None:
        """Propagate updated authorized shared roots to UploadService."""
        self.upload_service.update_shared_directories(directories)

    async def serve_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_addr: PeerAddress,
    ) -> None:
        """Delegate incoming TCP connections to UploadService."""
        await self.upload_service.serve_connection(reader, writer, peer_addr)

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
    ) -> DownloadResult:
        """Download a file while respecting max_concurrent_transfers."""
        cancel_event = asyncio.Event()
        self._anonymous_cancel_events.add(cancel_event)
        self._cancel_events[file_id] = cancel_event
        try:
            async with self._download_semaphore:
                return await self.download_service.download_file(
                    host=host,
                    port=port,
                    file_id=file_id,
                    file_name=file_name,
                    source_peer_id=source_peer_id,
                    expected_sha256=expected_sha256,
                    expected_size=expected_size,
                    download_dir=download_dir,
                    on_progress=on_progress,
                    cancel_event=cancel_event,
                )
        finally:
            self._anonymous_cancel_events.discard(cancel_event)
            if self._cancel_events.get(file_id) is cancel_event:
                self._cancel_events.pop(file_id, None)

    async def download_with_failover(
        self,
        sources: Sequence[PeerSearchResult],
        *,
        download_dir: Path | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> DownloadResult:
        """Try downloading from candidate sources in order until one succeeds."""
        if not sources:
            raise TransferError("No candidate peer sources supplied")

        errors: list[str] = []
        for idx, source in enumerate(sources):
            try:
                return await self.download_file(
                    host=source.source_ip,
                    port=source.source_tcp_port,
                    file_id=source.file_id,
                    file_name=source.file_name,
                    source_peer_id=source.source_peer_id,
                    expected_sha256=source.sha256,
                    expected_size=source.file_size,
                    download_dir=download_dir,
                    on_progress=on_progress,
                )
            except TransferCancelledError:
                raise
            except TransferError as exc:
                errors.append(
                    f"{source.source_display_name}"
                    f"({source.source_ip}:{source.source_tcp_port}): {exc}"
                )
                if idx + 1 >= len(sources):
                    break
        raise TransferError("All source peers failed: " + "; ".join(errors))

    async def resume_download(
        self,
        identifier: str,
        *,
        host: str | None = None,
        port: int | None = None,
        download_dir: Path | None = None,
        restart_if_changed: bool = False,
        on_progress: ProgressCallback | None = None,
    ) -> DownloadResult:
        """Resume an interrupted download identified by transfer_id or file_id."""
        cancel_event = asyncio.Event()
        self._anonymous_cancel_events.add(cancel_event)
        self._cancel_events[identifier] = cancel_event
        try:
            async with self._download_semaphore:
                return await self.download_service.resume_download(
                    identifier,
                    host=host,
                    port=port,
                    download_dir=download_dir,
                    restart_if_changed=restart_if_changed,
                    on_progress=on_progress,
                    cancel_event=cancel_event,
                )
        finally:
            self._anonymous_cancel_events.discard(cancel_event)
            if self._cancel_events.get(identifier) is cancel_event:
                self._cancel_events.pop(identifier, None)

    def cancel_transfer(self, key: str) -> bool:
        """Signal cancellation for an active download matching file_id or transfer key."""
        event = self._cancel_events.get(key)
        if event is None:
            return False
        event.set()
        return True

    def cancel_all(self) -> None:
        """Signal all in-flight downloads to stop cleanly."""
        for event in list(self._anonymous_cancel_events):
            event.set()

    async def list_history(self, *, limit: int = 50) -> list[DownloadRecord]:
        """Return persisted download history ordered newest first."""
        return await self._db.run(lambda: self._download_repo.list_downloads(limit=limit))

    async def list_checkpoints(self) -> dict[str, TransferCheckpoint]:
        """Return active resume checkpoints keyed by transfer_id."""
        return await self._db.run(self._download_repo.list_checkpoints)
