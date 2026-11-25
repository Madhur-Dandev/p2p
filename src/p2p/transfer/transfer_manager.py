"""Coordinates bounded concurrent downloads, uploads, and graceful cancellation."""

from __future__ import annotations

import asyncio

from p2p.config import AppConfig
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRecord, DownloadRepository
from p2p.transfer.download import DownloadResult, DownloadService
from p2p.transfer.progress import ProgressCallback
from p2p.transfer.upload import UploadService


class TransferManager:
    """Facade managing concurrent peer-to-peer uploads, downloads, and history."""

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
        self._cancel_events: set[asyncio.Event] = set()

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
        return len(self._cancel_events)

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
        on_progress: ProgressCallback | None = None,
    ) -> DownloadResult:
        """Download a file while respecting max_concurrent_transfers."""
        cancel_event = asyncio.Event()
        self._cancel_events.add(cancel_event)
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
                    on_progress=on_progress,
                    cancel_event=cancel_event,
                )
        finally:
            self._cancel_events.discard(cancel_event)

    def cancel_all(self) -> None:
        """Signal all in-flight downloads to stop cleanly."""
        for event in list(self._cancel_events):
            event.set()

    async def list_history(self, *, limit: int = 50) -> list[DownloadRecord]:
        """Return persisted download history ordered newest first."""
        return await self._db.run(lambda: self._download_repo.list_downloads(limit=limit))
