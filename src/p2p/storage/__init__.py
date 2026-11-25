"""SQLite persistence layer, migrations, and metadata repositories."""

from p2p.storage.database import Database
from p2p.storage.migrations import CURRENT_SCHEMA_VERSION, apply_migrations, get_schema_version
from p2p.storage.repositories import (
    DownloadRecord,
    DownloadRepository,
    FileIndexRepository,
    PeerRepository,
    TransferCheckpoint,
)

__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "Database",
    "DownloadRecord",
    "DownloadRepository",
    "FileIndexRepository",
    "PeerRepository",
    "TransferCheckpoint",
    "apply_migrations",
    "get_schema_version",
]
