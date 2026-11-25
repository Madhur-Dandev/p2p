"""Shared-directory scanning, SHA-256 hashing, and SQLite index synchronization."""

from p2p.indexing.file_index import FileIndexer
from p2p.indexing.file_metadata import (
    IndexedFile,
    IndexSummary,
    ScannedFile,
    ScanResult,
    compute_file_id,
)
from p2p.indexing.hashing import (
    compute_sha256_file,
    compute_sha256_file_async,
    compute_sha256_stream,
)
from p2p.indexing.scanner import scan_shared_directory

__all__ = [
    "FileIndexer",
    "IndexSummary",
    "IndexedFile",
    "ScanResult",
    "ScannedFile",
    "compute_file_id",
    "compute_sha256_file",
    "compute_sha256_file_async",
    "compute_sha256_stream",
    "scan_shared_directory",
]
