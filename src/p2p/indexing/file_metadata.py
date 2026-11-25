"""Metadata models and deterministic file ID generation for shared-directory indexing."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


def compute_file_id(share_root: Path, relative_path: str) -> str:
    """Compute a stable, opaque 24-hex-character file ID without leaking filesystem paths."""
    canonical_root = str(share_root.resolve(strict=False))
    digest = hashlib.sha256(f"{canonical_root}::{relative_path}".encode()).hexdigest()
    return digest[:24]


@dataclass(frozen=True, slots=True)
class ScannedFile:
    """Filesystem entry discovered during recursive shared-directory scanning."""

    file_id: str
    share_root: str
    relative_path: str
    file_name: str
    full_path: Path
    file_size: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class IndexedFile:
    """Persisted shared-file metadata record stored in SQLite."""

    file_id: str
    share_root: str
    relative_path: str
    file_name: str
    file_size: int
    mtime_ns: int
    sha256: str
    indexed_at: str


@dataclass(slots=True)
class ScanResult:
    """Result returned by scanning one authorized directory tree."""

    share_root: Path
    files: list[ScannedFile]
    skipped_symlinks: int = 0
    skipped_errors: int = 0


@dataclass(frozen=True, slots=True)
class IndexSummary:
    """Summary counters produced by an index rebuild operation."""

    scanned: int
    added: int
    updated: int
    unchanged_cached: int
    removed: int
    skipped_symlinks: int
    skipped_errors: int
    duration_sec: float
