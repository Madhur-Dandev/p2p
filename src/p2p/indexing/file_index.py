"""Incremental SQLite-backed shared-directory file indexer."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from p2p.constants import (
    DEFAULT_EXCLUDED_DIRS,
    DEFAULT_TRANSFER_CHUNK_BYTES,
    MAX_FILENAME_LENGTH,
)
from p2p.exceptions import FileNotFoundInIndexError, PathValidationError
from p2p.indexing.file_metadata import IndexedFile, IndexSummary
from p2p.indexing.hashing import compute_sha256_file
from p2p.indexing.scanner import scan_shared_directory
from p2p.logging_config import get_context_logger
from p2p.security.path_validation import resolve_shared_file
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository
from p2p.utils.time_utils import monotonic_now, utc_now_iso


class FileIndexer:
    """Indexes authorized shared directories into SQLite with mtime/size hash caching."""

    def __init__(
        self,
        db: Database,
        *,
        shared_directories: Sequence[Path],
        excluded_dirs: Sequence[str] = DEFAULT_EXCLUDED_DIRS,
        follow_symlinks: bool = False,
        hash_chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
        max_filename_length: int = MAX_FILENAME_LENGTH,
    ) -> None:
        self._db = db
        self._repo = FileIndexRepository(db)
        self._shared_directories = [
            Path(d).expanduser().resolve(strict=False) for d in shared_directories
        ]
        self._excluded_dirs = tuple(excluded_dirs)
        self._follow_symlinks = follow_symlinks
        self._hash_chunk_size = hash_chunk_size
        self._max_filename_length = max_filename_length
        self._logger = get_context_logger("indexing.file_index")

    def rebuild(self, *, force_rehash: bool = False) -> IndexSummary:
        """Scan configured directories, update changed/added files, and prune deleted files."""
        started = monotonic_now()
        scanned_count = 0
        added_count = 0
        updated_count = 0
        cached_count = 0
        removed_count = 0
        skipped_symlinks = 0
        skipped_errors = 0

        active_roots: list[str] = []
        valid_dirs: list[Path] = []
        for directory in self._shared_directories:
            try:
                resolved = directory.expanduser().resolve(strict=True)
            except OSError as exc:
                skipped_errors += 1
                self._logger.warning("Skipping inaccessible shared root %s: %s", directory, exc)
                continue
            if not resolved.is_dir():
                skipped_errors += 1
                self._logger.warning("Skipping non-directory shared root %s", resolved)
                continue
            valid_dirs.append(resolved)
            active_roots.append(str(resolved))

        with self._db.transaction():
            removed_count += self._repo.delete_unconfigured_roots(active_roots)

            for root_dir in valid_dirs:
                try:
                    scan = scan_shared_directory(
                        root_dir,
                        excluded_dirs=self._excluded_dirs,
                        follow_symlinks=self._follow_symlinks,
                        max_filename_length=self._max_filename_length,
                    )
                except PathValidationError as exc:
                    skipped_errors += 1
                    self._logger.warning("Failed scanning %s: %s", root_dir, exc)
                    continue

                skipped_symlinks += scan.skipped_symlinks
                skipped_errors += scan.skipped_errors
                scanned_count += len(scan.files)

                existing_map = {
                    row.file_id: row for row in self._repo.list_by_share_root(str(scan.share_root))
                }
                seen_ids: set[str] = set()
                to_upsert: list[IndexedFile] = []
                now_iso = utc_now_iso()

                for item in scan.files:
                    seen_ids.add(item.file_id)
                    existing = existing_map.get(item.file_id)
                    if (
                        not force_rehash
                        and existing is not None
                        and existing.file_size == item.file_size
                        and existing.mtime_ns == item.mtime_ns
                    ):
                        cached_count += 1
                        continue

                    try:
                        sha256_hex = compute_sha256_file(
                            item.full_path,
                            chunk_size=self._hash_chunk_size,
                        )
                    except OSError as exc:
                        skipped_errors += 1
                        self._logger.warning("Failed hashing %s: %s", item.full_path, exc)
                        continue

                    if existing is None:
                        added_count += 1
                    else:
                        updated_count += 1

                    to_upsert.append(
                        IndexedFile(
                            file_id=item.file_id,
                            share_root=item.share_root,
                            relative_path=item.relative_path,
                            file_name=item.file_name,
                            file_size=item.file_size,
                            mtime_ns=item.mtime_ns,
                            sha256=sha256_hex,
                            indexed_at=now_iso,
                        )
                    )

                self._repo.upsert_files(to_upsert)
                deleted_ids = [fid for fid in existing_map if fid not in seen_ids]
                removed_count += self._repo.delete_by_file_ids(deleted_ids)

        duration = monotonic_now() - started
        summary = IndexSummary(
            scanned=scanned_count,
            added=added_count,
            updated=updated_count,
            unchanged_cached=cached_count,
            removed=removed_count,
            skipped_symlinks=skipped_symlinks,
            skipped_errors=skipped_errors,
            duration_sec=duration,
        )
        self._logger.info(
            "Index rebuild complete: scanned=%d added=%d updated=%d cached=%d removed=%d "
            "symlinks_skipped=%d errors=%d duration=%.3fs",
            summary.scanned,
            summary.added,
            summary.updated,
            summary.unchanged_cached,
            summary.removed,
            summary.skipped_symlinks,
            summary.skipped_errors,
            summary.duration_sec,
        )
        return summary

    async def rebuild_async(self, *, force_rehash: bool = False) -> IndexSummary:
        """Run index rebuild off the asyncio event loop."""
        return await self._db.run(lambda: self.rebuild(force_rehash=force_rehash))

    def revalidate_file(self, file_id: str, *, verify_hash: bool = False) -> IndexedFile:
        """Verify indexed file still exists securely on disk and refresh stale metadata."""
        record = self._repo.get_by_file_id(file_id)
        if record is None:
            raise FileNotFoundInIndexError(f"File not found in index: {file_id}")

        try:
            verified_path = resolve_shared_file(Path(record.share_root), record.relative_path)
            st = verified_path.stat()
        except (PathValidationError, OSError) as exc:
            self._repo.delete_by_file_ids([file_id])
            raise FileNotFoundInIndexError(
                f"Indexed file is no longer accessible on disk: {file_id}"
            ) from exc

        metadata_changed = st.st_size != record.file_size or st.st_mtime_ns != record.mtime_ns
        if not verify_hash and not metadata_changed:
            return record

        fresh_hash = compute_sha256_file(verified_path, chunk_size=self._hash_chunk_size)
        if not metadata_changed and fresh_hash == record.sha256:
            return record

        updated = IndexedFile(
            file_id=record.file_id,
            share_root=record.share_root,
            relative_path=record.relative_path,
            file_name=record.file_name,
            file_size=st.st_size,
            mtime_ns=st.st_mtime_ns,
            sha256=fresh_hash,
            indexed_at=utc_now_iso(),
        )
        self._repo.upsert_files([updated])
        return updated
