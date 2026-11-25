"""Recursive, symlink-safe shared-directory scanner."""

from __future__ import annotations

import os
import stat
from collections.abc import Sequence
from pathlib import Path

from p2p.constants import DEFAULT_EXCLUDED_DIRS, MAX_FILENAME_LENGTH
from p2p.exceptions import PathValidationError
from p2p.indexing.file_metadata import ScannedFile, ScanResult, compute_file_id
from p2p.logging_config import get_context_logger


def scan_shared_directory(
    share_root: Path,
    *,
    excluded_dirs: Sequence[str] = DEFAULT_EXCLUDED_DIRS,
    follow_symlinks: bool = False,
    max_filename_length: int = MAX_FILENAME_LENGTH,
) -> ScanResult:
    """Recursively scan share_root for regular files without traversing symlinks."""
    logger = get_context_logger("indexing.scanner")
    try:
        root_real = share_root.expanduser().resolve(strict=True)
    except OSError as exc:
        raise PathValidationError(f"Shared directory is inaccessible: {share_root}") from exc

    if not root_real.is_dir():
        raise PathValidationError(f"Shared path is not a directory: {root_real}")

    excluded = frozenset(excluded_dirs)
    result = ScanResult(share_root=root_real, files=[])
    stack: list[Path] = [root_real]

    while stack:
        current_dir = stack.pop()
        try:
            with os.scandir(current_dir) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink() and not follow_symlinks:
                            result.skipped_symlinks += 1
                            logger.debug("Skipping symlink during index scan: %s", entry.path)
                            continue

                        if entry.is_dir(follow_symlinks=follow_symlinks):
                            if entry.name in excluded:
                                continue
                            stack.append(Path(entry.path))
                            continue

                        if entry.is_file(follow_symlinks=follow_symlinks):
                            st = entry.stat(follow_symlinks=follow_symlinks)
                            if not stat.S_ISREG(st.st_mode):
                                continue
                            if len(entry.name) > max_filename_length:
                                result.skipped_errors += 1
                                logger.warning(
                                    "Skipping file exceeding max_filename_length (%d): %s",
                                    max_filename_length,
                                    entry.path,
                                )
                                continue
                            full_path = Path(entry.path)
                            rel_path = full_path.relative_to(root_real).as_posix()
                            result.files.append(
                                ScannedFile(
                                    file_id=compute_file_id(root_real, rel_path),
                                    share_root=str(root_real),
                                    relative_path=rel_path,
                                    file_name=entry.name,
                                    full_path=full_path,
                                    file_size=st.st_size,
                                    mtime_ns=st.st_mtime_ns,
                                )
                            )
                    except OSError as exc:
                        result.skipped_errors += 1
                        logger.warning("Skipping inaccessible entry %s: %s", entry.path, exc)
        except OSError as exc:
            result.skipped_errors += 1
            logger.warning("Skipping inaccessible directory %s: %s", current_dir, exc)

    result.files.sort(key=lambda item: item.relative_path)
    return result
