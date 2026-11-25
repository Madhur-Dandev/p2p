"""Symlink-safe canonical path validation for shared-directory access and downloads."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from p2p.constants import MAX_FILENAME_LENGTH
from p2p.exceptions import PathValidationError, TransferError
from p2p.network.protocol import validate_file_name


def _validate_relative_posix_path(relative_path: str) -> PurePosixPath:
    if not isinstance(relative_path, str) or not relative_path:
        raise PathValidationError("Relative path must be a non-empty string")
    if "\x00" in relative_path or "\\" in relative_path:
        raise PathValidationError(f"Invalid characters in relative path: {relative_path!r}")

    posix = PurePosixPath(relative_path)
    if posix.is_absolute():
        raise PathValidationError(f"Relative path must not be absolute: {relative_path!r}")
    if not posix.parts:
        raise PathValidationError("Relative path must not be empty")
    for part in posix.parts:
        if part in {"", ".", ".."}:
            raise PathValidationError(f"Path traversal segment rejected: {relative_path!r}")
    return posix


def ensure_no_symlink_components(candidate: Path, stop_at: Path) -> None:
    """Verify no path component strictly inside stop_at is a symbolic link."""
    current = candidate
    while current != stop_at:
        if current.is_symlink():
            raise PathValidationError(f"Symbolic link rejected inside shared path: {current}")
        parent = current.parent
        if parent == current:
            raise PathValidationError(f"Candidate path {candidate} is not inside root {stop_at}")
        current = parent


def resolve_shared_file(share_root: Path, relative_path: str) -> Path:
    """Resolve relative_path inside share_root, blocking traversal and symlinks."""
    rel = _validate_relative_posix_path(relative_path)
    try:
        root_real = share_root.expanduser().resolve(strict=True)
    except OSError as exc:
        raise PathValidationError(f"Shared root is inaccessible: {share_root}") from exc

    if not root_real.is_dir():
        raise PathValidationError(f"Shared root is not a directory: {root_real}")

    candidate = root_real.joinpath(*rel.parts)
    ensure_no_symlink_components(candidate, root_real)

    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise PathValidationError(f"Shared file does not exist: {relative_path}") from exc

    if not resolved.is_relative_to(root_real):
        raise PathValidationError(
            f"Resolved path {resolved} escapes authorized directory {root_real}"
        )
    if resolved.is_symlink() or not resolved.is_file():
        raise PathValidationError(f"Target is not a regular file: {relative_path}")
    return resolved


def open_shared_file_readonly(share_root: Path, relative_path: str) -> BinaryIO:
    """Safely open an authorized regular file read-only without following symlinks."""
    verified = resolve_shared_file(share_root, relative_path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(verified, flags)
    except OSError as exc:
        raise PathValidationError(f"Cannot safely open shared file {relative_path}: {exc}") from exc

    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise PathValidationError(f"Refusing non-regular file descriptor: {relative_path}")
        return os.fdopen(fd, "rb")
    except Exception:
        os.close(fd)
        raise


def resolve_download_destination(
    download_dir: Path,
    file_name: str,
    *,
    transfer_id: str,
    max_filename_length: int = MAX_FILENAME_LENGTH,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Compute validated (final_path, part_path) inside download_dir."""
    safe_name = validate_file_name(file_name, max_length=max_filename_length)
    dest_root = download_dir.expanduser().resolve(strict=False)
    dest_root.mkdir(parents=True, exist_ok=True)
    if dest_root.is_symlink() or not dest_root.is_dir():
        raise PathValidationError(f"Download directory is invalid: {dest_root}")

    final_path = dest_root / safe_name
    if not final_path.resolve(strict=False).is_relative_to(dest_root):
        raise PathValidationError(f"Destination path escapes download directory: {safe_name!r}")
    if final_path.is_symlink():
        raise PathValidationError(f"Refusing symlink destination: {final_path}")
    if final_path.exists() and not overwrite:
        raise TransferError(f"Destination file already exists (overwrite disabled): {final_path}")

    # Keep temporary .part filename <= 255 bytes even when safe_name is long
    digest_tag = hashlib.sha256(f"{safe_name}:{transfer_id}".encode()).hexdigest()[:12]
    stem_budget = max(1, max_filename_length - len(f"..{digest_tag}.part"))
    trimmed_name = safe_name[:stem_budget]
    part_name = f".{trimmed_name}.{digest_tag}.part"
    part_path = dest_root / part_name
    if part_path.is_symlink():
        raise PathValidationError(f"Refusing symlink temporary file: {part_path}")
    return final_path, part_path


def validate_resume_part_file(
    download_dir: Path,
    part_path_str: str,
    destination_path_str: str,
    *,
    overwrite: bool = False,
) -> tuple[Path, Path]:
    """Validate that a checkpoint's .part and destination paths remain safe inside download_dir."""
    if not part_path_str or not destination_path_str:
        raise TransferError("Download checkpoint is missing part_path or destination_path")

    raw_root = download_dir.expanduser()
    dest_root = raw_root.resolve(strict=False)
    if dest_root.is_symlink() or not dest_root.is_dir():
        raise PathValidationError(f"Download directory is invalid: {dest_root}")

    part_candidate = Path(part_path_str).expanduser()
    final_candidate = Path(destination_path_str).expanduser()
    if part_candidate.is_relative_to(raw_root):
        part_candidate = dest_root / part_candidate.relative_to(raw_root)
    if final_candidate.is_relative_to(raw_root):
        final_candidate = dest_root / final_candidate.relative_to(raw_root)

    ensure_no_symlink_components(part_candidate, dest_root)
    ensure_no_symlink_components(final_candidate, dest_root)

    final_resolved = final_candidate.resolve(strict=False)
    if not final_resolved.is_relative_to(dest_root):
        raise PathValidationError(
            f"Resume destination escapes download directory: {destination_path_str!r}"
        )
    if final_resolved.is_symlink():
        raise PathValidationError(f"Refusing symlink destination: {final_resolved}")
    if final_resolved.exists() and not overwrite:
        raise TransferError(
            f"Destination file already exists (overwrite disabled): {final_resolved}"
        )

    try:
        part_resolved = part_candidate.resolve(strict=True)
    except OSError as exc:
        raise TransferError(
            f"Partial transfer file not found for resume: {part_candidate}"
        ) from exc

    if not part_resolved.is_relative_to(dest_root):
        raise PathValidationError(f"Partial file escapes download directory: {part_path_str!r}")
    if part_resolved.is_symlink() or not part_resolved.is_file():
        raise PathValidationError(f"Partial resume target is not a regular file: {part_resolved}")

    return final_resolved, part_resolved


def open_part_file_safe(part_path: Path, *, offset: int = 0) -> BinaryIO:
    """Open a .part staging file without following symlinks and seek to offset."""
    if offset < 0:
        raise PathValidationError(f"Invalid negative offset {offset}")
    parent = part_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    if parent.is_symlink():
        raise PathValidationError(f"Refusing symlink parent directory for part file: {parent}")

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    flags = (os.O_CREAT | os.O_RDWR | nofollow) if offset == 0 else (os.O_RDWR | nofollow)
    mode = "r+b"

    try:
        fd = os.open(part_path, flags, 0o600)
    except OSError as exc:
        raise PathValidationError(f"Cannot safely open .part file {part_path}: {exc}") from exc

    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise PathValidationError(f"Refusing non-regular .part descriptor: {part_path}")
        if offset > st.st_size:
            raise TransferError(
                f"Requested resume offset {offset} exceeds .part file size {st.st_size}"
            )
        os.ftruncate(fd, offset)
        os.lseek(fd, offset, os.SEEK_SET)
        return os.fdopen(fd, mode)
    except Exception:
        os.close(fd)
        raise
