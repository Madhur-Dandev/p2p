"""Streaming SHA-256 hashing helpers using bounded memory buffers."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import BinaryIO

from p2p.constants import DEFAULT_TRANSFER_CHUNK_BYTES


def compute_sha256_stream(
    handle: BinaryIO,
    *,
    chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
) -> str:
    """Stream bytes from an open binary file handle into SHA-256 and return lowercase hex."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    digest = hashlib.sha256()
    while True:
        chunk = handle.read(chunk_size)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


def compute_sha256_file(
    file_path: Path,
    *,
    chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
) -> str:
    """Calculate SHA-256 hex digest of file_path by streaming bounded chunks from disk."""
    with file_path.open("rb") as handle:
        return compute_sha256_stream(handle, chunk_size=chunk_size)


async def compute_sha256_file_async(
    file_path: Path,
    *,
    chunk_size: int = DEFAULT_TRANSFER_CHUNK_BYTES,
) -> str:
    """Compute SHA-256 on a worker thread so large files do not block the asyncio event loop."""
    return await asyncio.to_thread(compute_sha256_file, file_path, chunk_size=chunk_size)
