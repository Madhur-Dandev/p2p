"""Unit tests for streaming SHA-256 hashing."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from p2p.indexing.hashing import compute_sha256_file, compute_sha256_file_async


@pytest.mark.unit
async def test_streaming_sha256_matches_hashlib_across_empty_and_chunked_files(
    tmp_path: Path,
) -> None:
    empty_file = tmp_path / "empty.bin"
    empty_file.write_bytes(b"")
    assert compute_sha256_file(empty_file) == hashlib.sha256(b"").hexdigest()

    payload = (b"0123456789abcdef" * 500) + b"tail"
    data_file = tmp_path / "chunked.bin"
    data_file.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()

    # Use prime chunk size 7 to exercise arbitrary chunk boundaries
    assert compute_sha256_file(data_file, chunk_size=7) == expected
    assert await compute_sha256_file_async(data_file, chunk_size=13) == expected


@pytest.mark.unit
def test_invalid_chunk_size_raises(tmp_path: Path) -> None:
    sample = tmp_path / "sample.txt"
    sample.write_bytes(b"abc")
    with pytest.raises(ValueError, match="positive"):
        compute_sha256_file(sample, chunk_size=0)
