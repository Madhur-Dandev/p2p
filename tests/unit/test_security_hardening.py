"""Unit tests for security hardening: rate limits, disk quotas, filename/part safety."""

from __future__ import annotations

import shutil
from collections import namedtuple
from pathlib import Path

import pytest

from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    PathValidationError,
    ResourceLimitError,
    TransferError,
    UnsupportedVersionError,
)
from p2p.network.framing import pack_frame, unpack_header
from p2p.network.protocol import MessageType, validate_file_name
from p2p.security.limits import SlidingWindowRateLimiter, check_download_disk_space
from p2p.security.path_validation import open_part_file_safe, validate_resume_part_file


@pytest.mark.unit
def test_sliding_window_rate_limiter_enforces_quota_and_recovers_over_time() -> None:
    now = [100.0]
    limiter = SlidingWindowRateLimiter(
        max_requests=3,
        window_sec=10.0,
        max_keys=2,
        clock=lambda: now[0],
    )

    assert limiter.allow("192.168.1.10") is True
    assert limiter.allow("192.168.1.10") is True
    assert limiter.allow("192.168.1.10") is True
    assert limiter.allow("192.168.1.10") is False

    # Different IP still gets own quota; max_keys=2 evicts oldest cleanly
    assert limiter.allow("192.168.1.20") is True
    assert limiter.allow("192.168.1.30") is True

    # After window slides past 10s, .10 recovers (even cleanly after eviction or expiry)
    now[0] += 11.0
    assert limiter.allow("192.168.1.10") is True
    limiter.reset("192.168.1.10")
    assert limiter.allow("192.168.1.10") is True


@pytest.mark.unit
def test_disk_space_and_quota_enforcement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dl_dir = tmp_path / "dl"
    dl_dir.mkdir()
    (dl_dir / "existing.bin").write_bytes(b"X" * 1000)

    usage_cls = namedtuple("usage", ["total", "used", "free"])
    monkeypatch.setattr(shutil, "disk_usage", lambda _: usage_cls(10_000_000, 9_500_000, 500_000))

    with pytest.raises(ResourceLimitError, match="Insufficient free disk space"):
        check_download_disk_space(dl_dir, 400_000, min_free_margin_bytes=200_000)

    monkeypatch.setattr(
        shutil, "disk_usage", lambda _: usage_cls(100_000_000, 10_000_000, 90_000_000)
    )
    check_download_disk_space(dl_dir, 500, max_dir_bytes=2000)
    with pytest.raises(ResourceLimitError, match="Download directory quota exceeded"):
        check_download_disk_space(dl_dir, 1500, max_dir_bytes=2000)


@pytest.mark.unit
def test_validate_file_name_and_part_file_symlink_protections(tmp_path: Path) -> None:
    for unsafe in ("../escape", "dir/file.txt", "back\\slash", "bad\nname.txt", "esc\x1b.bin", "."):
        with pytest.raises(MalformedMessageError):
            validate_file_name(unsafe)

    assert validate_file_name("report_2026 (final).pdf") == "report_2026 (final).pdf"

    dl_dir = tmp_path / "dl"
    dl_dir.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"secret")

    symlink_part = dl_dir / ".evil.part"
    symlink_part.symlink_to(outside)

    with pytest.raises(PathValidationError):
        open_part_file_safe(symlink_part, offset=0)

    with pytest.raises(PathValidationError):
        validate_resume_part_file(dl_dir, str(symlink_part), str(dl_dir / "final.bin"))

    missing_part = dl_dir / ".missing.part"
    with pytest.raises(TransferError, match="not found"):
        validate_resume_part_file(dl_dir, str(missing_part), str(dl_dir / "final.bin"))


@pytest.mark.unit
def test_binary_header_fuzz_rejections() -> None:
    # Bad magic, unsupported version, illegal flags on control frame, oversized frame
    with pytest.raises(FramingError):
        unpack_header(b"XX\x01\x10\x00\x00" + b"\x00" * 16 + b"\x00\x00\x00\x10")

    with pytest.raises(UnsupportedVersionError):
        unpack_header(b"P2\x99\x10\x00\x00" + b"\x00" * 16 + b"\x00\x00\x00\x10")

    with pytest.raises(FramingError):
        unpack_header(b"P2\x01\x10\x00\x01" + b"\x00" * 16 + b"\x00\x00\x00\x10")

    with pytest.raises(ResourceLimitError):
        unpack_header(b"P2\x01\x32\x00\x00" + b"\x00" * 16 + b"\xff\xff\xff\xff")

    valid_frame = pack_frame(MessageType.PING, b'{"peer_id":"x"}')
    assert len(valid_frame) == 26 + len(b'{"peer_id":"x"}')
