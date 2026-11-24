"""Time and monotonic clock helpers for networking and persistence."""

from __future__ import annotations

import time
from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def utc_now_iso() -> str:
    """Return the current UTC timestamp formatted as an ISO-8601 string with 'Z' suffix."""
    return utc_now().isoformat(timespec="seconds").replace("+00:00", "Z")


def utc_timestamp() -> float:
    """Return the current Unix epoch time in seconds as a float."""
    return time.time()


def monotonic_now() -> float:
    """Return monotonic clock seconds for elapsed durations, timeouts, and heartbeats."""
    return time.monotonic()
