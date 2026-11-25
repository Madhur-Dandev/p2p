"""Resource safety helpers: sliding-window rate limiting and disk space checks."""

from __future__ import annotations

import os
import shutil
import threading
from collections import OrderedDict, deque
from collections.abc import Callable
from pathlib import Path

from p2p.exceptions import ResourceLimitError, ValidationError
from p2p.utils.time_utils import monotonic_now


class SlidingWindowRateLimiter:
    """Bounded-memory per-key sliding-window rate limiter safe for concurrent calls."""

    def __init__(
        self,
        *,
        max_requests: int = 200,
        window_sec: float = 10.0,
        max_keys: int = 1024,
        clock: Callable[[], float] = monotonic_now,
    ) -> None:
        if max_requests <= 0:
            raise ValidationError("max_requests must be > 0")
        if window_sec <= 0:
            raise ValidationError("window_sec must be > 0")
        if max_keys <= 0:
            raise ValidationError("max_keys must be > 0")
        self._max_requests = max_requests
        self._window_sec = window_sec
        self._max_keys = max_keys
        self._clock = clock
        self._buckets: OrderedDict[str, deque[float]] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        """Record an event for key if within rate quota; return False if throttled."""
        now = self._clock()
        cutoff = now - self._window_sec
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = deque()
                self._buckets[key] = bucket
                while len(self._buckets) > self._max_keys:
                    self._buckets.popitem(last=False)
            else:
                self._buckets.move_to_end(key)

            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= self._max_requests:
                return False

            bucket.append(now)
            return True

    def reset(self, key: str | None = None) -> None:
        """Clear recorded request timestamps for one key or all keys."""
        with self._lock:
            if key is None:
                self._buckets.clear()
            else:
                self._buckets.pop(key, None)


def _compute_dir_bytes(directory: Path) -> int:
    total = 0
    stack: list[Path] = [directory]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_size
        except OSError:
            continue
    return total


def check_download_disk_space(
    download_dir: Path,
    required_bytes: int,
    *,
    min_free_margin_bytes: int = 1_048_576,
    max_dir_bytes: int = 0,
) -> None:
    """Verify sufficient free disk space and optional quota remain before writing bytes."""
    if required_bytes < 0:
        raise ValidationError(f"required_bytes must be >= 0, got {required_bytes}")
    resolved = download_dir.expanduser().resolve(strict=False)
    resolved.mkdir(parents=True, exist_ok=True)

    usage = shutil.disk_usage(resolved)
    needed_free = required_bytes + (min_free_margin_bytes if required_bytes > 0 else 0)
    if usage.free < needed_free:
        raise ResourceLimitError(
            f"Insufficient free disk space in {resolved}: "
            f"free={usage.free} B, required={needed_free} B"
        )

    if max_dir_bytes > 0:
        current_used = _compute_dir_bytes(resolved)
        if current_used + required_bytes > max_dir_bytes:
            raise ResourceLimitError(
                f"Download directory quota exceeded: current={current_used} B + "
                f"incoming={required_bytes} B > max={max_dir_bytes} B"
            )
