"""Exclusive PID file locking and stale-process detection for the background daemon."""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
from pathlib import Path

from p2p.exceptions import DaemonAlreadyRunningError, DaemonError, PathValidationError


def is_process_alive(pid: int) -> bool:
    """Return True if a POSIX process with pid exists."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def read_pid(pid_path: Path) -> int | None:
    """Read an integer PID from pid_path if present and valid."""
    try:
        if pid_path.is_symlink() or not pid_path.exists():
            return None
        raw = pid_path.read_text(encoding="utf-8").strip()
        return int(raw) if raw else None
    except (OSError, ValueError):
        return None


class PidFileLock:
    """Manages an exclusive advisory flock PID file preventing duplicate daemons."""

    def __init__(self, pid_path: Path) -> None:
        raw = Path(pid_path).expanduser()
        self._raw_path = raw
        self.pid_path = raw.parent.resolve(strict=False) / raw.name
        self._fd: int | None = None

    def acquire(self) -> int:
        """Acquire exclusive lock on pid_path and write current PID."""
        if self._fd is not None:
            return os.getpid()

        if self._raw_path.is_symlink() or self.pid_path.is_symlink():
            raise PathValidationError(f"Refusing symlink PID file: {self._raw_path}")
        if self._raw_path.parent.is_symlink():
            raise PathValidationError(
                f"Refusing symlink parent directory for PID file: {self._raw_path.parent}"
            )
        self.pid_path.parent.mkdir(parents=True, exist_ok=True)
        if self.pid_path.parent.is_symlink():
            raise PathValidationError(
                f"Refusing symlink parent directory for PID file: {self.pid_path.parent}"
            )
        if self.pid_path.is_symlink():
            raise PathValidationError(f"Refusing symlink PID file: {self.pid_path}")

        nofollow = getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.pid_path, os.O_CREAT | os.O_RDWR | nofollow, 0o600)
        except OSError as exc:
            raise DaemonError(f"Unable to open PID file {self.pid_path}: {exc}") from exc

        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EAGAIN, errno.EACCES}:
                    os.lseek(fd, 0, os.SEEK_SET)
                    existing = os.read(fd, 64).decode("utf-8", errors="ignore").strip()
                    pid_desc = existing or "unknown"
                    raise DaemonAlreadyRunningError(
                        f"P2P daemon is already running (PID {pid_desc})"
                    ) from exc
                raise DaemonError(f"Failed locking PID file {self.pid_path}: {exc}") from exc

            pid = os.getpid()
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, f"{pid}\n".encode("ascii"))
            os.fsync(fd)
            self._fd = fd
            return pid
        except Exception:
            os.close(fd)
            raise

    def release(self) -> None:
        """Remove PID file and release advisory file lock idempotently."""
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            with contextlib.suppress(OSError):
                self.pid_path.unlink(missing_ok=True)
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
