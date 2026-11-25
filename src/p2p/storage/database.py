"""SQLite database connection manager with serialized locking, WAL mode, and transactions."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TypeVar

from p2p.exceptions import StorageError
from p2p.logging_config import get_context_logger
from p2p.storage.migrations import apply_migrations, get_schema_version

T = TypeVar("T")


class Database:
    """Serialized SQLite database manager safe for multi-coroutine asyncio applications.

    Concurrency model:
        One SQLite connection (isolation_level=None, WAL journal mode, foreign_keys=ON)
        is guarded by a re-entrant threading.RLock. Synchronous callers use
        `with db.transaction():` directly; asynchronous callers offload operations via
        `await db.run(...)` (`asyncio.to_thread`), ensuring only one thread accesses the
        connection at a time without blocking the asyncio event loop.
    """

    def __init__(self, db_path: Path | str) -> None:
        self._raw_path = str(db_path)
        self._is_memory = self._raw_path == ":memory:"
        self._path = Path(self._raw_path) if not self._is_memory else Path(":memory:")
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._tx_depth = 0
        self._logger = get_context_logger("storage.database")

    @property
    def path(self) -> Path:
        """Return database file path (or Path(':memory:') for in-memory test DBs)."""
        return self._path

    @property
    def is_open(self) -> bool:
        """Return True while the SQLite connection is open."""
        return self._conn is not None

    def initialize(self) -> int:
        """Open the SQLite database, configure pragmas, and apply pending migrations."""
        with self._lock:
            if self._conn is not None:
                return get_schema_version(self._conn)

            if not self._is_memory:
                self._path.parent.mkdir(parents=True, exist_ok=True)

            try:
                conn = sqlite3.connect(
                    self._raw_path,
                    timeout=5.0,
                    isolation_level=None,
                    check_same_thread=False,
                )
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA busy_timeout = 5000")
                if not self._is_memory:
                    conn.execute("PRAGMA journal_mode = WAL")
                    conn.execute("PRAGMA synchronous = NORMAL")
                self._conn = conn
                version = apply_migrations(conn)
                self._logger.info(
                    "SQLite database ready at %s (schema_version=%d)",
                    self._raw_path,
                    version,
                )
                return version
            except sqlite3.Error as exc:
                if self._conn is not None:
                    self._conn.close()
                    self._conn = None
                raise StorageError(f"Failed to initialize SQLite database: {exc}") from exc

    def close(self) -> None:
        """Close the SQLite connection cleanly (idempotent)."""
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            except sqlite3.Error as exc:
                self._logger.warning("Error closing SQLite connection: %s", exc)
            finally:
                self._conn = None
                self._tx_depth = 0

    def __enter__(self) -> Database:
        self.initialize()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StorageError("Database has not been initialized")
        return self._conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Acquire the connection lock for read queries without opening a write transaction."""
        with self._lock:
            yield self._require_conn()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Execute one or more statements inside an atomic SQLite transaction."""
        with self._lock:
            conn = self._require_conn()
            is_outer = self._tx_depth == 0
            savepoint_name = f"sp_{self._tx_depth}"
            try:
                if is_outer:
                    conn.execute("BEGIN IMMEDIATE")
                else:
                    conn.execute(f"SAVEPOINT {savepoint_name}")
                self._tx_depth += 1
                yield conn
            except Exception as exc:
                self._tx_depth -= 1
                try:
                    if is_outer:
                        conn.execute("ROLLBACK")
                    else:
                        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                except sqlite3.Error:
                    pass
                if isinstance(exc, sqlite3.Error):
                    raise StorageError(f"SQLite transaction failed: {exc}") from exc
                raise
            else:
                self._tx_depth -= 1
                try:
                    if is_outer:
                        conn.execute("COMMIT")
                    else:
                        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                except sqlite3.Error as exc:
                    raise StorageError(f"SQLite commit failed: {exc}") from exc

    async def run(self, operation: Callable[[], T]) -> T:
        """Run a synchronous database operation off the asyncio event loop."""
        return await asyncio.to_thread(operation)
