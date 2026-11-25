"""SQLite schema versioning and forward migrations."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from p2p.exceptions import StorageError
from p2p.utils.time_utils import utc_now_iso

CURRENT_SCHEMA_VERSION = 1


_V1_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS peers (
        peer_id TEXT PRIMARY KEY,
        ip_address TEXT NOT NULL,
        tcp_port INTEGER NOT NULL CHECK (tcp_port BETWEEN 1 AND 65535),
        udp_port INTEGER CHECK (udp_port IS NULL OR udp_port BETWEEN 1 AND 65535),
        display_name TEXT NOT NULL,
        protocol_version INTEGER NOT NULL CHECK (protocol_version >= 1),
        capabilities_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('active', 'stale', 'removed')),
        source TEXT NOT NULL CHECK (source IN ('udp', 'static')),
        first_seen_utc TEXT NOT NULL,
        last_seen_utc TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_peers_status_last_seen ON peers (status, last_seen_utc)",
    """
    CREATE TABLE IF NOT EXISTS shared_files (
        file_id TEXT PRIMARY KEY,
        share_root TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_size INTEGER NOT NULL CHECK (file_size >= 0),
        mtime_ns INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        indexed_at TEXT NOT NULL,
        UNIQUE (share_root, relative_path)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_shared_files_name ON shared_files (file_name COLLATE NOCASE)",
    "CREATE INDEX IF NOT EXISTS idx_shared_files_sha256 ON shared_files (sha256)",
    "CREATE INDEX IF NOT EXISTS idx_shared_files_root ON shared_files (share_root)",
    """
    CREATE TABLE IF NOT EXISTS downloads (
        transfer_id TEXT PRIMARY KEY,
        file_id TEXT NOT NULL,
        file_name TEXT NOT NULL,
        source_peer_id TEXT NOT NULL,
        source_ip TEXT NOT NULL,
        source_tcp_port INTEGER NOT NULL CHECK (source_tcp_port BETWEEN 1 AND 65535),
        expected_sha256 TEXT NOT NULL,
        expected_size INTEGER NOT NULL CHECK (expected_size >= 0),
        bytes_downloaded INTEGER NOT NULL CHECK (bytes_downloaded >= 0),
        destination_path TEXT NOT NULL,
        part_path TEXT NOT NULL,
        status TEXT NOT NULL,
        error_message TEXT,
        started_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads (status)",
    "CREATE INDEX IF NOT EXISTS idx_downloads_updated_at ON downloads (updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS transfer_checkpoints (
        transfer_id TEXT PRIMARY KEY
            REFERENCES downloads (transfer_id) ON DELETE CASCADE,
        resume_offset INTEGER NOT NULL CHECK (resume_offset >= 0),
        expected_sha256 TEXT NOT NULL,
        expected_size INTEGER NOT NULL CHECK (expected_size >= 0),
        part_path TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
)


def _migrate_v1(conn: sqlite3.Connection) -> None:
    """Create core tables and indexes for peers, shared files, downloads, and checkpoints."""
    for statement in _V1_STATEMENTS:
        conn.execute(statement)


_MIGRATIONS: list[tuple[int, Callable[[sqlite3.Connection], None]]] = [
    (1, _migrate_v1),
]


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied migration version, or 0 if uninitialized."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    if row is None:
        return 0
    ver_row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
    ).fetchone()
    return int(ver_row["version"]) if ver_row is not None else 0


def apply_migrations(conn: sqlite3.Connection) -> int:
    """Create schema_migrations if needed and apply all pending schema versions atomically."""
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        current = get_schema_version(conn)
        for version, migration_fn in _MIGRATIONS:
            if version <= current:
                continue
            conn.execute("BEGIN")
            try:
                migration_fn(conn)
                conn.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                    (version, utc_now_iso()),
                )
                conn.execute("COMMIT")
                current = version
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return current
    except sqlite3.Error as exc:
        raise StorageError(f"Database migration failed: {exc}") from exc
