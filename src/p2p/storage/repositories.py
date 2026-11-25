"""Parameterized SQLite repositories for peers, shared files, downloads, and checkpoints."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass

from p2p.exceptions import StorageError
from p2p.indexing.file_metadata import IndexedFile
from p2p.network.peer_registry import PeerInfo, PeerStatus
from p2p.storage.database import Database
from p2p.utils.time_utils import monotonic_now, utc_now_iso


def _escape_like_pattern(term: str) -> str:
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@dataclass(frozen=True, slots=True)
class DownloadRecord:
    """Persisted download history and active state record."""

    transfer_id: str
    file_id: str
    file_name: str
    source_peer_id: str
    source_ip: str
    source_tcp_port: int
    expected_sha256: str
    expected_size: int
    bytes_downloaded: int
    destination_path: str
    part_path: str
    status: str
    error_message: str | None
    started_at: str
    updated_at: str
    completed_at: str | None


@dataclass(frozen=True, slots=True)
class TransferCheckpoint:
    """Resumable transfer checkpoint referencing a partial (.part) file."""

    transfer_id: str
    resume_offset: int
    expected_sha256: str
    expected_size: int
    part_path: str
    updated_at: str


class PeerRepository:
    """SQLite persistence for known peer metadata."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert_peer(self, peer: PeerInfo) -> None:
        """Insert or update a peer record atomically."""
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO peers (
                        peer_id, ip_address, tcp_port, udp_port, display_name,
                        protocol_version, capabilities_json, status, source,
                        first_seen_utc, last_seen_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(peer_id) DO UPDATE SET
                        ip_address = excluded.ip_address,
                        tcp_port = excluded.tcp_port,
                        udp_port = excluded.udp_port,
                        display_name = excluded.display_name,
                        protocol_version = excluded.protocol_version,
                        capabilities_json = excluded.capabilities_json,
                        status = excluded.status,
                        source = excluded.source,
                        last_seen_utc = excluded.last_seen_utc
                    """,
                    (
                        peer.peer_id,
                        peer.ip_address,
                        peer.tcp_port,
                        peer.udp_port,
                        peer.display_name,
                        peer.protocol_version,
                        json.dumps(list(peer.capabilities)),
                        str(peer.status),
                        peer.source,
                        peer.first_seen_utc,
                        peer.last_seen_utc,
                    ),
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to upsert peer {peer.peer_id}: {exc}") from exc

    def get_peer(self, peer_id: str) -> PeerInfo | None:
        """Fetch a persisted peer by peer_id."""
        try:
            with self._db.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM peers WHERE peer_id = ?",
                    (peer_id,),
                ).fetchone()
            return _row_to_peer(row) if row is not None else None
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to read peer {peer_id}: {exc}") from exc

    def list_peers(self, *, include_stale: bool = True) -> list[PeerInfo]:
        """List persisted peers ordered by most recent heartbeat."""
        try:
            with self._db.connection() as conn:
                if include_stale:
                    rows = conn.execute(
                        "SELECT * FROM peers ORDER BY last_seen_utc DESC, peer_id ASC"
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM peers WHERE status = ? "
                        "ORDER BY last_seen_utc DESC, peer_id ASC",
                        (str(PeerStatus.ACTIVE),),
                    ).fetchall()
            return [_row_to_peer(r) for r in rows]
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to list peers: {exc}") from exc

    def update_status(self, peer_id: str, status: PeerStatus) -> bool:
        """Update a peer's stored availability status."""
        try:
            with self._db.transaction() as conn:
                cur = conn.execute(
                    "UPDATE peers SET status = ? WHERE peer_id = ?",
                    (str(status), peer_id),
                )
            return cur.rowcount > 0
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to update status for peer {peer_id}: {exc}") from exc

    def delete_peer(self, peer_id: str) -> bool:
        """Delete a peer record from SQLite."""
        try:
            with self._db.transaction() as conn:
                cur = conn.execute("DELETE FROM peers WHERE peer_id = ?", (peer_id,))
            return cur.rowcount > 0
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to delete peer {peer_id}: {exc}") from exc


def _row_to_peer(row: sqlite3.Row) -> PeerInfo:
    caps_raw = json.loads(row["capabilities_json"])
    caps = tuple(str(c) for c in caps_raw) if isinstance(caps_raw, list) else ()
    return PeerInfo(
        peer_id=str(row["peer_id"]),
        ip_address=str(row["ip_address"]),
        tcp_port=int(row["tcp_port"]),
        udp_port=int(row["udp_port"]) if row["udp_port"] is not None else None,
        display_name=str(row["display_name"]),
        protocol_version=int(row["protocol_version"]),
        capabilities=caps,
        first_seen_utc=str(row["first_seen_utc"]),
        last_seen_utc=str(row["last_seen_utc"]),
        last_seen_monotonic=monotonic_now(),
        status=PeerStatus(str(row["status"])),
        source=str(row["source"]),
    )


def _row_to_file(row: sqlite3.Row) -> IndexedFile:
    return IndexedFile(
        file_id=str(row["file_id"]),
        share_root=str(row["share_root"]),
        relative_path=str(row["relative_path"]),
        file_name=str(row["file_name"]),
        file_size=int(row["file_size"]),
        mtime_ns=int(row["mtime_ns"]),
        sha256=str(row["sha256"]),
        indexed_at=str(row["indexed_at"]),
    )


def _row_to_download(row: sqlite3.Row) -> DownloadRecord:
    return DownloadRecord(
        transfer_id=str(row["transfer_id"]),
        file_id=str(row["file_id"]),
        file_name=str(row["file_name"]),
        source_peer_id=str(row["source_peer_id"]),
        source_ip=str(row["source_ip"]),
        source_tcp_port=int(row["source_tcp_port"]),
        expected_sha256=str(row["expected_sha256"]),
        expected_size=int(row["expected_size"]),
        bytes_downloaded=int(row["bytes_downloaded"]),
        destination_path=str(row["destination_path"]),
        part_path=str(row["part_path"]),
        status=str(row["status"]),
        error_message=str(row["error_message"]) if row["error_message"] is not None else None,
        started_at=str(row["started_at"]),
        updated_at=str(row["updated_at"]),
        completed_at=str(row["completed_at"]) if row["completed_at"] is not None else None,
    )


class FileIndexRepository:
    """SQLite persistence and query operations for indexed local shared files."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def upsert_files(self, records: Sequence[IndexedFile]) -> None:
        """Insert or update a batch of shared file records in one atomic transaction."""
        if not records:
            return
        try:
            with self._db.transaction() as conn:
                conn.executemany(
                    """
                    INSERT INTO shared_files (
                        file_id, share_root, relative_path, file_name,
                        file_size, mtime_ns, sha256, indexed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(file_id) DO UPDATE SET
                        share_root = excluded.share_root,
                        relative_path = excluded.relative_path,
                        file_name = excluded.file_name,
                        file_size = excluded.file_size,
                        mtime_ns = excluded.mtime_ns,
                        sha256 = excluded.sha256,
                        indexed_at = excluded.indexed_at
                    """,
                    [
                        (
                            item.file_id,
                            item.share_root,
                            item.relative_path,
                            item.file_name,
                            item.file_size,
                            item.mtime_ns,
                            item.sha256,
                            item.indexed_at,
                        )
                        for item in records
                    ],
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to upsert shared file records: {exc}") from exc

    def get_by_file_id(self, file_id: str) -> IndexedFile | None:
        """Return indexed file record by opaque file_id."""
        try:
            with self._db.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM shared_files WHERE file_id = ?",
                    (file_id,),
                ).fetchone()
            return _row_to_file(row) if row is not None else None
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to query file_id {file_id}: {exc}") from exc

    def list_all(self) -> list[IndexedFile]:
        """Return all indexed files ordered deterministically."""
        try:
            with self._db.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM shared_files ORDER BY share_root ASC, relative_path ASC"
                ).fetchall()
            return [_row_to_file(r) for r in rows]
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to list indexed files: {exc}") from exc

    def list_by_share_root(self, share_root: str) -> list[IndexedFile]:
        """Return all indexed files rooted under share_root."""
        try:
            with self._db.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM shared_files WHERE share_root = ? ORDER BY relative_path ASC",
                    (share_root,),
                ).fetchall()
            return [_row_to_file(r) for r in rows]
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to list files for root {share_root}: {exc}") from exc

    def delete_by_file_ids(self, file_ids: Sequence[str]) -> int:
        """Delete removed files by file_id inside a transaction."""
        if not file_ids:
            return 0
        try:
            with self._db.transaction() as conn:
                placeholders = ", ".join("?" for _ in file_ids)
                cur = conn.execute(
                    f"DELETE FROM shared_files WHERE file_id IN ({placeholders})",  # noqa: S608
                    tuple(file_ids),
                )
            return int(cur.rowcount)
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to delete removed file entries: {exc}") from exc

    def delete_unconfigured_roots(self, active_roots: Sequence[str]) -> int:
        """Remove indexed entries belonging to directories no longer shared."""
        try:
            with self._db.transaction() as conn:
                if not active_roots:
                    cur = conn.execute("DELETE FROM shared_files")
                else:
                    placeholders = ", ".join("?" for _ in active_roots)
                    cur = conn.execute(
                        f"DELETE FROM shared_files WHERE share_root NOT IN ({placeholders})",  # noqa: S608
                        tuple(active_roots),
                    )
            return int(cur.rowcount)
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to purge unconfigured share roots: {exc}") from exc

    def search_by_name(
        self,
        query: str,
        *,
        exact: bool = False,
        limit: int = 100,
    ) -> list[IndexedFile]:
        """Search indexed filenames case-insensitively with escaped LIKE wildcards."""
        try:
            with self._db.connection() as conn:
                if exact:
                    rows = conn.execute(
                        """
                        SELECT * FROM shared_files
                        WHERE file_name = ? COLLATE NOCASE
                        ORDER BY file_name ASC, relative_path ASC
                        LIMIT ?
                        """,
                        (query, limit),
                    ).fetchall()
                else:
                    escaped = f"%{_escape_like_pattern(query)}%"
                    rows = conn.execute(
                        """
                        SELECT * FROM shared_files
                        WHERE file_name LIKE ? COLLATE NOCASE ESCAPE '\\'
                        ORDER BY file_name ASC, relative_path ASC
                        LIMIT ?
                        """,
                        (escaped, limit),
                    ).fetchall()
            return [_row_to_file(r) for r in rows]
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to search shared files: {exc}") from exc

    def count_files(self) -> int:
        """Return total number of indexed shared files."""
        try:
            with self._db.connection() as conn:
                row = conn.execute("SELECT COUNT(*) AS cnt FROM shared_files").fetchone()
            return int(row["cnt"]) if row is not None else 0
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to count shared files: {exc}") from exc


class DownloadRepository:
    """SQLite persistence for download history, state transitions, and checkpoints."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def create_download(self, record: DownloadRecord) -> None:
        """Persist a newly created download entry."""
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO downloads (
                        transfer_id, file_id, file_name, source_peer_id,
                        source_ip, source_tcp_port, expected_sha256, expected_size,
                        bytes_downloaded, destination_path, part_path, status,
                        error_message, started_at, updated_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.transfer_id,
                        record.file_id,
                        record.file_name,
                        record.source_peer_id,
                        record.source_ip,
                        record.source_tcp_port,
                        record.expected_sha256,
                        record.expected_size,
                        record.bytes_downloaded,
                        record.destination_path,
                        record.part_path,
                        record.status,
                        record.error_message,
                        record.started_at,
                        record.updated_at,
                        record.completed_at,
                    ),
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to create download {record.transfer_id}: {exc}") from exc

    def update_state(
        self,
        transfer_id: str,
        *,
        status: str,
        file_name: str | None = None,
        destination_path: str | None = None,
        part_path: str | None = None,
        bytes_downloaded: int | None = None,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
        error_message: str | None = None,
        completed_at: str | None = None,
    ) -> None:
        """Update transfer state, metadata, and timestamps atomically."""
        now = utc_now_iso()
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    """
                    UPDATE downloads
                    SET status = ?,
                        file_name = COALESCE(?, file_name),
                        destination_path = COALESCE(?, destination_path),
                        part_path = COALESCE(?, part_path),
                        bytes_downloaded = COALESCE(?, bytes_downloaded),
                        expected_sha256 = COALESCE(?, expected_sha256),
                        expected_size = COALESCE(?, expected_size),
                        error_message = ?,
                        updated_at = ?,
                        completed_at = COALESCE(?, completed_at)
                    WHERE transfer_id = ?
                    """,
                    (
                        status,
                        file_name,
                        destination_path,
                        part_path,
                        bytes_downloaded,
                        expected_sha256,
                        expected_size,
                        error_message,
                        now,
                        completed_at,
                        transfer_id,
                    ),
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to update download {transfer_id}: {exc}") from exc

    def upsert_checkpoint(self, checkpoint: TransferCheckpoint) -> None:
        """Save or refresh a resumable transfer checkpoint."""
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO transfer_checkpoints (
                        transfer_id, resume_offset, expected_sha256,
                        expected_size, part_path, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(transfer_id) DO UPDATE SET
                        resume_offset = excluded.resume_offset,
                        expected_sha256 = excluded.expected_sha256,
                        expected_size = excluded.expected_size,
                        part_path = excluded.part_path,
                        updated_at = excluded.updated_at
                    """,
                    (
                        checkpoint.transfer_id,
                        checkpoint.resume_offset,
                        checkpoint.expected_sha256,
                        checkpoint.expected_size,
                        checkpoint.part_path,
                        checkpoint.updated_at,
                    ),
                )
        except sqlite3.Error as exc:
            raise StorageError(
                f"Failed to save checkpoint for {checkpoint.transfer_id}: {exc}"
            ) from exc

    def get_download(self, transfer_id: str) -> DownloadRecord | None:
        """Retrieve a download record by transfer_id."""
        try:
            with self._db.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM downloads WHERE transfer_id = ?",
                    (transfer_id,),
                ).fetchone()
            return _row_to_download(row) if row is not None else None
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to fetch download {transfer_id}: {exc}") from exc

    def get_checkpoint(self, transfer_id: str) -> TransferCheckpoint | None:
        """Retrieve a saved resume checkpoint if present."""
        try:
            with self._db.connection() as conn:
                row = conn.execute(
                    "SELECT * FROM transfer_checkpoints WHERE transfer_id = ?",
                    (transfer_id,),
                ).fetchone()
            if row is None:
                return None
            return TransferCheckpoint(
                transfer_id=str(row["transfer_id"]),
                resume_offset=int(row["resume_offset"]),
                expected_sha256=str(row["expected_sha256"]),
                expected_size=int(row["expected_size"]),
                part_path=str(row["part_path"]),
                updated_at=str(row["updated_at"]),
            )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to fetch checkpoint {transfer_id}: {exc}") from exc

    def delete_checkpoint(self, transfer_id: str) -> None:
        """Remove a resume checkpoint after transfer completion or abort."""
        try:
            with self._db.transaction() as conn:
                conn.execute(
                    "DELETE FROM transfer_checkpoints WHERE transfer_id = ?",
                    (transfer_id,),
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to delete checkpoint {transfer_id}: {exc}") from exc

    def list_downloads(self, *, limit: int = 50) -> list[DownloadRecord]:
        """List recent download records ordered newest first."""
        try:
            with self._db.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM downloads "
                    "ORDER BY updated_at DESC, started_at DESC, rowid DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [_row_to_download(r) for r in rows]
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to list downloads: {exc}") from exc

    def list_checkpoints(self) -> dict[str, TransferCheckpoint]:
        """Return all active resume checkpoints keyed by transfer_id."""
        try:
            with self._db.connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM transfer_checkpoints ORDER BY updated_at DESC"
                ).fetchall()
            return {
                str(row["transfer_id"]): TransferCheckpoint(
                    transfer_id=str(row["transfer_id"]),
                    resume_offset=int(row["resume_offset"]),
                    expected_sha256=str(row["expected_sha256"]),
                    expected_size=int(row["expected_size"]),
                    part_path=str(row["part_path"]),
                    updated_at=str(row["updated_at"]),
                )
                for row in rows
            }
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to list transfer checkpoints: {exc}") from exc

    def find_resumable(self, identifier: str) -> tuple[DownloadRecord, TransferCheckpoint] | None:
        """Look up a resumable (DownloadRecord, TransferCheckpoint) by transfer_id or file_id."""
        try:
            with self._db.connection() as conn:
                row = conn.execute(
                    """
                    SELECT d.*,
                           c.resume_offset AS cp_resume_offset,
                           c.expected_sha256 AS cp_expected_sha256,
                           c.expected_size AS cp_expected_size,
                           c.part_path AS cp_part_path,
                           c.updated_at AS cp_updated_at
                    FROM downloads d
                    JOIN transfer_checkpoints c ON d.transfer_id = c.transfer_id
                    WHERE d.transfer_id = ?
                    """,
                    (identifier,),
                ).fetchone()
                if row is None:
                    row = conn.execute(
                        """
                        SELECT d.*,
                               c.resume_offset AS cp_resume_offset,
                               c.expected_sha256 AS cp_expected_sha256,
                               c.expected_size AS cp_expected_size,
                               c.part_path AS cp_part_path,
                               c.updated_at AS cp_updated_at
                        FROM downloads d
                        JOIN transfer_checkpoints c ON d.transfer_id = c.transfer_id
                        WHERE d.file_id = ?
                        ORDER BY c.updated_at DESC, d.started_at DESC, c.rowid DESC
                        LIMIT 1
                        """,
                        (identifier,),
                    ).fetchone()
            if row is None:
                return None
            record = _row_to_download(row)
            checkpoint = TransferCheckpoint(
                transfer_id=record.transfer_id,
                resume_offset=int(row["cp_resume_offset"]),
                expected_sha256=str(row["cp_expected_sha256"]),
                expected_size=int(row["cp_expected_size"]),
                part_path=str(row["cp_part_path"]),
                updated_at=str(row["cp_updated_at"]),
            )
            return record, checkpoint
        except sqlite3.Error as exc:
            raise StorageError(
                f"Failed to query resumable download for {identifier}: {exc}"
            ) from exc
