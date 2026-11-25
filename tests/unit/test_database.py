"""Unit tests for SQLite schema migrations, transactions, and repositories."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.exceptions import StorageError
from p2p.indexing.file_metadata import IndexedFile
from p2p.network.peer_registry import PeerInfo, PeerStatus
from p2p.storage.database import Database
from p2p.storage.migrations import CURRENT_SCHEMA_VERSION, get_schema_version
from p2p.storage.repositories import (
    DownloadRecord,
    DownloadRepository,
    FileIndexRepository,
    PeerRepository,
    TransferCheckpoint,
)
from p2p.utils.identifiers import generate_peer_id
from p2p.utils.time_utils import monotonic_now, utc_now_iso


@pytest.mark.unit
def test_schema_migrations_and_transaction_rollback(tmp_path: Path) -> None:
    db_path = tmp_path / "db" / "node.db"
    with Database(db_path) as db:
        assert db.is_open
        with db.connection() as conn:
            assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION

        # Re-initializing on existing DB is idempotent
        assert db.initialize() == CURRENT_SCHEMA_VERSION

        repo = FileIndexRepository(db)
        sample = IndexedFile(
            file_id="id_tx_1",
            share_root="/shared",
            relative_path="a.txt",
            file_name="a.txt",
            file_size=10,
            mtime_ns=123,
            sha256="a" * 64,
            indexed_at=utc_now_iso(),
        )

        # Raised error inside transaction must roll back all writes
        with pytest.raises(RuntimeError, match="abort"), db.transaction():
            repo.upsert_files([sample])
            raise RuntimeError("abort")

        assert repo.get_by_file_id("id_tx_1") is None

        # Normal commit persists cleanly
        repo.upsert_files([sample])
        assert repo.get_by_file_id("id_tx_1") == sample


@pytest.mark.unit
def test_peer_file_and_download_repositories(tmp_path: Path) -> None:
    db_path = tmp_path / "p2p.db"
    with Database(db_path) as db:
        peer_repo = PeerRepository(db)
        file_repo = FileIndexRepository(db)
        dl_repo = DownloadRepository(db)

        peer_id = generate_peer_id()
        now = utc_now_iso()
        peer = PeerInfo(
            peer_id=peer_id,
            ip_address="192.168.1.50",
            tcp_port=9000,
            udp_port=9999,
            display_name="peer-db",
            protocol_version=1,
            capabilities=("search_v1",),
            first_seen_utc=now,
            last_seen_utc=now,
            last_seen_monotonic=monotonic_now(),
            status=PeerStatus.ACTIVE,
            source="udp",
        )
        peer_repo.upsert_peer(peer)
        loaded_peer = peer_repo.get_peer(peer_id)
        assert loaded_peer is not None
        assert loaded_peer.display_name == "peer-db"
        assert peer_repo.update_status(peer_id, PeerStatus.STALE) is True
        assert peer_repo.list_peers(include_stale=False) == []
        assert len(peer_repo.list_peers(include_stale=True)) == 1

        # LIKE wildcard escaping: search for "100%" or "a_b" must not match arbitrary names
        f1 = IndexedFile("f1", "/s", "100%_notes.txt", "100%_notes.txt", 1, 1, "b" * 64, now)
        f2 = IndexedFile("f2", "/s", "1000notes.txt", "1000notes.txt", 1, 1, "c" * 64, now)
        file_repo.upsert_files([f1, f2])
        assert [f.file_id for f in file_repo.search_by_name("100%")] == ["f1"]
        assert [f.file_id for f in file_repo.search_by_name("100%_NOTES.TXT", exact=True)] == ["f1"]

        # Download + checkpoint persistence
        dl = DownloadRecord(
            transfer_id="xfer-1",
            file_id="f1",
            file_name="100%_notes.txt",
            source_peer_id=peer_id,
            source_ip="192.168.1.50",
            source_tcp_port=9000,
            expected_sha256="b" * 64,
            expected_size=100,
            bytes_downloaded=50,
            destination_path="/dl/100%_notes.txt",
            part_path="/dl/.100%_notes.txt.part",
            status="transfer_interrupted",
            error_message="reset",
            started_at=now,
            updated_at=now,
            completed_at=None,
        )
        dl_repo.create_download(dl)
        dl_repo.upsert_checkpoint(
            TransferCheckpoint(
                transfer_id="xfer-1",
                resume_offset=50,
                expected_sha256="b" * 64,
                expected_size=100,
                part_path="/dl/.100%_notes.txt.part",
                updated_at=now,
            )
        )
        cp = dl_repo.get_checkpoint("xfer-1")
        assert cp is not None and cp.resume_offset == 50

        dl_repo.update_state(
            "xfer-1",
            status="completed",
            bytes_downloaded=100,
            completed_at=now,
        )
        dl_repo.delete_checkpoint("xfer-1")
        assert dl_repo.get_checkpoint("xfer-1") is None
        loaded_dl = dl_repo.get_download("xfer-1")
        assert loaded_dl is not None and loaded_dl.status == "completed"


@pytest.mark.unit
def test_uninitialized_database_raises_storage_error(tmp_path: Path) -> None:
    db = Database(tmp_path / "unopened.db")
    with pytest.raises(StorageError, match="not been initialized"):
        FileIndexRepository(db).count_files()
