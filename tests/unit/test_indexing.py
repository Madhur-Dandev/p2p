"""Unit tests for shared-directory scanning, caching, renames, deletes, and revalidation."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from p2p.exceptions import FileNotFoundInIndexError
from p2p.indexing.file_index import FileIndexer
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository


@pytest.mark.unit
def test_indexer_handles_nested_spaces_empty_duplicates_excluded_and_symlinks(
    tmp_path: Path,
) -> None:
    share = tmp_path / "shared"
    nested_a = share / "course notes"
    nested_b = share / "archive"
    excluded = share / ".git"
    nested_a.mkdir(parents=True)
    nested_b.mkdir(parents=True)
    excluded.mkdir(parents=True)

    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")

    (nested_a / "lecture 01.txt").write_bytes(b"hello world")
    (nested_a / "empty.txt").write_bytes(b"")
    (nested_a / "readme.md").write_bytes(b"dir-a")
    (nested_b / "readme.md").write_bytes(b"dir-b")
    (excluded / "config").write_bytes(b"ignored")
    (share / "symlink_outside.txt").symlink_to(outside)

    with Database(tmp_path / "index.db") as db:
        indexer = FileIndexer(db, shared_directories=[share])
        summary1 = indexer.rebuild()

        assert summary1.scanned == 4
        assert summary1.added == 4
        assert summary1.updated == 0
        assert summary1.unchanged_cached == 0
        assert summary1.skipped_symlinks == 1

        rows = FileIndexRepository(db).list_all()
        assert len(rows) == 4
        # Duplicate filenames in distinct directories get distinct file_ids & relative paths
        readmes = [r for r in rows if r.file_name == "readme.md"]
        assert len(readmes) == 2
        assert readmes[0].file_id != readmes[1].file_id
        assert {r.relative_path for r in readmes} == {
            "archive/readme.md",
            "course notes/readme.md",
        }

        empty_entry = next(r for r in rows if r.file_name == "empty.txt")
        assert empty_entry.file_size == 0
        assert empty_entry.sha256 == hashlib.sha256(b"").hexdigest()

        # Second rebuild uses (size, mtime_ns) cache without re-hashing unchanged files
        summary2 = indexer.rebuild()
        assert summary2.scanned == 4
        assert summary2.added == 0
        assert summary2.updated == 0
        assert summary2.unchanged_cached == 4

        # Modify a file, rename another, and delete empty.txt
        lecture = nested_a / "lecture 01.txt"
        lecture.write_bytes(b"hello updated content!")
        os.utime(lecture, ns=(lecture.stat().st_atime_ns, lecture.stat().st_mtime_ns + 1_000_000))
        (nested_b / "readme.md").rename(nested_b / "readme_renamed.md")
        (nested_a / "empty.txt").unlink()

        summary3 = indexer.rebuild()
        assert summary3.scanned == 3
        assert summary3.added == 1  # readme_renamed.md
        assert summary3.updated == 1  # lecture 01.txt
        assert summary3.unchanged_cached == 1  # course notes/readme.md
        assert summary3.removed == 2  # empty.txt + old archive/readme.md


@pytest.mark.unit
def test_revalidate_file_detects_post_index_changes_and_deletion(tmp_path: Path) -> None:
    share = tmp_path / "share"
    share.mkdir()
    doc = share / "data.bin"
    doc.write_bytes(b"original-bytes")

    with Database(tmp_path / "idx.db") as db:
        indexer = FileIndexer(db, shared_directories=[share])
        indexer.rebuild()
        row = FileIndexRepository(db).list_all()[0]

        # Modify content WITHOUT changing file size to test strict verify_hash=True
        doc.write_bytes(b"modified-bytes")
        assert len(b"original-bytes") == len(b"modified-bytes")
        refreshed = indexer.revalidate_file(row.file_id, verify_hash=True)
        assert refreshed.sha256 == hashlib.sha256(b"modified-bytes").hexdigest()

        # Delete file -> revalidation prunes stale index entry and raises
        doc.unlink()
        with pytest.raises(FileNotFoundInIndexError):
            indexer.revalidate_file(row.file_id)
        assert FileIndexRepository(db).get_by_file_id(row.file_id) is None
