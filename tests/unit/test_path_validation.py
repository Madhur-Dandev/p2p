"""Unit tests for canonical symlink-safe path containment and download targets."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.exceptions import MalformedMessageError, PathValidationError, TransferError
from p2p.security.path_validation import (
    open_shared_file_readonly,
    resolve_download_destination,
    resolve_shared_file,
)


@pytest.mark.unit
def test_resolve_shared_file_accepts_nested_and_rejects_traversal_and_symlinks(
    tmp_path: Path,
) -> None:
    share_root = tmp_path / "shared"
    nested = share_root / "sub"
    nested.mkdir(parents=True)
    target = nested / "notes.txt"
    target.write_text("allowed", encoding="utf-8")

    outside = tmp_path / "secret.txt"
    outside.write_text("top-secret", encoding="utf-8")

    outside_dir = tmp_path / "outside_dir"
    outside_dir.mkdir()
    (outside_dir / "hidden.txt").write_text("hidden", encoding="utf-8")

    # Valid file inside share_root
    resolved = resolve_shared_file(share_root, "sub/notes.txt")
    assert resolved == target.resolve()
    with open_shared_file_readonly(share_root, "sub/notes.txt") as handle:
        assert handle.read() == b"allowed"

    # Traversal and malformed paths
    for bad_rel in ("", "../secret.txt", "sub/../../secret.txt", "/etc/passwd", "a\\b", "bad\x00"):
        with pytest.raises(PathValidationError):
            resolve_shared_file(share_root, bad_rel)

    # Symlink to outside file
    link_file = share_root / "escape.txt"
    link_file.symlink_to(outside)
    with pytest.raises(PathValidationError, match="Symbolic link"):
        resolve_shared_file(share_root, "escape.txt")
    with pytest.raises(PathValidationError):
        open_shared_file_readonly(share_root, "escape.txt")

    # Symlink directory component pointing outside share_root
    link_dir = share_root / "linked_dir"
    link_dir.symlink_to(outside_dir)
    with pytest.raises(PathValidationError, match="Symbolic link"):
        resolve_shared_file(share_root, "linked_dir/hidden.txt")


@pytest.mark.unit
def test_resolve_download_destination_guards_traversal_overwrite_and_filename_length(
    tmp_path: Path,
) -> None:
    dl_dir = tmp_path / "downloads"

    final_p, part_p = resolve_download_destination(
        dl_dir,
        "report final.pdf",
        transfer_id="xfer-1",
    )
    assert final_p == dl_dir.resolve() / "report final.pdf"
    assert part_p.parent == dl_dir.resolve()
    assert part_p.name.endswith(".part")
    assert len(part_p.name) <= 255

    # Traversal filenames rejected
    for unsafe_name in ("../evil.txt", "sub/file.txt", "..", ".", "a\x00b"):
        with pytest.raises(MalformedMessageError):
            resolve_download_destination(dl_dir, unsafe_name, transfer_id="xfer-2")

    # Long 255-char filename still produces <= 255-byte .part name
    long_name = "a" * 255
    final_long, part_long = resolve_download_destination(dl_dir, long_name, transfer_id="xfer-long")
    assert len(final_long.name) == 255
    assert len(part_long.name.encode("utf-8")) <= 255

    # Refuses silent overwrite when file already exists
    final_p.write_text("existing", encoding="utf-8")
    with pytest.raises(TransferError, match="already exists"):
        resolve_download_destination(dl_dir, "report final.pdf", transfer_id="xfer-3")

    # Allows explicit overwrite=True
    final_ov, _ = resolve_download_destination(
        dl_dir, "report final.pdf", transfer_id="xfer-4", overwrite=True
    )
    assert final_ov == final_p
