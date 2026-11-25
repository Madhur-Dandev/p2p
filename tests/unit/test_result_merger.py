"""Unit tests for cross-peer search result deduplication and ranking."""

from __future__ import annotations

import pytest

from p2p.search.result_merger import PeerSearchResult, merge_search_results


def _make_hit(
    *,
    file_id: str,
    file_name: str,
    file_size: int,
    sha256: str,
    peer_id: str,
    display_name: str,
    ip: str,
    port: int,
) -> PeerSearchResult:
    return PeerSearchResult(
        file_id=file_id,
        file_name=file_name,
        file_size=file_size,
        sha256=sha256,
        indexed_at="2026-11-25T00:00:00Z",
        source_peer_id=peer_id,
        source_display_name=display_name,
        source_ip=ip,
        source_tcp_port=port,
    )


@pytest.mark.unit
def test_merge_deduplicates_replicas_and_ranks_exact_prefix_and_availability() -> None:
    hash_shared = "a" * 64
    hash_exact = "b" * 64
    hash_collision = "c" * 64

    hits = [
        # Duplicate exact hit from same (peer, file_id) must collapse cleanly
        _make_hit(
            file_id="f_a1",
            file_name="systems_notes.pdf",
            file_size=4096,
            sha256=hash_shared,
            peer_id="00000000-0000-4000-8000-000000000001",
            display_name="peer-1",
            ip="192.168.1.10",
            port=9001,
        ),
        _make_hit(
            file_id="f_a1",
            file_name="systems_notes.pdf",
            file_size=4096,
            sha256=hash_shared,
            peer_id="00000000-0000-4000-8000-000000000001",
            display_name="peer-1",
            ip="192.168.1.10",
            port=9001,
        ),
        # Second peer hosting identical bytes (same sha256 + file_size)
        _make_hit(
            file_id="f_b2",
            file_name="systems_notes.pdf",
            file_size=4096,
            sha256=hash_shared,
            peer_id="00000000-0000-4000-8000-000000000002",
            display_name="peer-2",
            ip="192.168.1.11",
            port=9002,
        ),
        # Exact match for query "notes" must rank first despite single source
        _make_hit(
            file_id="f_exact",
            file_name="notes",
            file_size=512,
            sha256=hash_exact,
            peer_id="00000000-0000-4000-8000-000000000003",
            display_name="peer-3",
            ip="192.168.1.12",
            port=9003,
        ),
        # Distinct content sharing same filename must stay separate
        _make_hit(
            file_id="f_other",
            file_name="systems_notes.pdf",
            file_size=8192,
            sha256=hash_collision,
            peer_id="00000000-0000-4000-8000-000000000003",
            display_name="peer-3",
            ip="192.168.1.12",
            port=9003,
        ),
    ]

    merged = merge_search_results(hits, query="notes")
    assert len(merged) == 3

    # Exact match wins rank 0
    assert merged[0].file_name == "notes"
    assert merged[0].source_count == 1

    # Between the two remaining non-prefix results, higher source_count ranks higher
    assert merged[1].sha256 == hash_shared
    assert merged[1].source_count == 2
    assert merged[1].all_file_ids == ("f_a1", "f_b2")
    assert merged[1].primary_source.ip_address == "192.168.1.10"

    assert merged[2].sha256 == hash_collision
    assert merged[2].file_size == 8192
    assert merged[2].source_count == 1
