"""Search result data models and cross-peer deduplication/ranking helpers."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PeerSearchResult:
    """Single unmerged search hit annotated with hosting peer endpoint metadata."""

    file_id: str
    file_name: str
    file_size: int
    sha256: str
    indexed_at: str
    source_peer_id: str
    source_display_name: str
    source_ip: str
    source_tcp_port: int


@dataclass(frozen=True, slots=True)
class PeerFileSource:
    """Concrete hosting peer source capable of serving a logical file."""

    peer_id: str
    display_name: str
    ip_address: str
    tcp_port: int
    file_id: str
    file_name: str
    indexed_at: str


@dataclass(frozen=True, slots=True)
class MergedSearchResult:
    """Logical file search result aggregating one or more peers with matching content."""

    file_name: str
    file_size: int
    sha256: str
    sources: tuple[PeerFileSource, ...]

    @property
    def primary_source(self) -> PeerFileSource:
        """Return the highest-priority peer source for deterministic selection."""
        return self.sources[0]

    @property
    def primary_file_id(self) -> str:
        """Return the primary hosting peer's file_id."""
        return self.sources[0].file_id

    @property
    def source_count(self) -> int:
        """Return how many distinct peer sources host this exact file."""
        return len(self.sources)

    @property
    def all_file_ids(self) -> tuple[str, ...]:
        """Return all source file_ids associated with this merged result."""
        return tuple(src.file_id for src in self.sources)


@dataclass(frozen=True, slots=True)
class PeerQueryStatus:
    """Per-peer outcome telemetry emitted by a distributed search operation."""

    peer_id: str
    display_name: str
    ip_address: str
    tcp_port: int
    ok: bool
    elapsed_ms: float
    result_count: int
    truncated: bool = False
    error: str | None = None


def merge_search_results(
    hits: Sequence[PeerSearchResult],
    *,
    query: str = "",
) -> list[MergedSearchResult]:
    """Deduplicate identical content across peers while preserving distinct files.

    Files are grouped by ``(sha256, file_size)`` so identical replicas hosted by
    multiple peers merge into one logical entry listing all available sources,
    while distinct files sharing the same filename remain separate entries.
    """
    grouped: dict[tuple[str, int], dict[tuple[str, str], PeerFileSource]] = defaultdict(dict)
    for hit in hits:
        content_key = (hit.sha256, hit.file_size)
        source_key = (hit.source_peer_id, hit.file_id)
        grouped[content_key].setdefault(
            source_key,
            PeerFileSource(
                peer_id=hit.source_peer_id,
                display_name=hit.source_display_name,
                ip_address=hit.source_ip,
                tcp_port=hit.source_tcp_port,
                file_id=hit.file_id,
                file_name=hit.file_name,
                indexed_at=hit.indexed_at,
            ),
        )

    normalized_query = query.strip().casefold()
    merged: list[MergedSearchResult] = []
    for (sha256_hex, file_size), source_map in grouped.items():
        ordered_sources = tuple(
            sorted(
                source_map.values(),
                key=lambda s: (s.peer_id, s.ip_address, s.tcp_port, s.file_id),
            )
        )
        # Prefer exact match filename if any replica matches query directly, else shortest name
        chosen_name = min(
            (src.file_name for src in ordered_sources),
            key=lambda name: (
                0 if normalized_query and name.casefold() == normalized_query else 1,
                len(name),
                name.casefold(),
            ),
        )
        merged.append(
            MergedSearchResult(
                file_name=chosen_name,
                file_size=file_size,
                sha256=sha256_hex,
                sources=ordered_sources,
            )
        )

    merged.sort(
        key=lambda item: (
            0 if normalized_query and item.file_name.casefold() == normalized_query else 1,
            0 if normalized_query and item.file_name.casefold().startswith(normalized_query) else 1,
            -item.source_count,
            item.file_name.casefold(),
            item.file_size,
            item.sha256,
        )
    )
    return merged
