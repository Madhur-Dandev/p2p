"""Bounded-timeout asynchronous multi-peer file search service."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass

from p2p.constants import (
    DEFAULT_SEARCH_TIMEOUT_SEC,
    MAX_QUERY_LENGTH,
    MAX_SEARCH_RESULTS_PER_PEER,
    MAX_SIMULTANEOUS_CONNECTIONS,
)
from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    ResourceLimitError,
    UnsupportedVersionError,
    ValidationError,
)
from p2p.logging_config import get_context_logger
from p2p.network.peer_registry import PeerInfo, PeerRegistry
from p2p.network.protocol import (
    SearchRequestMessage,
    SearchResponseMessage,
    TransferErrorMessage,
    validate_search_query,
)
from p2p.network.tcp_client import TcpClient
from p2p.search.local_search import LocalSearchService
from p2p.search.result_merger import (
    MergedSearchResult,
    PeerQueryStatus,
    PeerSearchResult,
    merge_search_results,
)
from p2p.utils.time_utils import monotonic_now

_MAX_CACHED_FILE_IDS = 4096


@dataclass(frozen=True, slots=True)
class DistributedSearchReport:
    """Complete result bundle returned by a distributed search across peers."""

    query: str
    results: list[MergedSearchResult]
    raw_hits: list[PeerSearchResult]
    peer_statuses: list[PeerQueryStatus]
    peers_queried: int
    peers_succeeded: int
    peers_failed: int
    any_truncated: bool
    duration_sec: float


class DistributedSearchService:
    """Fans out SEARCH_REQUEST queries concurrently across active peers with timeouts."""

    def __init__(
        self,
        *,
        local_peer_id: str,
        tcp_client: TcpClient,
        registry: PeerRegistry,
        local_search: LocalSearchService | None = None,
        display_name: str = "local-node",
        local_ip: str = "127.0.0.1",
        local_tcp_port: int = 0,
        search_timeout_sec: float = DEFAULT_SEARCH_TIMEOUT_SEC,
        max_query_length: int = MAX_QUERY_LENGTH,
        max_concurrent_queries: int = MAX_SIMULTANEOUS_CONNECTIONS,
        mark_stale_on_error: bool = True,
    ) -> None:
        if search_timeout_sec <= 0:
            raise ValidationError("search_timeout_sec must be > 0")
        if max_concurrent_queries <= 0:
            raise ValidationError("max_concurrent_queries must be > 0")

        self._local_peer_id = local_peer_id
        self._tcp_client = tcp_client
        self._registry = registry
        self._local_search = local_search
        self._display_name = display_name
        self._local_ip = local_ip
        self._local_tcp_port = local_tcp_port
        self._search_timeout_sec = search_timeout_sec
        self._max_query_length = max_query_length
        self._semaphore = asyncio.Semaphore(max_concurrent_queries)
        self._mark_stale_on_error = mark_stale_on_error
        self._source_cache: OrderedDict[str, list[PeerSearchResult]] = OrderedDict()
        self._logger = get_context_logger("search.distributed", peer_id=local_peer_id)

    def set_local_tcp_port(self, port: int) -> None:
        """Update the advertised local TCP port once TcpServer has bound."""
        self._local_tcp_port = port

    def resolve_file_id(self, file_id: str) -> list[PeerSearchResult]:
        """Return cached remote/local source records matching file_id."""
        return list(self._source_cache.get(file_id, ()))

    def clear_cache(self) -> None:
        """Clear cached file_id -> source mappings."""
        self._source_cache.clear()

    def _cache_hits(self, hits: list[PeerSearchResult]) -> None:
        by_id: dict[str, list[PeerSearchResult]] = {}
        for hit in hits:
            by_id.setdefault(hit.file_id, []).append(hit)
        for fid, records in by_id.items():
            # Preserve unique (peer_id, ip, port, file_id) entries
            seen: dict[tuple[str, str, int], PeerSearchResult] = {}
            for existing in self._source_cache.get(fid, ()):
                seen[(existing.source_peer_id, existing.source_ip, existing.source_tcp_port)] = (
                    existing
                )
            for rec in records:
                seen[(rec.source_peer_id, rec.source_ip, rec.source_tcp_port)] = rec
            if fid in self._source_cache:
                self._source_cache.move_to_end(fid)
            self._source_cache[fid] = list(seen.values())
            while len(self._source_cache) > _MAX_CACHED_FILE_IDS:
                self._source_cache.popitem(last=False)

    async def _query_single_peer(
        self,
        peer: PeerInfo,
        *,
        clean_query: str,
        exact: bool,
        max_results: int,
        deadline: float,
    ) -> tuple[list[PeerSearchResult], PeerQueryStatus]:
        started = monotonic_now()
        try:
            async with self._semaphore:
                remaining = deadline - monotonic_now()
                if remaining <= 0:
                    raise TimeoutError("Aggregate search timeout budget exhausted")

                req = SearchRequestMessage(
                    peer_id=self._local_peer_id,
                    query=clean_query,
                    max_results=max_results,
                    exact=exact,
                )
                decoded = await asyncio.wait_for(
                    self._tcp_client.request(
                        peer.ip_address,
                        peer.tcp_port,
                        req,
                        timeout_sec=remaining,
                    ),
                    timeout=remaining,
                )
        except TimeoutError:
            elapsed_ms = (monotonic_now() - started) * 1000.0
            if self._mark_stale_on_error:
                self._registry.mark_stale(peer.peer_id)
            self._logger.warning(
                "Search timed out for peer %s (%s:%d) after %.1fms",
                peer.display_name,
                peer.ip_address,
                peer.tcp_port,
                elapsed_ms,
            )
            return [], PeerQueryStatus(
                peer_id=peer.peer_id,
                display_name=peer.display_name,
                ip_address=peer.ip_address,
                tcp_port=peer.tcp_port,
                ok=False,
                elapsed_ms=elapsed_ms,
                result_count=0,
                error="Search request timed out",
            )
        except (
            OSError,
            ConnectionError,
            FramingError,
            UnsupportedVersionError,
            MalformedMessageError,
            ResourceLimitError,
        ) as exc:
            elapsed_ms = (monotonic_now() - started) * 1000.0
            if self._mark_stale_on_error and isinstance(exc, (OSError, ConnectionError)):
                self._registry.mark_stale(peer.peer_id)
            self._logger.warning(
                "Search failed for peer %s (%s:%d): %s",
                peer.display_name,
                peer.ip_address,
                peer.tcp_port,
                exc,
            )
            return [], PeerQueryStatus(
                peer_id=peer.peer_id,
                display_name=peer.display_name,
                ip_address=peer.ip_address,
                tcp_port=peer.tcp_port,
                ok=False,
                elapsed_ms=elapsed_ms,
                result_count=0,
                error=str(exc) or type(exc).__name__,
            )

        elapsed_ms = (monotonic_now() - started) * 1000.0
        payload = decoded.payload
        if isinstance(payload, TransferErrorMessage):
            return [], PeerQueryStatus(
                peer_id=peer.peer_id,
                display_name=peer.display_name,
                ip_address=peer.ip_address,
                tcp_port=peer.tcp_port,
                ok=False,
                elapsed_ms=elapsed_ms,
                result_count=0,
                error=f"{payload.code}: {payload.message}",
            )

        if not isinstance(payload, SearchResponseMessage):
            return [], PeerQueryStatus(
                peer_id=peer.peer_id,
                display_name=peer.display_name,
                ip_address=peer.ip_address,
                tcp_port=peer.tcp_port,
                ok=False,
                elapsed_ms=elapsed_ms,
                result_count=0,
                error=f"Unexpected response message {decoded.msg_type.name}",
            )

        hits = [
            PeerSearchResult(
                file_id=rec.file_id,
                file_name=rec.file_name,
                file_size=rec.file_size,
                sha256=rec.sha256,
                indexed_at=rec.indexed_at,
                source_peer_id=peer.peer_id,
                source_display_name=peer.display_name,
                source_ip=peer.ip_address,
                source_tcp_port=peer.tcp_port,
            )
            for rec in payload.results
        ]
        status = PeerQueryStatus(
            peer_id=peer.peer_id,
            display_name=peer.display_name,
            ip_address=peer.ip_address,
            tcp_port=peer.tcp_port,
            ok=True,
            elapsed_ms=elapsed_ms,
            result_count=len(hits),
            truncated=payload.truncated,
        )
        return hits, status

    async def search(
        self,
        query: str,
        *,
        exact: bool = False,
        max_results: int = MAX_SEARCH_RESULTS_PER_PEER,
        include_local: bool = False,
        include_stale: bool = False,
        timeout_sec: float | None = None,
    ) -> DistributedSearchReport:
        """Query all target peers concurrently and return deduplicated merged results."""
        clean_query = validate_search_query(query, max_length=self._max_query_length)
        clamped_limit = max(1, min(max_results, MAX_SEARCH_RESULTS_PER_PEER))
        effective_timeout = (
            self._search_timeout_sec if timeout_sec is None else max(0.05, float(timeout_sec))
        )

        started = monotonic_now()
        deadline = started + effective_timeout
        peers = self._registry.list_peers(include_stale=include_stale)

        raw_hits: list[PeerSearchResult] = []
        statuses: list[PeerQueryStatus] = []
        any_truncated = False

        if peers:
            outcomes = await asyncio.gather(
                *(
                    self._query_single_peer(
                        peer,
                        clean_query=clean_query,
                        exact=exact,
                        max_results=clamped_limit,
                        deadline=deadline,
                    )
                    for peer in peers
                )
            )
            for peer_hits, status in outcomes:
                raw_hits.extend(peer_hits)
                statuses.append(status)
                if status.truncated:
                    any_truncated = True

        if include_local and self._local_search is not None:
            local_started = monotonic_now()
            local_matches, local_truncated = await self._local_search.search_async(
                clean_query,
                exact=exact,
                max_results=clamped_limit,
            )
            local_ms = (monotonic_now() - local_started) * 1000.0
            local_hits = [
                PeerSearchResult(
                    file_id=item.file_id,
                    file_name=item.file_name,
                    file_size=item.file_size,
                    sha256=item.sha256,
                    indexed_at=item.indexed_at,
                    source_peer_id=self._local_peer_id,
                    source_display_name=self._display_name,
                    source_ip=self._local_ip,
                    source_tcp_port=self._local_tcp_port or 9000,
                )
                for item in local_matches
            ]
            raw_hits.extend(local_hits)
            if local_truncated:
                any_truncated = True
            statuses.append(
                PeerQueryStatus(
                    peer_id=self._local_peer_id,
                    display_name=f"{self._display_name} (local)",
                    ip_address=self._local_ip,
                    tcp_port=self._local_tcp_port,
                    ok=True,
                    elapsed_ms=local_ms,
                    result_count=len(local_hits),
                    truncated=local_truncated,
                )
            )

        self._cache_hits(raw_hits)
        merged = merge_search_results(raw_hits, query=clean_query)
        remote_statuses = [st for st in statuses if st.peer_id != self._local_peer_id]
        succeeded = sum(1 for st in remote_statuses if st.ok)
        failed = len(remote_statuses) - succeeded
        duration = monotonic_now() - started

        self._logger.info(
            "Distributed search query=%r peers=%d ok=%d failed=%d merged=%d duration=%.3fs",
            clean_query,
            len(peers),
            succeeded,
            failed,
            len(merged),
            duration,
        )
        return DistributedSearchReport(
            query=clean_query,
            results=merged,
            raw_hits=raw_hits,
            peer_statuses=statuses,
            peers_queried=len(peers),
            peers_succeeded=succeeded,
            peers_failed=failed,
            any_truncated=any_truncated,
            duration_sec=duration,
        )
