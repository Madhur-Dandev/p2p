"""File search services (local SQLite search, distributed peer search, and result merging)."""

from p2p.search.distributed_search import DistributedSearchReport, DistributedSearchService
from p2p.search.local_search import LocalSearchService
from p2p.search.result_merger import (
    MergedSearchResult,
    PeerFileSource,
    PeerQueryStatus,
    PeerSearchResult,
    merge_search_results,
)

__all__ = [
    "DistributedSearchReport",
    "DistributedSearchService",
    "LocalSearchService",
    "MergedSearchResult",
    "PeerFileSource",
    "PeerQueryStatus",
    "PeerSearchResult",
    "merge_search_results",
]
