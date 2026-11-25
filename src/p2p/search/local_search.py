"""Local SQLite file search service (Stage 1 of distributed search)."""

from __future__ import annotations

from p2p.constants import MAX_QUERY_LENGTH, MAX_SEARCH_RESULTS_PER_PEER
from p2p.indexing.file_metadata import IndexedFile
from p2p.network.protocol import (
    SearchRequestMessage,
    SearchResponseMessage,
    SearchResultRecord,
    validate_search_query,
)
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository


class LocalSearchService:
    """Queries the local node's SQLite shared-file index."""

    def __init__(
        self,
        db: Database,
        *,
        peer_id: str,
        max_query_length: int = MAX_QUERY_LENGTH,
    ) -> None:
        self._db = db
        self._repo = FileIndexRepository(db)
        self._peer_id = peer_id
        self._max_query_length = max_query_length

    def search(
        self,
        query: str,
        *,
        exact: bool = False,
        max_results: int = MAX_SEARCH_RESULTS_PER_PEER,
    ) -> tuple[list[IndexedFile], bool]:
        """Search local shared files and return (matches, truncated)."""
        clean_query = validate_search_query(query, max_length=self._max_query_length)
        clamped_limit = max(1, min(max_results, MAX_SEARCH_RESULTS_PER_PEER))
        rows = self._repo.search_by_name(
            clean_query,
            exact=exact,
            limit=clamped_limit + 1,
        )
        truncated = len(rows) > clamped_limit
        return rows[:clamped_limit], truncated

    async def search_async(
        self,
        query: str,
        *,
        exact: bool = False,
        max_results: int = MAX_SEARCH_RESULTS_PER_PEER,
    ) -> tuple[list[IndexedFile], bool]:
        """Run a local SQLite search off the asyncio event loop."""
        return await self._db.run(lambda: self.search(query, exact=exact, max_results=max_results))

    async def handle_search_request(
        self,
        request: SearchRequestMessage,
    ) -> SearchResponseMessage:
        """Build a protocol SEARCH_RESPONSE for an incoming SEARCH_REQUEST."""
        matches, truncated = await self.search_async(
            request.query,
            exact=request.exact,
            max_results=request.max_results,
        )
        records = tuple(
            SearchResultRecord(
                file_id=item.file_id,
                file_name=item.file_name,
                file_size=item.file_size,
                sha256=item.sha256,
                indexed_at=item.indexed_at,
            )
            for item in matches
        )
        return SearchResponseMessage(
            peer_id=self._peer_id,
            results=records,
            truncated=truncated,
        )
