"""Integration tests for Milestone 6 local SQLite search and TCP SEARCH_REQUEST handling."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.indexing.file_index import FileIndexer
from p2p.network.protocol import SearchRequestMessage, SearchResponseMessage
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import TcpServer
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id


@pytest.mark.integration
async def test_local_search_service_and_tcp_search_query(tmp_path: Path) -> None:
    share_dir = tmp_path / "shared"
    sub_dir = share_dir / "lectures"
    sub_dir.mkdir(parents=True)
    (sub_dir / "Computer Networks Notes.pdf").write_bytes(b"%PDF-1.4 sample")
    (sub_dir / "networking_lab_1.py").write_bytes(b"print('hello')")
    (sub_dir / "100%_coverage.txt").write_bytes(b"escaped")

    server_id = generate_peer_id()
    client_id = generate_peer_id()

    with Database(tmp_path / "server.db") as db:
        indexer = FileIndexer(db, shared_directories=[share_dir])
        summary = await indexer.rebuild_async()
        assert summary.added == 3

        local_search = LocalSearchService(db, peer_id=server_id)

        # Direct local partial, exact, case-insensitive, escaped, and empty searches
        partial, trunc = await local_search.search_async("NETWORK")
        assert trunc is False
        assert {m.file_name for m in partial} == {
            "Computer Networks Notes.pdf",
            "networking_lab_1.py",
        }

        exact, _ = await local_search.search_async("NETWORKING_LAB_1.PY", exact=True)
        assert [m.file_name for m in exact] == ["networking_lab_1.py"]

        escaped, _ = await local_search.search_async("100%")
        assert [m.file_name for m in escaped] == ["100%_coverage.txt"]

        empty, _ = await local_search.search_async("nonexistent-query")
        assert empty == []

        capped, is_trunc = await local_search.search_async("network", max_results=1)
        assert len(capped) == 1
        assert is_trunc is True

        # Remote TCP peer sending SEARCH_REQUEST -> receives real SEARCH_RESPONSE
        upload = UploadService(
            db,
            peer_id=server_id,
            shared_directories=[share_dir],
            search_service=local_search,
        )
        server = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=server_id,
            handler=upload.serve_connection,
        )
        client = TcpClient(peer_id=client_id)

        async with server:
            upload.set_tcp_port(server.bound_port)
            decoded = await client.request(
                "127.0.0.1",
                server.bound_port,
                SearchRequestMessage(peer_id=client_id, query="Networks Notes"),
            )
            assert isinstance(decoded.payload, SearchResponseMessage)
            assert decoded.payload.peer_id == server_id
            assert len(decoded.payload.results) == 1
            rec = decoded.payload.results[0]
            assert rec.file_name == "Computer Networks Notes.pdf"
            assert rec.file_size == len(b"%PDF-1.4 sample")
            assert len(rec.sha256) == 64
