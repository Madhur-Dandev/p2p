"""Integration tests for Milestone 7 distributed multi-peer search & bounded timeouts."""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path

import pytest

from p2p.indexing.file_index import FileIndexer
from p2p.network.peer_registry import PeerRegistry, PeerStatus
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import PeerAddress, TcpServer
from p2p.search.distributed_search import DistributedSearchService
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id
from p2p.utils.time_utils import monotonic_now


def _unused_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.mark.integration
async def test_distributed_search_fans_out_merges_replicas_and_isolates_slow_or_dead_peers(
    tmp_path: Path,
) -> None:
    dir_a = tmp_path / "share_a"
    dir_b = tmp_path / "share_b"
    dir_local = tmp_path / "share_local"
    for d in (dir_a, dir_b, dir_local):
        d.mkdir()

    shared_replica = b"replicated textbook bytes"
    (dir_a / "distributed_systems.pdf").write_bytes(shared_replica)
    (dir_a / "unique_alpha.txt").write_bytes(b"only-on-a")
    # Peer B hosts the exact same content (same SHA-256 + size) and a distinct guide
    (dir_b / "distributed_systems.pdf").write_bytes(shared_replica)
    (dir_b / "systems_lab.md").write_bytes(b"only-on-b")
    (dir_local / "local_systems.txt").write_bytes(b"hosted-locally")

    peer_a_id = generate_peer_id()
    peer_b_id = generate_peer_id()
    peer_slow_id = generate_peer_id()
    peer_dead_id = generate_peer_id()
    client_id = generate_peer_id()

    async def hung_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: PeerAddress,
    ) -> None:
        _ = reader, writer, peer
        await asyncio.sleep(5.0)

    with (
        Database(tmp_path / "a.db") as db_a,
        Database(tmp_path / "b.db") as db_b,
        Database(tmp_path / "c.db") as db_client,
    ):
        await FileIndexer(db_a, shared_directories=[dir_a]).rebuild_async()
        await FileIndexer(db_b, shared_directories=[dir_b]).rebuild_async()
        await FileIndexer(db_client, shared_directories=[dir_local]).rebuild_async()

        upload_a = UploadService(
            db_a,
            peer_id=peer_a_id,
            shared_directories=[dir_a],
            search_service=LocalSearchService(db_a, peer_id=peer_a_id),
        )
        upload_b = UploadService(
            db_b,
            peer_id=peer_b_id,
            shared_directories=[dir_b],
            search_service=LocalSearchService(db_b, peer_id=peer_b_id),
        )

        srv_a = TcpServer(
            host="127.0.0.1", port=0, peer_id=peer_a_id, handler=upload_a.serve_connection
        )
        srv_b = TcpServer(
            host="127.0.0.1", port=0, peer_id=peer_b_id, handler=upload_b.serve_connection
        )
        srv_slow = TcpServer(host="127.0.0.1", port=0, peer_id=peer_slow_id, handler=hung_handler)

        dead_port = _unused_tcp_port()

        async with srv_a, srv_b, srv_slow:
            registry = PeerRegistry(
                local_peer_id=client_id,
                stale_timeout_sec=30.0,
                remove_timeout_sec=120.0,
            )
            registry.register_or_update(
                peer_id=peer_a_id,
                ip_address="127.0.0.1",
                tcp_port=srv_a.bound_port,
                display_name="node-a",
            )
            registry.register_or_update(
                peer_id=peer_b_id,
                ip_address="127.0.0.1",
                tcp_port=srv_b.bound_port,
                display_name="node-b",
            )
            registry.register_or_update(
                peer_id=peer_slow_id,
                ip_address="127.0.0.1",
                tcp_port=srv_slow.bound_port,
                display_name="slow-node",
            )
            registry.register_or_update(
                peer_id=peer_dead_id,
                ip_address="127.0.0.1",
                tcp_port=dead_port,
                display_name="dead-node",
            )

            tcp_client = TcpClient(
                peer_id=client_id, connect_timeout_sec=1.0, control_timeout_sec=2.0
            )
            dist_search = DistributedSearchService(
                local_peer_id=client_id,
                tcp_client=tcp_client,
                registry=registry,
                local_search=LocalSearchService(db_client, peer_id=client_id),
                display_name="client",
                search_timeout_sec=0.45,
            )

            started = monotonic_now()
            report = await dist_search.search("systems", include_local=False)
            elapsed = monotonic_now() - started

            # Bounded cleanly close to search_timeout_sec despite hung_handler sleeping 5s
            assert elapsed < 1.5
            assert report.peers_queried == 4
            assert report.peers_succeeded == 2
            assert report.peers_failed == 2

            names = {item.file_name: item for item in report.results}
            assert set(names.keys()) == {"distributed_systems.pdf", "systems_lab.md"}
            # Identical PDF hosted on Node A and Node B merged into one entry with 2 sources
            replicated = names["distributed_systems.pdf"]
            assert replicated.source_count == 2
            assert {s.display_name for s in replicated.sources} == {"node-a", "node-b"}
            assert dist_search.resolve_file_id(replicated.primary_file_id)

            # Unresponsive slow/dead peers were downgraded to STALE cleanly
            slow_info = registry.get_peer(peer_slow_id)
            dead_info = registry.get_peer(peer_dead_id)
            assert slow_info is not None and slow_info.status == PeerStatus.STALE
            assert dead_info is not None and dead_info.status == PeerStatus.STALE

            # Including local index picks up local_systems.txt without counting local as remote peer
            with_local = await dist_search.search(
                "systems", include_local=True, include_stale=False
            )
            assert with_local.peers_queried == 2
            assert with_local.peers_succeeded == 2
            assert any(r.file_name == "local_systems.txt" for r in with_local.results)
