"""Unit tests for PeerRegistry deduplication, stale transitions, and validation."""

from __future__ import annotations

import pytest

from p2p.exceptions import ValidationError
from p2p.network.peer_registry import PeerRegistry, PeerStatus
from p2p.utils.identifiers import generate_peer_id


class FakeClock:
    """Deterministic monotonic clock for testing timeout transitions."""

    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.mark.unit
def test_register_deduplicates_updates_and_ignores_self() -> None:
    local_id = generate_peer_id()
    remote_id = generate_peer_id()
    clock = FakeClock()
    registry = PeerRegistry(
        local_peer_id=local_id,
        stale_timeout_sec=10.0,
        remove_timeout_sec=30.0,
        clock=clock,
    )

    assert (
        registry.register_or_update(
            peer_id=local_id,
            ip_address="127.0.0.1",
            tcp_port=9000,
        )
        is None
    )
    assert len(registry) == 0

    first = registry.register_or_update(
        peer_id=remote_id,
        ip_address="192.168.1.20",
        tcp_port=9001,
        display_name="peer-a",
        capabilities=["search_v1"],
    )
    assert first is not None
    info1, is_new1, prev1 = first
    assert is_new1 is True
    assert prev1 is None
    assert info1.status == PeerStatus.ACTIVE
    first_seen = info1.first_seen_utc

    clock.advance(4.0)
    second = registry.register_or_update(
        peer_id=remote_id,
        ip_address="192.168.1.25",
        tcp_port=9002,
        display_name="peer-a-renamed",
    )
    assert second is not None
    info2, is_new2, prev2 = second
    assert is_new2 is False
    assert prev2 == PeerStatus.ACTIVE
    assert len(registry) == 1
    assert info2.ip_address == "192.168.1.25"
    assert info2.tcp_port == 9002
    assert info2.first_seen_utc == first_seen


@pytest.mark.unit
def test_stale_recovery_and_permanent_removal_lifecycle() -> None:
    clock = FakeClock(initial=1000.0)
    registry = PeerRegistry(
        local_peer_id=generate_peer_id(),
        stale_timeout_sec=15.0,
        remove_timeout_sec=45.0,
        clock=clock,
    )
    peer_id = generate_peer_id()
    registry.register_or_update(peer_id=peer_id, ip_address="10.0.0.5", tcp_port=9000)

    # Not stale yet at 14s
    clock.advance(14.0)
    assert registry.refresh_statuses() == []
    assert len(registry.active_peers()) == 1

    # Crosses 15s -> becomes STALE
    clock.advance(2.0)
    transitions = registry.refresh_statuses()
    assert len(transitions) == 1
    assert transitions[0][1] == PeerStatus.ACTIVE
    assert transitions[0][2] == PeerStatus.STALE
    assert registry.active_peers() == []
    assert len(registry.list_peers(include_stale=True)) == 1

    # Heartbeat arrives at 20s -> recovers to ACTIVE
    clock.advance(4.0)
    res = registry.register_or_update(peer_id=peer_id, ip_address="10.0.0.5", tcp_port=9000)
    assert res is not None
    assert res[1] is False
    assert res[2] == PeerStatus.STALE
    assert len(registry.active_peers()) == 1

    # Silence for 46s -> purged as REMOVED
    clock.advance(46.0)
    purged = registry.refresh_statuses()
    assert len(purged) == 1
    assert purged[0][2] == PeerStatus.REMOVED
    assert len(registry) == 0
    assert registry.get_peer(peer_id) is None


@pytest.mark.unit
def test_peer_registry_rejects_invalid_inputs() -> None:
    with pytest.raises(ValidationError):
        PeerRegistry(local_peer_id="bad-uuid")
    with pytest.raises(ValidationError):
        PeerRegistry(
            local_peer_id=generate_peer_id(), stale_timeout_sec=20.0, remove_timeout_sec=10.0
        )

    registry = PeerRegistry(local_peer_id=generate_peer_id())
    remote_id = generate_peer_id()

    with pytest.raises(ValidationError):
        registry.register_or_update(peer_id=remote_id, ip_address="not-an-ip", tcp_port=9000)
    with pytest.raises(ValidationError):
        registry.register_or_update(peer_id=remote_id, ip_address="127.0.0.1", tcp_port=0)
    with pytest.raises(ValidationError):
        registry.register_or_update(
            peer_id=remote_id,
            ip_address="127.0.0.1",
            tcp_port=True,  # type: ignore[arg-type]
        )
