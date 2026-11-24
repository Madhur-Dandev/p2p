"""In-memory peer registry tracking discovered and static peers with liveness states."""

from __future__ import annotations

import ipaddress
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum

from p2p.constants import (
    DEFAULT_PEER_REMOVE_TIMEOUT_SEC,
    DEFAULT_PEER_STALE_TIMEOUT_SEC,
    PROTOCOL_VERSION,
)
from p2p.exceptions import ValidationError
from p2p.utils.identifiers import validate_peer_id
from p2p.utils.time_utils import monotonic_now, utc_now_iso


class PeerStatus(StrEnum):
    """Availability lifecycle states for a known peer."""

    ACTIVE = "active"
    STALE = "stale"
    REMOVED = "removed"


@dataclass(slots=True)
class PeerInfo:
    """Tracked metadata for a discovered or statically configured peer."""

    peer_id: str
    ip_address: str
    tcp_port: int
    udp_port: int | None
    display_name: str
    protocol_version: int
    capabilities: tuple[str, ...]
    first_seen_utc: str
    last_seen_utc: str
    last_seen_monotonic: float
    status: PeerStatus
    source: str = "udp"


PeerTransition = tuple[PeerInfo, PeerStatus, PeerStatus]


def _validate_ip_address(raw_ip: str) -> str:
    try:
        parsed = ipaddress.ip_address(raw_ip.strip())
    except ValueError as exc:
        raise ValidationError(f"Invalid peer IP address: {raw_ip!r}") from exc
    return str(parsed)


def _validate_tcp_port(port: int) -> int:
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65_535):
        raise ValidationError(f"Invalid peer TCP port: {port!r}")
    return port


class PeerRegistry:
    """Tracks peer identities, endpoints, last-seen timestamps, and stale transitions."""

    def __init__(
        self,
        *,
        local_peer_id: str,
        stale_timeout_sec: float = DEFAULT_PEER_STALE_TIMEOUT_SEC,
        remove_timeout_sec: float = DEFAULT_PEER_REMOVE_TIMEOUT_SEC,
        clock: Callable[[], float] = monotonic_now,
    ) -> None:
        self._local_peer_id = validate_peer_id(local_peer_id)
        if stale_timeout_sec <= 0:
            raise ValidationError("stale_timeout_sec must be > 0")
        if remove_timeout_sec < stale_timeout_sec:
            raise ValidationError("remove_timeout_sec must be >= stale_timeout_sec")

        self._stale_timeout_sec = stale_timeout_sec
        self._remove_timeout_sec = remove_timeout_sec
        self._clock = clock
        self._peers: dict[str, PeerInfo] = {}

    @property
    def local_peer_id(self) -> str:
        """Return the owning node's peer ID (excluded from registration)."""
        return self._local_peer_id

    def __len__(self) -> int:
        return len(self._peers)

    def register_or_update(
        self,
        *,
        peer_id: str,
        ip_address: str,
        tcp_port: int,
        udp_port: int | None = None,
        display_name: str = "",
        protocol_version: int = PROTOCOL_VERSION,
        capabilities: Iterable[str] = (),
        source: str = "udp",
    ) -> tuple[PeerInfo, bool, PeerStatus | None] | None:
        """Register a new peer or refresh an existing peer's metadata and liveness.

        Returns None if peer_id matches the local node. Otherwise returns
        (peer_info, is_new, previous_status).
        """
        normalized_id = validate_peer_id(peer_id)
        if normalized_id == self._local_peer_id:
            return None

        normalized_ip = _validate_ip_address(ip_address)
        valid_tcp_port = _validate_tcp_port(tcp_port)
        if udp_port is not None and (
            isinstance(udp_port, bool)
            or not isinstance(udp_port, int)
            or not (1 <= udp_port <= 65_535)
        ):
            raise ValidationError(f"Invalid peer UDP port: {udp_port!r}")
        if protocol_version < 1:
            raise ValidationError(f"Invalid protocol_version: {protocol_version!r}")

        clean_name = display_name.strip()[:64] or normalized_id[:8]
        clean_caps = tuple(str(c).strip() for c in capabilities if str(c).strip())
        now_iso = utc_now_iso()
        now_mono = self._clock()

        existing = self._peers.get(normalized_id)
        if existing is None:
            info = PeerInfo(
                peer_id=normalized_id,
                ip_address=normalized_ip,
                tcp_port=valid_tcp_port,
                udp_port=udp_port,
                display_name=clean_name,
                protocol_version=protocol_version,
                capabilities=clean_caps,
                first_seen_utc=now_iso,
                last_seen_utc=now_iso,
                last_seen_monotonic=now_mono,
                status=PeerStatus.ACTIVE,
                source=source,
            )
            self._peers[normalized_id] = info
            return info, True, None

        prev_status = existing.status
        existing.ip_address = normalized_ip
        existing.tcp_port = valid_tcp_port
        if udp_port is not None:
            existing.udp_port = udp_port
        if display_name.strip():
            existing.display_name = clean_name
        existing.protocol_version = protocol_version
        if clean_caps:
            existing.capabilities = clean_caps
        existing.last_seen_utc = now_iso
        existing.last_seen_monotonic = now_mono
        existing.status = PeerStatus.ACTIVE
        existing.source = source
        return existing, False, prev_status

    def refresh_statuses(self) -> list[PeerTransition]:
        """Advance peer states to STALE or purge REMOVED peers based on elapsed silence."""
        now_mono = self._clock()
        transitions: list[PeerTransition] = []
        to_remove: list[str] = []

        for peer_id, peer in self._peers.items():
            elapsed = now_mono - peer.last_seen_monotonic
            if elapsed >= self._remove_timeout_sec:
                prev_status = peer.status
                peer.status = PeerStatus.REMOVED
                transitions.append((peer, prev_status, PeerStatus.REMOVED))
                to_remove.append(peer_id)
            elif elapsed >= self._stale_timeout_sec and peer.status != PeerStatus.STALE:
                prev_status = peer.status
                peer.status = PeerStatus.STALE
                transitions.append((peer, prev_status, PeerStatus.STALE))

        for peer_id in to_remove:
            self._peers.pop(peer_id, None)

        return transitions

    def mark_stale(self, peer_id: str) -> bool:
        """Explicitly mark a known peer STALE (for example after a failed TCP probe)."""
        normalized_id = validate_peer_id(peer_id)
        peer = self._peers.get(normalized_id)
        if peer is None or peer.status == PeerStatus.STALE:
            return False
        peer.status = PeerStatus.STALE
        return True

    def remove_peer(self, peer_id: str) -> bool:
        """Immediately remove a peer from the active registry."""
        normalized_id = validate_peer_id(peer_id)
        return self._peers.pop(normalized_id, None) is not None

    def get_peer(self, peer_id: str) -> PeerInfo | None:
        """Look up a peer by ID if currently tracked."""
        normalized_id = validate_peer_id(peer_id)
        return self._peers.get(normalized_id)

    def list_peers(self, *, include_stale: bool = True) -> list[PeerInfo]:
        """Return tracked peers sorted deterministically by peer_id."""
        peers = list(self._peers.values())
        if not include_stale:
            peers = [p for p in peers if p.status == PeerStatus.ACTIVE]
        return sorted(peers, key=lambda p: p.peer_id)

    def active_peers(self) -> list[PeerInfo]:
        """Return only currently ACTIVE peers."""
        return self.list_peers(include_stale=False)
