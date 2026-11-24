"""Decentralized UDP LAN peer discovery, heartbeats, and static-peer fallback."""

from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntEnum

from p2p.constants import (
    DEFAULT_BIND_HOST,
    DEFAULT_BROADCAST_ADDRESS,
    DEFAULT_DISCOVERY_PORT,
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    MAX_UDP_DATAGRAM_BYTES,
    PROTOCOL_VERSION,
)
from p2p.exceptions import (
    DiscoveryError,
    FramingError,
    MalformedMessageError,
    ResourceLimitError,
    UnsupportedVersionError,
    ValidationError,
)
from p2p.logging_config import get_context_logger
from p2p.network.framing import unpack_datagram_frame
from p2p.network.peer_registry import PeerInfo, PeerRegistry, PeerStatus
from p2p.network.protocol import (
    AnnounceMessage,
    DiscoverMessage,
    HeartbeatMessage,
    PingMessage,
    PongMessage,
    ProtocolPayload,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.network.tcp_client import TcpClient
from p2p.utils.identifiers import generate_request_id, request_id_to_str, validate_peer_id

_DEFAULT_CAPABILITIES: tuple[str, ...] = ("tcp_ping_v1",)


class DiscoveryMessageType(IntEnum):
    """UDP discovery wire message codes matching MessageType."""

    DISCOVER = 0x01
    ANNOUNCE = 0x02
    HEARTBEAT = 0x03


@dataclass(slots=True)
class DiscoveryPacket:
    """Validated UDP discovery message decoded from the wire."""

    msg_type: DiscoveryMessageType
    version: int
    request_id: bytes
    peer_id: str
    tcp_port: int
    display_name: str
    capabilities: tuple[str, ...]


PeerEventCallback = Callable[[str, PeerInfo], None]


def parse_host_port(entry: str) -> tuple[str, int]:
    """Parse a 'host:port' or '[ipv6]:port' endpoint string into (host, port)."""
    candidate = entry.strip()
    if not candidate:
        raise ValidationError("Endpoint entry must not be empty")
    if candidate.startswith("["):
        closing = candidate.find("]:")
        if closing == -1:
            raise ValidationError(f"Invalid IPv6 endpoint format: {entry!r}")
        host = candidate[1:closing].strip()
        port_raw = candidate[closing + 2 :]
    else:
        if ":" not in candidate:
            raise ValidationError(f"Invalid endpoint format (expected 'host:port'): {entry!r}")
        host, port_raw = candidate.rsplit(":", 1)
        host = host.strip()
    if not host:
        raise ValidationError(f"Missing host in endpoint: {entry!r}")
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValidationError(f"Invalid port in endpoint {entry!r}") from exc
    if not (1 <= port <= 65_535):
        raise ValidationError(f"Port must be between 1 and 65535, got {port}")
    return host, port


def encode_discovery_packet(
    *,
    msg_type: DiscoveryMessageType,
    peer_id: str,
    tcp_port: int,
    display_name: str = "",
    capabilities: Sequence[str] = _DEFAULT_CAPABILITIES,
    request_id: bytes | None = None,
) -> bytes:
    """Encode a discovery message using the 26-byte protocol header + JSON payload."""
    payload: ProtocolPayload
    if msg_type == DiscoveryMessageType.DISCOVER:
        payload = DiscoverMessage(
            peer_id=peer_id,
            tcp_port=tcp_port,
            display_name=display_name,
            capabilities=tuple(capabilities),
        )
    elif msg_type == DiscoveryMessageType.ANNOUNCE:
        payload = AnnounceMessage(
            peer_id=peer_id,
            tcp_port=tcp_port,
            display_name=display_name,
            capabilities=tuple(capabilities),
        )
    elif msg_type == DiscoveryMessageType.HEARTBEAT:
        payload = HeartbeatMessage(
            peer_id=peer_id,
            tcp_port=tcp_port,
            display_name=display_name,
            capabilities=tuple(capabilities),
        )
    else:
        raise ValidationError(f"Unsupported discovery message type: {msg_type!r}")

    encoded = encode_message(
        payload,
        request_id=request_id,
        max_control_bytes=MAX_UDP_DATAGRAM_BYTES,
    )
    if len(encoded) > MAX_UDP_DATAGRAM_BYTES:
        raise DiscoveryError(
            f"Discovery datagram size {len(encoded)} exceeds UDP limit {MAX_UDP_DATAGRAM_BYTES}"
        )
    return encoded


def decode_discovery_packet(data: bytes) -> DiscoveryPacket:
    """Decode and strictly validate a raw UDP discovery datagram."""
    frame = unpack_datagram_frame(data, max_bytes=MAX_UDP_DATAGRAM_BYTES)
    try:
        disc_type = DiscoveryMessageType(int(frame.header.msg_type))
    except ValueError as exc:
        raise MalformedMessageError(
            f"Invalid discovery message type: {int(frame.header.msg_type):#x}"
        ) from exc

    decoded = decode_message(frame)
    payload = decoded.payload
    if not isinstance(payload, (DiscoverMessage, AnnounceMessage, HeartbeatMessage)):
        raise MalformedMessageError(
            f"Unexpected payload type for discovery: {type(payload).__name__}"
        )

    return DiscoveryPacket(
        msg_type=disc_type,
        version=PROTOCOL_VERSION,
        request_id=decoded.request_id,
        peer_id=payload.peer_id,
        tcp_port=payload.tcp_port,
        display_name=payload.display_name,
        capabilities=payload.capabilities,
    )


class _DiscoveryProtocol(asyncio.DatagramProtocol):
    """Internal asyncio DatagramProtocol adapter forwarding packets to DiscoveryService."""

    def __init__(self, service: DiscoveryService) -> None:
        self._service = service

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self._service._handle_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        self._service._handle_socket_error(exc)


@dataclass(slots=True)
class DiscoveryStats:
    """Diagnostic counters for discovery traffic and validation errors."""

    sent_discover: int = 0
    sent_announce: int = 0
    sent_heartbeat: int = 0
    send_errors: int = 0
    received_valid: int = 0
    dropped_invalid: int = 0
    ignored_self: int = 0
    static_probes_ok: int = 0
    static_probes_failed: int = 0


class DiscoveryService:
    """Manages UDP discovery, periodic heartbeats, stale reaping, and static fallback."""

    def __init__(
        self,
        *,
        local_peer_id: str,
        display_name: str,
        tcp_port: int,
        registry: PeerRegistry,
        bind_host: str = DEFAULT_BIND_HOST,
        udp_port: int = DEFAULT_DISCOVERY_PORT,
        broadcast_address: str = DEFAULT_BROADCAST_ADDRESS,
        broadcast_enabled: bool = True,
        heartbeat_interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL_SEC,
        static_peers: Sequence[str] = (),
        discovery_targets: Sequence[tuple[str, int]] = (),
        tcp_client: TcpClient | None = None,
        on_event: PeerEventCallback | None = None,
    ) -> None:
        self._local_peer_id = validate_peer_id(local_peer_id)
        self._display_name = display_name.strip()[:64] or self._local_peer_id[:8]
        if isinstance(tcp_port, bool) or not (1 <= tcp_port <= 65_535):
            raise ValidationError(f"tcp_port must be in 1..65535, got {tcp_port!r}")
        if not (0 <= udp_port <= 65_535):
            raise ValidationError(f"udp_port must be in 0..65535, got {udp_port!r}")
        if heartbeat_interval_sec <= 0:
            raise ValidationError("heartbeat_interval_sec must be > 0")

        self._tcp_port = tcp_port
        self._registry = registry
        self._bind_host = bind_host
        self._udp_port = udp_port
        self._broadcast_address = broadcast_address
        self._broadcast_enabled = broadcast_enabled
        self._heartbeat_interval_sec = heartbeat_interval_sec
        self._static_endpoints: list[tuple[str, int]] = [
            parse_host_port(entry) for entry in static_peers
        ]
        self._discovery_targets: list[tuple[str, int]] = list(discovery_targets)
        self._tcp_client = (
            tcp_client if tcp_client is not None else TcpClient(peer_id=self._local_peer_id)
        )
        self._on_event = on_event

        self._transport: asyncio.DatagramTransport | None = None
        self._bound_port: int | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reaper_task: asyncio.Task[None] | None = None
        self._running = False
        self._stats = DiscoveryStats()
        self._logger = get_context_logger("network.discovery", peer_id=self._local_peer_id)

    @property
    def is_running(self) -> bool:
        """Return True while the UDP endpoint and background loops are active."""
        return self._running

    @property
    def bound_port(self) -> int:
        """Return the bound UDP port (resolves ephemeral port 0 after start)."""
        if self._bound_port is None:
            raise RuntimeError("DiscoveryService has not been started")
        return self._bound_port

    @property
    def stats(self) -> DiscoveryStats:
        """Return a snapshot of discovery counters."""
        return DiscoveryStats(
            sent_discover=self._stats.sent_discover,
            sent_announce=self._stats.sent_announce,
            sent_heartbeat=self._stats.sent_heartbeat,
            send_errors=self._stats.send_errors,
            received_valid=self._stats.received_valid,
            dropped_invalid=self._stats.dropped_invalid,
            ignored_self=self._stats.ignored_self,
            static_probes_ok=self._stats.static_probes_ok,
            static_probes_failed=self._stats.static_probes_failed,
        )

    async def __aenter__(self) -> DiscoveryService:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """Bind the UDP socket, emit initial discovery probes, and start maintenance loops."""
        if self._running:
            return

        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                with contextlib.suppress(OSError):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            if self._broadcast_enabled:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind((self._bind_host, self._udp_port))
            sock.setblocking(False)
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _DiscoveryProtocol(self),
                sock=sock,
            )
        except Exception:
            sock.close()
            raise

        self._transport = transport
        sockname = transport.get_extra_info("sockname")
        self._bound_port = int(sockname[1]) if isinstance(sockname, tuple) else self._udp_port
        self._running = True

        self._logger.info(
            "UDP discovery listening on %s:%d (broadcast=%s, target=%s, static_peers=%d)",
            self._bind_host,
            self._bound_port,
            self._broadcast_enabled,
            self._broadcast_address,
            len(self._static_endpoints),
        )

        # Initial discovery & static-peer probes so late joiners find peers immediately
        self.send_discover()
        self.send_announce()
        if self._static_endpoints:
            await self.probe_static_peers()

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="p2p-heartbeat")
        self._reaper_task = asyncio.create_task(self._reaper_loop(), name="p2p-reaper")

    async def stop(self) -> None:
        """Cancel background loops and close the UDP socket cleanly."""
        if not self._running:
            return
        self._running = False

        tasks = [t for t in (self._heartbeat_task, self._reaper_task) if t is not None]
        self._heartbeat_task = None
        self._reaper_task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self._transport is not None:
            self._transport.close()
            self._transport = None
        self._logger.info("UDP discovery stopped")

    def _targets(self) -> list[tuple[str, int]]:
        targets: list[tuple[str, int]] = []
        if self._broadcast_enabled:
            broadcast_port = self._udp_port if self._udp_port > 0 else self.bound_port
            targets.append((self._broadcast_address, broadcast_port))
        for target in self._discovery_targets:
            if target not in targets:
                targets.append(target)
        return targets

    def _send_to(self, datagram: bytes, target: tuple[str, int]) -> bool:
        if self._transport is None or self._transport.is_closing():
            return False
        try:
            self._transport.sendto(datagram, target)
            return True
        except OSError as exc:
            self._stats.send_errors += 1
            self._logger.warning(
                "Failed sending UDP packet to %s:%d: %s", target[0], target[1], exc
            )
            return False

    def send_discover(self, *, target: tuple[str, int] | None = None) -> bytes:
        """Broadcast or unicast a DISCOVER packet and return its 16-byte request_id."""
        req_id = generate_request_id()
        datagram = encode_discovery_packet(
            msg_type=DiscoveryMessageType.DISCOVER,
            peer_id=self._local_peer_id,
            tcp_port=self._tcp_port,
            display_name=self._display_name,
            request_id=req_id,
        )
        destinations = [target] if target is not None else self._targets()
        for dest in destinations:
            if self._send_to(datagram, dest):
                self._stats.sent_discover += 1
        return req_id

    def send_announce(
        self,
        *,
        target: tuple[str, int] | None = None,
        request_id: bytes | None = None,
    ) -> None:
        """Send an ANNOUNCE packet either to targets or as a unicast reply to DISCOVER."""
        datagram = encode_discovery_packet(
            msg_type=DiscoveryMessageType.ANNOUNCE,
            peer_id=self._local_peer_id,
            tcp_port=self._tcp_port,
            display_name=self._display_name,
            request_id=request_id,
        )
        destinations = [target] if target is not None else self._targets()
        for dest in destinations:
            if self._send_to(datagram, dest):
                self._stats.sent_announce += 1

    def send_heartbeat(self) -> None:
        """Emit periodic HEARTBEAT packets to configured discovery targets."""
        datagram = encode_discovery_packet(
            msg_type=DiscoveryMessageType.HEARTBEAT,
            peer_id=self._local_peer_id,
            tcp_port=self._tcp_port,
            display_name=self._display_name,
        )
        for dest in self._targets():
            if self._send_to(datagram, dest):
                self._stats.sent_heartbeat += 1

    async def probe_static_peers(self) -> None:
        """Probe configured static TCP peers when UDP broadcast is unavailable."""
        if not self._static_endpoints:
            return
        await asyncio.gather(
            *(self._probe_single_static_peer(host, port) for host, port in self._static_endpoints),
            return_exceptions=False,
        )

    async def _probe_single_static_peer(self, host: str, port: int) -> None:
        try:
            async with await self._tcp_client.connect(host, port) as conn:
                decoded = await conn.request_message(PingMessage(peer_id=self._local_peer_id))
                remote_ip, _ = conn.remote_address
        except (
            OSError,
            TimeoutError,
            FramingError,
            UnsupportedVersionError,
            MalformedMessageError,
            ResourceLimitError,
        ) as exc:
            self._stats.static_probes_failed += 1
            self._logger.debug("Static peer probe failed for %s:%d: %s", host, port, exc)
            return

        if not isinstance(decoded.payload, PongMessage):
            self._stats.static_probes_failed += 1
            self._logger.warning("Unexpected static probe response from %s:%d", host, port)
            return

        pong = decoded.payload
        try:
            result = self._registry.register_or_update(
                peer_id=pong.peer_id,
                ip_address=remote_ip,
                tcp_port=pong.tcp_port,
                display_name=pong.display_name,
                source="static",
            )
        except ValidationError as exc:
            self._stats.static_probes_failed += 1
            self._logger.warning("Rejected invalid static peer %s:%d: %s", host, port, exc)
            return

        self._stats.static_probes_ok += 1
        if result is not None:
            self._emit_registration_log(*result, req_id=decoded.request_id_str)

    def _handle_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        src_ip, src_port = str(addr[0]), int(addr[1])
        try:
            packet = decode_discovery_packet(data)
        except (
            FramingError,
            UnsupportedVersionError,
            MalformedMessageError,
            ResourceLimitError,
            ValidationError,
            DiscoveryError,
        ) as exc:
            self._stats.dropped_invalid += 1
            self._logger.warning(
                "Dropped malformed UDP packet from %s:%d: %s", src_ip, src_port, exc
            )
            return

        if packet.peer_id == self._local_peer_id:
            self._stats.ignored_self += 1
            return

        self._stats.received_valid += 1
        result = self._registry.register_or_update(
            peer_id=packet.peer_id,
            ip_address=src_ip,
            tcp_port=packet.tcp_port,
            udp_port=src_port,
            display_name=packet.display_name,
            protocol_version=packet.version,
            capabilities=packet.capabilities,
            source="udp",
        )
        if result is not None:
            self._emit_registration_log(*result, req_id=request_id_to_str(packet.request_id))

        if packet.msg_type == DiscoveryMessageType.DISCOVER:
            self.send_announce(target=(src_ip, src_port), request_id=packet.request_id)

    def _emit_registration_log(
        self,
        peer: PeerInfo,
        is_new: bool,
        prev_status: PeerStatus | None,
        *,
        req_id: str | None = None,
    ) -> None:
        peer_logger = get_context_logger(
            "network.discovery",
            peer_id=peer.peer_id,
            request_id=req_id,
        )
        if is_new:
            peer_logger.info(
                "Discovered peer %s at %s:%d via %s",
                peer.display_name,
                peer.ip_address,
                peer.tcp_port,
                peer.source,
            )
            if self._on_event is not None:
                self._on_event("discovered", peer)
        elif prev_status == PeerStatus.STALE:
            peer_logger.info(
                "Peer %s recovered to ACTIVE (%s:%d)",
                peer.display_name,
                peer.ip_address,
                peer.tcp_port,
            )
            if self._on_event is not None:
                self._on_event("recovered", peer)

    def _handle_socket_error(self, exc: Exception) -> None:
        self._stats.send_errors += 1
        self._logger.debug("UDP transport error received: %s", exc)

    async def _heartbeat_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self._heartbeat_interval_sec)
                if not self._running:
                    break
                self.send_heartbeat()
                if self._static_endpoints:
                    await self.probe_static_peers()
        except asyncio.CancelledError:
            raise

    async def _reaper_loop(self) -> None:
        interval = max(0.05, min(self._heartbeat_interval_sec / 2.0, 1.0))
        try:
            while self._running:
                await asyncio.sleep(interval)
                if not self._running:
                    break
                transitions = self._registry.refresh_statuses()
                for peer, prev_status, new_status in transitions:
                    peer_logger = get_context_logger("network.discovery", peer_id=peer.peer_id)
                    if new_status == PeerStatus.STALE:
                        peer_logger.warning(
                            "Peer %s marked STALE (%s -> %s, last_seen=%s)",
                            peer.display_name,
                            prev_status,
                            new_status,
                            peer.last_seen_utc,
                        )
                        if self._on_event is not None:
                            self._on_event("stale", peer)
                    elif new_status == PeerStatus.REMOVED:
                        peer_logger.info(
                            "Peer %s purged from active registry (%s -> %s)",
                            peer.display_name,
                            prev_status,
                            new_status,
                        )
                        if self._on_event is not None:
                            self._on_event("removed", peer)
        except asyncio.CancelledError:
            raise
