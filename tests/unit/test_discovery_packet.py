"""Unit tests for UDP discovery wire encoding, decoding, and validation."""

from __future__ import annotations

import json
import struct

import pytest

from p2p.constants import (
    HEADER_FORMAT,
    MAX_UDP_DATAGRAM_BYTES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
)
from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    ResourceLimitError,
    UnsupportedVersionError,
    ValidationError,
)
from p2p.network.discovery import (
    DiscoveryMessageType,
    decode_discovery_packet,
    encode_discovery_packet,
    parse_host_port,
)
from p2p.utils.identifiers import generate_peer_id, generate_request_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "msg_type",
    [
        DiscoveryMessageType.DISCOVER,
        DiscoveryMessageType.ANNOUNCE,
        DiscoveryMessageType.HEARTBEAT,
    ],
)
def test_encode_decode_round_trip(msg_type: DiscoveryMessageType) -> None:
    peer_id = generate_peer_id()
    req_id = generate_request_id()
    raw = encode_discovery_packet(
        msg_type=msg_type,
        peer_id=peer_id,
        tcp_port=9005,
        display_name="macbook-node",
        capabilities=["search_v1", "transfer_v1"],
        request_id=req_id,
    )
    assert len(raw) <= MAX_UDP_DATAGRAM_BYTES

    decoded = decode_discovery_packet(raw)
    assert decoded.msg_type == msg_type
    assert decoded.version == PROTOCOL_VERSION
    assert decoded.request_id == req_id
    assert decoded.peer_id == peer_id
    assert decoded.tcp_port == 9005
    assert decoded.display_name == "macbook-node"
    assert decoded.capabilities == ("search_v1", "transfer_v1")


@pytest.mark.unit
def test_decode_rejects_bad_headers_and_oversized_datagrams() -> None:
    req_id = generate_request_id()
    valid_body = json.dumps({"peer_id": generate_peer_id(), "tcp_port": 9000}).encode("utf-8")

    # Too short
    with pytest.raises(FramingError, match="shorter than"):
        decode_discovery_packet(b"P2\x01")

    # Wrong magic
    bad_magic = struct.pack(HEADER_FORMAT, b"XX", 1, 1, 0, req_id, len(valid_body)) + valid_body
    with pytest.raises(FramingError, match="magic"):
        decode_discovery_packet(bad_magic)

    # Unsupported version
    bad_ver = (
        struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 99, 1, 0, req_id, len(valid_body)) + valid_body
    )
    with pytest.raises(UnsupportedVersionError):
        decode_discovery_packet(bad_ver)

    # Unknown message type
    bad_type = (
        struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 0x20, 0, req_id, len(valid_body)) + valid_body
    )
    with pytest.raises(MalformedMessageError, match="message type"):
        decode_discovery_packet(bad_type)

    # Mismatched payload_len
    bad_len = (
        struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 1, 0, req_id, len(valid_body) + 5)
        + valid_body
    )
    with pytest.raises(FramingError, match="mismatches"):
        decode_discovery_packet(bad_len)

    # Oversized UDP packet
    with pytest.raises(ResourceLimitError, match="exceeds limit"):
        decode_discovery_packet(b"x" * (MAX_UDP_DATAGRAM_BYTES + 1))


@pytest.mark.unit
def test_decode_rejects_malformed_json_and_fields() -> None:
    req_id = generate_request_id()

    for bad_payload in (
        b"{bad-json",
        b"[1, 2]",
        json.dumps({"peer_id": "not-a-uuid", "tcp_port": 9000}).encode("utf-8"),
        json.dumps({"peer_id": generate_peer_id(), "tcp_port": True}).encode("utf-8"),
        json.dumps({"peer_id": generate_peer_id(), "tcp_port": 70000}).encode("utf-8"),
        json.dumps({"peer_id": generate_peer_id(), "tcp_port": 9000, "capabilities": "bad"}).encode(
            "utf-8"
        ),
    ):
        datagram = (
            struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 2, 0, req_id, len(bad_payload))
            + bad_payload
        )
        with pytest.raises((MalformedMessageError, ValidationError)):
            decode_discovery_packet(datagram)


@pytest.mark.unit
def test_parse_host_port_handles_ipv4_hostname_and_ipv6() -> None:
    assert parse_host_port("192.168.1.50:9000") == ("192.168.1.50", 9000)
    assert parse_host_port("peer2.local:9001") == ("peer2.local", 9001)
    assert parse_host_port("[::1]:9002") == ("::1", 9002)

    for bad in ("", "127.0.0.1", ":9000", "127.0.0.1:0", "127.0.0.1:70000", "[::1:9000"):
        with pytest.raises(ValidationError):
            parse_host_port(bad)
