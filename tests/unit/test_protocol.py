"""Unit tests for protocol schemas, validation, serialization, and RequestCorrelator."""

from __future__ import annotations

import asyncio

import pytest

from p2p.constants import FLAG_END_OF_STREAM, HEADER_SIZE
from p2p.exceptions import MalformedMessageError, ProtocolError, ResourceLimitError, ValidationError
from p2p.network.framing import Frame, FrameHeader, pack_frame, unpack_header
from p2p.network.protocol import (
    AnnounceMessage,
    DiscoverMessage,
    FileDataMessage,
    FileInfoMessage,
    GetFileMessage,
    HeartbeatMessage,
    MessageType,
    PingMessage,
    PongMessage,
    ProtocolPayload,
    RequestCorrelator,
    SearchRequestMessage,
    SearchResponseMessage,
    SearchResultRecord,
    TransferErrorMessage,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.utils.identifiers import generate_peer_id, generate_request_id

_SAMPLE_HASH = "a" * 64


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        DiscoverMessage(peer_id=generate_peer_id(), tcp_port=9000, display_name="peer-a"),
        AnnounceMessage(
            peer_id=generate_peer_id(),
            tcp_port=9001,
            display_name="peer-b",
            capabilities=("search_v1", "resume_v1"),
            timestamp="2026-11-25T00:00:00Z",
        ),
        HeartbeatMessage(peer_id=generate_peer_id(), tcp_port=9002, shared_file_count=42),
        PingMessage(peer_id=generate_peer_id(), sent_at_ms=123456),
        PongMessage(peer_id=generate_peer_id(), tcp_port=9000, display_name="peer-a"),
        SearchRequestMessage(
            peer_id=generate_peer_id(),
            query="distributed systems notes",
            max_results=25,
            exact=True,
        ),
        SearchResponseMessage(
            peer_id=generate_peer_id(),
            results=(
                SearchResultRecord(
                    file_id="file_001-abc",
                    file_name="notes.pdf",
                    file_size=1024,
                    sha256=_SAMPLE_HASH,
                    indexed_at="2026-11-25T00:00:00Z",
                ),
            ),
            truncated=False,
        ),
        GetFileMessage(
            peer_id=generate_peer_id(),
            file_id="file_001-abc",
            offset=512,
            expected_sha256=_SAMPLE_HASH,
            chunk_size=65536,
        ),
        FileInfoMessage(
            file_id="file_001-abc",
            file_name="notes.pdf",
            file_size=1024,
            sha256=_SAMPLE_HASH,
            offset=512,
            chunk_size=65536,
        ),
        FileDataMessage(data=b"\x00\x01\x02binary-chunk", end_of_stream=True),
        TransferErrorMessage(code="FILE_NOT_FOUND", message="No such file ID", retryable=False),
    ],
)
def test_all_protocol_messages_round_trip(payload: ProtocolPayload) -> None:
    req_id = generate_request_id()
    raw = encode_message(payload, request_id=req_id)
    header = unpack_header(raw[:HEADER_SIZE])
    frame = Frame(header=header, payload=raw[HEADER_SIZE:])
    decoded = decode_message(frame)

    assert decoded.request_id == req_id
    assert decoded.payload == payload


@pytest.mark.unit
def test_protocol_validation_rejects_unsafe_fields() -> None:
    pid = generate_peer_id()

    with pytest.raises(ValidationError):
        PingMessage(peer_id="invalid-uuid")

    with pytest.raises(MalformedMessageError):
        PongMessage(peer_id=pid, tcp_port=0)

    for bad_file_id in ("", "../etc/passwd", "dir/file", "a.b"):
        with pytest.raises(MalformedMessageError):
            GetFileMessage(peer_id=pid, file_id=bad_file_id)

    for bad_name in ("", "../secret.txt", "a/b.txt", "a\\b.txt", "..", "bad\x00.txt"):
        with pytest.raises(MalformedMessageError):
            FileInfoMessage(
                file_id="valid_id",
                file_name=bad_name,
                file_size=10,
                sha256=_SAMPLE_HASH,
                offset=0,
                chunk_size=4096,
            )

    with pytest.raises(MalformedMessageError, match="offset"):
        FileInfoMessage(
            file_id="valid_id",
            file_name="ok.txt",
            file_size=100,
            sha256=_SAMPLE_HASH,
            offset=101,
            chunk_size=4096,
        )

    for bad_query in ("", "   ", "bad\x00query", "x" * 300):
        with pytest.raises(MalformedMessageError):
            SearchRequestMessage(peer_id=pid, query=bad_query)

    with pytest.raises(MalformedMessageError):
        SearchResultRecord(
            file_id="valid_id",
            file_name="ok.txt",
            file_size=10,
            sha256="not-a-sha256",
            indexed_at="2026-11-25T00:00:00Z",
        )


@pytest.mark.unit
def test_decode_message_rejects_malformed_json_and_missing_keys() -> None:
    req_id = generate_request_id()
    for bad_body in (b"{bad-json", b"[1, 2]", b"{}", b'{"peer_id": 123}'):
        raw = pack_frame(MessageType.PING, bad_body, request_id=req_id)
        header = unpack_header(raw[:HEADER_SIZE])
        frame = Frame(header=header, payload=raw[HEADER_SIZE:])
        with pytest.raises(MalformedMessageError):
            decode_message(frame)


@pytest.mark.unit
async def test_request_correlator_matches_rejects_stale_and_enforces_limits() -> None:
    correlator = RequestCorrelator(max_outstanding=2)
    req1 = generate_request_id()
    req2 = generate_request_id()
    req3 = generate_request_id()
    unrelated_req = generate_request_id()

    fut1 = correlator.register(req1)
    assert correlator.outstanding_count == 1

    # Duplicate active request_id rejected
    with pytest.raises(ProtocolError, match="Duplicate"):
        correlator.register(req1)

    fut2 = correlator.register(req2)
    # Capacity limit enforced
    with pytest.raises(ResourceLimitError):
        correlator.register(req3)

    # Unrelated/late request_id does not resolve fut1 or fut2
    unrelated_frame = Frame(
        header=FrameHeader(b"P2", 1, MessageType.PONG, 0, unrelated_req, 2),
        payload=b"{}",
    )
    assert correlator.resolve(unrelated_frame) is False
    assert not fut1.done()

    # Matching request_id resolves fut1 cleanly; duplicate delivery returns False
    good_frame = Frame(
        header=FrameHeader(b"P2", 1, MessageType.PONG, 0, req1, 2),
        payload=b"{}",
    )
    assert correlator.resolve(good_frame) is True
    assert await fut1 == good_frame
    assert correlator.resolve(good_frame) is False

    # Timeout cleans up req2 so late reply afterwards is discarded
    with pytest.raises(TimeoutError):
        await correlator.await_response(req2, fut2, timeout_sec=0.05)
    assert correlator.outstanding_count == 0
    late_frame2 = Frame(
        header=FrameHeader(b"P2", 1, MessageType.PONG, 0, req2, 2),
        payload=b"{}",
    )
    assert correlator.resolve(late_frame2) is False


@pytest.mark.unit
async def test_request_correlator_fail_all_unblocks_waiters() -> None:
    correlator = RequestCorrelator()
    req = generate_request_id()
    fut = correlator.register(req)

    count = correlator.fail_all(ConnectionError("socket reset"))
    assert count == 1
    assert correlator.outstanding_count == 0
    with pytest.raises(ConnectionError, match="socket reset"):
        await asyncio.wait_for(fut, timeout=0.2)

    # Verify FILE_DATA flag mapping
    empty_eof = encode_message(FileDataMessage(data=b"", end_of_stream=True), request_id=req)
    hdr = unpack_header(empty_eof[:HEADER_SIZE])
    assert hdr.flags == FLAG_END_OF_STREAM
