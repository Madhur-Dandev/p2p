"""Unit tests for the 26-byte binary framing layer."""

from __future__ import annotations

import asyncio
import struct

import pytest

from p2p.constants import (
    FLAG_END_OF_STREAM,
    FLAG_NONE,
    HEADER_FORMAT,
    HEADER_SIZE,
    MAX_UDP_DATAGRAM_BYTES,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
)
from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    ResourceLimitError,
    UnsupportedVersionError,
)
from p2p.network.framing import (
    pack_frame,
    read_frame,
    unpack_datagram_frame,
    unpack_header,
)
from p2p.network.protocol import MessageType
from p2p.utils.identifiers import generate_request_id


def _feed_chunked(reader: asyncio.StreamReader, data: bytes, chunk_size: int) -> None:
    for idx in range(0, len(data), chunk_size):
        reader.feed_data(data[idx : idx + chunk_size])
    reader.feed_eof()


@pytest.mark.unit
async def test_pack_and_read_frame_handles_fragmented_tcp_streams() -> None:
    req1 = generate_request_id()
    req2 = generate_request_id()
    req3 = generate_request_id()

    raw1 = pack_frame(MessageType.PING, b'{"peer_id":"x"}', request_id=req1)
    raw2 = pack_frame(
        MessageType.FILE_DATA,
        b"\x00\x01\x02\xff",
        request_id=req2,
        flags=FLAG_NONE,
    )
    raw3 = pack_frame(
        MessageType.FILE_DATA,
        b"",
        request_id=req3,
        flags=FLAG_END_OF_STREAM,
    )

    reader = asyncio.StreamReader()
    # Feed 5 bytes at a time across 3 back-to-back frames
    _feed_chunked(reader, raw1 + raw2 + raw3, chunk_size=5)

    f1 = await read_frame(reader)
    f2 = await read_frame(reader)
    f3 = await read_frame(reader)
    f4 = await read_frame(reader)

    assert f1 is not None and f1.header.msg_type == MessageType.PING
    assert f1.header.request_id == req1
    assert f1.payload == b'{"peer_id":"x"}'

    assert f2 is not None and f2.header.msg_type == MessageType.FILE_DATA
    assert f2.header.flags == FLAG_NONE
    assert f2.payload == b"\x00\x01\x02\xff"

    assert f3 is not None and f3.header.msg_type == MessageType.FILE_DATA
    assert f3.header.flags == FLAG_END_OF_STREAM
    assert f3.payload == b""

    assert f4 is None


@pytest.mark.unit
def test_unpack_header_rejects_invalid_headers_and_flags() -> None:
    req_id = generate_request_id()
    assert HEADER_SIZE == 26

    # Bad magic
    with pytest.raises(FramingError, match="magic"):
        unpack_header(struct.pack(HEADER_FORMAT, b"XX", 1, 0x10, 0, req_id, 10))

    # Bad version
    with pytest.raises(UnsupportedVersionError):
        unpack_header(struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 2, 0x10, 0, req_id, 10))

    # Unknown message type
    with pytest.raises(MalformedMessageError, match="Unknown message type"):
        unpack_header(struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 0x99, 0, req_id, 10))

    # Disallowed flag on control frame
    with pytest.raises(FramingError, match="Invalid flags"):
        unpack_header(
            struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 0x10, FLAG_END_OF_STREAM, req_id, 10)
        )

    # Empty control frame
    with pytest.raises(FramingError, match="must not have empty payload"):
        unpack_header(struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 0x10, 0, req_id, 0))


@pytest.mark.unit
async def test_read_frame_rejects_oversized_and_truncated_streams_early() -> None:
    req_id = generate_request_id()

    # Feed ONLY the 26-byte header claiming 5 MiB payload; must reject immediately without body
    reader_big = asyncio.StreamReader()
    reader_big.feed_data(
        struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, PROTOCOL_VERSION, 0x10, 0, req_id, 5_000_000)
    )
    with pytest.raises(ResourceLimitError, match="exceeds limit"):
        await read_frame(reader_big, max_control_bytes=1024, timeout_sec=0.5)

    # Truncated header
    reader_hdr = asyncio.StreamReader()
    reader_hdr.feed_data(b"P2\x01\x10")
    reader_hdr.feed_eof()
    with pytest.raises(FramingError, match="Truncated 26-byte header"):
        await read_frame(reader_hdr)

    # Truncated payload
    reader_body = asyncio.StreamReader()
    reader_body.feed_data(
        struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, PROTOCOL_VERSION, 0x10, 0, req_id, 50) + b"abc"
    )
    reader_body.feed_eof()
    with pytest.raises(FramingError, match="Truncated payload"):
        await read_frame(reader_body)


@pytest.mark.unit
def test_unpack_datagram_frame_bounds() -> None:
    req_id = generate_request_id()
    raw = pack_frame(MessageType.DISCOVER, b'{"peer_id":"x"}', request_id=req_id)
    frame = unpack_datagram_frame(raw)
    assert frame.header.request_id == req_id

    with pytest.raises(ResourceLimitError):
        unpack_datagram_frame(b"x" * (MAX_UDP_DATAGRAM_BYTES + 1))

    with pytest.raises(FramingError, match="mismatches"):
        bad = struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, 1, 0x01, 0, req_id, 20) + b"{}"
        unpack_datagram_frame(bad)
