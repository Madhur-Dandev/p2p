"""Binary message framing codec for TCP streams and UDP datagrams (26-byte header)."""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass

from p2p.constants import (
    DEFAULT_CONTROL_TIMEOUT_SEC,
    FLAG_END_OF_STREAM,
    FLAG_NONE,
    HEADER_FORMAT,
    HEADER_SIZE,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
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
from p2p.network.protocol import MessageType
from p2p.utils.identifiers import generate_request_id

_HEADER_STRUCT = struct.Struct(HEADER_FORMAT)


@dataclass(frozen=True, slots=True)
class FrameHeader:
    """Fixed 26-byte big-endian frame header defined in docs/protocol.md."""

    magic: bytes
    version: int
    msg_type: MessageType
    flags: int
    request_id: bytes
    payload_len: int


@dataclass(frozen=True, slots=True)
class Frame:
    """Complete framed wire message pairing a validated header and raw payload bytes."""

    header: FrameHeader
    payload: bytes


def max_payload_for_type(
    msg_type: MessageType,
    *,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
) -> int:
    """Return the maximum permitted payload length in bytes for msg_type."""
    if msg_type == MessageType.FILE_DATA:
        return min(max_data_bytes, MAX_FRAME_PAYLOAD_BYTES)
    return min(max_control_bytes, MAX_FRAME_PAYLOAD_BYTES)


def validate_header_fields(
    *,
    magic: bytes,
    version: int,
    raw_msg_type: int,
    flags: int,
    request_id: bytes,
    payload_len: int,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
) -> FrameHeader:
    """Validate raw unpacked header fields prior to allocating or reading payload bytes."""
    if magic != PROTOCOL_MAGIC:
        raise FramingError(f"Invalid protocol magic: {magic!r}")
    if version != PROTOCOL_VERSION:
        raise UnsupportedVersionError(f"Unsupported protocol version: {version}")

    try:
        msg_type = MessageType(raw_msg_type)
    except ValueError as exc:
        raise MalformedMessageError(f"Unknown message type: {raw_msg_type:#x}") from exc

    if not isinstance(request_id, bytes) or len(request_id) != 16:
        raise FramingError("request_id must be exactly 16 bytes")

    allowed_flags = FLAG_END_OF_STREAM if msg_type == MessageType.FILE_DATA else FLAG_NONE
    if flags & ~allowed_flags:
        raise FramingError(f"Invalid flags {flags:#06x} for message type {msg_type.name}")

    if msg_type != MessageType.FILE_DATA and payload_len == 0:
        raise FramingError(f"Control frame {msg_type.name} must not have empty payload")

    limit = max_payload_for_type(
        msg_type,
        max_control_bytes=max_control_bytes,
        max_data_bytes=max_data_bytes,
    )
    if payload_len > limit:
        raise ResourceLimitError(
            f"{msg_type.name} payload length {payload_len} bytes exceeds limit {limit} bytes"
        )

    return FrameHeader(
        magic=magic,
        version=version,
        msg_type=msg_type,
        flags=flags,
        request_id=request_id,
        payload_len=payload_len,
    )


def unpack_header(
    header_bytes: bytes,
    *,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
) -> FrameHeader:
    """Unpack and validate a 26-byte binary header."""
    if len(header_bytes) != HEADER_SIZE:
        raise FramingError(f"Expected {HEADER_SIZE}-byte header, got {len(header_bytes)} bytes")
    magic, version, raw_msg_type, flags, request_id, payload_len = _HEADER_STRUCT.unpack(
        header_bytes
    )
    return validate_header_fields(
        magic=magic,
        version=version,
        raw_msg_type=raw_msg_type,
        flags=flags,
        request_id=request_id,
        payload_len=payload_len,
        max_control_bytes=max_control_bytes,
        max_data_bytes=max_data_bytes,
    )


def pack_frame(
    msg_type: MessageType,
    payload: bytes,
    *,
    request_id: bytes | None = None,
    flags: int = FLAG_NONE,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
) -> bytes:
    """Validate fields and encode a complete 26-byte header + payload frame."""
    req_id = generate_request_id() if request_id is None else request_id
    validate_header_fields(
        magic=PROTOCOL_MAGIC,
        version=PROTOCOL_VERSION,
        raw_msg_type=int(msg_type),
        flags=flags,
        request_id=req_id,
        payload_len=len(payload),
        max_control_bytes=max_control_bytes,
        max_data_bytes=max_data_bytes,
    )
    header_bytes = _HEADER_STRUCT.pack(
        PROTOCOL_MAGIC,
        PROTOCOL_VERSION,
        int(msg_type),
        flags,
        req_id,
        len(payload),
    )
    return header_bytes + payload


def unpack_datagram_frame(
    data: bytes,
    *,
    max_bytes: int = MAX_UDP_DATAGRAM_BYTES,
) -> Frame:
    """Decode and validate a complete single-datagram frame."""
    if len(data) > max_bytes:
        raise ResourceLimitError(
            f"UDP datagram size {len(data)} bytes exceeds limit {max_bytes} bytes"
        )
    if len(data) < HEADER_SIZE:
        raise FramingError(
            f"UDP datagram shorter than {HEADER_SIZE}-byte header ({len(data)} bytes)"
        )
    header = unpack_header(
        data[:HEADER_SIZE],
        max_control_bytes=max_bytes - HEADER_SIZE,
        max_data_bytes=max_bytes - HEADER_SIZE,
    )
    body = data[HEADER_SIZE:]
    if header.payload_len != len(body):
        raise FramingError(
            f"Header payload_len={header.payload_len} mismatches datagram body={len(body)}"
        )
    return Frame(header=header, payload=body)


async def write_frame(
    writer: asyncio.StreamWriter,
    frame_bytes: bytes,
    *,
    timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
) -> None:
    """Write pre-packed frame bytes to an asyncio StreamWriter and await drain."""
    writer.write(frame_bytes)
    await asyncio.wait_for(writer.drain(), timeout=timeout_sec)


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
    timeout_sec: float | None = DEFAULT_CONTROL_TIMEOUT_SEC,
) -> Frame | None:
    """Read and validate one complete frame from an asyncio StreamReader.

    Returns None only on clean EOF at a frame boundary; raises FramingError,
    UnsupportedVersionError, MalformedMessageError, ResourceLimitError, or TimeoutError
    on partial reads or invalid frames.
    """
    try:
        hdr_coro = reader.readexactly(HEADER_SIZE)
        header_bytes = (
            await asyncio.wait_for(hdr_coro, timeout=timeout_sec)
            if timeout_sec is not None
            else await hdr_coro
        )
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise FramingError(
            f"Truncated {HEADER_SIZE}-byte header ({len(exc.partial)} bytes before EOF)"
        ) from exc

    header = unpack_header(
        header_bytes,
        max_control_bytes=max_control_bytes,
        max_data_bytes=max_data_bytes,
    )
    if header.payload_len == 0:
        return Frame(header=header, payload=b"")

    try:
        body_coro = reader.readexactly(header.payload_len)
        payload = (
            await asyncio.wait_for(body_coro, timeout=timeout_sec)
            if timeout_sec is not None
            else await body_coro
        )
    except asyncio.IncompleteReadError as exc:
        raise FramingError(
            f"Truncated payload for {header.msg_type.name}: "
            f"expected {header.payload_len} bytes, got {len(exc.partial)} before EOF"
        ) from exc

    return Frame(header=header, payload=payload)
