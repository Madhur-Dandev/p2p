"""Framed JSON IPC wire helpers for local CLI <-> daemon communication."""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

from p2p.constants import MAX_FRAME_PAYLOAD_BYTES
from p2p.exceptions import (
    ConfigError,
    DaemonAlreadyRunningError,
    DaemonError,
    DaemonNotRunningError,
    FileNotFoundInIndexError,
    FramingError,
    IntegrityError,
    P2PError,
    PathValidationError,
    PeerNotFoundError,
    ResourceLimitError,
    StorageError,
    TransferCancelledError,
    TransferError,
    TransferTimeoutError,
    ValidationError,
)

IPC_MAGIC = b"PI"
IPC_VERSION = 1
_IPC_HEADER_STRUCT = struct.Struct("!2sBI")
_IPC_HEADER_SIZE = _IPC_HEADER_STRUCT.size

_ERROR_TYPE_MAP: dict[str, type[P2PError]] = {
    "ConfigError": ConfigError,
    "ValidationError": ValidationError,
    "PathValidationError": PathValidationError,
    "ResourceLimitError": ResourceLimitError,
    "StorageError": StorageError,
    "PeerNotFoundError": PeerNotFoundError,
    "FileNotFoundInIndexError": FileNotFoundInIndexError,
    "IntegrityError": IntegrityError,
    "TransferTimeoutError": TransferTimeoutError,
    "TransferCancelledError": TransferCancelledError,
    "TransferError": TransferError,
    "DaemonNotRunningError": DaemonNotRunningError,
    "DaemonAlreadyRunningError": DaemonAlreadyRunningError,
    "DaemonError": DaemonError,
}


def pack_ipc_message(
    payload: dict[str, Any],
    *,
    max_bytes: int = MAX_FRAME_PAYLOAD_BYTES,
) -> bytes:
    """Serialize a JSON IPC payload prefixed by a 7-byte big-endian length header."""
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if len(encoded) > max_bytes:
        raise ResourceLimitError(f"IPC message size {len(encoded)} exceeds limit {max_bytes}")
    header = _IPC_HEADER_STRUCT.pack(IPC_MAGIC, IPC_VERSION, len(encoded))
    return header + encoded


async def read_ipc_message(
    reader: asyncio.StreamReader,
    *,
    max_bytes: int = MAX_FRAME_PAYLOAD_BYTES,
    timeout_sec: float | None = None,
) -> dict[str, Any] | None:
    """Read one framed JSON IPC message; return None on clean EOF at frame boundary."""
    coro = reader.readexactly(_IPC_HEADER_SIZE)
    try:
        header_bytes = await (asyncio.wait_for(coro, timeout=timeout_sec) if timeout_sec else coro)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise FramingError(f"Truncated IPC header ({len(exc.partial)} bytes)") from exc

    magic, version, length = _IPC_HEADER_STRUCT.unpack(header_bytes)
    if magic != IPC_MAGIC:
        raise FramingError(f"Invalid IPC magic bytes: {magic!r}")
    if version != IPC_VERSION:
        raise FramingError(f"Unsupported IPC version: {version}")
    if length == 0 or length > max_bytes:
        raise ResourceLimitError(f"Invalid IPC payload length {length} (max {max_bytes})")

    body_coro = reader.readexactly(length)
    try:
        body_bytes = await (
            asyncio.wait_for(body_coro, timeout=timeout_sec) if timeout_sec else body_coro
        )
    except asyncio.IncompleteReadError as exc:
        raise FramingError("Truncated IPC message body") from exc

    try:
        decoded = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FramingError(f"Malformed IPC JSON body: {exc}") from exc
    if not isinstance(decoded, dict):
        raise FramingError("IPC payload must decode to a JSON object")
    return decoded


async def write_ipc_message(
    writer: asyncio.StreamWriter,
    payload: dict[str, Any],
    *,
    max_bytes: int = MAX_FRAME_PAYLOAD_BYTES,
    timeout_sec: float | None = None,
) -> None:
    """Serialize and flush a single IPC message frame to a StreamWriter."""
    raw = pack_ipc_message(payload, max_bytes=max_bytes)
    writer.write(raw)
    drain_coro = writer.drain()
    await (asyncio.wait_for(drain_coro, timeout=timeout_sec) if timeout_sec else drain_coro)


def exception_to_error_dict(exc: Exception) -> dict[str, str]:
    """Convert an exception to a wire-safe IPC error dictionary."""
    return {
        "type": type(exc).__name__,
        "message": str(exc) or type(exc).__name__,
    }


def error_dict_to_exception(error: dict[str, Any]) -> P2PError:
    """Reconstruct a domain exception from an IPC error payload."""
    err_type = str(error.get("type", "DaemonError"))
    message = str(error.get("message", "Daemon request failed"))
    exc_cls = _ERROR_TYPE_MAP.get(err_type, DaemonError)
    return exc_cls(message)
