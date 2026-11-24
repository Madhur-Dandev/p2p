"""Authoritative wire protocol types, validated message schemas, and request correlation."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any

from p2p.constants import (
    MAX_FILE_SIZE_BYTES,
    MAX_FILENAME_LENGTH,
    MAX_OUTSTANDING_REQUESTS,
    MAX_QUERY_LENGTH,
    MAX_SEARCH_RESULTS_PER_PEER,
)
from p2p.exceptions import MalformedMessageError, ProtocolError, ResourceLimitError, ValidationError
from p2p.utils.identifiers import request_id_to_str, validate_peer_id

if TYPE_CHECKING:
    from p2p.network.framing import Frame

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_ERROR_CODE_RE = re.compile(r"^[A-Z0-9_]{1,64}$")


class MessageType(IntEnum):
    """Numeric message type codes matching docs/protocol.md."""

    DISCOVER = 0x01
    ANNOUNCE = 0x02
    HEARTBEAT = 0x03

    PING = 0x10
    PONG = 0x11

    SEARCH_REQUEST = 0x20
    SEARCH_RESPONSE = 0x21

    GET_FILE = 0x30
    FILE_INFO = 0x31
    FILE_DATA = 0x32
    TRANSFER_ERROR = 0x3F


CONTROL_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {
        MessageType.DISCOVER,
        MessageType.ANNOUNCE,
        MessageType.HEARTBEAT,
        MessageType.PING,
        MessageType.PONG,
        MessageType.SEARCH_REQUEST,
        MessageType.SEARCH_RESPONSE,
        MessageType.GET_FILE,
        MessageType.FILE_INFO,
        MessageType.TRANSFER_ERROR,
    }
)


def validate_port(value: Any, field_name: str = "tcp_port") -> int:
    """Validate an integer TCP/UDP port in 1..65535 (rejecting booleans)."""
    if isinstance(value, bool) or not isinstance(value, int) or not (1 <= value <= 65_535):
        raise MalformedMessageError(f"Invalid {field_name}: expected 1..65535, got {value!r}")
    return value


def validate_non_negative_int(value: Any, field_name: str) -> int:
    """Validate a non-negative integer (rejecting booleans)."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MalformedMessageError(f"Invalid {field_name}: expected integer >= 0, got {value!r}")
    return value


def validate_positive_int(value: Any, field_name: str) -> int:
    """Validate a positive integer (rejecting booleans)."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MalformedMessageError(f"Invalid {field_name}: expected integer > 0, got {value!r}")
    return value


def validate_sha256_hex(value: Any, field_name: str = "sha256") -> str:
    """Validate a 64-character lowercase hexadecimal SHA-256 digest."""
    if not isinstance(value, str):
        raise MalformedMessageError(f"Invalid {field_name}: expected string")
    normalized = value.strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise MalformedMessageError(f"Invalid {field_name}: expected 64 hex characters")
    return normalized


def validate_file_id(value: Any, field_name: str = "file_id") -> str:
    """Validate an opaque file identifier (alphanumeric, '_' or '-', no path separators)."""
    if not isinstance(value, str) or not _FILE_ID_RE.fullmatch(value):
        raise MalformedMessageError(
            f"Invalid {field_name}: expected 1-128 characters matching [A-Za-z0-9_-]"
        )
    return value


def validate_file_name(value: Any, *, max_length: int = MAX_FILENAME_LENGTH) -> str:
    """Validate a plain filename containing no directory traversal or NUL bytes."""
    if not isinstance(value, str):
        raise MalformedMessageError("Invalid file_name: expected string")
    if not value or len(value) > max_length:
        raise MalformedMessageError(f"Invalid file_name length: must be 1..{max_length}")
    if "\x00" in value or "/" in value or "\\" in value or value in {".", ".."}:
        raise MalformedMessageError(f"Unsafe file_name rejected: {value!r}")
    return value


def validate_search_query(value: Any, *, max_length: int = MAX_QUERY_LENGTH) -> str:
    """Validate a non-empty search query string bounded by max_length."""
    if not isinstance(value, str):
        raise MalformedMessageError("Invalid query: expected string")
    if "\x00" in value:
        raise MalformedMessageError("Search query must not contain NUL bytes")
    trimmed = value.strip()
    if not trimmed or len(trimmed) > max_length:
        raise MalformedMessageError(f"Search query length must be 1..{max_length}")
    return trimmed


def validate_capabilities(value: Any) -> tuple[str, ...]:
    """Validate an optional capabilities string sequence."""
    if not isinstance(value, (list, tuple)) or not all(isinstance(c, str) for c in value):
        raise MalformedMessageError("capabilities must be a list of strings")
    return tuple(c.strip()[:32] for c in value[:16] if c.strip())


def validate_display_name(value: Any) -> str:
    """Validate an optional peer display name string."""
    if not isinstance(value, str):
        raise MalformedMessageError("display_name must be a string")
    return value.strip().replace("\x00", "")[:64]


@dataclass(frozen=True, slots=True)
class DiscoverMessage:
    """UDP discovery solicitation payload (0x01)."""

    peer_id: str
    tcp_port: int
    display_name: str = ""
    capabilities: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "tcp_port", validate_port(self.tcp_port, "tcp_port"))
        object.__setattr__(self, "display_name", validate_display_name(self.display_name))
        object.__setattr__(self, "capabilities", validate_capabilities(self.capabilities))


@dataclass(frozen=True, slots=True)
class AnnounceMessage:
    """UDP peer announcement payload (0x02)."""

    peer_id: str
    tcp_port: int
    display_name: str = ""
    capabilities: tuple[str, ...] = ()
    timestamp: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "tcp_port", validate_port(self.tcp_port, "tcp_port"))
        object.__setattr__(self, "display_name", validate_display_name(self.display_name))
        object.__setattr__(self, "capabilities", validate_capabilities(self.capabilities))
        if not isinstance(self.timestamp, str):
            raise MalformedMessageError("timestamp must be a string")


@dataclass(frozen=True, slots=True)
class HeartbeatMessage:
    """UDP periodic liveness heartbeat payload (0x03)."""

    peer_id: str
    tcp_port: int
    display_name: str = ""
    capabilities: tuple[str, ...] = ()
    shared_file_count: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "tcp_port", validate_port(self.tcp_port, "tcp_port"))
        object.__setattr__(self, "display_name", validate_display_name(self.display_name))
        object.__setattr__(self, "capabilities", validate_capabilities(self.capabilities))
        if self.shared_file_count is not None:
            object.__setattr__(
                self,
                "shared_file_count",
                validate_non_negative_int(self.shared_file_count, "shared_file_count"),
            )


@dataclass(frozen=True, slots=True)
class PingMessage:
    """TCP reachability check request (0x10)."""

    peer_id: str
    sent_at_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        if self.sent_at_ms is not None:
            object.__setattr__(
                self, "sent_at_ms", validate_non_negative_int(self.sent_at_ms, "sent_at_ms")
            )


@dataclass(frozen=True, slots=True)
class PongMessage:
    """TCP reachability check response (0x11)."""

    peer_id: str
    tcp_port: int
    status: str = "ok"
    display_name: str = ""
    sent_at_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "tcp_port", validate_port(self.tcp_port, "tcp_port"))
        if not isinstance(self.status, str) or not self.status.strip():
            raise MalformedMessageError("status must be a non-empty string")
        object.__setattr__(self, "display_name", validate_display_name(self.display_name))
        if self.sent_at_ms is not None:
            object.__setattr__(
                self, "sent_at_ms", validate_non_negative_int(self.sent_at_ms, "sent_at_ms")
            )


@dataclass(frozen=True, slots=True)
class SearchRequestMessage:
    """TCP file search query payload (0x20)."""

    peer_id: str
    query: str
    max_results: int = MAX_SEARCH_RESULTS_PER_PEER
    exact: bool = False
    hop_count: int = 0
    ttl: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "query", validate_search_query(self.query))
        max_res = validate_positive_int(self.max_results, "max_results")
        object.__setattr__(self, "max_results", min(max_res, MAX_SEARCH_RESULTS_PER_PEER))
        if not isinstance(self.exact, bool):
            raise MalformedMessageError("exact must be a boolean")
        object.__setattr__(
            self, "hop_count", validate_non_negative_int(self.hop_count, "hop_count")
        )
        object.__setattr__(self, "ttl", validate_positive_int(self.ttl, "ttl"))


@dataclass(frozen=True, slots=True)
class SearchResultRecord:
    """Single file metadata record inside SEARCH_RESPONSE."""

    file_id: str
    file_name: str
    file_size: int
    sha256: str
    indexed_at: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "file_id", validate_file_id(self.file_id))
        object.__setattr__(self, "file_name", validate_file_name(self.file_name))
        object.__setattr__(
            self, "file_size", validate_non_negative_int(self.file_size, "file_size")
        )
        object.__setattr__(self, "sha256", validate_sha256_hex(self.sha256))
        if not isinstance(self.indexed_at, str) or not self.indexed_at.strip():
            raise MalformedMessageError("indexed_at must be a non-empty timestamp string")


@dataclass(frozen=True, slots=True)
class SearchResponseMessage:
    """TCP file search response payload (0x21)."""

    peer_id: str
    results: tuple[SearchResultRecord, ...] = ()
    truncated: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        if len(self.results) > MAX_SEARCH_RESULTS_PER_PEER:
            raise MalformedMessageError(
                f"Too many search results: {len(self.results)} > {MAX_SEARCH_RESULTS_PER_PEER}"
            )
        if not isinstance(self.truncated, bool):
            raise MalformedMessageError("truncated must be a boolean")


@dataclass(frozen=True, slots=True)
class GetFileMessage:
    """TCP direct transfer or resume request payload (0x30)."""

    peer_id: str
    file_id: str
    offset: int = 0
    expected_sha256: str | None = None
    chunk_size: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        object.__setattr__(self, "file_id", validate_file_id(self.file_id))
        object.__setattr__(self, "offset", validate_non_negative_int(self.offset, "offset"))
        if self.expected_sha256 is not None:
            object.__setattr__(
                self,
                "expected_sha256",
                validate_sha256_hex(self.expected_sha256, "expected_sha256"),
            )
        if self.chunk_size is not None:
            object.__setattr__(
                self, "chunk_size", validate_positive_int(self.chunk_size, "chunk_size")
            )


@dataclass(frozen=True, slots=True)
class FileInfoMessage:
    """TCP pre-stream metadata confirmation payload (0x31)."""

    file_id: str
    file_name: str
    file_size: int
    sha256: str
    offset: int
    chunk_size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "file_id", validate_file_id(self.file_id))
        object.__setattr__(self, "file_name", validate_file_name(self.file_name))
        size = validate_non_negative_int(self.file_size, "file_size")
        if MAX_FILE_SIZE_BYTES > 0 and size > MAX_FILE_SIZE_BYTES:
            raise ResourceLimitError(f"file_size {size} exceeds maximum {MAX_FILE_SIZE_BYTES}")
        object.__setattr__(self, "file_size", size)
        object.__setattr__(self, "sha256", validate_sha256_hex(self.sha256))
        offset = validate_non_negative_int(self.offset, "offset")
        if offset > size:
            raise MalformedMessageError(f"offset {offset} exceeds file_size {size}")
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "chunk_size", validate_positive_int(self.chunk_size, "chunk_size"))


@dataclass(frozen=True, slots=True)
class FileDataMessage:
    """TCP raw binary file data chunk payload (0x32)."""

    data: bytes
    end_of_stream: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise MalformedMessageError("FILE_DATA data must be bytes")
        if not isinstance(self.end_of_stream, bool):
            raise MalformedMessageError("end_of_stream must be a boolean")


@dataclass(frozen=True, slots=True)
class TransferErrorMessage:
    """TCP structured error response payload (0x3F)."""

    code: str
    message: str
    retryable: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not _ERROR_CODE_RE.fullmatch(self.code):
            raise MalformedMessageError(f"Invalid error code: {self.code!r}")
        if not isinstance(self.message, str) or not self.message.strip():
            raise MalformedMessageError("Error message must be a non-empty string")
        object.__setattr__(self, "message", self.message.strip()[:512])
        if not isinstance(self.retryable, bool):
            raise MalformedMessageError("retryable must be a boolean")


ProtocolPayload = (
    DiscoverMessage
    | AnnounceMessage
    | HeartbeatMessage
    | PingMessage
    | PongMessage
    | SearchRequestMessage
    | SearchResponseMessage
    | GetFileMessage
    | FileInfoMessage
    | FileDataMessage
    | TransferErrorMessage
)

MESSAGE_TYPE_FOR_PAYLOAD: dict[type[ProtocolPayload], MessageType] = {
    DiscoverMessage: MessageType.DISCOVER,
    AnnounceMessage: MessageType.ANNOUNCE,
    HeartbeatMessage: MessageType.HEARTBEAT,
    PingMessage: MessageType.PING,
    PongMessage: MessageType.PONG,
    SearchRequestMessage: MessageType.SEARCH_REQUEST,
    SearchResponseMessage: MessageType.SEARCH_RESPONSE,
    GetFileMessage: MessageType.GET_FILE,
    FileInfoMessage: MessageType.FILE_INFO,
    FileDataMessage: MessageType.FILE_DATA,
    TransferErrorMessage: MessageType.TRANSFER_ERROR,
}


@dataclass(frozen=True, slots=True)
class DecodedMessage:
    """Decoded wire frame combining protocol header metadata with validated payload."""

    msg_type: MessageType
    request_id: bytes
    flags: int
    payload: ProtocolPayload

    @property
    def request_id_str(self) -> str:
        """Return canonical UUID string representation of request_id."""
        return request_id_to_str(self.request_id)


class RequestCorrelator:
    """Tracks bounded in-flight requests by 16-byte request_id and rejects stale replies."""

    def __init__(self, *, max_outstanding: int = MAX_OUTSTANDING_REQUESTS) -> None:
        if max_outstanding <= 0:
            raise ValidationError("max_outstanding must be > 0")
        self._max_outstanding = max_outstanding
        self._pending: dict[bytes, asyncio.Future[Frame]] = {}

    @property
    def outstanding_count(self) -> int:
        """Return number of currently awaited requests."""
        return len(self._pending)

    def register(self, request_id: bytes) -> asyncio.Future[Frame]:
        """Register an in-flight request_id and return a Future resolved on matching response."""
        if not isinstance(request_id, bytes) or len(request_id) != 16:
            raise ValidationError("request_id must be exactly 16 bytes")
        if request_id in self._pending:
            raise ProtocolError(
                f"Duplicate outstanding request_id: {request_id_to_str(request_id)}"
            )
        if len(self._pending) >= self._max_outstanding:
            raise ResourceLimitError(
                f"Outstanding request limit ({self._max_outstanding}) exceeded"
            )
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        return future

    def resolve(self, frame: Frame) -> bool:
        """Resolve matching pending future; return False if unknown, duplicate, or expired."""
        req_id = frame.header.request_id
        future = self._pending.get(req_id)
        if future is None or future.done():
            return False
        self._pending.pop(req_id, None)
        future.set_result(frame)
        return True

    def cancel(self, request_id: bytes) -> bool:
        """Remove and cancel an outstanding request cleanly."""
        future = self._pending.pop(request_id, None)
        if future is None:
            return False
        if not future.done():
            future.cancel()
        return True

    def fail_all(self, exc: BaseException) -> int:
        """Fail all pending requests (for example on unexpected socket closure)."""
        count = 0
        for req_id in list(self._pending.keys()):
            future = self._pending.pop(req_id)
            if not future.done():
                future.set_exception(exc)
                count += 1
        return count

    async def await_response(
        self,
        request_id: bytes,
        future: asyncio.Future[Frame],
        *,
        timeout_sec: float,
    ) -> Frame:
        """Await a registered request future within timeout_sec and always clean up."""
        try:
            return await asyncio.wait_for(future, timeout=timeout_sec)
        finally:
            self._pending.pop(request_id, None)
