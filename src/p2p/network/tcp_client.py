"""Asynchronous TCP client and connection wrapper with request correlation."""

from __future__ import annotations

import asyncio

from p2p.constants import (
    DEFAULT_CONNECT_TIMEOUT_SEC,
    DEFAULT_CONTROL_TIMEOUT_SEC,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_OUTSTANDING_REQUESTS,
)
from p2p.exceptions import FramingError, MalformedMessageError, ValidationError
from p2p.logging_config import get_context_logger
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import (
    DecodedMessage,
    PingMessage,
    PongMessage,
    ProtocolPayload,
    RequestCorrelator,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.network.tcp_server import _safe_close_writer
from p2p.utils.identifiers import (
    generate_peer_id,
    generate_request_id,
    request_id_to_str,
    validate_peer_id,
)
from p2p.utils.time_utils import monotonic_now


class TcpConnection:
    """Managed async TCP stream speaking 26-byte binary protocol frames."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        host: str,
        port: int,
        max_payload_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
        max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES,
        control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
        max_outstanding_requests: int = MAX_OUTSTANDING_REQUESTS,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.host = host
        self.port = port
        self._max_payload_bytes = max_payload_bytes
        self._max_data_chunk_bytes = max_data_chunk_bytes
        self._control_timeout_sec = control_timeout_sec
        self._correlator = RequestCorrelator(max_outstanding=max_outstanding_requests)
        self._closed = False
        self.ignored_unmatched_frames = 0
        self._logger = get_context_logger("network.tcp_client")

    @property
    def is_closed(self) -> bool:
        """Return True once the underlying writer has been closed or marked closed."""
        return self._closed or self.writer.is_closing()

    @property
    def remote_address(self) -> tuple[str, int]:
        """Return resolved remote (ip, port) from socket peername, falling back to host/port."""
        peername = self.writer.get_extra_info("peername")
        if isinstance(peername, tuple) and len(peername) >= 2:
            return str(peername[0]), int(peername[1])
        return (self.host, self.port)

    async def __aenter__(self) -> TcpConnection:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def send_message(
        self,
        payload: ProtocolPayload,
        *,
        request_id: bytes | None = None,
        timeout_sec: float | None = None,
    ) -> bytes:
        """Encode and transmit one framed protocol message, returning its 16-byte request_id."""
        if self.is_closed:
            raise ConnectionError("Cannot write to a closed TCP connection")
        req_id = generate_request_id() if request_id is None else request_id
        effective_timeout = self._control_timeout_sec if timeout_sec is None else timeout_sec
        raw = encode_message(
            payload,
            request_id=req_id,
            max_control_bytes=self._max_payload_bytes,
            max_data_bytes=self._max_data_chunk_bytes,
        )
        await write_frame(self.writer, raw, timeout_sec=effective_timeout)
        return req_id

    async def recv_message(self, *, timeout_sec: float | None = None) -> DecodedMessage:
        """Read and decode one protocol message, raising FramingError on unexpected EOF."""
        effective_timeout = self._control_timeout_sec if timeout_sec is None else timeout_sec
        frame = await read_frame(
            self.reader,
            max_control_bytes=self._max_payload_bytes,
            max_data_bytes=self._max_data_chunk_bytes,
            timeout_sec=effective_timeout,
        )
        if frame is None:
            raise FramingError(
                f"Peer {self.host}:{self.port} closed TCP connection before sending a response"
            )
        return decode_message(frame)

    async def request_message(
        self,
        payload: ProtocolPayload,
        *,
        timeout_sec: float | None = None,
    ) -> DecodedMessage:
        """Send a request with a fresh request_id and await the correlated response."""
        effective_timeout = self._control_timeout_sec if timeout_sec is None else timeout_sec
        req_id = generate_request_id()
        future = self._correlator.register(req_id)
        deadline = monotonic_now() + effective_timeout

        try:
            await self.send_message(payload, request_id=req_id, timeout_sec=effective_timeout)
            while not future.done():
                remaining = deadline - monotonic_now()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Timed out waiting for correlated response {request_id_to_str(req_id)}"
                    )
                frame = await read_frame(
                    self.reader,
                    max_control_bytes=self._max_payload_bytes,
                    max_data_bytes=self._max_data_chunk_bytes,
                    timeout_sec=remaining,
                )
                if frame is None:
                    raise FramingError(
                        f"Peer {self.host}:{self.port} closed TCP connection before replying"
                    )
                if not self._correlator.resolve(frame):
                    self.ignored_unmatched_frames += 1
                    self._logger.warning(
                        "Discarded unmatched/late response frame %s with req=%s",
                        frame.header.msg_type.name,
                        request_id_to_str(frame.header.request_id),
                    )
            matched = await future
            return decode_message(matched)
        finally:
            self._correlator.cancel(req_id)

    async def close(self) -> None:
        """Close the TCP connection cleanly and fail any pending correlated requests."""
        if self._closed:
            return
        self._closed = True
        self._correlator.fail_all(ConnectionError("TCP connection closed"))
        await _safe_close_writer(self.writer)


class TcpClient:
    """Asyncio TCP client with bounded connection and control timeouts."""

    def __init__(
        self,
        *,
        peer_id: str | None = None,
        connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT_SEC,
        control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
        max_payload_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
        max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES,
    ) -> None:
        if connect_timeout_sec <= 0:
            raise ValidationError("connect_timeout_sec must be > 0")
        if control_timeout_sec <= 0:
            raise ValidationError("control_timeout_sec must be > 0")
        self._peer_id = validate_peer_id(peer_id) if peer_id is not None else generate_peer_id()
        self._connect_timeout_sec = connect_timeout_sec
        self._control_timeout_sec = control_timeout_sec
        self._max_payload_bytes = max_payload_bytes
        self._max_data_chunk_bytes = max_data_chunk_bytes
        self._logger = get_context_logger("network.tcp_client", peer_id=self._peer_id)

    async def connect(
        self,
        host: str,
        port: int,
        *,
        timeout_sec: float | None = None,
    ) -> TcpConnection:
        """Open an asynchronous TCP connection to (host, port) within connect_timeout_sec."""
        clean_host = host.strip()
        if not clean_host:
            raise ValidationError("Target host must not be empty")
        if not (1 <= port <= 65_535):
            raise ValidationError(f"Target port must be between 1 and 65535, got {port}")

        effective_timeout = self._connect_timeout_sec if timeout_sec is None else timeout_sec
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(clean_host, port),
            timeout=effective_timeout,
        )
        self._logger.debug("Opened TCP connection to %s:%d", clean_host, port)
        return TcpConnection(
            reader,
            writer,
            host=clean_host,
            port=port,
            max_payload_bytes=self._max_payload_bytes,
            max_data_chunk_bytes=self._max_data_chunk_bytes,
            control_timeout_sec=self._control_timeout_sec,
        )

    async def request(
        self,
        host: str,
        port: int,
        payload: ProtocolPayload,
        *,
        timeout_sec: float | None = None,
    ) -> DecodedMessage:
        """Connect, send one correlated request, await response, and close cleanly."""
        async with await self.connect(host, port, timeout_sec=timeout_sec) as conn:
            return await conn.request_message(payload, timeout_sec=timeout_sec)

    async def ping(
        self,
        host: str,
        port: int,
        *,
        sent_at_ms: int | None = None,
        timeout_sec: float | None = None,
    ) -> PongMessage:
        """Send a protocol PING frame and validate the correlated PONG response."""
        decoded = await self.request(
            host,
            port,
            PingMessage(peer_id=self._peer_id, sent_at_ms=sent_at_ms),
            timeout_sec=timeout_sec,
        )
        if not isinstance(decoded.payload, PongMessage):
            raise MalformedMessageError(f"Expected PONG response, got {decoded.msg_type.name}")
        return decoded.payload
