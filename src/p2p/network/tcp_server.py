"""Asynchronous TCP server for P2P control, search, and transfer connections."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from p2p.constants import (
    DEFAULT_BIND_HOST,
    DEFAULT_CONTROL_TIMEOUT_SEC,
    DEFAULT_SHUTDOWN_TIMEOUT_SEC,
    DEFAULT_TCP_PORT,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_SIMULTANEOUS_CONNECTIONS,
)
from p2p.exceptions import (
    FramingError,
    MalformedMessageError,
    ResourceLimitError,
    UnsupportedVersionError,
)
from p2p.logging_config import get_context_logger
from p2p.network.framing import read_frame, write_frame
from p2p.network.protocol import (
    DecodedMessage,
    PingMessage,
    PongMessage,
    ProtocolPayload,
    TransferErrorMessage,
)
from p2p.network.serialization import decode_message, encode_message
from p2p.utils.identifiers import generate_peer_id, validate_peer_id

PeerAddress = tuple[str, int]
TcpConnectionHandler = Callable[
    [asyncio.StreamReader, asyncio.StreamWriter, PeerAddress],
    Awaitable[None],
]
ControlMessageHandler = Callable[
    [DecodedMessage, PeerAddress],
    Awaitable[ProtocolPayload | None],
]


@dataclass(slots=True)
class TcpServerStats:
    """Runtime counters exposed for diagnostics and testing."""

    total_accepted: int = 0
    rejected_over_limit: int = 0
    active_connections: int = 0
    total_messages: int = 0
    total_errors: int = 0


def _format_peer_addr(writer: asyncio.StreamWriter) -> PeerAddress:
    peername = writer.get_extra_info("peername")
    if isinstance(peername, tuple) and len(peername) >= 2:
        return str(peername[0]), int(peername[1])
    return ("unknown", 0)


async def _safe_close_writer(writer: asyncio.StreamWriter) -> None:
    """Close an asyncio StreamWriter safely even if the remote peer already reset."""
    try:
        if not writer.is_closing():
            writer.close()
        await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
    except (OSError, TimeoutError, ConnectionError, asyncio.CancelledError):
        pass


class TcpServer:
    """Asyncio TCP server with bounded concurrency, protocol framing, and graceful shutdown."""

    def __init__(
        self,
        *,
        host: str = DEFAULT_BIND_HOST,
        port: int = DEFAULT_TCP_PORT,
        max_connections: int = MAX_SIMULTANEOUS_CONNECTIONS,
        shutdown_timeout_sec: float = DEFAULT_SHUTDOWN_TIMEOUT_SEC,
        control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC,
        max_payload_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
        max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES,
        peer_id: str | None = None,
        display_name: str = "p2p-node",
        handler: TcpConnectionHandler | None = None,
        message_handler: ControlMessageHandler | None = None,
    ) -> None:
        if not (0 <= port <= 65_535):
            raise ValueError(f"port must be between 0 and 65535, got {port}")
        if max_connections < 1:
            raise ValueError(f"max_connections must be >= 1, got {max_connections}")
        if shutdown_timeout_sec <= 0:
            raise ValueError(f"shutdown_timeout_sec must be > 0, got {shutdown_timeout_sec}")

        self._host = host
        self._port = port
        self._max_connections = max_connections
        self._shutdown_timeout_sec = shutdown_timeout_sec
        self._control_timeout_sec = control_timeout_sec
        self._max_payload_bytes = max_payload_bytes
        self._max_data_chunk_bytes = max_data_chunk_bytes
        self._peer_id = validate_peer_id(peer_id) if peer_id is not None else generate_peer_id()
        self._display_name = display_name.strip()[:64] or self._peer_id[:8]
        self._custom_handler = handler
        self._message_handler = message_handler

        self._server: asyncio.Server | None = None
        self._bound_port: int | None = None
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._closing = False
        self._stats = TcpServerStats()
        self._logger = get_context_logger("network.tcp_server", peer_id=self._peer_id)

    @property
    def is_running(self) -> bool:
        """Return True while the listening socket is open and accepting connections."""
        return self._server is not None and self._server.is_serving()

    @property
    def bound_port(self) -> int:
        """Return the active TCP port (resolves ephemeral port 0 after start)."""
        if self._bound_port is None:
            raise RuntimeError("TcpServer has not been started")
        return self._bound_port

    @property
    def stats(self) -> TcpServerStats:
        """Return a snapshot of server connection counters."""
        return TcpServerStats(
            total_accepted=self._stats.total_accepted,
            rejected_over_limit=self._stats.rejected_over_limit,
            active_connections=len(self._active_tasks),
            total_messages=self._stats.total_messages,
            total_errors=self._stats.total_errors,
        )

    async def __aenter__(self) -> TcpServer:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """Bind the listening socket and begin accepting connections."""
        if self._server is not None:
            return
        self._closing = False
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._host,
            port=self._port,
            reuse_address=True,
        )
        if self._server.sockets:
            sockname = self._server.sockets[0].getsockname()
            self._bound_port = int(sockname[1])
        else:
            self._bound_port = self._port
        self._logger.info(
            "TCP server listening on %s:%d (max_connections=%d)",
            self._host,
            self._bound_port,
            self._max_connections,
        )

    async def stop(self) -> None:
        """Stop accepting connections, drain/cancel active handlers, and close server."""
        if self._server is None:
            return
        self._closing = True
        self._server.close()

        pending = list(self._active_tasks)
        if pending:
            done, still_pending = await asyncio.wait(
                pending, timeout=min(self._shutdown_timeout_sec, 0.5)
            )
            _ = done
            for task in still_pending:
                task.cancel()
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)

        await self._server.wait_closed()
        self._server = None
        self._logger.info("TCP server stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer_addr = _format_peer_addr(writer)
        if self._closing or len(self._active_tasks) >= self._max_connections:
            self._stats.rejected_over_limit += 1
            self._logger.warning(
                "Rejecting TCP connection from %s:%d (closing=%s active=%d/%d)",
                peer_addr[0],
                peer_addr[1],
                self._closing,
                len(self._active_tasks),
                self._max_connections,
            )
            await _safe_close_writer(writer)
            return

        current_task = asyncio.current_task()
        if current_task is not None:
            self._active_tasks.add(current_task)
        self._stats.total_accepted += 1
        self._stats.active_connections = len(self._active_tasks)
        self._logger.debug("Accepted TCP connection from %s:%d", peer_addr[0], peer_addr[1])

        try:
            if self._custom_handler is not None:
                await self._custom_handler(reader, writer, peer_addr)
            else:
                await self._control_loop(reader, writer, peer_addr)
        except asyncio.CancelledError:
            self._logger.debug("Connection handler cancelled for %s:%d", peer_addr[0], peer_addr[1])
            raise
        except Exception:
            self._stats.total_errors += 1
            self._logger.exception(
                "Unhandled error in TCP connection handler for %s:%d",
                peer_addr[0],
                peer_addr[1],
            )
        finally:
            if current_task is not None:
                self._active_tasks.discard(current_task)
            self._stats.active_connections = len(self._active_tasks)
            await _safe_close_writer(writer)
            self._logger.debug("Closed TCP connection from %s:%d", peer_addr[0], peer_addr[1])

    async def _control_loop(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer_addr: PeerAddress,
    ) -> None:
        """Read 26-byte framed protocol messages and dispatch correlated replies."""
        while not self._closing:
            try:
                frame = await read_frame(
                    reader,
                    max_control_bytes=self._max_payload_bytes,
                    max_data_bytes=self._max_data_chunk_bytes,
                    timeout_sec=self._control_timeout_sec,
                )
                if frame is None:
                    return
                decoded = decode_message(frame)
            except (
                FramingError,
                UnsupportedVersionError,
                MalformedMessageError,
                ResourceLimitError,
            ) as exc:
                self._stats.total_errors += 1
                self._logger.warning(
                    "Closing connection from %s:%d due to protocol error: %s",
                    peer_addr[0],
                    peer_addr[1],
                    exc,
                )
                return
            except TimeoutError:
                self._logger.debug(
                    "Closing idle TCP connection from %s:%d after %.1fs",
                    peer_addr[0],
                    peer_addr[1],
                    self._control_timeout_sec,
                )
                return

            self._stats.total_messages += 1
            response = await self._dispatch_message(decoded, peer_addr)
            if response is None:
                continue

            encoded = encode_message(
                response,
                request_id=decoded.request_id,
                max_control_bytes=self._max_payload_bytes,
                max_data_bytes=self._max_data_chunk_bytes,
            )
            try:
                await write_frame(writer, encoded, timeout_sec=self._control_timeout_sec)
            except (ConnectionError, OSError, TimeoutError) as exc:
                self._stats.total_errors += 1
                self._logger.debug(
                    "Failed writing response to %s:%d: %s",
                    peer_addr[0],
                    peer_addr[1],
                    exc,
                )
                return

    async def _dispatch_message(
        self,
        decoded: DecodedMessage,
        peer_addr: PeerAddress,
    ) -> ProtocolPayload | None:
        if self._message_handler is not None:
            return await self._message_handler(decoded, peer_addr)

        if isinstance(decoded.payload, PingMessage):
            return PongMessage(
                peer_id=self._peer_id,
                tcp_port=self._bound_port or self._port,
                status="ok",
                display_name=self._display_name,
                sent_at_ms=decoded.payload.sent_at_ms,
            )

        return TransferErrorMessage(
            code="UNSUPPORTED_OPERATION",
            message=f"Operation {decoded.msg_type.name} is not handled on this endpoint",
            retryable=False,
        )
