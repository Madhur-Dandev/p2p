"""Asynchronous Unix domain socket IPC server for controlling the running daemon."""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from p2p.constants import MAX_FRAME_PAYLOAD_BYTES, MAX_UNIX_SOCKET_PATH_BYTES
from p2p.daemon.ipc_protocol import (
    exception_to_error_dict,
    read_ipc_message,
    write_ipc_message,
)
from p2p.exceptions import DaemonError, FramingError, PathValidationError, ResourceLimitError
from p2p.logging_config import get_context_logger

IpcHandler = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]


class IpcServer:
    """Listens on a private Unix domain socket (0600) and dispatches daemon commands."""

    def __init__(
        self,
        socket_path: Path,
        handler: IpcHandler,
        *,
        max_message_bytes: int = MAX_FRAME_PAYLOAD_BYTES,
        io_timeout_sec: float = 30.0,
    ) -> None:
        raw = Path(socket_path).expanduser()
        self._raw_socket_path = raw
        self._socket_path = raw.parent.resolve(strict=False) / raw.name
        encoded_len = len(str(self._socket_path).encode("utf-8"))
        if encoded_len > MAX_UNIX_SOCKET_PATH_BYTES:
            raise DaemonError(
                f"IPC socket_path exceeds {MAX_UNIX_SOCKET_PATH_BYTES} UTF-8 bytes "
                f"({encoded_len} B): {self._socket_path}"
            )
        self._handler = handler
        self._max_message_bytes = max_message_bytes
        self._io_timeout_sec = io_timeout_sec
        self._server: asyncio.AbstractServer | None = None
        self._client_tasks: set[asyncio.Task[None]] = set()
        self._logger = get_context_logger("daemon.ipc_server")

    @property
    def socket_path(self) -> Path:
        """Return resolved Unix socket filesystem path."""
        return self._socket_path

    @property
    def is_running(self) -> bool:
        """Return True while accepting local IPC connections."""
        return self._server is not None and self._server.is_serving()

    async def start(self) -> None:
        """Prepare secure Unix domain socket and start listening."""
        if self._server is not None:
            return

        await asyncio.to_thread(self._prepare_socket_parent_sync)
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=str(self._socket_path),
        )
        await asyncio.to_thread(os.chmod, self._socket_path, 0o600)
        self._logger.info("Daemon IPC listening on %s", self._socket_path)

    def _prepare_socket_parent_sync(self) -> None:
        if self._raw_socket_path.is_symlink() or self._socket_path.is_symlink():
            raise PathValidationError(f"Refusing symlink IPC socket path: {self._raw_socket_path}")
        if self._raw_socket_path.parent.is_symlink():
            raise PathValidationError(
                f"Refusing symlink directory for IPC socket: {self._raw_socket_path.parent}"
            )
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self._socket_path.parent.is_symlink():
            raise PathValidationError(
                f"Refusing symlink directory for IPC socket: {self._socket_path.parent}"
            )
        if self._socket_path.is_symlink():
            raise PathValidationError(f"Refusing symlink IPC socket path: {self._socket_path}")
        if self._socket_path.exists():
            # Caller already holds PidFileLock, so an existing socket file is stale
            self._socket_path.unlink()

    async def stop(self) -> None:
        """Stop accepting IPC clients, finish/cancel handlers, and unlink socket path."""
        if self._server is None:
            return
        server = self._server
        self._server = None
        server.close()

        pending = list(self._client_tasks)
        if pending:
            _, still_pending = await asyncio.wait(pending, timeout=1.0)
            for task in still_pending:
                task.cancel()
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)

        await server.wait_closed()
        await asyncio.to_thread(self._unlink_socket_sync)
        self._logger.info("Daemon IPC server stopped")

    def _unlink_socket_sync(self) -> None:
        with contextlib.suppress(OSError):
            self._socket_path.unlink(missing_ok=True)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)
        try:
            req = await read_ipc_message(
                reader,
                max_bytes=self._max_message_bytes,
                timeout_sec=self._io_timeout_sec,
            )
            if req is None:
                return
            action = req.get("action")
            params = req.get("params", {})
            if not isinstance(action, str) or not action.strip():
                response: dict[str, Any] = {
                    "ok": False,
                    "error": {"type": "ValidationError", "message": "Missing IPC action"},
                }
            elif not isinstance(params, dict):
                response = {
                    "ok": False,
                    "error": {"type": "ValidationError", "message": "IPC params must be an object"},
                }
            else:
                try:
                    result = await self._handler(action, params)
                    response = {"ok": True, "result": result}
                except Exception as exc:
                    self._logger.warning("IPC action %r failed: %s", action, exc)
                    response = {"ok": False, "error": exception_to_error_dict(exc)}

            await write_ipc_message(
                writer,
                response,
                max_bytes=self._max_message_bytes,
                timeout_sec=self._io_timeout_sec,
            )
        except (FramingError, ResourceLimitError, TimeoutError, OSError, ConnectionError) as exc:
            self._logger.debug("IPC client connection ended: %s", exc)
        finally:
            if task is not None:
                self._client_tasks.discard(task)
            try:
                if not writer.is_closing():
                    writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except (OSError, TimeoutError, ConnectionError, asyncio.CancelledError):
                pass
