"""Structured rotating logging with correlation context (peer_id, request_id, transfer_id)."""

from __future__ import annotations

import logging
import sys
import time
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from p2p.config import LoggingConfig
from p2p.constants import APP_NAME, VALID_LOG_LEVELS
from p2p.exceptions import ConfigError

LOG_FORMAT = (
    "%(asctime)s.%(msecs)03dZ | %(levelname)-8s | %(name)s | "
    "peer=%(peer_id)s req=%(request_id)s xfer=%(transfer_id)s | %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class ContextFilter(logging.Filter):
    """Inject default correlation fields into every log record."""

    def __init__(self, default_peer_id: str = "-") -> None:
        super().__init__()
        self.default_peer_id = default_peer_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "peer_id") or not record.peer_id:
            record.peer_id = self.default_peer_id
        if not hasattr(record, "request_id") or not record.request_id:
            record.request_id = "-"
        if not hasattr(record, "transfer_id") or not record.transfer_id:
            record.transfer_id = "-"
        return True


class UtcFormatter(logging.Formatter):
    """Formatter emitting timestamps strictly in UTC."""

    converter = time.gmtime


class ContextLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Logger adapter attaching peer, request, and transfer identifiers."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        merged_extra = dict(self.extra or {})
        call_extra = kwargs.get("extra")
        if isinstance(call_extra, dict):
            merged_extra.update(call_extra)
        kwargs["extra"] = merged_extra
        return msg, kwargs


def shutdown_logging() -> None:
    """Flush and close all handlers attached to the application logger."""
    logger = logging.getLogger(APP_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        try:
            handler.flush()
        finally:
            handler.close()


def configure_logging(
    config: LoggingConfig,
    *,
    node_peer_id: str = "-",
    level_override: str | None = None,
) -> logging.Logger:
    """Configure rotating file and optional stderr logging for the 'p2p' namespace."""
    raw_level = level_override if level_override is not None else config.level
    normalized_level = raw_level.upper()
    if normalized_level not in VALID_LOG_LEVELS:
        raise ConfigError(f"Invalid log level {raw_level!r}; expected one of {VALID_LOG_LEVELS}")

    numeric_level = getattr(logging, normalized_level)
    logger = logging.getLogger(APP_NAME)
    shutdown_logging()

    logger.setLevel(numeric_level)
    logger.propagate = False

    context_filter = ContextFilter(default_peer_id=node_peer_id)
    formatter = UtcFormatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    config.file_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        filename=config.file_path,
        maxBytes=config.max_bytes,
        backupCount=config.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(context_filter)
    logger.addHandler(file_handler)

    if config.console_enabled:
        console_handler = logging.StreamHandler(stream=sys.stderr)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(context_filter)
        logger.addHandler(console_handler)

    return logger


def get_context_logger(
    name: str,
    *,
    peer_id: str | None = None,
    request_id: str | None = None,
    transfer_id: str | None = None,
) -> ContextLoggerAdapter:
    """Return a ContextLoggerAdapter scoped under the application logger."""
    logger_name = (
        name if name.startswith(f"{APP_NAME}.") or name == APP_NAME else f"{APP_NAME}.{name}"
    )
    extra: dict[str, str] = {}
    if peer_id is not None:
        extra["peer_id"] = peer_id
    if request_id is not None:
        extra["request_id"] = request_id
    if transfer_id is not None:
        extra["transfer_id"] = transfer_id
    return ContextLoggerAdapter(logging.getLogger(logger_name), extra)


def read_recent_logs(log_path: Path, lines: int = 50) -> list[str]:
    """Read up to the last N lines from a log file in bounded memory."""
    if lines <= 0:
        raise ConfigError(f"lines must be positive, got {lines}")
    if not log_path.exists():
        return []
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        recent = deque(handle, maxlen=lines)
    return [line.rstrip("\r\n") for line in recent]
