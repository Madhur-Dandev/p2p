"""Unit tests for structured rotating logging and log reading."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.config import LoggingConfig
from p2p.exceptions import ConfigError
from p2p.logging_config import (
    configure_logging,
    get_context_logger,
    read_recent_logs,
    shutdown_logging,
)


@pytest.mark.unit
def test_logging_writes_structured_context_and_avoids_duplicate_handlers(tmp_path: Path) -> None:
    log_path = tmp_path / "logs" / "p2p.log"
    cfg = LoggingConfig(
        level="INFO",
        file_path=log_path,
        max_bytes=1024 * 1024,
        backup_count=2,
        console_enabled=False,
    )

    try:
        logger = configure_logging(cfg, node_peer_id="peer-local-1")
        logger.info("startup complete")

        # Reconfiguring replaces prior handlers instead of duplicating log lines
        logger = configure_logging(cfg, node_peer_id="peer-local-1")
        ctx = get_context_logger(
            "transfer",
            peer_id="peer-remote-2",
            request_id="req-abc",
            transfer_id="xfer-99",
        )
        ctx.info("chunk verified")
        logger.debug("filtered debug message")
    finally:
        shutdown_logging()

    lines = read_recent_logs(log_path, lines=10)
    assert len(lines) == 2
    assert "INFO     | p2p | peer=peer-local-1 req=- xfer=- | startup complete" in lines[0]
    assert (
        "INFO     | p2p.transfer | peer=peer-remote-2 req=req-abc xfer=xfer-99 | chunk verified"
        in lines[1]
    )


@pytest.mark.unit
def test_read_recent_logs_tails_and_handles_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.log"
    assert read_recent_logs(missing, lines=10) == []

    log_path = tmp_path / "app.log"
    log_path.write_text("line1\nline2\nline3\nline4\nline5\n", encoding="utf-8")
    assert read_recent_logs(log_path, lines=2) == ["line4", "line5"]

    with pytest.raises(ConfigError):
        read_recent_logs(log_path, lines=0)


@pytest.mark.unit
def test_invalid_log_level_override_raises(tmp_path: Path) -> None:
    cfg = LoggingConfig(
        level="INFO",
        file_path=tmp_path / "p2p.log",
        console_enabled=False,
    )
    with pytest.raises(ConfigError, match="Invalid log level"):
        configure_logging(cfg, level_override="VERBOSE")
