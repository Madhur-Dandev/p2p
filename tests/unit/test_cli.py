"""Unit and subprocess tests for the CLI entry point."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from p2p.cli import run_cli
from p2p.constants import APP_VERSION


@pytest.mark.unit
def test_module_help_and_version_via_subprocess() -> None:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{src_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else src_dir
    )

    help_proc = subprocess.run(
        [sys.executable, "-m", "p2p", "--help"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert help_proc.returncode == 0
    assert "Decentralized Peer-to-Peer File Discovery and Transfer System" in help_proc.stdout
    assert "config" in help_proc.stdout
    assert "share" in help_proc.stdout
    assert "logs" in help_proc.stdout

    ver_proc = subprocess.run(
        [sys.executable, "-m", "p2p", "--version"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert ver_proc.returncode == 0
    assert APP_VERSION in ver_proc.stdout


@pytest.mark.unit
def test_cli_config_share_and_logs_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with tempfile.TemporaryDirectory(prefix="p2phome-") as short_home:
        monkeypatch.setenv("HOME", short_home)
        cfg_path = tmp_path / "peer.toml"
        share_dir = tmp_path / "shared"
        share_dir.mkdir()

        # config init
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(
            [
                "--config",
                str(cfg_path),
                "config",
                "init",
                "--peer-id",
                "00000000-0000-4000-8000-000000000099",
                "--tcp-port",
                "9111",
            ],
            stdout=out,
            stderr=err,
        )
        assert code == 0, err.getvalue()
        assert cfg_path.exists()
        assert "00000000-0000-4000-8000-000000000099" in out.getvalue()

        # Re-initializing without --force must fail cleanly
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(["--config", str(cfg_path), "config", "init"], stdout=out, stderr=err)
        assert code == 1
        assert "already exists" in err.getvalue()

        # config validate
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(["--config", str(cfg_path), "config", "validate"], stdout=out, stderr=err)
        assert code == 0
        assert "Configuration is valid" in out.getvalue()

        # config show --format json
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(
            ["--config", str(cfg_path), "config", "show", "--format", "json"],
            stdout=out,
            stderr=err,
        )
        assert code == 0
        parsed = json.loads(out.getvalue())
        assert parsed["network"]["tcp_port"] == 9111
        assert parsed["node"]["peer_id"] == "00000000-0000-4000-8000-000000000099"

        # share add, list, remove
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(
            ["--config", str(cfg_path), "share", "add", str(share_dir)],
            stdout=out,
            stderr=err,
        )
        assert code == 0, err.getvalue()
        assert str(share_dir.resolve()) in out.getvalue()

        out, err = io.StringIO(), io.StringIO()
        code = run_cli(["--config", str(cfg_path), "share", "list"], stdout=out, stderr=err)
        assert code == 0
        assert "[ok]" in out.getvalue()

        # logs command reads real log entries written by previous commands
        out, err = io.StringIO(), io.StringIO()
        code = run_cli(["--config", str(cfg_path), "logs", "--lines", "10"], stdout=out, stderr=err)
        assert code == 0
        assert "Initialized configuration" in out.getvalue()
        assert "Registered shared directory" in out.getvalue()

        out, err = io.StringIO(), io.StringIO()
        code = run_cli(
            ["--config", str(cfg_path), "share", "remove", str(share_dir)],
            stdout=out,
            stderr=err,
        )
        assert code == 0
        assert "Removed shared directory" in out.getvalue()


@pytest.mark.unit
@pytest.mark.parametrize(
    "argv",
    [
        ["start"],
        ["stop"],
        ["status"],
        ["peers"],
    ],
)
def test_planned_commands_fail_honestly_until_implemented(
    argv: list[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("P2P_CONFIG", raising=False)
    assert os.environ.get("HOME") == str(tmp_path)

    out, err = io.StringIO(), io.StringIO()
    code = run_cli(argv, stdout=out, stderr=err)
    assert code == 2
    assert "not yet implemented" in err.getvalue()


@pytest.mark.unit
def test_cli_index_search_and_downloads_empty_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with tempfile.TemporaryDirectory(prefix="p2phome-") as short_home:
        monkeypatch.setenv("HOME", short_home)
        cfg_path = tmp_path / "peer.toml"
        share_dir = tmp_path / "shared"
        share_dir.mkdir()
        (share_dir / "networking notes.txt").write_text("tcp udp", encoding="utf-8")

        out, err = io.StringIO(), io.StringIO()
        assert run_cli(["--config", str(cfg_path), "config", "init"], stdout=out, stderr=err) == 0
        assert (
            run_cli(
                ["--config", str(cfg_path), "share", "add", str(share_dir)],
                stdout=out,
                stderr=err,
            )
            == 0
        )

        # index rebuild
        out, err = io.StringIO(), io.StringIO()
        assert run_cli(["--config", str(cfg_path), "index", "rebuild"], stdout=out, stderr=err) == 0
        assert "added=1" in out.getvalue()

        # search local index
        out, err = io.StringIO(), io.StringIO()
        assert (
            run_cli(["--config", str(cfg_path), "search", "networking"], stdout=out, stderr=err)
            == 0
        )
        assert "networking notes.txt" in out.getvalue()

        # download without --peer reports actionable milestone requirement
        out, err = io.StringIO(), io.StringIO()
        assert (
            run_cli(["--config", str(cfg_path), "download", "abc123"], stdout=out, stderr=err) == 2
        )
        assert "--peer HOST:PORT is required" in err.getvalue()

        # downloads history empty check
        out, err = io.StringIO(), io.StringIO()
        assert run_cli(["--config", str(cfg_path), "downloads"], stdout=out, stderr=err) == 0
        assert "No download history recorded" in out.getvalue()


@pytest.mark.unit
def test_dev_subcommand_help_prints_usage() -> None:
    out, err = io.StringIO(), io.StringIO()
    code = run_cli(["dev"], stdout=out, stderr=err)
    assert code == 2
    assert "tcp-server" in out.getvalue()
    assert "tcp-ping" in out.getvalue()
    assert "peer-node" in out.getvalue()
