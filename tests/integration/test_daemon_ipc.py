"""End-to-end Milestone 11 system tests with real detached background P2P daemons & CLI."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from p2p.utils.time_utils import monotonic_now


def _run_cli_proc(
    args: list[str],
    *,
    env: dict[str, str],
    timeout_sec: float = 15.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "p2p", *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout_sec,
        check=False,
    )


def _write_daemon_config(
    cfg_path: Path,
    *,
    name: str,
    state_dir: Path,
    db_dir: Path,
    dl_dir: Path,
    env: dict[str, str],
) -> None:
    proc = _run_cli_proc(
        [
            "--config",
            str(cfg_path),
            "config",
            "init",
            "--display-name",
            name,
            "--tcp-port",
            "9000",
            "--udp-port",
            "9999",
        ],
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    text = cfg_path.read_text(encoding="utf-8")
    # Point socket/pid inside short tmp dir so socket_path stays <= 100 UTF-8 bytes on macOS
    sock_path = state_dir / f"{name}.sock"
    pid_path = state_dir / f"{name}.pid"
    log_path = db_dir / f"{name}.log"
    db_path = db_dir / f"{name}.db"

    text = re.sub(r'database_path = ".*"', f'database_path = "{db_path}"', text)
    text = re.sub(r'download_dir = ".*"', f'download_dir = "{dl_dir}"', text)
    text = re.sub(r'file_path = ".*"', f'file_path = "{log_path}"', text)
    text = re.sub(r'socket_path = ".*"', f'socket_path = "{sock_path}"', text)
    text = re.sub(r'pid_path = ".*"', f'pid_path = "{pid_path}"', text)
    cfg_path.write_text(text, encoding="utf-8")


@pytest.mark.integration
def test_two_background_daemons_full_lifecycle_search_download_without_peer_flag(
    tmp_path: Path,
) -> None:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else src_dir

    with tempfile.TemporaryDirectory(prefix="h-") as short_state:
        short_dir = Path(short_state)
        env["HOME"] = short_state
        env.pop("P2P_CONFIG", None)
        share_a = tmp_path / "share_a"
        share_a.mkdir()
        dl_a = tmp_path / "dl_a"
        dl_b = tmp_path / "dl_b"
        cfg_a = tmp_path / "node_a.toml"
        cfg_b = tmp_path / "node_b.toml"

        _write_daemon_config(
            cfg_a,
            name="node-a",
            state_dir=short_dir,
            db_dir=tmp_path,
            dl_dir=dl_a,
            env=env,
        )
        _write_daemon_config(
            cfg_b,
            name="node-b",
            state_dir=short_dir,
            db_dir=tmp_path,
            dl_dir=dl_b,
            env=env,
        )

        # Start Daemon A cleanly in background on ephemeral ports
        start_a = _run_cli_proc(
            [
                "--config",
                str(cfg_a),
                "start",
                "--host",
                "127.0.0.1",
                "--tcp-port",
                "0",
                "--udp-port",
                "0",
                "--no-broadcast",
            ],
            env=env,
        )
        assert start_a.returncode == 0, start_a.stderr
        assert "Daemon started:" in start_a.stdout
        port_match = re.search(r"tcp_port=(\d+)", start_a.stdout)
        assert port_match is not None, start_a.stdout
        tcp_a = int(port_match.group(1))

        # Share & index a real binary file on already-running Daemon A (hot reload!)
        payload = os.urandom(150_000)
        sample_file = share_a / "final report 2026.bin"
        sample_file.write_bytes(payload)
        expected_sha = hashlib.sha256(payload).hexdigest()

        try:
            share_proc = _run_cli_proc(
                ["--config", str(cfg_a), "share", "add", str(share_a)],
                env=env,
            )
            assert share_proc.returncode == 0, share_proc.stderr
            assert "Daemon index refreshed" in share_proc.stdout

            status_a = _run_cli_proc(["--config", str(cfg_a), "status"], env=env)
            assert status_a.returncode == 0, status_a.stderr
            assert "Indexed files: 1" in status_a.stdout

            # Duplicate start must fail cleanly via PID lock
            dup_start = _run_cli_proc(["--config", str(cfg_a), "start"], env=env)
            assert dup_start.returncode == 1
            assert "already running" in dup_start.stderr.lower()

            # Start Daemon B with static fallback pointing to Daemon A
            start_b = _run_cli_proc(
                [
                    "--config",
                    str(cfg_b),
                    "start",
                    "--host",
                    "127.0.0.1",
                    "--tcp-port",
                    "0",
                    "--udp-port",
                    "0",
                    "--no-broadcast",
                    "--static-peer",
                    f"127.0.0.1:{tcp_a}",
                ],
                env=env,
            )
            assert start_b.returncode == 0, start_b.stderr

            try:
                # Wait briefly until Node B registers Node A as ACTIVE
                deadline = monotonic_now() + 5.0
                peers_out = ""
                while monotonic_now() < deadline:
                    peers_proc = _run_cli_proc(["--config", str(cfg_b), "peers"], env=env)
                    if peers_proc.returncode == 0 and "node-a" in peers_proc.stdout:
                        peers_out = peers_proc.stdout
                        break
                    time.sleep(0.05)
                assert "node-a" in peers_out

                # Distributed search on Node B returns Node A's file without local copy
                srch_proc = _run_cli_proc(
                    ["--config", str(cfg_b), "search", "final report"],
                    env=env,
                )
                assert srch_proc.returncode == 0, srch_proc.stderr
                assert "Distributed search matches" in srch_proc.stdout
                fid_match = re.search(r"file_id=([A-Za-z0-9_-]+)", srch_proc.stdout)
                assert fid_match is not None, srch_proc.stdout
                file_id = fid_match.group(1)

                # Download using ONLY file_id (no --peer flag!) resolved via Daemon B search cache
                dl_proc = _run_cli_proc(
                    ["--config", str(cfg_b), "download", file_id],
                    env=env,
                )
                assert dl_proc.returncode == 0, dl_proc.stderr
                assert "Download completed:" in dl_proc.stdout
                assert expected_sha in dl_proc.stdout
                assert (dl_b / "final report 2026.bin").read_bytes() == payload

                hist_proc = _run_cli_proc(["--config", str(cfg_b), "downloads"], env=env)
                assert hist_proc.returncode == 0, hist_proc.stderr
                assert "status=completed" in hist_proc.stdout
            finally:
                stop_b = _run_cli_proc(["--config", str(cfg_b), "stop"], env=env)
                assert stop_b.returncode == 0, stop_b.stderr
                assert not (short_dir / "node-b.sock").exists()
                assert not (short_dir / "node-b.pid").exists()
        finally:
            stop_a = _run_cli_proc(["--config", str(cfg_a), "stop"], env=env)
            assert stop_a.returncode == 0, stop_a.stderr
            assert not (short_dir / "node-a.sock").exists()
            assert not (short_dir / "node-a.pid").exists()
