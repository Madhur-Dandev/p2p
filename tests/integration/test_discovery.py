"""Integration tests for Milestone 3 UDP peer discovery, heartbeats, and fallback."""

from __future__ import annotations

import asyncio
import os
import re
import selectors
import signal
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import IO

import pytest

from p2p.network.discovery import (
    DiscoveryMessageType,
    DiscoveryService,
    encode_discovery_packet,
)
from p2p.network.peer_registry import PeerRegistry, PeerStatus
from p2p.network.tcp_server import TcpServer
from p2p.utils.identifiers import generate_peer_id
from p2p.utils.time_utils import monotonic_now


async def _wait_until(
    predicate: Callable[[], bool],
    *,
    max_wait_sec: float = 3.0,
    interval: float = 0.02,
) -> None:
    deadline = monotonic_now() + max_wait_sec
    while not predicate():
        if monotonic_now() >= deadline:
            raise AssertionError(f"Condition not met within {max_wait_sec:.1f}s")
        await asyncio.sleep(interval)


@pytest.mark.integration
async def test_late_joiner_and_duplicate_announcements_deduplicated() -> None:
    id1 = generate_peer_id()
    id2 = generate_peer_id()

    reg1 = PeerRegistry(local_peer_id=id1, stale_timeout_sec=5.0, remove_timeout_sec=15.0)
    reg2 = PeerRegistry(local_peer_id=id2, stale_timeout_sec=5.0, remove_timeout_sec=15.0)

    disc1 = DiscoveryService(
        local_peer_id=id1,
        display_name="peer-one",
        tcp_port=9101,
        registry=reg1,
        bind_host="127.0.0.1",
        udp_port=0,
        broadcast_enabled=False,
        heartbeat_interval_sec=0.2,
    )
    async with disc1:
        # peer2 starts late and targets peer1's UDP port directly
        disc2 = DiscoveryService(
            local_peer_id=id2,
            display_name="peer-two",
            tcp_port=9102,
            registry=reg2,
            bind_host="127.0.0.1",
            udp_port=0,
            broadcast_enabled=False,
            heartbeat_interval_sec=0.2,
            discovery_targets=[("127.0.0.1", disc1.bound_port)],
        )
        async with disc2:
            await _wait_until(
                lambda: reg1.get_peer(id2) is not None and reg2.get_peer(id1) is not None
            )

            # Send repeated announcements & heartbeats -> must NOT duplicate peer entries
            disc2.send_announce(target=("127.0.0.1", disc1.bound_port))
            disc2.send_announce(target=("127.0.0.1", disc1.bound_port))
            disc2.send_heartbeat()
            await asyncio.sleep(0.1)

            assert len(reg1) == 1
            assert len(reg2) == 1
            p2_on_1 = reg1.get_peer(id2)
            p1_on_2 = reg2.get_peer(id1)
            assert p2_on_1 is not None and p2_on_1.tcp_port == 9102
            assert p1_on_2 is not None and p1_on_2.tcp_port == 9101
            assert p2_on_1.status == PeerStatus.ACTIVE
            assert p1_on_2.status == PeerStatus.ACTIVE


@pytest.mark.integration
async def test_temporary_disconnection_marks_stale_then_recovers() -> None:
    id1 = generate_peer_id()
    id2 = generate_peer_id()

    reg1 = PeerRegistry(local_peer_id=id1, stale_timeout_sec=0.35, remove_timeout_sec=5.0)
    reg2 = PeerRegistry(local_peer_id=id2, stale_timeout_sec=0.35, remove_timeout_sec=5.0)

    disc1 = DiscoveryService(
        local_peer_id=id1,
        display_name="observer",
        tcp_port=9201,
        registry=reg1,
        bind_host="127.0.0.1",
        udp_port=0,
        broadcast_enabled=False,
        heartbeat_interval_sec=0.1,
    )
    async with disc1:
        disc2 = DiscoveryService(
            local_peer_id=id2,
            display_name="transient",
            tcp_port=9202,
            registry=reg2,
            bind_host="127.0.0.1",
            udp_port=0,
            broadcast_enabled=False,
            heartbeat_interval_sec=0.1,
            discovery_targets=[("127.0.0.1", disc1.bound_port)],
        )
        await disc2.start()
        p2_udp_port = disc2.bound_port

        await _wait_until(
            lambda: (p := reg1.get_peer(id2)) is not None and p.status == PeerStatus.ACTIVE
        )
        first_seen = reg1.get_peer(id2)
        assert first_seen is not None
        original_first_seen = first_seen.first_seen_utc

        # Simulate peer2 disconnecting temporarily
        await disc2.stop()
        await _wait_until(
            lambda: (p := reg1.get_peer(id2)) is not None and p.status == PeerStatus.STALE,
            max_wait_sec=2.0,
        )

        # Restart peer2 -> heartbeats/announce recover it back to ACTIVE
        disc2_restarted = DiscoveryService(
            local_peer_id=id2,
            display_name="transient",
            tcp_port=9202,
            registry=reg2,
            bind_host="127.0.0.1",
            udp_port=p2_udp_port,
            broadcast_enabled=False,
            heartbeat_interval_sec=0.1,
            discovery_targets=[("127.0.0.1", disc1.bound_port)],
        )
        async with disc2_restarted:
            await _wait_until(
                lambda: (p := reg1.get_peer(id2)) is not None and p.status == PeerStatus.ACTIVE,
                max_wait_sec=2.0,
            )
            recovered = reg1.get_peer(id2)
            assert recovered is not None
            assert recovered.first_seen_utc == original_first_seen


@pytest.mark.integration
async def test_static_peer_fallback_when_broadcast_disabled() -> None:
    id1 = generate_peer_id()
    id2 = generate_peer_id()

    tcp2 = TcpServer(host="127.0.0.1", port=0, peer_id=id2, display_name="static-target")
    async with tcp2:
        reg1 = PeerRegistry(local_peer_id=id1, stale_timeout_sec=2.0, remove_timeout_sec=10.0)
        disc1 = DiscoveryService(
            local_peer_id=id1,
            display_name="static-client",
            tcp_port=9301,
            registry=reg1,
            bind_host="127.0.0.1",
            udp_port=0,
            broadcast_enabled=False,
            heartbeat_interval_sec=0.25,
            static_peers=[f"127.0.0.1:{tcp2.bound_port}"],
        )
        async with disc1:
            await _wait_until(lambda: reg1.get_peer(id2) is not None)
            peer2 = reg1.get_peer(id2)
            assert peer2 is not None
            assert peer2.ip_address == "127.0.0.1"
            assert peer2.tcp_port == tcp2.bound_port
            assert peer2.display_name == "static-target"
            assert peer2.source == "static"
            assert disc1.stats.static_probes_ok >= 1


@pytest.mark.integration
async def test_malformed_and_self_udp_packets_ignored_safely() -> None:
    id1 = generate_peer_id()
    reg1 = PeerRegistry(local_peer_id=id1)
    disc1 = DiscoveryService(
        local_peer_id=id1,
        display_name="hardened-node",
        tcp_port=9401,
        registry=reg1,
        bind_host="127.0.0.1",
        udp_port=0,
        broadcast_enabled=False,
        heartbeat_interval_sec=5.0,
    )

    async with disc1:
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            target = ("127.0.0.1", disc1.bound_port)
            raw_sock.sendto(b"garbage-bytes", target)
            raw_sock.sendto(b"P2\x99" + b"\x00" * 30, target)
            self_packet = encode_discovery_packet(
                msg_type=DiscoveryMessageType.ANNOUNCE,
                peer_id=id1,
                tcp_port=9401,
            )
            raw_sock.sendto(self_packet, target)
            await _wait_until(
                lambda: disc1.stats.dropped_invalid >= 2 and disc1.stats.ignored_self >= 1
            )
        finally:
            raw_sock.close()

        assert len(reg1) == 0
        assert disc1.is_running


def _read_until_match(
    stream: IO[bytes],
    pattern: str,
    *,
    max_wait_sec: float = 5.0,
) -> str:
    fd = stream.fileno()
    selector = selectors.DefaultSelector()
    selector.register(fd, selectors.EVENT_READ)
    deadline = monotonic_now() + max_wait_sec
    collected: list[str] = []
    pending = bytearray()
    try:
        while True:
            while b"\n" in pending:
                raw_line, _, rest = pending.partition(b"\n")
                pending = bytearray(rest)
                line = raw_line.decode("utf-8", errors="replace")
                collected.append(line)
                if re.search(pattern, line):
                    return line

            remaining = deadline - monotonic_now()
            if remaining <= 0 or not selector.select(timeout=remaining):
                raise AssertionError(
                    f"Timed out waiting for {pattern!r}; lines seen: {collected!r}"
                )
            chunk = os.read(fd, 4096)
            if not chunk:
                raise AssertionError(f"EOF before matching {pattern!r}; lines seen: {collected!r}")
            pending.extend(chunk)
    finally:
        selector.close()


@pytest.mark.integration
def test_dev_peer_node_two_processes_discover_each_other(tmp_path: Path) -> None:
    src_dir = str(Path(__file__).resolve().parents[2] / "src")
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{src_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else src_dir
    )

    cfg1 = tmp_path / "peer1.toml"
    cfg2 = tmp_path / "peer2.toml"
    for cfg, pid, name in (
        (cfg1, "00000000-0000-4000-8000-000000000101", "node-alpha"),
        (cfg2, "00000000-0000-4000-8000-000000000102", "node-beta"),
    ):
        cfg.write_text(
            "[node]\n"
            f'peer_id = "{pid}"\n'
            f'display_name = "{name}"\n'
            "[logging]\n"
            f'file_path = "{tmp_path / (name + ".log")}"\n'
            "console_enabled = false\n"
            "[daemon]\n"
            f'socket_path = "/var/run/p2p-{name}.sock"\n',
            encoding="utf-8",
        )

    proc1 = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "p2p",
            "--config",
            str(cfg1),
            "dev",
            "peer-node",
            "--host",
            "127.0.0.1",
            "--tcp-port",
            "0",
            "--udp-port",
            "0",
            "--no-broadcast",
            "--heartbeat",
            "0.2",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        env=env,
    )
    proc2: subprocess.Popen[bytes] | None = None
    try:
        assert proc1.stdout is not None
        banner1 = _read_until_match(proc1.stdout, r"Peer node running")
        match1 = re.search(r"tcp=127\.0\.0\.1:(\d+) udp=127\.0\.0\.1:(\d+)", banner1)
        assert match1 is not None, f"Unexpected banner1: {banner1!r}"
        udp1 = int(match1.group(2))

        proc2 = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "p2p",
                "--config",
                str(cfg2),
                "dev",
                "peer-node",
                "--host",
                "127.0.0.1",
                "--tcp-port",
                "0",
                "--udp-port",
                "0",
                "--no-broadcast",
                "--udp-target",
                f"127.0.0.1:{udp1}",
                "--heartbeat",
                "0.2",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            env=env,
        )
        assert proc2.stdout is not None
        event1 = _read_until_match(proc1.stdout, r"\[event:discovered\].*node-beta")
        event2 = _read_until_match(proc2.stdout, r"\[event:discovered\].*node-alpha")
        assert "[event:discovered]" in event1 and "node-beta" in event1
        assert "[event:discovered]" in event2 and "node-alpha" in event2
    finally:
        for proc in (proc2, proc1):
            if proc is None:
                continue
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)
