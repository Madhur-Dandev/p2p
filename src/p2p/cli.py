"""Command-line interface for the Decentralized P2P File Discovery and Transfer System."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import TextIO

from p2p.config import (
    AppConfig,
    add_shared_directory,
    create_default_config,
    load_config,
    remove_shared_directory,
    resolve_config_path,
    save_config,
)
from p2p.constants import APP_VERSION, VALID_LOG_LEVELS
from p2p.exceptions import P2PError
from p2p.logging_config import (
    configure_logging,
    get_context_logger,
    read_recent_logs,
    shutdown_logging,
)
from p2p.network.discovery import DiscoveryService, parse_host_port
from p2p.network.peer_registry import PeerInfo, PeerRegistry
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import TcpServer
from p2p.utils.time_utils import monotonic_now

_PLANNED_COMMANDS: dict[str, tuple[str, str]] = {
    "start": ("Milestone 11", "Background daemon lifecycle and IPC (use 'dev peer-node' now)"),
    "stop": ("Milestone 11", "Daemon IPC shutdown control"),
    "status": ("Milestone 11", "Daemon IPC status reporting"),
    "peers": ("Milestone 11", "Query daemon peer registry over IPC (use 'dev peer-node' now)"),
    "index": ("Milestone 5", "SQLite storage and shared-directory indexing"),
    "search": ("Milestone 6 & 7", "Local SQLite search and distributed peer queries"),
    "download": ("Milestone 8 & 9", "Direct TCP file transfer, SHA-256 verification, and resume"),
    "downloads": ("Milestone 8 & 9", "SQLite download history and transfer state tracking"),
}


def _parse_log_level(value: str) -> str:
    upper = value.upper()
    if upper not in VALID_LOG_LEVELS:
        valid = ", ".join(VALID_LOG_LEVELS)
        raise argparse.ArgumentTypeError(f"Invalid log level {value!r}; choose from: {valid}")
    return upper


def _build_parser_and_subparsers() -> tuple[
    argparse.ArgumentParser, dict[str, argparse.ArgumentParser]
]:
    """Build the top-level CLI parser and expose subparsers without private attribute access."""
    parser = argparse.ArgumentParser(
        prog="p2p",
        description=(
            "Decentralized Peer-to-Peer File Discovery and Transfer System "
            "(Milestone 4: foundation, async TCP, UDP discovery, and binary protocol)."
        ),
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to TOML configuration file (overrides P2P_CONFIG and default path).",
    )
    parser.add_argument(
        "--log-level",
        type=_parse_log_level,
        default=None,
        metavar="LEVEL",
        help=f"Override configured log level ({', '.join(VALID_LOG_LEVELS)}).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {APP_VERSION}",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    # config subcommands
    config_parser = subparsers.add_parser(
        "config",
        help="Initialize, inspect, and validate node configuration.",
    )
    config_sub = config_parser.add_subparsers(dest="config_action", metavar="ACTION")

    config_init = config_sub.add_parser("init", help="Create a new TOML configuration file.")
    config_init.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing configuration file.",
    )
    config_init.add_argument(
        "--peer-id",
        default=None,
        help="Explicit UUIDv4 peer ID (generated automatically if omitted).",
    )
    config_init.add_argument(
        "--display-name",
        default=None,
        help="Human-readable peer name.",
    )
    config_init.add_argument(
        "--tcp-port",
        type=int,
        default=None,
        help="TCP listening port (1-65535).",
    )
    config_init.add_argument(
        "--udp-port",
        type=int,
        default=None,
        help="UDP discovery port (1-65535).",
    )

    config_show = config_sub.add_parser("show", help="Display effective node configuration.")
    config_show.add_argument(
        "--format",
        choices=("toml", "json"),
        default="toml",
        help="Output format (default: toml).",
    )

    config_sub.add_parser("validate", help="Validate the resolved configuration file.")

    # share subcommands
    share_parser = subparsers.add_parser(
        "share",
        help="Manage authorized shared directories.",
    )
    share_sub = share_parser.add_subparsers(dest="share_action", metavar="ACTION")

    share_add = share_sub.add_parser("add", help="Add an authorized directory to share.")
    share_add.add_argument("directory", type=Path, help="Local directory path to share.")

    share_sub.add_parser("list", help="List configured shared directories.")

    share_remove = share_sub.add_parser(
        "remove", help="Remove a directory from shared directories."
    )
    share_remove.add_argument("directory", type=Path, help="Directory path to stop sharing.")

    # logs command
    logs_parser = subparsers.add_parser("logs", help="View recent application log entries.")
    logs_parser.add_argument(
        "-n",
        "--lines",
        type=int,
        default=50,
        help="Number of trailing log lines to display (default: 50).",
    )

    # Temporary development mode (Milestone 2 foreground TCP check; replaced by daemon IPC in M11)
    dev_parser = subparsers.add_parser(
        "dev",
        help="Temporary foreground development commands (superseded by daemon IPC in M11).",
    )
    dev_sub = dev_parser.add_subparsers(dest="dev_action", metavar="ACTION")

    dev_server = dev_sub.add_parser(
        "tcp-server",
        help="Run a foreground TCP test server (P2P v1 PING/PONG framing).",
    )
    dev_server.add_argument(
        "--host",
        default=None,
        help="Override bind host from config (default: network.bind_host).",
    )
    dev_server.add_argument(
        "--port",
        type=int,
        default=None,
        help="Override TCP port from config (default: network.tcp_port; 0 = ephemeral).",
    )

    dev_ping = dev_sub.add_parser(
        "tcp-ping",
        help="Send a correlated P2P v1 PING frame to a running TCP peer.",
    )
    dev_ping.add_argument("host", help="Target host or IP address.")
    dev_ping.add_argument("port", type=int, help="Target TCP port (1-65535).")

    dev_peer = dev_sub.add_parser(
        "peer-node",
        help="Run a foreground TCP + UDP peer node (Milestone 3 discovery demo).",
    )
    dev_peer.add_argument(
        "--host",
        default=None,
        help="Override bind host from config (default: network.bind_host).",
    )
    dev_peer.add_argument(
        "--tcp-port",
        type=int,
        default=None,
        help="Override TCP port from config (0 = ephemeral).",
    )
    dev_peer.add_argument(
        "--udp-port",
        type=int,
        default=None,
        help="Override UDP discovery port from config (0 = ephemeral).",
    )
    dev_peer.add_argument(
        "--udp-target",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="Additional unicast UDP discovery target (repeatable, useful on localhost).",
    )
    dev_peer.add_argument(
        "--static-peer",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="Additional fallback TCP peer endpoint (repeatable).",
    )
    dev_peer.add_argument(
        "--no-broadcast",
        action="store_true",
        help="Disable UDP broadcast announcements (for testing static/unicast fallback).",
    )
    dev_peer.add_argument(
        "--heartbeat",
        type=float,
        default=None,
        help="Override heartbeat interval in seconds.",
    )
    dev_peer.add_argument(
        "--stale-timeout",
        type=float,
        default=None,
        help="Override stale peer timeout in seconds.",
    )

    # Planned milestone commands (defined honestly until their milestone is implemented)
    subparsers.add_parser(
        "start",
        help="Start the P2P node daemon [planned: Milestone 11].",
    )
    subparsers.add_parser(
        "stop",
        help="Stop the running P2P node daemon [planned: Milestone 11].",
    )
    subparsers.add_parser(
        "status",
        help="Show daemon and network status [planned: Milestone 11].",
    )
    subparsers.add_parser(
        "peers",
        help="List discovered and static peers [planned: Milestone 3 & 11].",
    )

    index_parser = subparsers.add_parser(
        "index",
        help="Manage the local shared-file index [planned: Milestone 5].",
    )
    index_sub = index_parser.add_subparsers(dest="index_action", metavar="ACTION")
    index_sub.add_parser("rebuild", help="Rebuild the local file index.")

    search_parser = subparsers.add_parser(
        "search",
        help="Search for files across local and remote peers [planned: Milestone 6 & 7].",
    )
    search_parser.add_argument("query", help="Filename or substring to search for.")

    download_parser = subparsers.add_parser(
        "download",
        help="Download a file by file ID from a peer [planned: Milestone 8 & 9].",
    )
    download_parser.add_argument("file_id", help="Target file identifier from search results.")

    subparsers.add_parser(
        "downloads",
        help="List download history and active/partial transfers [planned: Milestone 8 & 9].",
    )

    return parser, {"config": config_parser, "share": share_parser, "dev": dev_parser}


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI argument parser."""
    parser, _ = _build_parser_and_subparsers()
    return parser


def _handle_config(
    args: argparse.Namespace,
    config_parser: argparse.ArgumentParser,
    out: TextIO,
    err: TextIO,
) -> int:
    action = args.config_action
    if action is None:
        config_parser.print_help(file=out)
        return 2

    if action == "init":
        target = resolve_config_path(args.config)
        cfg = create_default_config(peer_id=args.peer_id, config_path=target)
        if args.display_name is not None:
            cfg.node.display_name = args.display_name
        if args.tcp_port is not None:
            cfg.network.tcp_port = args.tcp_port
        if args.udp_port is not None:
            cfg.discovery.udp_port = args.udp_port
        written = save_config(cfg, target, overwrite=args.force)
        configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)
        logger = get_context_logger("cli", peer_id=cfg.node.peer_id)
        logger.info("Initialized configuration at %s", written)
        print(f"Configuration written to {written}", file=out)
        print(f"Peer ID: {cfg.node.peer_id}", file=out)
        return 0

    if action == "show":
        cfg = load_config(args.config)
        source_label = str(cfg.config_path) if cfg.config_path else "<defaults (no file on disk)>"
        if args.format == "json":
            payload = cfg.to_dict()
            payload["_source"] = source_label
            print(json.dumps(payload, indent=2), file=out)
        else:
            print(f"# Source: {source_label}", file=out)
            print(cfg.to_toml(), end="", file=out)
        return 0

    if action == "validate":
        target = resolve_config_path(args.config)
        if not target.exists():
            print(f"Configuration file does not exist: {target}", file=err)
            return 1
        cfg = load_config(target)
        print(f"Configuration is valid: {target} (peer_id={cfg.node.peer_id})", file=out)
        return 0

    print(f"Unsupported config action: {action}", file=err)
    return 2


def _handle_share(
    args: argparse.Namespace,
    share_parser: argparse.ArgumentParser,
    out: TextIO,
    err: TextIO,
) -> int:
    action = args.share_action
    if action is None:
        share_parser.print_help(file=out)
        return 2

    if action == "add":
        cfg, added_dir, added_new = add_shared_directory(
            args.directory,
            explicit_config_path=args.config,
        )
        configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)
        logger = get_context_logger("cli", peer_id=cfg.node.peer_id)
        if added_new:
            logger.info("Registered shared directory: %s", added_dir)
            print(f"Added shared directory: {added_dir}", file=out)
        else:
            print(f"Directory already shared: {added_dir}", file=out)
        return 0

    if action == "list":
        cfg = load_config(args.config)
        if not cfg.sharing.directories:
            print("No shared directories configured.", file=out)
            return 0
        for idx, shared_dir in enumerate(cfg.sharing.directories, start=1):
            status = "ok" if shared_dir.is_dir() else "missing"
            print(f"{idx}. {shared_dir} [{status}]", file=out)
        return 0

    if action == "remove":
        cfg, removed_dir, removed = remove_shared_directory(
            args.directory,
            explicit_config_path=args.config,
        )
        configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)
        logger = get_context_logger("cli", peer_id=cfg.node.peer_id)
        if removed:
            logger.info("Removed shared directory: %s", removed_dir)
            print(f"Removed shared directory: {removed_dir}", file=out)
            return 0
        print(f"Directory was not in shared list: {removed_dir}", file=err)
        return 1

    print(f"Unsupported share action: {action}", file=err)
    return 2


def _handle_logs(args: argparse.Namespace, out: TextIO, err: TextIO) -> int:
    cfg: AppConfig = load_config(args.config)
    log_path = cfg.logging.file_path
    if not log_path.exists():
        print(f"No log file found at {log_path}", file=out)
        return 0
    lines = read_recent_logs(log_path, lines=args.lines)
    if not lines:
        print(f"Log file is empty: {log_path}", file=out)
        return 0
    for line in lines:
        print(line, file=out)
    return 0


async def _run_dev_tcp_server(cfg: AppConfig, host: str, port: int, out: TextIO) -> int:
    server = TcpServer(
        host=host,
        port=port,
        max_connections=cfg.limits.max_simultaneous_connections,
        shutdown_timeout_sec=cfg.daemon.shutdown_timeout_sec,
        control_timeout_sec=cfg.network.control_timeout_sec,
        max_payload_bytes=cfg.limits.max_control_payload_bytes,
        peer_id=cfg.node.peer_id,
        display_name=cfg.node.display_name,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    async with server:
        print(
            f"[dev mode] TCP server listening on {host}:{server.bound_port} "
            f"(peer_id={cfg.node.peer_id}). Press Ctrl+C to stop.",
            file=out,
            flush=True,
        )
        await stop_event.wait()
    print("[dev mode] TCP server stopped.", file=out, flush=True)
    return 0


async def _run_dev_tcp_ping(
    cfg: AppConfig,
    host: str,
    port: int,
    out: TextIO,
) -> int:
    client = TcpClient(
        peer_id=cfg.node.peer_id,
        connect_timeout_sec=cfg.network.connect_timeout_sec,
        control_timeout_sec=cfg.network.control_timeout_sec,
        max_payload_bytes=cfg.limits.max_control_payload_bytes,
        max_data_chunk_bytes=cfg.limits.max_data_chunk_bytes,
    )
    started = monotonic_now()
    pong = await client.ping(host, port, sent_at_ms=int(started * 1000))
    elapsed_ms = (monotonic_now() - started) * 1000.0
    print(f"[dev mode] PONG from {host}:{port} in {elapsed_ms:.2f} ms:", file=out)
    print(json.dumps(asdict(pong), indent=2), file=out)
    return 0


async def _run_dev_peer_node(
    cfg: AppConfig,
    args: argparse.Namespace,
    out: TextIO,
) -> int:
    bind_host = args.host if args.host is not None else cfg.network.bind_host
    tcp_port = args.tcp_port if args.tcp_port is not None else cfg.network.tcp_port
    udp_port = args.udp_port if args.udp_port is not None else cfg.discovery.udp_port
    heartbeat = (
        args.heartbeat if args.heartbeat is not None else cfg.discovery.heartbeat_interval_sec
    )
    stale_timeout = (
        args.stale_timeout if args.stale_timeout is not None else cfg.discovery.stale_timeout_sec
    )
    remove_timeout = max(cfg.discovery.remove_timeout_sec, stale_timeout)
    udp_targets = [parse_host_port(item) for item in args.udp_target]
    static_peers = list(cfg.discovery.static_peers) + list(args.static_peer)

    server = TcpServer(
        host=bind_host,
        port=tcp_port,
        max_connections=cfg.limits.max_simultaneous_connections,
        shutdown_timeout_sec=cfg.daemon.shutdown_timeout_sec,
        control_timeout_sec=cfg.network.control_timeout_sec,
        max_payload_bytes=cfg.limits.max_control_payload_bytes,
        peer_id=cfg.node.peer_id,
        display_name=cfg.node.display_name,
    )
    registry = PeerRegistry(
        local_peer_id=cfg.node.peer_id,
        stale_timeout_sec=stale_timeout,
        remove_timeout_sec=remove_timeout,
    )

    def _on_peer_event(event: str, peer: PeerInfo) -> None:
        print(
            f"[event:{event}] peer_id={peer.peer_id} name={peer.display_name} "
            f"endpoint={peer.ip_address}:{peer.tcp_port} status={peer.status} source={peer.source}",
            file=out,
            flush=True,
        )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    async with server:
        discovery = DiscoveryService(
            local_peer_id=cfg.node.peer_id,
            display_name=cfg.node.display_name,
            tcp_port=server.bound_port,
            registry=registry,
            bind_host=bind_host,
            udp_port=udp_port,
            broadcast_address=cfg.discovery.broadcast_address,
            broadcast_enabled=cfg.discovery.enabled and not args.no_broadcast,
            heartbeat_interval_sec=heartbeat,
            static_peers=static_peers,
            discovery_targets=udp_targets,
            tcp_client=TcpClient(
                peer_id=cfg.node.peer_id,
                connect_timeout_sec=cfg.network.connect_timeout_sec,
                control_timeout_sec=cfg.network.control_timeout_sec,
                max_payload_bytes=cfg.limits.max_control_payload_bytes,
            ),
            on_event=_on_peer_event,
        )
        async with discovery:
            print(
                f"[dev mode] Peer node running: peer_id={cfg.node.peer_id} "
                f"name={cfg.node.display_name} tcp={bind_host}:{server.bound_port} "
                f"udp={bind_host}:{discovery.bound_port}. Press Ctrl+C to stop.",
                file=out,
                flush=True,
            )
            await stop_event.wait()
    print("[dev mode] Peer node stopped.", file=out, flush=True)
    return 0


def _handle_dev(
    args: argparse.Namespace,
    dev_parser: argparse.ArgumentParser,
    out: TextIO,
    err: TextIO,
) -> int:
    action = args.dev_action
    if action is None:
        dev_parser.print_help(file=out)
        return 2

    cfg = load_config(args.config)
    configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)

    if action == "tcp-server":
        bind_host = args.host if args.host is not None else cfg.network.bind_host
        bind_port = args.port if args.port is not None else cfg.network.tcp_port
        try:
            return asyncio.run(_run_dev_tcp_server(cfg, bind_host, bind_port, out))
        except KeyboardInterrupt:
            return 0
        except OSError as exc:
            print(f"Failed to start TCP server on {bind_host}:{bind_port}: {exc}", file=err)
            return 1

    if action == "tcp-ping":
        try:
            return asyncio.run(_run_dev_tcp_ping(cfg, args.host, args.port, out))
        except (OSError, TimeoutError) as exc:
            print(f"TCP request to {args.host}:{args.port} failed: {exc}", file=err)
            return 1

    if action == "peer-node":
        try:
            return asyncio.run(_run_dev_peer_node(cfg, args, out))
        except KeyboardInterrupt:
            return 0
        except OSError as exc:
            print(f"Failed to run peer node: {exc}", file=err)
            return 1

    print(f"Unsupported dev action: {action}", file=err)
    return 2


def run_cli(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Execute the CLI with optional explicit argv and output streams."""
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr
    parser, subparsers = _build_parser_and_subparsers()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command is None:
        parser.print_help(file=out)
        return 2

    try:
        if args.command == "config":
            return _handle_config(args, subparsers["config"], out, err)
        if args.command == "share":
            return _handle_share(args, subparsers["share"], out, err)
        if args.command == "logs":
            return _handle_logs(args, out, err)
        if args.command == "dev":
            return _handle_dev(args, subparsers["dev"], out, err)
        if args.command in _PLANNED_COMMANDS:
            milestone, description = _PLANNED_COMMANDS[args.command]
            print(
                f"Command '{args.command}' is not yet implemented "
                f"(scheduled for {milestone}: {description}).",
                file=err,
            )
            return 2

        print(f"Unknown command: {args.command}", file=err)
        return 2
    except P2PError as exc:
        print(f"Error: {exc}", file=err)
        return 1
    finally:
        shutdown_logging()
