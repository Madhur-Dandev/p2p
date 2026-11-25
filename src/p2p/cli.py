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
from p2p.constants import APP_VERSION, MAX_SEARCH_RESULTS_PER_PEER, VALID_LOG_LEVELS
from p2p.exceptions import P2PError
from p2p.indexing.file_index import FileIndexer
from p2p.logging_config import (
    configure_logging,
    get_context_logger,
    read_recent_logs,
    shutdown_logging,
)
from p2p.network.discovery import DiscoveryService, parse_host_port
from p2p.network.peer_registry import PeerInfo, PeerRegistry, PeerStatus
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import TcpServer
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRepository, PeerRepository
from p2p.transfer.progress import TransferProgress
from p2p.transfer.transfer_manager import TransferManager
from p2p.utils.time_utils import monotonic_now

_PLANNED_COMMANDS: dict[str, tuple[str, str]] = {
    "start": ("Milestone 11", "Background daemon lifecycle and IPC (use 'dev peer-node' now)"),
    "stop": ("Milestone 11", "Daemon IPC shutdown control"),
    "status": ("Milestone 11", "Daemon IPC status reporting"),
    "peers": ("Milestone 11", "Query daemon peer registry over IPC (use 'dev peer-node' now)"),
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
            "(Milestones 1–6 & 8: config, TCP/UDP, SQLite indexing, local search, direct transfer)."
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
        help="Manage the local shared-file index.",
    )
    index_sub = index_parser.add_subparsers(dest="index_action", metavar="ACTION")
    index_rebuild = index_sub.add_parser("rebuild", help="Rebuild the local file index.")
    index_rebuild.add_argument(
        "--force-rehash",
        action="store_true",
        help="Recompute SHA-256 hashes for all files regardless of cached size/mtime.",
    )

    search_parser = subparsers.add_parser(
        "search",
        help="Search for files (local SQLite index now; remote peer search in Milestone 7).",
    )
    search_parser.add_argument("query", help="Filename or substring to search for.")
    search_parser.add_argument(
        "--exact",
        action="store_true",
        help="Match exact filename (case-insensitive) instead of substring.",
    )
    search_parser.add_argument(
        "--limit",
        type=int,
        default=MAX_SEARCH_RESULTS_PER_PEER,
        help=f"Maximum results to return (default: {MAX_SEARCH_RESULTS_PER_PEER}).",
    )

    download_parser = subparsers.add_parser(
        "download",
        help="Download a file by file ID directly over TCP from a peer.",
    )
    download_parser.add_argument("file_id", help="Target file identifier.")
    download_parser.add_argument(
        "--peer",
        default=None,
        metavar="HOST:PORT",
        help="Source peer TCP endpoint (required until Milestone 7 & 11 daemon lookup).",
    )
    download_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Override configured storage.download_dir destination.",
    )

    downloads_parser = subparsers.add_parser(
        "downloads",
        help="List download history and transfer states stored in SQLite.",
    )
    downloads_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum history rows to display (default: 50).",
    )

    return parser, {
        "config": config_parser,
        "share": share_parser,
        "dev": dev_parser,
        "index": index_parser,
    }


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


def _handle_index(
    args: argparse.Namespace,
    index_parser: argparse.ArgumentParser,
    out: TextIO,
    err: TextIO,
) -> int:
    action = args.index_action
    if action is None:
        index_parser.print_help(file=out)
        return 2

    cfg = load_config(args.config)
    configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)

    if action == "rebuild":
        with Database(cfg.storage.database_path) as db:
            indexer = FileIndexer(
                db,
                shared_directories=cfg.sharing.directories,
                excluded_dirs=cfg.sharing.excluded_dirs,
                follow_symlinks=cfg.sharing.follow_symlinks,
                hash_chunk_size=cfg.transfer.chunk_size_bytes,
                max_filename_length=cfg.limits.max_filename_length,
            )
            summary = indexer.rebuild(force_rehash=args.force_rehash)
        print(
            f"Index rebuilt: scanned={summary.scanned} added={summary.added} "
            f"updated={summary.updated} cached={summary.unchanged_cached} "
            f"removed={summary.removed} symlinks_skipped={summary.skipped_symlinks} "
            f"errors={summary.skipped_errors} duration={summary.duration_sec:.3f}s",
            file=out,
        )
        return 0

    print(f"Unsupported index action: {action}", file=err)
    return 2


def _handle_search(args: argparse.Namespace, out: TextIO) -> int:
    cfg = load_config(args.config)
    configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)

    with Database(cfg.storage.database_path) as db:
        service = LocalSearchService(
            db,
            peer_id=cfg.node.peer_id,
            max_query_length=cfg.limits.max_query_length,
        )
        matches, truncated = service.search(
            args.query,
            exact=args.exact,
            max_results=args.limit,
        )

    if not matches:
        print(
            f"No local files matched {args.query!r} (remote peer search arrives in Milestone 7).",
            file=out,
        )
        return 0

    print(
        f"Local search matches for {args.query!r} ({len(matches)} shown"
        f"{', truncated' if truncated else ''}):",
        file=out,
    )
    for item in matches:
        print(
            f"  file_id={item.file_id} | name={item.file_name} | "
            f"size={item.file_size} B | sha256={item.sha256[:12]}... | path={item.relative_path}",
            file=out,
        )
    return 0


async def _run_download(cfg: AppConfig, args: argparse.Namespace, out: TextIO) -> int:
    host, port = parse_host_port(args.peer)

    last_percent = -10.0

    def _on_progress(progress: TransferProgress) -> None:
        nonlocal last_percent
        if progress.percent >= last_percent + 20.0 or progress.percent >= 100.0:
            last_percent = progress.percent
            print(
                f"  [{progress.state.value}] {progress.file_name}: "
                f"{progress.bytes_transferred}/{progress.total_bytes} B "
                f"({progress.percent:.1f}%, {progress.throughput_mbps:.2f} Mbps)",
                file=out,
                flush=True,
            )

    with Database(cfg.storage.database_path) as db:
        manager = TransferManager(cfg, db)
        result = await manager.download_file(
            host=host,
            port=port,
            file_id=args.file_id,
            on_progress=_on_progress,
        )

    print(
        f"Download completed: {result.destination_path} "
        f"({result.bytes_downloaded} B, {result.throughput_mbps:.2f} Mbps, "
        f"sha256={result.actual_sha256})",
        file=out,
    )
    return 0


def _handle_download(args: argparse.Namespace, out: TextIO, err: TextIO) -> int:
    if args.peer is None:
        print(
            "Option --peer HOST:PORT is required until Milestone 7 (distributed search) "
            "and Milestone 11 (daemon IPC) are implemented.",
            file=err,
        )
        return 2

    cfg = load_config(args.config)
    if args.output_dir is not None:
        cfg.storage.download_dir = Path(args.output_dir).expanduser().resolve(strict=False)
    configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)
    return asyncio.run(_run_download(cfg, args, out))


def _handle_downloads(args: argparse.Namespace, out: TextIO) -> int:
    cfg = load_config(args.config)
    configure_logging(cfg.logging, node_peer_id=cfg.node.peer_id, level_override=args.log_level)

    with Database(cfg.storage.database_path) as db:
        records = DownloadRepository(db).list_downloads(limit=args.limit)

    if not records:
        print("No download history recorded.", file=out)
        return 0

    for rec in records:
        err_suffix = f" error={rec.error_message!r}" if rec.error_message else ""
        print(
            f"transfer_id={rec.transfer_id} | file={rec.file_name} ({rec.file_id}) | "
            f"status={rec.status} | bytes={rec.bytes_downloaded}/{rec.expected_size} | "
            f"dest={rec.destination_path or '-'} | updated={rec.updated_at}{err_suffix}",
            file=out,
        )
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

    with Database(cfg.storage.database_path) as db:
        indexer = FileIndexer(
            db,
            shared_directories=cfg.sharing.directories,
            excluded_dirs=cfg.sharing.excluded_dirs,
            follow_symlinks=cfg.sharing.follow_symlinks,
            hash_chunk_size=cfg.transfer.chunk_size_bytes,
            max_filename_length=cfg.limits.max_filename_length,
        )
        if cfg.sharing.directories:
            await indexer.rebuild_async()

        search_service = LocalSearchService(
            db,
            peer_id=cfg.node.peer_id,
            max_query_length=cfg.limits.max_query_length,
        )
        transfer_manager = TransferManager(cfg, db, search_service=search_service)
        peer_repo = PeerRepository(db)

        server = TcpServer(
            host=bind_host,
            port=tcp_port,
            max_connections=cfg.limits.max_simultaneous_connections,
            shutdown_timeout_sec=cfg.daemon.shutdown_timeout_sec,
            control_timeout_sec=cfg.network.control_timeout_sec,
            max_payload_bytes=cfg.limits.max_control_payload_bytes,
            max_data_chunk_bytes=cfg.limits.max_data_chunk_bytes,
            peer_id=cfg.node.peer_id,
            display_name=cfg.node.display_name,
            handler=transfer_manager.serve_connection,
        )
        registry = PeerRegistry(
            local_peer_id=cfg.node.peer_id,
            stale_timeout_sec=stale_timeout,
            remove_timeout_sec=remove_timeout,
        )

        def _on_peer_event(event: str, peer: PeerInfo) -> None:
            if peer.status == PeerStatus.REMOVED:
                peer_repo.update_status(peer.peer_id, PeerStatus.REMOVED)
            else:
                peer_repo.upsert_peer(peer)
            print(
                f"[event:{event}] peer_id={peer.peer_id} name={peer.display_name} "
                f"endpoint={peer.ip_address}:{peer.tcp_port} status={peer.status} "
                f"source={peer.source}",
                file=out,
                flush=True,
            )

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, stop_event.set)

        try:
            async with server:
                transfer_manager.upload_service.set_tcp_port(server.bound_port)
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
                    tcp_client=transfer_manager.tcp_client,
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
        finally:
            transfer_manager.cancel_all()
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
        if args.command == "index":
            return _handle_index(args, subparsers["index"], out, err)
        if args.command == "search":
            return _handle_search(args, out)
        if args.command == "download":
            return _handle_download(args, out, err)
        if args.command == "downloads":
            return _handle_downloads(args, out)
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
