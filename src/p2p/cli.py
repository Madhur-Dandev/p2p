"""Command-line interface for the Decentralized P2P File Discovery and Transfer System."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
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

_PLANNED_COMMANDS: dict[str, tuple[str, str]] = {
    "start": ("Milestone 2/3/11", "TCP server, UDP discovery, and daemon lifecycle"),
    "stop": ("Milestone 11", "Daemon IPC shutdown control"),
    "status": ("Milestone 11", "Daemon IPC status reporting"),
    "peers": ("Milestone 3 & 11", "UDP peer discovery, peer registry, and daemon IPC"),
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
            "(Milestone 1: foundation, configuration, logging, and CLI)."
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

    # Planned milestone commands (defined honestly until their milestone is implemented)
    subparsers.add_parser(
        "start",
        help="Start the P2P node daemon [planned: Milestone 2/3/11].",
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

    return parser, {"config": config_parser, "share": share_parser}


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
        if args.command in _PLANNED_COMMANDS:
            milestone, description = _PLANNED_COMMANDS[args.command]
            print(
                f"Command '{args.command}' is not yet implemented in Milestone 1 "
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
