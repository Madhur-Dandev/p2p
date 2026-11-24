"""Centralized protocol, network, security, and storage constants."""

from __future__ import annotations

import struct
from pathlib import Path

APP_NAME = "p2p"
APP_VERSION = "0.1.0"

# Binary frame specification (big-endian / network byte order):
# magic (2B) | version (1B) | msg_type (1B) | flags (2B) | request_id (16B) | payload_len (4B)
PROTOCOL_MAGIC = b"P2"
PROTOCOL_VERSION = 1
HEADER_FORMAT = "!2sBBH16sI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 26 bytes

# Frame flags bitmask
FLAG_NONE = 0x0000
FLAG_END_OF_STREAM = 0x0001

# Default network endpoints and discovery intervals
DEFAULT_BIND_HOST = "0.0.0.0"  # noqa: S104  # Intentional: LAN P2P listener binds all interfaces
DEFAULT_TCP_PORT = 9000
DEFAULT_DISCOVERY_PORT = 9999
DEFAULT_BROADCAST_ADDRESS = "255.255.255.255"
DEFAULT_HEARTBEAT_INTERVAL_SEC = 10.0
DEFAULT_PEER_STALE_TIMEOUT_SEC = 35.0
DEFAULT_PEER_REMOVE_TIMEOUT_SEC = 300.0

# Network operation timeouts (seconds)
DEFAULT_CONNECT_TIMEOUT_SEC = 5.0
DEFAULT_CONTROL_TIMEOUT_SEC = 5.0
DEFAULT_SEARCH_TIMEOUT_SEC = 3.0
DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC = 30.0
DEFAULT_SHUTDOWN_TIMEOUT_SEC = 10.0

# Protocol and resource security limits
MAX_CONTROL_PAYLOAD_BYTES = 65_536  # 64 KiB cap for JSON control messages
MAX_DATA_CHUNK_BYTES = 262_144  # 256 KiB cap for individual FILE_DATA frames
DEFAULT_TRANSFER_CHUNK_BYTES = 65_536  # 64 KiB default streaming chunk size
MAX_FRAME_PAYLOAD_BYTES = 1_048_576  # 1 MiB absolute framing ceiling
MAX_UDP_DATAGRAM_BYTES = 1_200  # Avoids IP fragmentation on standard 1500B Ethernet MTU
MAX_SIMULTANEOUS_CONNECTIONS = 64
MAX_CONCURRENT_TRANSFERS = 8
MAX_OUTSTANDING_REQUESTS = 128
MAX_QUERY_LENGTH = 256
MAX_FILENAME_LENGTH = 255
MAX_SEARCH_RESULTS_PER_PEER = 100
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024  # 10 GiB
MAX_UNIX_SOCKET_PATH_BYTES = 100  # Safely below macOS 104-byte sockaddr_un.sun_path limit

# Filesystem defaults (computed dynamically via Path.home() helpers so $HOME changes apply)
DEFAULT_DB_FILENAME = "p2p.db"
DEFAULT_LOG_FILENAME = "p2p.log"
DEFAULT_SOCKET_FILENAME = "p2p.sock"
DEFAULT_PID_FILENAME = "p2p.pid"


def default_config_dir() -> Path:
    """Return the default configuration directory for the active user."""
    return Path.home() / ".config" / APP_NAME


def default_config_path() -> Path:
    """Return the default configuration file path for the active user."""
    return default_config_dir() / "config.toml"


def default_data_dir() -> Path:
    """Return the default persistent data directory for the active user."""
    return Path.home() / ".local" / "share" / APP_NAME


def default_state_dir() -> Path:
    """Return the default runtime state directory for the active user."""
    return Path.home() / ".local" / "state" / APP_NAME


def default_download_dir() -> Path:
    """Return the default download destination directory for the active user."""
    return Path.home() / "Downloads" / APP_NAME


DEFAULT_EXCLUDED_DIRS: tuple[str, ...] = (
    ".git",
    "__pycache__",
    ".venv",
    "node_modules",
    ".DS_Store",
)

# Logging defaults
VALID_LOG_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MiB
DEFAULT_LOG_BACKUP_COUNT = 5
