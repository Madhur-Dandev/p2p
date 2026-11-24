"""Configuration management using Python's tomllib and typed dataclasses."""

from __future__ import annotations

import ipaddress
import os
import socket
import tempfile
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from p2p.constants import (
    DEFAULT_BIND_HOST,
    DEFAULT_BROADCAST_ADDRESS,
    DEFAULT_CONNECT_TIMEOUT_SEC,
    DEFAULT_CONTROL_TIMEOUT_SEC,
    DEFAULT_DB_FILENAME,
    DEFAULT_DISCOVERY_PORT,
    DEFAULT_EXCLUDED_DIRS,
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    DEFAULT_LOG_BACKUP_COUNT,
    DEFAULT_LOG_FILENAME,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_MAX_BYTES,
    DEFAULT_PEER_REMOVE_TIMEOUT_SEC,
    DEFAULT_PEER_STALE_TIMEOUT_SEC,
    DEFAULT_PID_FILENAME,
    DEFAULT_SEARCH_TIMEOUT_SEC,
    DEFAULT_SHUTDOWN_TIMEOUT_SEC,
    DEFAULT_SOCKET_FILENAME,
    DEFAULT_TCP_PORT,
    DEFAULT_TRANSFER_CHUNK_BYTES,
    DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC,
    MAX_CONCURRENT_TRANSFERS,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_FILE_SIZE_BYTES,
    MAX_FILENAME_LENGTH,
    MAX_OUTSTANDING_REQUESTS,
    MAX_QUERY_LENGTH,
    MAX_SIMULTANEOUS_CONNECTIONS,
    MAX_UNIX_SOCKET_PATH_BYTES,
    VALID_LOG_LEVELS,
    default_config_path,
    default_data_dir,
    default_download_dir,
    default_state_dir,
)
from p2p.exceptions import ConfigError, ValidationError
from p2p.utils.identifiers import generate_peer_id, validate_peer_id

CONFIG_ENV_VAR = "P2P_CONFIG"

_ALLOWED_SECTIONS = frozenset(
    {
        "node",
        "network",
        "discovery",
        "storage",
        "sharing",
        "transfer",
        "limits",
        "logging",
        "daemon",
    }
)

_ALLOWED_KEYS: dict[str, frozenset[str]] = {
    "node": frozenset({"peer_id", "display_name"}),
    "network": frozenset(
        {
            "bind_host",
            "tcp_port",
            "advertise_host",
            "connect_timeout_sec",
            "control_timeout_sec",
        }
    ),
    "discovery": frozenset(
        {
            "enabled",
            "udp_port",
            "broadcast_address",
            "heartbeat_interval_sec",
            "stale_timeout_sec",
            "remove_timeout_sec",
            "static_peers",
        }
    ),
    "storage": frozenset({"data_dir", "state_dir", "database_path", "download_dir"}),
    "sharing": frozenset({"directories", "excluded_dirs", "follow_symlinks"}),
    "transfer": frozenset(
        {
            "chunk_size_bytes",
            "idle_timeout_sec",
            "search_timeout_sec",
            "overwrite_existing",
        }
    ),
    "limits": frozenset(
        {
            "max_control_payload_bytes",
            "max_data_chunk_bytes",
            "max_simultaneous_connections",
            "max_concurrent_transfers",
            "max_outstanding_requests",
            "max_query_length",
            "max_filename_length",
            "max_file_size_bytes",
        }
    ),
    "logging": frozenset({"level", "file_path", "max_bytes", "backup_count", "console_enabled"}),
    "daemon": frozenset({"socket_path", "pid_path", "shutdown_timeout_sec"}),
}


def _default_display_name() -> str:
    hostname = socket.gethostname().strip()
    return hostname or "p2p-node"


def _expand_path(raw: str | Path) -> Path:
    return Path(os.path.expanduser(str(raw))).resolve(strict=False)


def _require_str(mapping: dict[str, Any], key: str, default: str) -> str:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, str):
        raise ConfigError(f"Expected string for '{key}', got {type(value).__name__}")
    return value


def _require_bool(mapping: dict[str, Any], key: str, default: bool) -> bool:
    if key not in mapping:
        return default
    value = mapping[key]
    if not isinstance(value, bool):
        raise ConfigError(f"Expected boolean for '{key}', got {type(value).__name__}")
    return value


def _require_int(mapping: dict[str, Any], key: str, default: int) -> int:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Expected integer for '{key}', got {type(value).__name__}")
    return value


def _require_float(mapping: dict[str, Any], key: str, default: float) -> float:
    if key not in mapping:
        return default
    value = mapping[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"Expected number for '{key}', got {type(value).__name__}")
    return float(value)


def _require_str_list(mapping: dict[str, Any], key: str, default: list[str]) -> list[str]:
    if key not in mapping:
        return list(default)
    value = mapping[key]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"Expected list of strings for '{key}'")
    return list(value)


def _validate_port(port: int, field_name: str) -> None:
    if not (1 <= port <= 65_535):
        raise ConfigError(f"{field_name} must be between 1 and 65535, got {port}")


def _validate_static_peer(entry: str) -> None:
    candidate = entry.strip()
    if not candidate:
        raise ConfigError("static_peers entries must not be empty")
    if candidate.startswith("["):
        closing = candidate.find("]:")
        if closing == -1:
            raise ConfigError(
                f"Invalid IPv6 static_peer format (expected '[host]:port'): {entry!r}"
            )
        host = candidate[1:closing].strip()
        port_raw = candidate[closing + 2 :]
    else:
        if ":" not in candidate:
            raise ConfigError(f"Invalid static_peer format (expected 'host:port'): {entry!r}")
        host, port_raw = candidate.rsplit(":", 1)
        host = host.strip()
    if not host:
        raise ConfigError(f"Missing host in static_peer entry: {entry!r}")
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ConfigError(f"Invalid port in static_peer entry {entry!r}: {port_raw!r}") from exc
    _validate_port(port, f"static_peer port ({entry})")


def _toml_quote(value: str) -> str:
    escaped: list[str] = ['"']
    for ch in value:
        code = ord(ch)
        if ch == "\\":
            escaped.append("\\\\")
        elif ch == '"':
            escaped.append('\\"')
        elif ch == "\b":
            escaped.append("\\b")
        elif ch == "\t":
            escaped.append("\\t")
        elif ch == "\n":
            escaped.append("\\n")
        elif ch == "\f":
            escaped.append("\\f")
        elif ch == "\r":
            escaped.append("\\r")
        elif code < 0x20 or code == 0x7F:
            escaped.append(f"\\u{code:04x}")
        else:
            escaped.append(ch)
    escaped.append('"')
    return "".join(escaped)


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Path):
        return _toml_quote(str(value))
    if isinstance(value, str):
        return _toml_quote(value)
    if isinstance(value, list):
        rendered = ", ".join(_toml_value(item) for item in value)
        return f"[{rendered}]"
    raise ConfigError(f"Unsupported TOML serialization type: {type(value).__name__}")


@dataclass(slots=True)
class NodeConfig:
    """Identity settings for the local peer."""

    peer_id: str = field(default_factory=generate_peer_id)
    display_name: str = field(default_factory=_default_display_name)


@dataclass(slots=True)
class NetworkConfig:
    """TCP listener and connection timing settings."""

    bind_host: str = DEFAULT_BIND_HOST
    tcp_port: int = DEFAULT_TCP_PORT
    advertise_host: str | None = None
    connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT_SEC
    control_timeout_sec: float = DEFAULT_CONTROL_TIMEOUT_SEC


@dataclass(slots=True)
class DiscoveryConfig:
    """UDP discovery, heartbeat, and static fallback settings."""

    enabled: bool = True
    udp_port: int = DEFAULT_DISCOVERY_PORT
    broadcast_address: str = DEFAULT_BROADCAST_ADDRESS
    heartbeat_interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL_SEC
    stale_timeout_sec: float = DEFAULT_PEER_STALE_TIMEOUT_SEC
    remove_timeout_sec: float = DEFAULT_PEER_REMOVE_TIMEOUT_SEC
    static_peers: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StorageConfig:
    """Persistent database, state, and download directory locations."""

    data_dir: Path = field(default_factory=lambda: _expand_path(default_data_dir()))
    state_dir: Path = field(default_factory=lambda: _expand_path(default_state_dir()))
    database_path: Path = field(
        default_factory=lambda: _expand_path(default_data_dir() / DEFAULT_DB_FILENAME)
    )
    download_dir: Path = field(default_factory=lambda: _expand_path(default_download_dir()))


@dataclass(slots=True)
class SharingConfig:
    """Authorized shared directories and indexer exclusions."""

    directories: list[Path] = field(default_factory=list)
    excluded_dirs: list[str] = field(default_factory=lambda: list(DEFAULT_EXCLUDED_DIRS))
    follow_symlinks: bool = False


@dataclass(slots=True)
class TransferConfig:
    """Streaming transfer and search timing parameters."""

    chunk_size_bytes: int = DEFAULT_TRANSFER_CHUNK_BYTES
    idle_timeout_sec: float = DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC
    search_timeout_sec: float = DEFAULT_SEARCH_TIMEOUT_SEC
    overwrite_existing: bool = False


@dataclass(slots=True)
class LimitsConfig:
    """Security ceilings protecting memory, file handles, and event loop stability."""

    max_control_payload_bytes: int = MAX_CONTROL_PAYLOAD_BYTES
    max_data_chunk_bytes: int = MAX_DATA_CHUNK_BYTES
    max_simultaneous_connections: int = MAX_SIMULTANEOUS_CONNECTIONS
    max_concurrent_transfers: int = MAX_CONCURRENT_TRANSFERS
    max_outstanding_requests: int = MAX_OUTSTANDING_REQUESTS
    max_query_length: int = MAX_QUERY_LENGTH
    max_filename_length: int = MAX_FILENAME_LENGTH
    max_file_size_bytes: int = MAX_FILE_SIZE_BYTES


@dataclass(slots=True)
class LoggingConfig:
    """Structured logging configuration."""

    level: str = DEFAULT_LOG_LEVEL
    file_path: Path = field(
        default_factory=lambda: _expand_path(default_state_dir() / DEFAULT_LOG_FILENAME)
    )
    max_bytes: int = DEFAULT_LOG_MAX_BYTES
    backup_count: int = DEFAULT_LOG_BACKUP_COUNT
    console_enabled: bool = True


@dataclass(slots=True)
class DaemonConfig:
    """Background daemon IPC and lifecycle settings."""

    socket_path: Path = field(
        default_factory=lambda: _expand_path(default_state_dir() / DEFAULT_SOCKET_FILENAME)
    )
    pid_path: Path = field(
        default_factory=lambda: _expand_path(default_state_dir() / DEFAULT_PID_FILENAME)
    )
    shutdown_timeout_sec: float = DEFAULT_SHUTDOWN_TIMEOUT_SEC


@dataclass(slots=True)
class AppConfig:
    """Complete validated runtime configuration for a P2P node."""

    node: NodeConfig = field(default_factory=NodeConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    sharing: SharingConfig = field(default_factory=SharingConfig)
    transfer: TransferConfig = field(default_factory=TransferConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    daemon: DaemonConfig = field(default_factory=DaemonConfig)
    config_path: Path | None = None

    def validate(self) -> None:
        """Validate all configuration fields and cross-field invariants."""
        try:
            self.node.peer_id = validate_peer_id(self.node.peer_id)
        except ValidationError as exc:
            raise ConfigError(f"Invalid node.peer_id: {exc}") from exc

        if not self.node.display_name.strip():
            raise ConfigError("node.display_name must not be empty")

        if not self.network.bind_host.strip():
            raise ConfigError("network.bind_host must not be empty")
        _validate_port(self.network.tcp_port, "network.tcp_port")
        if self.network.advertise_host is not None and not self.network.advertise_host.strip():
            self.network.advertise_host = None
        if self.network.connect_timeout_sec <= 0:
            raise ConfigError("network.connect_timeout_sec must be > 0")
        if self.network.control_timeout_sec <= 0:
            raise ConfigError("network.control_timeout_sec must be > 0")

        _validate_port(self.discovery.udp_port, "discovery.udp_port")
        try:
            ipaddress.IPv4Address(self.discovery.broadcast_address)
        except ipaddress.AddressValueError as exc:
            raise ConfigError(
                f"discovery.broadcast_address must be a valid IPv4 address, "
                f"got {self.discovery.broadcast_address!r}"
            ) from exc
        if self.discovery.heartbeat_interval_sec <= 0:
            raise ConfigError("discovery.heartbeat_interval_sec must be > 0")
        if self.discovery.stale_timeout_sec <= self.discovery.heartbeat_interval_sec:
            raise ConfigError(
                "discovery.stale_timeout_sec must be strictly greater than "
                "discovery.heartbeat_interval_sec"
            )
        if self.discovery.remove_timeout_sec < self.discovery.stale_timeout_sec:
            raise ConfigError("discovery.remove_timeout_sec must be >= discovery.stale_timeout_sec")
        for peer_entry in self.discovery.static_peers:
            _validate_static_peer(peer_entry)

        if self.limits.max_control_payload_bytes <= 0:
            raise ConfigError("limits.max_control_payload_bytes must be > 0")
        if self.limits.max_data_chunk_bytes < 1024:
            raise ConfigError("limits.max_data_chunk_bytes must be >= 1024")
        if self.limits.max_simultaneous_connections <= 0:
            raise ConfigError("limits.max_simultaneous_connections must be > 0")
        if self.limits.max_concurrent_transfers <= 0:
            raise ConfigError("limits.max_concurrent_transfers must be > 0")
        if self.limits.max_outstanding_requests <= 0:
            raise ConfigError("limits.max_outstanding_requests must be > 0")
        if self.limits.max_query_length <= 0:
            raise ConfigError("limits.max_query_length must be > 0")
        if self.limits.max_filename_length <= 0:
            raise ConfigError("limits.max_filename_length must be > 0")
        if self.limits.max_file_size_bytes < 0:
            raise ConfigError("limits.max_file_size_bytes must be >= 0")

        if not (1024 <= self.transfer.chunk_size_bytes <= self.limits.max_data_chunk_bytes):
            raise ConfigError(
                f"transfer.chunk_size_bytes must be between 1024 and "
                f"{self.limits.max_data_chunk_bytes}, got {self.transfer.chunk_size_bytes}"
            )
        if self.transfer.idle_timeout_sec <= 0:
            raise ConfigError("transfer.idle_timeout_sec must be > 0")
        if self.transfer.search_timeout_sec <= 0:
            raise ConfigError("transfer.search_timeout_sec must be > 0")

        normalized_level = self.logging.level.upper()
        if normalized_level not in VALID_LOG_LEVELS:
            raise ConfigError(
                f"logging.level must be one of {VALID_LOG_LEVELS}, got {self.logging.level!r}"
            )
        self.logging.level = normalized_level
        if self.logging.max_bytes <= 0:
            raise ConfigError("logging.max_bytes must be > 0")
        if self.logging.backup_count < 0:
            raise ConfigError("logging.backup_count must be >= 0")

        if self.daemon.shutdown_timeout_sec <= 0:
            raise ConfigError("daemon.shutdown_timeout_sec must be > 0")
        socket_bytes = len(str(self.daemon.socket_path).encode("utf-8"))
        if socket_bytes > MAX_UNIX_SOCKET_PATH_BYTES:
            raise ConfigError(
                f"daemon.socket_path exceeds {MAX_UNIX_SOCKET_PATH_BYTES} UTF-8 bytes "
                f"({socket_bytes} bytes): {self.daemon.socket_path}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration into plain JSON/TOML-friendly dictionaries."""
        raw = asdict(self)
        raw.pop("config_path", None)

        def _convert(obj: Any) -> Any:
            if isinstance(obj, Path):
                return str(obj)
            if isinstance(obj, list):
                return [_convert(item) for item in obj]
            if isinstance(obj, dict):
                return {key: _convert(val) for key, val in obj.items()}
            return obj

        converted: dict[str, Any] = _convert(raw)
        if converted["network"]["advertise_host"] is None:
            converted["network"]["advertise_host"] = ""
        return converted

    def to_toml(self) -> str:
        """Serialize the configuration into a valid TOML document string."""
        data = self.to_dict()
        lines: list[str] = [
            "# P2P File Discovery and Transfer System configuration",
            "",
        ]
        for section, values in data.items():
            lines.append(f"[{section}]")
            for key, value in values.items():
                lines.append(f"{key} = {_toml_value(value)}")
            lines.append("")
        return "\n".join(lines)


def resolve_config_path(explicit_path: Path | None = None) -> Path:
    """Resolve configuration file path from CLI flag, environment variable, or default."""
    if explicit_path is not None:
        return _expand_path(explicit_path)
    env_value = os.environ.get(CONFIG_ENV_VAR, "").strip()
    if env_value:
        return _expand_path(env_value)
    return _expand_path(default_config_path())


def create_default_config(
    *,
    peer_id: str | None = None,
    config_path: Path | None = None,
) -> AppConfig:
    """Construct a validated default AppConfig instance."""
    config = AppConfig(
        node=NodeConfig(peer_id=peer_id or generate_peer_id()),
        config_path=config_path,
    )
    config.validate()
    return config


def parse_config_dict(raw: dict[str, Any], *, config_path: Path | None = None) -> AppConfig:
    """Parse a raw TOML dictionary into a validated AppConfig."""
    unknown_sections = set(raw.keys()) - _ALLOWED_SECTIONS
    if unknown_sections:
        joined = ", ".join(sorted(unknown_sections))
        raise ConfigError(f"Unknown configuration section(s): {joined}")

    sections: dict[str, dict[str, Any]] = {}
    for section in _ALLOWED_SECTIONS:
        section_val = raw.get(section, {})
        if not isinstance(section_val, dict):
            raise ConfigError(f"Section [{section}] must be a TOML table")
        unknown_keys = set(section_val.keys()) - _ALLOWED_KEYS[section]
        if unknown_keys:
            joined = ", ".join(sorted(unknown_keys))
            raise ConfigError(f"Unknown key(s) in [{section}]: {joined}")
        sections[section] = section_val

    node_sec = sections["node"]
    net_sec = sections["network"]
    disc_sec = sections["discovery"]
    stor_sec = sections["storage"]
    share_sec = sections["sharing"]
    xfer_sec = sections["transfer"]
    lim_sec = sections["limits"]
    log_sec = sections["logging"]
    daemon_sec = sections["daemon"]

    data_dir = _expand_path(_require_str(stor_sec, "data_dir", str(default_data_dir())))
    state_dir = _expand_path(_require_str(stor_sec, "state_dir", str(default_state_dir())))
    database_path = _expand_path(
        _require_str(stor_sec, "database_path", str(data_dir / DEFAULT_DB_FILENAME))
    )
    download_dir = _expand_path(_require_str(stor_sec, "download_dir", str(default_download_dir())))

    raw_advertise = _require_str(net_sec, "advertise_host", "")
    advertise_host = raw_advertise.strip() or None

    raw_share_dirs = _require_str_list(share_sec, "directories", [])
    share_dirs: list[Path] = []
    seen_dirs: set[Path] = set()
    for raw_dir in raw_share_dirs:
        expanded = _expand_path(raw_dir)
        if expanded not in seen_dirs:
            seen_dirs.add(expanded)
            share_dirs.append(expanded)

    config = AppConfig(
        node=NodeConfig(
            peer_id=_require_str(node_sec, "peer_id", generate_peer_id()),
            display_name=_require_str(node_sec, "display_name", _default_display_name()),
        ),
        network=NetworkConfig(
            bind_host=_require_str(net_sec, "bind_host", DEFAULT_BIND_HOST),
            tcp_port=_require_int(net_sec, "tcp_port", DEFAULT_TCP_PORT),
            advertise_host=advertise_host,
            connect_timeout_sec=_require_float(
                net_sec, "connect_timeout_sec", DEFAULT_CONNECT_TIMEOUT_SEC
            ),
            control_timeout_sec=_require_float(
                net_sec, "control_timeout_sec", DEFAULT_CONTROL_TIMEOUT_SEC
            ),
        ),
        discovery=DiscoveryConfig(
            enabled=_require_bool(disc_sec, "enabled", True),
            udp_port=_require_int(disc_sec, "udp_port", DEFAULT_DISCOVERY_PORT),
            broadcast_address=_require_str(
                disc_sec, "broadcast_address", DEFAULT_BROADCAST_ADDRESS
            ),
            heartbeat_interval_sec=_require_float(
                disc_sec, "heartbeat_interval_sec", DEFAULT_HEARTBEAT_INTERVAL_SEC
            ),
            stale_timeout_sec=_require_float(
                disc_sec, "stale_timeout_sec", DEFAULT_PEER_STALE_TIMEOUT_SEC
            ),
            remove_timeout_sec=_require_float(
                disc_sec, "remove_timeout_sec", DEFAULT_PEER_REMOVE_TIMEOUT_SEC
            ),
            static_peers=_require_str_list(disc_sec, "static_peers", []),
        ),
        storage=StorageConfig(
            data_dir=data_dir,
            state_dir=state_dir,
            database_path=database_path,
            download_dir=download_dir,
        ),
        sharing=SharingConfig(
            directories=share_dirs,
            excluded_dirs=_require_str_list(
                share_sec, "excluded_dirs", list(DEFAULT_EXCLUDED_DIRS)
            ),
            follow_symlinks=_require_bool(share_sec, "follow_symlinks", False),
        ),
        transfer=TransferConfig(
            chunk_size_bytes=_require_int(
                xfer_sec, "chunk_size_bytes", DEFAULT_TRANSFER_CHUNK_BYTES
            ),
            idle_timeout_sec=_require_float(
                xfer_sec, "idle_timeout_sec", DEFAULT_TRANSFER_IDLE_TIMEOUT_SEC
            ),
            search_timeout_sec=_require_float(
                xfer_sec, "search_timeout_sec", DEFAULT_SEARCH_TIMEOUT_SEC
            ),
            overwrite_existing=_require_bool(xfer_sec, "overwrite_existing", False),
        ),
        limits=LimitsConfig(
            max_control_payload_bytes=_require_int(
                lim_sec, "max_control_payload_bytes", MAX_CONTROL_PAYLOAD_BYTES
            ),
            max_data_chunk_bytes=_require_int(
                lim_sec, "max_data_chunk_bytes", MAX_DATA_CHUNK_BYTES
            ),
            max_simultaneous_connections=_require_int(
                lim_sec, "max_simultaneous_connections", MAX_SIMULTANEOUS_CONNECTIONS
            ),
            max_concurrent_transfers=_require_int(
                lim_sec, "max_concurrent_transfers", MAX_CONCURRENT_TRANSFERS
            ),
            max_outstanding_requests=_require_int(
                lim_sec, "max_outstanding_requests", MAX_OUTSTANDING_REQUESTS
            ),
            max_query_length=_require_int(lim_sec, "max_query_length", MAX_QUERY_LENGTH),
            max_filename_length=_require_int(lim_sec, "max_filename_length", MAX_FILENAME_LENGTH),
            max_file_size_bytes=_require_int(lim_sec, "max_file_size_bytes", MAX_FILE_SIZE_BYTES),
        ),
        logging=LoggingConfig(
            level=_require_str(log_sec, "level", DEFAULT_LOG_LEVEL),
            file_path=_expand_path(
                _require_str(log_sec, "file_path", str(state_dir / DEFAULT_LOG_FILENAME))
            ),
            max_bytes=_require_int(log_sec, "max_bytes", DEFAULT_LOG_MAX_BYTES),
            backup_count=_require_int(log_sec, "backup_count", DEFAULT_LOG_BACKUP_COUNT),
            console_enabled=_require_bool(log_sec, "console_enabled", True),
        ),
        daemon=DaemonConfig(
            socket_path=_expand_path(
                _require_str(daemon_sec, "socket_path", str(state_dir / DEFAULT_SOCKET_FILENAME))
            ),
            pid_path=_expand_path(
                _require_str(daemon_sec, "pid_path", str(state_dir / DEFAULT_PID_FILENAME))
            ),
            shutdown_timeout_sec=_require_float(
                daemon_sec, "shutdown_timeout_sec", DEFAULT_SHUTDOWN_TIMEOUT_SEC
            ),
        ),
        config_path=config_path,
    )
    config.validate()
    return config


def load_config(explicit_path: Path | None = None) -> AppConfig:
    """Load configuration from TOML file, or return defaults if implicit path does not exist."""
    resolved = resolve_config_path(explicit_path)
    if not resolved.exists():
        if explicit_path is not None or os.environ.get(CONFIG_ENV_VAR, "").strip():
            raise ConfigError(
                f"Configuration file not found: {resolved}. "
                "Create one with: python -m p2p --config <path> config init"
            )
        return create_default_config(config_path=None)

    if resolved.is_dir():
        raise ConfigError(f"Configuration path is a directory, expected a file: {resolved}")

    try:
        with resolved.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"Failed to read configuration from {resolved}: {exc}") from exc

    return parse_config_dict(raw, config_path=resolved)


def save_config(config: AppConfig, path: Path, *, overwrite: bool = False) -> Path:
    """Validate and atomically write configuration to a TOML file."""
    target = _expand_path(path)
    config.validate()
    if target.exists() and not overwrite:
        raise ConfigError(
            f"Configuration file already exists at {target}. Pass --force to overwrite."
        )

    target.parent.mkdir(parents=True, exist_ok=True)
    rendered = config.to_toml()
    # Ensure serialized TOML parses cleanly before committing atomically.
    tomllib.loads(rendered)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=".p2p-config.",
            suffix=".tmp",
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(rendered)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, target)
    except OSError as exc:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise ConfigError(f"Failed to write configuration to {target}: {exc}") from exc

    config.config_path = target
    return target


def add_shared_directory(
    directory: Path,
    *,
    explicit_config_path: Path | None = None,
) -> tuple[AppConfig, Path, bool]:
    """Validate and persist an authorized shared directory in the configuration file.

    Returns (updated_config, canonical_directory_path, added_new).
    """
    target_dir = _expand_path(directory)
    if not target_dir.exists():
        raise ConfigError(f"Shared directory does not exist: {target_dir}")
    if not target_dir.is_dir():
        raise ConfigError(f"Shared path is not a directory: {target_dir}")

    cfg_path = resolve_config_path(explicit_config_path)
    if cfg_path.exists():
        config = load_config(cfg_path)
    else:
        config = create_default_config(config_path=cfg_path)

    if target_dir in config.sharing.directories:
        return config, target_dir, False

    config.sharing.directories.append(target_dir)
    save_config(config, cfg_path, overwrite=True)
    return config, target_dir, True


def remove_shared_directory(
    directory: Path,
    *,
    explicit_config_path: Path | None = None,
) -> tuple[AppConfig, Path, bool]:
    """Remove a directory from the configured shared directories list."""
    target_dir = _expand_path(directory)
    cfg_path = resolve_config_path(explicit_config_path)
    if not cfg_path.exists():
        raise ConfigError(f"Configuration file not found: {cfg_path}")

    config = load_config(cfg_path)
    if target_dir not in config.sharing.directories:
        return config, target_dir, False

    config.sharing.directories = [d for d in config.sharing.directories if d != target_dir]
    save_config(config, cfg_path, overwrite=True)
    return config, target_dir, True
