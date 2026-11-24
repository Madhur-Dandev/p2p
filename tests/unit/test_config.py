"""Unit tests for TOML configuration loading, validation, and serialization."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.config import (
    CONFIG_ENV_VAR,
    add_shared_directory,
    create_default_config,
    load_config,
    parse_config_dict,
    remove_shared_directory,
    save_config,
)
from p2p.exceptions import ConfigError


@pytest.mark.unit
def test_default_config_is_valid() -> None:
    cfg = create_default_config()
    cfg.validate()
    assert cfg.network.tcp_port == 9000
    assert cfg.discovery.udp_port == 9999
    assert cfg.sharing.follow_symlinks is False
    assert cfg.transfer.overwrite_existing is False


@pytest.mark.unit
def test_save_and_load_round_trip_preserves_values(tmp_path: Path) -> None:
    cfg_path = tmp_path / "peer.toml"
    shared_dir = tmp_path / 'shared "quotes" & \\ dir'
    shared_dir.mkdir()

    cfg = create_default_config(peer_id="00000000-0000-4000-8000-000000000042")
    cfg.node.display_name = 'node-"alpha"\\1'
    cfg.network.tcp_port = 9101
    cfg.network.advertise_host = "192.168.1.42"
    cfg.discovery.static_peers = ["127.0.0.1:9102", "[::1]:9103"]
    cfg.sharing.directories = [shared_dir.resolve()]
    cfg.logging.level = "DEBUG"

    saved = save_config(cfg, cfg_path)
    assert saved == cfg_path.resolve()

    loaded = load_config(cfg_path)
    assert loaded.node.peer_id == "00000000-0000-4000-8000-000000000042"
    assert loaded.node.display_name == 'node-"alpha"\\1'
    assert loaded.network.tcp_port == 9101
    assert loaded.network.advertise_host == "192.168.1.42"
    assert loaded.discovery.static_peers == ["127.0.0.1:9102", "[::1]:9103"]
    assert loaded.sharing.directories == [shared_dir.resolve()]
    assert loaded.logging.level == "DEBUG"


@pytest.mark.unit
def test_save_config_refuses_overwrite_without_force(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.toml"
    cfg = create_default_config()
    save_config(cfg, cfg_path)

    with pytest.raises(ConfigError, match="already exists"):
        save_config(cfg, cfg_path, overwrite=False)

    save_config(cfg, cfg_path, overwrite=True)


@pytest.mark.unit
def test_load_missing_explicit_config_or_env_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "does-not-exist.toml"
    with pytest.raises(ConfigError, match="not found"):
        load_config(missing)

    monkeypatch.setenv(CONFIG_ENV_VAR, str(missing))
    with pytest.raises(ConfigError, match="not found"):
        load_config(None)


@pytest.mark.unit
def test_unknown_section_or_key_rejected() -> None:
    with pytest.raises(ConfigError, match="Unknown configuration section"):
        parse_config_dict({"unknown_sec": {"x": 1}})

    with pytest.raises(ConfigError, match="Unknown key"):
        parse_config_dict({"network": {"tcp_prot": 9000}})


@pytest.mark.unit
def test_boolean_rejected_for_integer_fields() -> None:
    with pytest.raises(ConfigError, match="Expected integer"):
        parse_config_dict({"network": {"tcp_port": True}})


@pytest.mark.unit
@pytest.mark.parametrize(
    ("patch", "error_fragment"),
    [
        ({"network": {"tcp_port": 0}}, "network.tcp_port"),
        ({"network": {"tcp_port": 65536}}, "network.tcp_port"),
        ({"discovery": {"udp_port": 70000}}, "discovery.udp_port"),
        ({"discovery": {"broadcast_address": "not-an-ip"}}, "broadcast_address"),
        (
            {
                "discovery": {
                    "heartbeat_interval_sec": 10.0,
                    "stale_timeout_sec": 10.0,
                }
            },
            "stale_timeout_sec",
        ),
        (
            {
                "discovery": {
                    "stale_timeout_sec": 40.0,
                    "remove_timeout_sec": 30.0,
                }
            },
            "remove_timeout_sec",
        ),
        ({"discovery": {"static_peers": ["192.168.1.5"]}}, "host:port"),
        ({"discovery": {"static_peers": ["192.168.1.5:99999"]}}, "static_peer port"),
        ({"logging": {"level": "TRACE"}}, "logging.level"),
        (
            {"transfer": {"chunk_size_bytes": 512}},
            "chunk_size_bytes",
        ),
        (
            {"daemon": {"socket_path": "/var/run/p2p/" + ("x" * 95) + ".sock"}},
            "100 UTF-8 bytes",
        ),
    ],
)
def test_invalid_config_values_raise_config_error(
    patch: dict[str, object], error_fragment: str
) -> None:
    with pytest.raises(ConfigError, match=error_fragment):
        parse_config_dict(patch)


@pytest.mark.unit
def test_example_config_file_is_valid() -> None:
    example_path = Path(__file__).resolve().parents[2] / "config.example.toml"
    cfg = load_config(example_path)
    assert cfg.node.peer_id == "00000000-0000-4000-8000-000000000001"
    assert cfg.network.tcp_port == 9000
    assert cfg.discovery.udp_port == 9999


@pytest.mark.unit
def test_add_and_remove_shared_directory(tmp_path: Path) -> None:
    cfg_path = tmp_path / "node.toml"
    share_dir = tmp_path / "shared_docs"
    share_dir.mkdir()
    plain_file = tmp_path / "not_a_dir.txt"
    plain_file.write_text("hello", encoding="utf-8")

    cfg, added_path, added_new = add_shared_directory(share_dir, explicit_config_path=cfg_path)
    assert added_new is True
    assert added_path == share_dir.resolve()
    assert cfg.sharing.directories == [share_dir.resolve()]

    # Adding again is idempotent
    _, _, added_again = add_shared_directory(share_dir, explicit_config_path=cfg_path)
    assert added_again is False

    # Missing directory or regular file must fail
    with pytest.raises(ConfigError, match="does not exist"):
        add_shared_directory(tmp_path / "missing", explicit_config_path=cfg_path)
    with pytest.raises(ConfigError, match="not a directory"):
        add_shared_directory(plain_file, explicit_config_path=cfg_path)

    # Removing works and persists
    cfg_after, _, removed = remove_shared_directory(share_dir, explicit_config_path=cfg_path)
    assert removed is True
    assert cfg_after.sharing.directories == []
    assert load_config(cfg_path).sharing.directories == []
