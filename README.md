# Design and Implementation of a Decentralized Peer-to-Peer File Discovery and Transfer System Using Python

A modular, decentralized peer-to-peer (P2P) file discovery and transfer application built incrementally in Python 3.12+ using `asyncio`, `sqlite3`, `hashlib`, `tomllib`, and `argparse`.

> **Current Stage:** **Milestones 1–6 & 8 — SQLite Indexing, Local Search, & Direct TCP File Transfer** (builds on Milestones 1–4 with symlink-safe path validation, SQLite schema/migrations and repositories, recursive SHA-256 shared-directory indexing, local SQLite file search, direct TCP streaming transfers over `.part` temporary files with SHA-256 verification, and download history tracking). Distributed multi-peer search (Milestone 7), offset-based resume (Milestone 9), daemon IPC (Milestone 11), and benchmarking (Milestone 12) remain ahead.

## Project Overview

Every participating node acts symmetrically as both client and server on a Local Area Network (LAN), without relying on a centralized file-storage or index server:

- **Control Plane:** UDP peer discovery/heartbeats and TCP framed JSON queries (`PING`, `PONG`, `SEARCH_REQUEST`, `SEARCH_RESPONSE`, `GET_FILE`, `FILE_INFO`, `TRANSFER_ERROR`).
- **Data Plane:** Direct peer-to-peer TCP binary streaming (`FILE_DATA`) in bounded chunks with SHA-256 verification and resumable `.part` downloads.
- **Persistence:** Local SQLite database storing peer registry, shared-file index metadata, download history, and resume state.

See [docs/architecture.md](docs/architecture.md) and [docs/protocol.md](docs/protocol.md) for the authoritative specifications.

## Technology Stack

- **Language:** Python 3.12+ (pure standard library runtime core)
- **Networking:** `asyncio` (`start_server`, `open_connection`, `create_datagram_endpoint`), `socket`, `struct`, `ipaddress`
- **Database:** `sqlite3` (WAL mode, explicit schema migrations, parameterized repositories)
- **Integrity:** `hashlib` SHA-256 streaming (`64 KiB` chunks; verified before `.part` promotion)
- **Configuration:** `tomllib` (standard library) + atomic TOML serialization
- **CLI:** `argparse`
- **Testing & Quality:** `pytest`, `pytest-asyncio`, `ruff`, `mypy`

## Installation & Virtual Environment Setup

Create an isolated Python 3.12 virtual environment and install the package in editable mode:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

*(In offline environments where `setuptools` and `pytest` are already installed in system Python 3.12, you can create the environment with `python3.12 -m venv --system-site-packages .venv` and run `pip install --no-build-isolation --no-deps -e .`.)*

## Configuration

Generate a configuration file with a fresh UUIDv4 `peer_id`:

```bash
python -m p2p config init
```

Or copy and customize [config.example.toml](config.example.toml), then point to it with `--config /path/to/config.toml` or `P2P_CONFIG=/path/to/config.toml`.

## Usage (Milestones 1–6 & 8)

```bash
# Show CLI help and available / planned commands
python -m p2p --help

# Initialize, inspect, and validate configuration
python -m p2p --config ./local-peer.toml config init
python -m p2p --config ./local-peer.toml config show
python -m p2p --config ./local-peer.toml config validate

# Manage authorized shared directories
python -m p2p --config ./local-peer.toml share add ~/Documents/shared
python -m p2p --config ./local-peer.toml share list
python -m p2p --config ./local-peer.toml share remove ~/Documents/shared

# View recent rotating log entries
python -m p2p --config ./local-peer.toml logs --lines 20

# Index local shared files into SQLite and search the local index
python -m p2p --config ./peer1.toml index rebuild
python -m p2p --config ./peer1.toml search "sample"

# Run two local peers (Terminal 1 shares & indexes files; Terminal 2 downloads over TCP)
# Terminal 1 (Node A):
python -m p2p --config ./peer1.toml dev peer-node --host 127.0.0.1 --tcp-port 9001 --udp-port 9998 --no-broadcast --udp-target 127.0.0.1:9999
# Terminal 2 (Node B):
python -m p2p --config ./peer2.toml dev peer-node --host 127.0.0.1 --tcp-port 9002 --udp-port 9999 --no-broadcast --udp-target 127.0.0.1:9998
# Terminal 3 (TCP PING, direct TCP download, and SQLite download history):
python -m p2p --config ./peer2.toml dev tcp-ping 127.0.0.1 9001
python -m p2p --config ./peer2.toml download <file_id> --peer 127.0.0.1:9001
python -m p2p --config ./peer2.toml downloads
```

## Network & Security Scope

- **Target Environment:** Local Area Network (LAN) / loopback multi-peer testing. UDP broadcast discovery is bounded to the local broadcast domain and does **not** cross Internet routers; static peer entries (`static_peers`) provide an explicit fallback when broadcast is filtered.
- **Current Security Boundary:** The initial architecture prioritizes path-traversal prevention, strict binary frame validation, bounded memory/connection limits, and SHA-256 content verification. It does **not** yet implement TLS transport encryption or peer identity authentication and is therefore intended for trusted LANs rather than untrusted public networks.

## Testing & Code Quality

```bash
python -m pytest -v
python -m ruff check src tests
```

*(Using `python -m ...` ensures tools run under the active virtual environment's interpreter even when `.venv` inherits system packages via `--system-site-packages`.)*

## Development Roadmap

1. **Milestone 1 (Complete):** Project foundation, architecture & protocol specs, TOML config, rotating logging, CLI, unit tests.
2. **Milestone 2 (Complete):** Asynchronous TCP server/client communication and framing tests.
3. **Milestone 3 (Complete):** UDP LAN discovery, heartbeats, static fallback, and peer registry.
4. **Milestone 4 (Complete):** Binary frame codec, payload validation, and request correlation.
5. **Milestone 5 (Complete):** SQLite storage migrations and secure shared-directory SHA-256 indexing.
6. **Milestone 6 (Complete):** Local file search and `SEARCH_REQUEST` / `SEARCH_RESPONSE` handler.
7. **Milestone 7:** Distributed search across peers with bounded timeouts and result merging.
8. **Milestone 8 (Complete):** Direct TCP streaming file transfer, `.part` verification, and SQLite download history.
9. **Milestone 9:** Resumable downloads (`.part` offset resume) and enhanced transfer recovery.
10. **Milestone 10:** Security hardening and graceful shutdown audit.
11. **Milestone 11:** Background daemon and Unix domain socket IPC integration.
12. **Milestone 12:** End-to-end validation, benchmarks, graphs, and final documentation.
