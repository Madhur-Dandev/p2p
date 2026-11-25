# Design and Implementation of a Decentralized Peer-to-Peer File Discovery and Transfer System Using Python

A modular, decentralized peer-to-peer (P2P) file discovery and transfer application built in Python 3.12+ using `asyncio`, `sqlite3`, `hashlib`, `tomllib`, and `argparse`.

> **Project Status:** **All 12 Milestones Complete** — Full decentralized architecture featuring automatic UDP LAN discovery & static fallback, unified 26-byte binary framing with `request_id` correlation, SQLite WAL persistence & migrations, symlink-safe SHA-256 shared-directory indexing, distributed multi-peer search with replica merging, direct TCP chunk streaming with `.part` staging and offset resume recovery, security rate/quota/O_NOFOLLOW hardening, detached background daemon with `0600` Unix domain socket IPC, and reproducible benchmarks with Matplotlib visualizations.

## Project Overview

Every participating node acts symmetrically as both client and server on a Local Area Network (LAN), without relying on a centralized file-storage or index server:

- **Control Plane:** UDP LAN discovery/heartbeats (`DISCOVER`, `ANNOUNCE`, `HEARTBEAT`), TCP health checks (`PING`, `PONG`), distributed metadata queries (`SEARCH_REQUEST`, `SEARCH_RESPONSE`), transfer negotiation (`GET_FILE`, `FILE_INFO`, `TRANSFER_ERROR`), and private local Unix domain socket daemon IPC (`p2p.sock`).
- **Data Plane:** Direct peer-to-peer TCP raw binary streaming (`FILE_DATA`) in `64 KiB` chunks into `.part` temporary files, offset-based recovery (`GET_FILE` with `offset` & `expected_sha256`), and full-file SHA-256 verification before atomic `os.replace()`.
- **Persistence:** Local WAL-mode SQLite database (`p2p.db`) storing discovered/static peers, authorized shared-file index metadata, download history, and resumable transfer checkpoints.

### Documentation Index

- [Architecture Overview](docs/architecture.md)
- [Wire Protocol Specification (`P2P v1` & Unix IPC)](docs/protocol.md)
- [SQLite Schema & Concurrency Model](docs/database_schema.md)
- [Security Threat Model & Controls](docs/security_threat_model.md)
- [Complete User & LAN Operations Guide](docs/user_guide.md)
- [Testing & Performance Evaluation](docs/testing_and_evaluation.md)
- [Measured Benchmark Report & Charts](benchmarks/report.md)

## Technology Stack

- **Language:** Python 3.12+ (zero required third-party runtime dependencies)
- **Networking:** `asyncio` (`start_server`, `open_connection`, `create_datagram_endpoint`, Unix sockets), `socket`, `struct`, `ipaddress`
- **Database:** `sqlite3` (WAL mode, explicit schema migrations, `RLock` + `BEGIN IMMEDIATE` / `SAVEPOINT`, parameterized SQL)
- **Integrity:** `hashlib` SHA-256 streaming (`64 KiB` chunks; constant $O(1)$ heap footprint)
- **Configuration:** `tomllib` (standard library) + atomic TOML serialization
- **CLI & Optional UI:** `argparse` core with optional `rich` interactive tables (automatic plain-text fallback for scripts/pipes)
- **Testing & Quality & Plots:** `pytest`, `pytest-asyncio`, `ruff`, `matplotlib` (optional `mypy`)

## Installation

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev,ui,viz]"
```

*(In offline environments where `setuptools`, `pytest`, `ruff`, `rich`, or `matplotlib` are available in system Python 3.12: `python3.12 -m venv --system-site-packages .venv && .venv/bin/pip install --no-build-isolation --no-deps -e .`)*

## Quick Start Usage

```bash
# 1. Initialize configuration (UUIDv4 peer_id, ports, paths)
python -m p2p config init
python -m p2p config show

# 2. Authorize shared folder(s) & start the background daemon
python -m p2p share add ~/Documents/shared
python -m p2p start
python -m p2p status

# 3. Discover peers, run distributed search, download without --peer, verify history
python -m p2p peers
python -m p2p search "notes"
python -m p2p download <FILE_ID>
python -m p2p downloads

# 4. Resume an interrupted download or inspect logs, then stop cleanly
python -m p2p resume <TRANSFER_ID_OR_FILE_ID>
python -m p2p logs --lines 20
python -m p2p stop
```

Or copy and customize [config.example.toml](config.example.toml) and point commands via `--config /path/to/config.toml` or `P2P_CONFIG=...`. See [docs/user_guide.md](docs/user_guide.md) for full two-laptop LAN and localhost multi-daemon examples.

## Network & Security Scope

- **Target Environment:** Local Area Network (LAN) / localhost testing. UDP broadcast discovery is bounded to the local broadcast domain and does **not** cross Internet routers; `static_peers` provides an explicit TCP fallback across routed VLANs or restricted Wi-Fi.
- **Security Posture:** Enforces strict symlink-free canonical path containment (`O_NOFOLLOW`), pre-allocation binary frame validation, sliding-window per-IP rate limiting, disk space & quota checks, `0600` PID/Unix-socket locks, and full-file SHA-256 verification. Does **not** implement TLS encryption or peer cryptographic authentication yet and is designed for trusted LAN deployments rather than untrusted public networks. See [docs/security_threat_model.md](docs/security_threat_model.md).

## Testing, Quality & Benchmarks

```bash
# Format, lint, and run all automated tests (unit + integration + performance)
.venv/bin/python -m ruff format --check src tests scripts
.venv/bin/python -m ruff check src tests scripts
.venv/bin/python -m pytest -v

# Re-generate CSV benchmark tables and Matplotlib PNG charts under benchmarks/
.venv/bin/python scripts/run_benchmarks.py --output-dir benchmarks
```

## Development Roadmap

1. **Milestone 1 (Complete):** Project foundation, architecture & protocol specs, TOML config, rotating logging, CLI, unit tests.
2. **Milestone 2 (Complete):** Asynchronous TCP server/client communication and framing tests.
3. **Milestone 3 (Complete):** UDP LAN discovery, heartbeats, static fallback, and peer registry.
4. **Milestone 4 (Complete):** Binary frame codec, payload validation, and request correlation.
5. **Milestone 5 (Complete):** SQLite storage migrations and secure shared-directory SHA-256 indexing.
6. **Milestone 6 (Complete):** Local file search and `SEARCH_REQUEST` / `SEARCH_RESPONSE` handler.
7. **Milestone 7 (Complete):** Distributed search across peers with bounded timeouts and replica merging.
8. **Milestone 8 (Complete):** Direct TCP streaming file transfer, `.part` verification, and SQLite download history.
9. **Milestone 9 (Complete):** Resumable downloads (`.part` offset resume) and source-change recovery.
10. **Milestone 10 (Complete):** Security hardening (rate limits, disk quotas, `O_NOFOLLOW`, frame fuzzing, graceful shutdown).
11. **Milestone 11 (Complete):** Background daemon lifecycle (`flock` PID lock) and `0600` Unix domain socket IPC integration.
12. **Milestone 12 (Complete):** End-to-end validation, reproducible benchmarks, Matplotlib graphs, and complete documentation.
