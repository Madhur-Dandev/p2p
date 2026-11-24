# System Architecture

**Project Title:** Design and Implementation of a Decentralized Peer-to-Peer File Discovery and Transfer System Using Python

## 1. Architectural Principles

1. **No Central Index or File Server:** Every participating node is symmetric — it acts as both a server (announcing availability, indexing its authorized directories, answering search queries, serving file streams) and a client (discovering peers, issuing searches, downloading and verifying files).
2. **Control Plane vs. Data Plane Separation:**
   - **Control Plane:** Small, bounded JSON messages (`DISCOVER`, `ANNOUNCE`, `HEARTBEAT`, `PING`, `PONG`, `SEARCH_REQUEST`, `SEARCH_RESPONSE`, `GET_FILE`, `FILE_INFO`, `TRANSFER_ERROR`) framed with a fixed 26-byte binary header.
   - **Data Plane:** Raw binary byte streaming via `FILE_DATA` frames bounded by configurable chunk size (`64 KiB` default, `256 KiB` ceiling), avoiding Base64/JSON bloat and keeping RAM usage constant regardless of file size.
3. **Single-Event-Loop Concurrency (`asyncio`) with Offloaded Blocking Work:**
   - UDP discovery and TCP socket I/O run asynchronously on Python's `asyncio` event loop.
   - CPU-bound SHA-256 hashing of large files and synchronous SQLite transactions are isolated from stalling network I/O (offloaded via `asyncio.to_thread()` with serialized DB access).
4. **Defense in Depth:**
   - Remote peers never supply filesystem paths; transfers address opaque internal `file_id` handles mapped through canonicalized, symlink-safe path containment checks.
   - All network inputs and payload lengths are validated before memory allocation.

## 2. Layered System Diagram

```mermaid
flowchart TD
    User["User / Terminal"] --> CLI["CLI Layer (p2p.cli)"]
    CLI -->|"Local Unix Domain Socket IPC (Milestone 11)"| Daemon["Daemon Service Layer (p2p.daemon)"]

    subgraph Node["Peer Node Process"]
        Daemon --> Discovery["Discovery Service (UDP)"]
        Daemon --> Search["Search Service"]
        Daemon --> Transfer["Transfer Manager (TCP)"]
        Daemon --> Indexer["Directory Scanner & SHA-256 Indexer"]

        Discovery --> Registry["Peer Registry (Active / Stale / Removed)"]
        Search --> Registry
        Search --> DB["SQLite Storage Layer"]
        Indexer --> DB
        Transfer --> DB
        Transfer --> Security["Security & Path Validation"]
        Indexer --> Security
        Security --> FS["Authorized Shared Directories & Download Dir"]
    end

    Discovery <-->|"UDP Broadcast / Static Peers"| LAN["LAN Peers"]
    Search <-->|"TCP Framed Control Messages"| LAN
    Transfer <-->|"TCP Framed Raw Byte Streams"| LAN
```

## 3. Module Responsibilities & Interaction Map

| Module / Package | Target Milestone | Responsibility |
| --- | --- | --- |
| `p2p.constants`, `p2p.exceptions` | Milestone 1 (Implemented) | Centralized protocol limits, default timeouts/ports, and domain exception hierarchy. |
| `p2p.utils` | Milestone 1 (Implemented) | UUIDv4 peer/request/transfer ID generation and validation; UTC and monotonic clocks. |
| `p2p.config` | Milestone 1 (Implemented) | Typed configuration dataclasses, `tomllib` parsing, validation, atomic TOML writes, shared-directory config management. |
| `p2p.logging_config` | Milestone 1 (Implemented) | UTC rotating file + stderr logging with `peer_id`, `request_id`, and `transfer_id` correlation fields. |
| `p2p.cli`, `p2p.main` | Milestone 1 (Implemented) | `argparse` CLI entry point (`python -m p2p`) supporting config, share, logs, and honest milestone gating for later subsystems. |
| `p2p.network.framing`, `p2p.network.protocol`, `p2p.network.serialization` | Milestone 2 & 4 (Planned) | 26-byte binary header encoding/decoding, stream reassembly (`readexactly`), message schemas, request correlation. |
| `p2p.network.tcp_server`, `p2p.network.tcp_client` | Milestone 2 (Planned) | `asyncio.start_server` / `asyncio.open_connection` handlers with connection limits and timeouts. |
| `p2p.network.discovery`, `p2p.network.peer_registry` | Milestone 3 (Planned) | UDP discovery announcements, periodic heartbeats, static fallback, and `ACTIVE`/`STALE`/`REMOVED` peer tracking. |
| `p2p.storage` | Milestone 5 (Planned) | SQLite schema migrations, parameterized repositories for peers, shared files, downloads, and resume checkpoints. |
| `p2p.indexing` | Milestone 5 (Planned) | Recursive non-symlink directory scanning, metadata caching `(size, mtime_ns)`, streaming SHA-256 hashing. |
| `p2p.search` | Milestone 6 & 7 (Planned) | Local SQLite matching plus bounded-timeout concurrent queries across active peers and result deduplication. |
| `p2p.transfer` | Milestone 8 & 9 (Planned) | Explicit transfer state machine, bounded-chunk TCP streaming, `.part` temporary files, SHA-256 verification, offset-validated resume. |
| `p2p.security` | Milestone 10 (Planned) | Canonical symlink-safe path containment, strict payload/field validation, and connection/transfer semaphores. |
| `p2p.daemon` | Milestone 11 (Planned) | Long-running service orchestrator, PID/lock management, Unix domain socket IPC server/client, graceful shutdown. |

## 4. Implementation Status

- **Completed:** Milestone 1 (foundation, package metadata, constants, exceptions, utilities, TOML configuration, rotating contextual logging, CLI entry point, and initial unit tests).
- **Next:** Milestone 2 (minimal asynchronous TCP client/server communication).
