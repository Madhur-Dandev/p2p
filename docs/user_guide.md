# User & Operations Guide

## 1. Installation

Create an isolated Python 3.12 virtual environment and install the package cleanly:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev,ui,viz]"
```

If running in an offline environment with system packages inherited:

```bash
python3.12 -m venv --system-site-packages .venv
.venv/bin/pip install --no-build-isolation --no-deps -e .
```

## 2. Node Initialization & Configuration

Generate a TOML configuration with a fresh UUIDv4 `peer_id`:

```bash
python -m p2p config init
# Or target an explicit path:
python -m p2p --config ./peer1.toml config init --display-name alice --tcp-port 9001 --udp-port 9998
```

Useful config inspection commands:

```bash
python -m p2p --config ./peer1.toml config show
python -m p2p --config ./peer1.toml config show --format json
python -m p2p --config ./peer1.toml config validate
```

### Configuration Sections Summary ([config.example.toml](file:///Users/madhurdandev/p2p/config.example.toml))

- `[node]`: `peer_id` (UUIDv4), `display_name`
- `[network]`: `bind_host`, `tcp_port`, `advertise_host`, `connect_timeout_sec`, `control_timeout_sec`
- `[discovery]`: `enabled`, `udp_port`, `broadcast_address`, `heartbeat_interval_sec`, `stale_timeout_sec`, `remove_timeout_sec`, `static_peers`
- `[storage]`: `data_dir`, `state_dir`, `database_path`, `download_dir`
- `[sharing]`: `directories`, `excluded_dirs`, `follow_symlinks`
- `[transfer]`: `chunk_size_bytes`, `idle_timeout_sec`, `search_timeout_sec`, `overwrite_existing`
- `[limits]`: `max_control_payload_bytes`, `max_data_chunk_bytes`, `max_simultaneous_connections`, `max_concurrent_transfers`, `max_outstanding_requests`, `max_query_length`, `max_filename_length`, `max_file_size_bytes`
- `[logging]`: `level`, `file_path`, `max_bytes`, `backup_count`, `console_enabled`
- `[daemon]`: `socket_path` (must stay <= `100` UTF-8 bytes on macOS), `pid_path`, `shutdown_timeout_sec`

## 3. Sharing Directories & Building the Local Index

Authorize folders cleanly and build/refresh the SQLite metadata index:

```bash
python -m p2p --config ./peer1.toml share add ~/Documents/p2p_share
python -m p2p --config ./peer1.toml share list
python -m p2p --config ./peer1.toml index rebuild
# Recompute every SHA-256 hash bypassing (file_size, mtime_ns) caching:
python -m p2p --config ./peer1.toml index rebuild --force-rehash
```

If the background daemon is already running when you run `share add`, `share remove`, or `index rebuild`, the CLI automatically notifies the daemon over Unix socket IPC so its active shared roots and SQLite index refresh live without restarting.

## 4. Running Background Daemons & Transferring Files

### Two-Computer Wi-Fi / LAN Setup
On both laptops (firewall allowing TCP `9000` and UDP `9999`):

1. **Computer A:**
   ```bash
   python -m p2p config init --display-name laptop-a
   python -m p2p share add ~/SharedFiles
   python -m p2p start
   python -m p2p status
   ```
2. **Computer B:**
   ```bash
   python -m p2p config init --display-name laptop-b
   python -m p2p start
   # Confirm Computer A appeared via UDP broadcast discovery:
   python -m p2p peers
   # Search across active LAN peers and download directly by file_id:
   python -m p2p search "lecture"
   python -m p2p download <FILE_ID>
   ```

*(If campus/corporate Wi-Fi drops UDP broadcasts, pass `--static-peer <A_IP>:9000` to `start` or add it under `[discovery] static_peers` in TOML.)*

### Two-Node Loopback Test on a Single Machine
Use separate config files and short socket paths (`daemon.socket_path <= 100` bytes on macOS):

```bash
# Start Peer A & share files
python -m p2p --config ./nodeA.toml config init --display-name node-a
python -m p2p --config ./nodeA.toml start --host 127.0.0.1 --tcp-port 9001 --udp-port 9998 --no-broadcast --udp-target 127.0.0.1:9999
python -m p2p --config ./nodeA.toml share add ./sample_share

# Start Peer B pointing at Peer A, discover, search, download, inspect history, stop
python -m p2p --config ./nodeB.toml config init --display-name node-b
python -m p2p --config ./nodeB.toml start --host 127.0.0.1 --tcp-port 9002 --udp-port 9999 --no-broadcast --udp-target 127.0.0.1:9998 --static-peer 127.0.0.1:9001
python -m p2p --config ./nodeB.toml peers
python -m p2p --config ./nodeB.toml search "report"
python -m p2p --config ./nodeB.toml download <FILE_ID>
python -m p2p --config ./nodeB.toml downloads

# Clean shutdown
python -m p2p --config ./nodeA.toml stop
python -m p2p --config ./nodeB.toml stop
```

## 5. Resuming Interrupted Downloads & Viewing Logs

```bash
# Inspect history and see resumable_offset for any interrupted/cancelled transfer:
python -m p2p downloads

# Resume by transfer_id (UUID) or file_id:
python -m p2p resume <TRANSFER_ID_OR_FILE_ID>

# If the remote file changed while offline, restart cleanly from offset 0:
python -m p2p resume <TRANSFER_ID_OR_FILE_ID> --restart-if-changed

# Inspect recent rotating log entries:
python -m p2p logs --lines 50
```
