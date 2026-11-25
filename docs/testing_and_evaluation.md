# Automated Testing & Performance Evaluation

## 1. Test Pyramid & Coverage

The automated test suite under `tests/` contains **107 tests** partitioned cleanly across three markers configured in [pyproject.toml](file:///Users/madhurdandev/p2p/pyproject.toml):

- **Unit Tests (`80` tests, `-m unit`):** Fast, isolated logic tests running cleanly with zero network sockets (`test_cli.py`, `test_config.py`, `test_daemon_ipc_unit.py`, `test_database.py`, `test_discovery_packet.py`, `test_framing.py`, `test_hashing.py`, `test_identifiers.py`, `test_indexing.py`, `test_logging.py`, `test_path_validation.py`, `test_peer_registry.py`, `test_protocol.py`, `test_result_merger.py`, `test_security_hardening.py`, `test_transfer_state.py`).
- **Integration Tests (`26` tests, `-m integration`):** Live loopback TCP/UDP and Unix domain socket tests spanning multi-node discovery, stalled-peer fault isolation, correlated framing, full streaming transfers, `.part` offset resume, remote file-change detection, sliding-window rate limiting, Slowloris timeout protection, disk quota checks, and real detached multi-daemon `p2p start` / `search` / `download` / `stop` lifecycles.
- **Performance Tests (`1` test suite, `-m performance`):** Validates bounded $O(1)$ Python heap footprint during streaming transfers, sub-100ms local SQLite search latency, concurrent throughput scaling, and automated CSV + chart generation ([test_performance.py](file:///Users/madhurdandev/p2p/tests/performance/test_performance.py)).

Run the suite via:

```bash
.venv/bin/python -m ruff format --check src tests scripts
.venv/bin/python -m ruff check src tests scripts
.venv/bin/python -m pytest -m unit -v
.venv/bin/python -m pytest -m "integration or performance" -v
```

*(Note: `mypy` is listed under `[project.optional-dependencies] dev` in `pyproject.toml`, though not pre-installed in the offline `.venv` environment.)*

## 2. Benchmark Methodology & Measured Results

Reproducible benchmarks run via [run_benchmarks.py](file:///Users/madhurdandev/p2p/scripts/run_benchmarks.py) and [suite.py](file:///Users/madhurdandev/p2p/src/p2p/benchmarks/suite.py):

```bash
.venv/bin/python scripts/run_benchmarks.py --output-dir benchmarks
```

Key empirical measurements collected on **macOS (`arm64`, Python `3.12.6`, 8 CPUs)** — full tables and charts in [benchmarks/report.md](file:///Users/madhurdandev/p2p/benchmarks/report.md):

1. **Streaming Throughput & Constant $O(1)$ Heap:**
   - Moving from `16 KiB` to `256 KiB` chunks on a `32 MiB` file raised end-to-end loopback throughput (including full-file SHA-256 verification and atomic rename) from `19.93 MiB/s` (`167.1 Mbps`) to `159.17 MiB/s` (`1,335.2 Mbps`).
   - With `64 KiB` default chunks, increasing file size **32x** (from `1 MiB` to `32 MiB`) changed peak Python `tracemalloc` heap allocation from only `486.5 KiB` to `873.4 KiB`, confirming constant-memory streaming.
2. **Concurrent Loopback Scaling (`4 MiB` files):**
   - Aggregate throughput scaled from `122.22 MiB/s` (1 stream) to `156.33 MiB/s` (4 concurrent streams) cleanly within `TransferManager` semaphore limits.
3. **SQLite Indexing & Query Latency:**
   - Up to `2,500` indexed files, exact queries (`COLLATE NOCASE`) averaged **`0.041 ms`** and substring queries averaged **`0.739 ms`**.
4. **Resumable `.part` Recovery (`16 MiB` interrupted at `8 MiB`):**
   - Resuming transferred only the remaining `8 MiB` in `0.0777 s` (compared against `0.1370 s` full transfer), saving `0.0594 s` (~43%) while verifying the complete `16 MiB` SHA-256 prior to promotion.

### Visual Plots

- [Throughput vs Chunk Size](file:///Users/madhurdandev/p2p/benchmarks/plots/throughput_vs_chunk_size.png)
- [Peak Heap vs File Size](file:///Users/madhurdandev/p2p/benchmarks/plots/peak_memory_vs_file_size.png)
- [Concurrency Scaling](file:///Users/madhurdandev/p2p/benchmarks/plots/concurrency_scaling.png)
- [Search Latency Scaling](file:///Users/madhurdandev/p2p/benchmarks/plots/search_latency_scaling.png)
