"""End-to-end localhost performance benchmark suite measuring throughput, latency, and RAM."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import platform
import tempfile
import tracemalloc
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from p2p.config import AppConfig, LimitsConfig, NetworkConfig, NodeConfig, StorageConfig
from p2p.indexing.file_index import FileIndexer
from p2p.network.tcp_client import TcpClient
from p2p.network.tcp_server import TcpServer
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.storage.repositories import DownloadRecord, DownloadRepository, TransferCheckpoint
from p2p.transfer.download import DownloadService
from p2p.transfer.progress import TransferState
from p2p.transfer.transfer_manager import TransferManager
from p2p.transfer.upload import UploadService
from p2p.utils.identifiers import generate_peer_id, generate_transfer_id
from p2p.utils.time_utils import monotonic_now, utc_now_iso


@dataclass(frozen=True, slots=True)
class ThroughputBenchmarkRow:
    """Single transfer benchmark datapoint across file size and chunk size."""

    file_size_bytes: int
    chunk_size_bytes: int
    duration_sec: float
    throughput_mbps: float
    throughput_mib_s: float
    peak_python_heap_kib: float


@dataclass(frozen=True, slots=True)
class ConcurrencyBenchmarkRow:
    """Aggregate loopback throughput across concurrent simultaneous transfers."""

    concurrency: int
    file_size_bytes: int
    wall_time_sec: float
    aggregate_mbps: float
    aggregate_mib_s: float
    avg_transfer_mbps: float


@dataclass(frozen=True, slots=True)
class IndexSearchBenchmarkRow:
    """Indexing rebuild and SQLite query latency scaling across indexed file counts."""

    file_count: int
    cold_index_sec: float
    cached_rebuild_sec: float
    exact_search_ms: float
    substring_search_ms: float


@dataclass(frozen=True, slots=True)
class ResumeBenchmarkRow:
    """Comparison of full download versus 50%-interrupted .part resume."""

    file_size_bytes: int
    resume_offset_bytes: int
    fresh_download_sec: float
    resumed_download_sec: float
    bytes_transferred_on_resume: int
    time_saved_sec: float


@dataclass(frozen=True, slots=True)
class BenchmarkBundle:
    """Complete measured benchmark output ready for CSV export and visualization."""

    environment: dict[str, str | int]
    throughput: list[ThroughputBenchmarkRow]
    concurrency: list[ConcurrencyBenchmarkRow]
    indexing_search: list[IndexSearchBenchmarkRow]
    resume: list[ResumeBenchmarkRow]


def _write_random_file_sync(path: Path, size_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = os.urandom(min(size_bytes, 1_048_576)) if size_bytes > 0 else b""
    remaining = size_bytes
    with path.open("wb") as handle:
        while remaining > 0:
            to_write = min(remaining, len(block))
            handle.write(block[:to_write])
            remaining -= to_write


def _populate_many_files_sync(root: Path, count: int) -> str:
    root.mkdir(parents=True, exist_ok=True)
    target_name = f"bench_target_{count - 1:05d}_systems.txt"
    for i in range(count):
        bucket = root / f"bucket_{i % 20:02d}"
        bucket.mkdir(exist_ok=True)
        name = target_name if i == count - 1 else f"doc_{i:05d}_networks_lab.txt"
        (bucket / name).write_bytes(f"benchmark-entry-{i}\n".encode("ascii"))
    return target_name


def _create_partial_file_sync(
    part_path: Path,
    src_path: Path,
    offset: int,
) -> str:
    part_path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with src_path.open("rb") as src_f, part_path.open("wb") as part_f:
        total = 0
        while True:
            chunk = src_f.read(65_536)
            if not chunk:
                break
            digest.update(chunk)
            if total < offset:
                to_keep = min(len(chunk), offset - total)
                part_f.write(chunk[:to_keep])
                total += to_keep
    return digest.hexdigest()


async def benchmark_transfer_throughput(
    work_dir: Path,
    *,
    file_sizes: Sequence[int],
    chunk_sizes: Sequence[int],
) -> list[ThroughputBenchmarkRow]:
    """Measure TCP streaming throughput and peak Python heap footprint across sizes."""
    rows: list[ThroughputBenchmarkRow] = []
    max_chunk = max(max(chunk_sizes), 262_144)

    for size_bytes in file_sizes:
        run_dir = work_dir / f"tp_{size_bytes}"
        share_dir = run_dir / "shared"
        dl_dir = run_dir / "downloads"
        src_file = share_dir / f"payload_{size_bytes}.bin"
        await asyncio.to_thread(_write_random_file_sync, src_file, size_bytes)

        peer_a = generate_peer_id()
        peer_b = generate_peer_id()
        with Database(run_dir / "a.db") as db_a, Database(run_dir / "b.db") as db_b:
            await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
            from p2p.storage.repositories import FileIndexRepository

            indexed = await db_a.run(FileIndexRepository(db_a).list_all)
            fid = indexed[0].file_id

            upload = UploadService(
                db_a,
                peer_id=peer_a,
                shared_directories=[share_dir],
                max_data_chunk_bytes=max_chunk,
            )
            server = TcpServer(
                host="127.0.0.1",
                port=0,
                peer_id=peer_a,
                max_data_chunk_bytes=max_chunk,
                handler=upload.serve_connection,
            )
            async with server:
                for chunk_bytes in chunk_sizes:
                    iter_dl = dl_dir / f"chunk_{chunk_bytes}"
                    client = TcpClient(peer_id=peer_b, max_data_chunk_bytes=max_chunk)
                    downloader = DownloadService(
                        db_b,
                        client,
                        local_peer_id=peer_b,
                        download_dir=iter_dl,
                        chunk_size_bytes=chunk_bytes,
                        max_data_chunk_bytes=max_chunk,
                        overwrite_existing=True,
                    )
                    tracemalloc.start()
                    started = monotonic_now()
                    await downloader.download_file(
                        host="127.0.0.1",
                        port=server.bound_port,
                        file_id=fid,
                        file_name=src_file.name,
                        source_peer_id=peer_a,
                    )
                    elapsed = max(1e-6, monotonic_now() - started)
                    _, peak_bytes = tracemalloc.get_traced_memory()
                    tracemalloc.stop()

                    mbps = (size_bytes * 8.0 / elapsed) / 1_000_000.0
                    mib_s = (size_bytes / elapsed) / (1024.0 * 1024.0)
                    rows.append(
                        ThroughputBenchmarkRow(
                            file_size_bytes=size_bytes,
                            chunk_size_bytes=chunk_bytes,
                            duration_sec=elapsed,
                            throughput_mbps=mbps,
                            throughput_mib_s=mib_s,
                            peak_python_heap_kib=peak_bytes / 1024.0,
                        )
                    )
    return rows


async def benchmark_concurrent_transfers(
    work_dir: Path,
    *,
    concurrency_levels: Sequence[int],
    file_size_bytes: int,
) -> list[ConcurrencyBenchmarkRow]:
    """Measure aggregate TCP transfer scaling across 1..N concurrent streams."""
    rows: list[ConcurrencyBenchmarkRow] = []
    max_n = max(concurrency_levels)
    run_dir = work_dir / "concurrent"
    share_dir = run_dir / "shared"

    for i in range(max_n):
        await asyncio.to_thread(
            _write_random_file_sync, share_dir / f"blob_{i}.bin", file_size_bytes
        )

    peer_a = generate_peer_id()
    peer_b = generate_peer_id()

    with Database(run_dir / "a.db") as db_a, Database(run_dir / "b.db") as db_b:
        await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
        from p2p.storage.repositories import FileIndexRepository

        all_files = await db_a.run(FileIndexRepository(db_a).list_all)

        cfg_a = AppConfig(
            node=NodeConfig(peer_id=peer_a),
            network=NetworkConfig(bind_host="127.0.0.1", tcp_port=0),
            limits=LimitsConfig(
                max_concurrent_transfers=max(max_n, 8),
                max_simultaneous_connections=max(max_n * 2, 64),
            ),
        )
        cfg_a.sharing.directories = [share_dir]
        mgr_a = TransferManager(cfg_a, db_a)
        server = TcpServer(
            host="127.0.0.1",
            port=0,
            max_connections=max(max_n * 2, 64),
            peer_id=peer_a,
            handler=mgr_a.serve_connection,
        )

        async with server:
            for level in concurrency_levels:
                dl_dir = run_dir / f"dl_{level}"
                cfg_b = AppConfig(
                    node=NodeConfig(peer_id=peer_b),
                    storage=StorageConfig(database_path=run_dir / "b.db", download_dir=dl_dir),
                    limits=LimitsConfig(max_concurrent_transfers=level),
                )
                mgr_b = TransferManager(cfg_b, db_b)
                batch = all_files[:level]
                started = monotonic_now()
                results = await asyncio.gather(
                    *(
                        mgr_b.download_file(
                            host="127.0.0.1",
                            port=server.bound_port,
                            file_id=item.file_id,
                            file_name=item.file_name,
                            source_peer_id=peer_a,
                        )
                        for item in batch
                    )
                )
                wall = max(1e-6, monotonic_now() - started)
                total_bytes = sum(r.bytes_downloaded for r in results)
                agg_mbps = (total_bytes * 8.0 / wall) / 1_000_000.0
                agg_mib_s = (total_bytes / wall) / (1024.0 * 1024.0)
                avg_per = sum(r.throughput_mbps for r in results) / len(results)
                rows.append(
                    ConcurrencyBenchmarkRow(
                        concurrency=level,
                        file_size_bytes=file_size_bytes,
                        wall_time_sec=wall,
                        aggregate_mbps=agg_mbps,
                        aggregate_mib_s=agg_mib_s,
                        avg_transfer_mbps=avg_per,
                    )
                )
    return rows


async def benchmark_indexing_and_search(
    work_dir: Path,
    *,
    file_counts: Sequence[int],
) -> list[IndexSearchBenchmarkRow]:
    """Measure cold SHA-256 rebuild, cached rebuild, and SQLite search latencies."""
    rows: list[IndexSearchBenchmarkRow] = []
    for count in file_counts:
        run_dir = work_dir / f"index_{count}"
        share_dir = run_dir / "shared"
        exact_target = await asyncio.to_thread(_populate_many_files_sync, share_dir, count)
        peer_id = generate_peer_id()

        with Database(run_dir / "idx.db") as db:
            indexer = FileIndexer(db, shared_directories=[share_dir])
            cold_summary = await indexer.rebuild_async()
            cached_summary = await indexer.rebuild_async()
            search = LocalSearchService(db, peer_id=peer_id)

            iterations = 20
            exact_start = monotonic_now()
            for _ in range(iterations):
                matches, _ = search.search(exact_target, exact=True)
                if not matches:
                    raise RuntimeError(f"Expected match for {exact_target}")
            exact_ms = ((monotonic_now() - exact_start) / iterations) * 1000.0

            sub_start = monotonic_now()
            for _ in range(iterations):
                search.search("systems", exact=False, max_results=50)
            sub_ms = ((monotonic_now() - sub_start) / iterations) * 1000.0

            rows.append(
                IndexSearchBenchmarkRow(
                    file_count=count,
                    cold_index_sec=cold_summary.duration_sec,
                    cached_rebuild_sec=cached_summary.duration_sec,
                    exact_search_ms=exact_ms,
                    substring_search_ms=sub_ms,
                )
            )
    return rows


async def benchmark_resume_recovery(
    work_dir: Path,
    *,
    file_size_bytes: int,
) -> list[ResumeBenchmarkRow]:
    """Measure full transfer vs 50%-interrupted .part resume recovery."""
    run_dir = work_dir / "resume"
    share_dir = run_dir / "shared"
    dl_dir = run_dir / "downloads"
    src_file = share_dir / "resume_target.bin"
    await asyncio.to_thread(_write_random_file_sync, src_file, file_size_bytes)

    peer_a = generate_peer_id()
    peer_b = generate_peer_id()
    resume_offset = file_size_bytes // 2

    with Database(run_dir / "a.db") as db_a, Database(run_dir / "b.db") as db_b:
        await FileIndexer(db_a, shared_directories=[share_dir]).rebuild_async()
        from p2p.storage.repositories import FileIndexRepository

        rec = (await db_a.run(FileIndexRepository(db_a).list_all))[0]

        upload = UploadService(db_a, peer_id=peer_a, shared_directories=[share_dir])
        server = TcpServer(
            host="127.0.0.1",
            port=0,
            peer_id=peer_a,
            handler=upload.serve_connection,
        )
        async with server:
            downloader = DownloadService(
                db_b,
                TcpClient(peer_id=peer_b),
                local_peer_id=peer_b,
                download_dir=dl_dir,
                overwrite_existing=True,
            )
            t0 = monotonic_now()
            await downloader.download_file(
                host="127.0.0.1",
                port=server.bound_port,
                file_id=rec.file_id,
                file_name=rec.file_name,
                source_peer_id=peer_a,
            )
            fresh_sec = monotonic_now() - t0

            part_path = dl_dir / f".{rec.file_name}.resume_bench.part"
            final_path = dl_dir / rec.file_name
            sha = await asyncio.to_thread(
                _create_partial_file_sync, part_path, src_file, resume_offset
            )
            tid = generate_transfer_id()
            now_iso = utc_now_iso()
            repo_b = DownloadRepository(db_b)
            await db_b.run(
                lambda: repo_b.create_download(
                    DownloadRecord(
                        transfer_id=tid,
                        file_id=rec.file_id,
                        file_name=rec.file_name,
                        source_peer_id=peer_a,
                        source_ip="127.0.0.1",
                        source_tcp_port=server.bound_port,
                        expected_sha256=sha,
                        expected_size=file_size_bytes,
                        bytes_downloaded=resume_offset,
                        destination_path=str(final_path),
                        part_path=str(part_path),
                        status=str(TransferState.TRANSFER_INTERRUPTED),
                        error_message="Simulated midpoint disconnect",
                        started_at=now_iso,
                        updated_at=now_iso,
                        completed_at=None,
                    )
                )
            )
            await db_b.run(
                lambda: repo_b.upsert_checkpoint(
                    TransferCheckpoint(
                        transfer_id=tid,
                        resume_offset=resume_offset,
                        expected_sha256=sha,
                        expected_size=file_size_bytes,
                        part_path=str(part_path),
                        updated_at=now_iso,
                    )
                )
            )
            t1 = monotonic_now()
            res = await downloader.resume_download(tid)
            resumed_sec = monotonic_now() - t1

            return [
                ResumeBenchmarkRow(
                    file_size_bytes=file_size_bytes,
                    resume_offset_bytes=res.resume_offset,
                    fresh_download_sec=fresh_sec,
                    resumed_download_sec=resumed_sec,
                    bytes_transferred_on_resume=file_size_bytes - res.resume_offset,
                    time_saved_sec=max(0.0, fresh_sec - resumed_sec),
                )
            ]


async def run_all_benchmarks(
    work_dir: Path,
    *,
    quick: bool = False,
) -> BenchmarkBundle:
    """Execute throughput, concurrency, indexing/search, and resume benchmarks."""
    work_dir = await asyncio.to_thread(work_dir.resolve, False)
    if quick:
        file_sizes = (524_288, 2_097_152)
        chunk_sizes = (16_384, 65_536)
        concurrency_levels = (1, 2, 4)
        concurrent_size = 524_288
        file_counts = (100, 500)
        resume_size = 1_048_576
    else:
        file_sizes = (1_048_576, 8_388_608, 33_554_432)
        chunk_sizes = (16_384, 65_536, 131_072, 262_144)
        concurrency_levels = (1, 2, 4, 8)
        concurrent_size = 4_194_304
        file_counts = (100, 500, 1000, 2500)
        resume_size = 16_777_216

    tp_rows = await benchmark_transfer_throughput(
        work_dir, file_sizes=file_sizes, chunk_sizes=chunk_sizes
    )
    conc_rows = await benchmark_concurrent_transfers(
        work_dir, concurrency_levels=concurrency_levels, file_size_bytes=concurrent_size
    )
    idx_rows = await benchmark_indexing_and_search(work_dir, file_counts=file_counts)
    res_rows = await benchmark_resume_recovery(work_dir, file_size_bytes=resume_size)

    env_info: dict[str, str | int] = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count() or 1,
        "timestamp_utc": utc_now_iso(),
    }
    return BenchmarkBundle(
        environment=env_info,
        throughput=tp_rows,
        concurrency=conc_rows,
        indexing_search=idx_rows,
        resume=res_rows,
    )


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Sequence[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def generate_benchmark_plots(bundle: BenchmarkBundle, plots_dir: Path) -> list[Path]:
    """Generate PNG charts using Matplotlib with a headless Agg backend."""
    plots_dir.mkdir(parents=True, exist_ok=True)
    mpl_dir = Path(tempfile.gettempdir()) / "p2p_mpl_cache"
    mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(mpl_dir))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    created: list[Path] = []

    # 1. Throughput vs Chunk Size (grouped by file size)
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=140)
    sizes = sorted({r.file_size_bytes for r in bundle.throughput})
    for size in sizes:
        pts = sorted(
            (r for r in bundle.throughput if r.file_size_bytes == size),
            key=lambda r: r.chunk_size_bytes,
        )
        ax.plot(
            [f"{r.chunk_size_bytes // 1024} KiB" for r in pts],
            [r.throughput_mib_s for r in pts],
            marker="o",
            linewidth=2,
            label=f"{size // (1024 * 1024)} MiB file"
            if size >= 1024 * 1024
            else f"{size // 1024} KiB",
        )
    ax.set_title("TCP Streaming Throughput vs. Chunk Size (incl. SHA-256 Verification)")
    ax.set_xlabel("Chunk Size")
    ax.set_ylabel("Throughput (MiB/s)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    p1 = plots_dir / "throughput_vs_chunk_size.png"
    fig.savefig(p1)
    plt.close(fig)
    created.append(p1)

    # 2. Concurrency Scaling
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=140)
    labels = [str(r.concurrency) for r in bundle.concurrency]
    agg_vals = [r.aggregate_mib_s for r in bundle.concurrency]
    bars = ax.bar(labels, agg_vals)
    for bar, val in zip(bars, agg_vals, strict=True):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            val,
            f"{val:.1f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    ax.set_title("Aggregate Loopback Transfer Throughput vs. Concurrency")
    ax.set_xlabel("Concurrent Transfers")
    ax.set_ylabel("Aggregate Throughput (MiB/s)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    p2 = plots_dir / "concurrency_scaling.png"
    fig.savefig(p2)
    plt.close(fig)
    created.append(p2)

    # 3. Search & Index Scaling
    fig, (ax_idx, ax_srch) = plt.subplots(1, 2, figsize=(10.5, 4.4), dpi=140)
    counts = [str(r.file_count) for r in bundle.indexing_search]
    ax_idx.plot(
        counts,
        [r.cold_index_sec for r in bundle.indexing_search],
        marker="o",
        label="Cold SHA-256 Index",
    )
    ax_idx.plot(
        counts,
        [r.cached_rebuild_sec for r in bundle.indexing_search],
        marker="s",
        label="Cached Rebuild (mtime/size)",
    )
    ax_idx.set_title("Shared Directory Indexing Time")
    ax_idx.set_xlabel("Indexed Files")
    ax_idx.set_ylabel("Duration (seconds)")
    ax_idx.grid(True, linestyle="--", alpha=0.4)
    ax_idx.legend()

    ax_srch.plot(
        counts,
        [r.exact_search_ms for r in bundle.indexing_search],
        marker="o",
        label="Exact Filename",
    )
    ax_srch.plot(
        counts,
        [r.substring_search_ms for r in bundle.indexing_search],
        marker="s",
        label="Substring (COLLATE NOCASE)",
    )
    ax_srch.set_title("Local SQLite Search Latency")
    ax_srch.set_xlabel("Indexed Files")
    ax_srch.set_ylabel("Average Latency (ms)")
    ax_srch.grid(True, linestyle="--", alpha=0.4)
    ax_srch.legend()
    fig.tight_layout()
    p3 = plots_dir / "search_latency_scaling.png"
    fig.savefig(p3)
    plt.close(fig)
    created.append(p3)

    # 4. Peak Python Heap Memory vs File Size (64 KiB chunks where available)
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=140)
    mem_rows = []
    for size in sizes:
        matching = [r for r in bundle.throughput if r.file_size_bytes == size]
        preferred = next((r for r in matching if r.chunk_size_bytes == 65_536), matching[0])
        mem_rows.append(preferred)
    ax.bar(
        [
            f"{r.file_size_bytes // (1024 * 1024)} MiB"
            if r.file_size_bytes >= 1024 * 1024
            else f"{r.file_size_bytes // 1024} KiB"
            for r in mem_rows
        ],
        [r.peak_python_heap_kib for r in mem_rows],
    )
    ax.set_title("Peak Python Heap Allocation During Streaming Transfer (Constant O(1))")
    ax.set_xlabel("Transferred File Size")
    ax.set_ylabel("Peak tracemalloc Heap (KiB)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    p4 = plots_dir / "peak_memory_vs_file_size.png"
    fig.savefig(p4)
    plt.close(fig)
    created.append(p4)

    return created


def write_benchmark_artifacts(bundle: BenchmarkBundle, output_root: Path) -> list[Path]:
    """Write CSV tables, environment JSON, PNG plots, and Markdown report."""
    results_dir = output_root / "results"
    plots_dir = output_root / "plots"
    results_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    env_path = results_dir / "environment.json"
    env_path.write_text(json.dumps(bundle.environment, indent=2), encoding="utf-8")
    written.append(env_path)

    tp_csv = results_dir / "transfer_throughput.csv"
    _write_csv(
        tp_csv,
        [
            "file_size_bytes",
            "chunk_size_bytes",
            "duration_sec",
            "throughput_mbps",
            "throughput_mib_s",
            "peak_python_heap_kib",
        ],
        [asdict(r) for r in bundle.throughput],
    )
    written.append(tp_csv)

    conc_csv = results_dir / "concurrent_transfers.csv"
    _write_csv(
        conc_csv,
        [
            "concurrency",
            "file_size_bytes",
            "wall_time_sec",
            "aggregate_mbps",
            "aggregate_mib_s",
            "avg_transfer_mbps",
        ],
        [asdict(r) for r in bundle.concurrency],
    )
    written.append(conc_csv)

    idx_csv = results_dir / "search_and_index_scaling.csv"
    _write_csv(
        idx_csv,
        [
            "file_count",
            "cold_index_sec",
            "cached_rebuild_sec",
            "exact_search_ms",
            "substring_search_ms",
        ],
        [asdict(r) for r in bundle.indexing_search],
    )
    written.append(idx_csv)

    res_csv = results_dir / "resume_recovery.csv"
    _write_csv(
        res_csv,
        [
            "file_size_bytes",
            "resume_offset_bytes",
            "fresh_download_sec",
            "resumed_download_sec",
            "bytes_transferred_on_resume",
            "time_saved_sec",
        ],
        [asdict(r) for r in bundle.resume],
    )
    written.append(res_csv)

    plot_paths = generate_benchmark_plots(bundle, plots_dir)
    written.extend(plot_paths)

    report_path = output_root / "report.md"
    lines: list[str] = [
        "# Performance Benchmark Report",
        "",
        f"- **Python:** `{bundle.environment['python_version']}`",
        f"- **Platform:** `{bundle.environment['platform']}` (`{bundle.environment['machine']}`, "
        f"`{bundle.environment['cpu_count']}` CPUs)",
        f"- **Measured UTC:** `{bundle.environment['timestamp_utc']}`",
        "",
        "## 1. Transfer Throughput & Streaming Heap Allocation",
        "",
        (
            "| File Size | Chunk Size | Duration (s) | Throughput (MiB/s) | "
            "Throughput (Mbps) | Peak Heap (KiB) |"
        ),
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in bundle.throughput:
        lines.append(
            f"| {r.file_size_bytes:,} B | {r.chunk_size_bytes // 1024} KiB | "
            f"{r.duration_sec:.4f} | {r.throughput_mib_s:.2f} | "
            f"{r.throughput_mbps:.1f} | {r.peak_python_heap_kib:.1f} |"
        )

    lines.extend(
        [
            "",
            "![Throughput vs Chunk Size](plots/throughput_vs_chunk_size.png)",
            "![Peak Heap vs File Size](plots/peak_memory_vs_file_size.png)",
            "",
            "## 2. Concurrency Scaling (Loopback TCP)",
            "",
            (
                "| Concurrent Streams | Per-File Size | Wall Time (s) | "
                "Aggregate (MiB/s) | Aggregate (Mbps) |"
            ),
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for c in bundle.concurrency:
        lines.append(
            f"| {c.concurrency} | {c.file_size_bytes:,} B | {c.wall_time_sec:.4f} | "
            f"{c.aggregate_mib_s:.2f} | {c.aggregate_mbps:.1f} |"
        )

    lines.extend(
        [
            "",
            "![Concurrency Scaling](plots/concurrency_scaling.png)",
            "",
            "## 3. SQLite Indexing & Search Latency Scaling",
            "",
            (
                "| Files Indexed | Cold Index (s) | Cached Rebuild (s) | "
                "Exact Query (ms) | Substring Query (ms) |"
            ),
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for idx_r in bundle.indexing_search:
        lines.append(
            f"| {idx_r.file_count} | {idx_r.cold_index_sec:.4f} | "
            f"{idx_r.cached_rebuild_sec:.4f} | "
            f"{idx_r.exact_search_ms:.3f} | {idx_r.substring_search_ms:.3f} |"
        )

    lines.extend(
        [
            "",
            "![Search Latency Scaling](plots/search_latency_scaling.png)",
            "",
            "## 4. Resumable `.part` Recovery Comparison",
            "",
            "| File Size | Resume Offset | Fresh Download (s) | Resumed Transfer (s) | Saved (s) |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for rr in bundle.resume:
        lines.append(
            f"| {rr.file_size_bytes:,} B | {rr.resume_offset_bytes:,} B | "
            f"{rr.fresh_download_sec:.4f} | {rr.resumed_download_sec:.4f} | "
            f"{rr.time_saved_sec:.4f} |"
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    written.append(report_path)
    return written
