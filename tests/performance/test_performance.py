"""Performance regression and artifact-generation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from p2p.benchmarks.suite import run_all_benchmarks, write_benchmark_artifacts


@pytest.mark.performance
async def test_quick_benchmark_suite_bounds_heap_and_generates_charts(tmp_path: Path) -> None:
    work_dir = tmp_path / "work"
    out_dir = tmp_path / "out"
    bundle = await run_all_benchmarks(work_dir, quick=True)

    assert len(bundle.throughput) >= 4
    # Streaming peak Python heap must remain compact (O(1) bounded chunks, well below 2 MiB)
    for row in bundle.throughput:
        assert row.throughput_mib_s > 0.0
        assert row.peak_python_heap_kib < 2048.0

    assert len(bundle.concurrency) == 3
    for crow in bundle.concurrency:
        assert crow.aggregate_mbps > 0.0

    # Local SQLite search over 500 files must remain sub-100ms per query
    assert len(bundle.indexing_search) == 2
    assert bundle.indexing_search[-1].exact_search_ms < 100.0
    assert bundle.indexing_search[-1].substring_search_ms < 100.0

    artifacts = write_benchmark_artifacts(bundle, out_dir)
    assert any(p.name == "transfer_throughput.csv" and p.stat().st_size > 0 for p in artifacts)
    assert any(p.name == "throughput_vs_chunk_size.png" and p.stat().st_size > 0 for p in artifacts)
    assert any(p.name == "report.md" and p.stat().st_size > 0 for p in artifacts)
