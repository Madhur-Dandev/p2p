"""CLI runner generating reproducible CSV benchmarks and Matplotlib performance charts."""

from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

from p2p.benchmarks.suite import run_all_benchmarks, write_benchmark_artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description="Run P2P file transfer and search benchmarks.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks"),
        help="Directory to write CSV results, PNG charts, and report.md (default: benchmarks).",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run smaller fast benchmark matrix instead of the full multi-MiB suite.",
    )
    args = parser.parse_args()

    resolved_output = args.output_dir.resolve(strict=False)

    async def _run() -> None:
        with tempfile.TemporaryDirectory(prefix="p2p_bench_") as tmp_str:
            bundle = await run_all_benchmarks(Path(tmp_str), quick=args.quick)
            artifacts = write_benchmark_artifacts(bundle, resolved_output)
            print(f"Generated {len(artifacts)} benchmark artifacts under {args.output_dir}:")
            for item in artifacts:
                print(f"  - {item}")

    asyncio.run(_run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
