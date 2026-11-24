"""Application entry point for console scripts and module execution."""

from __future__ import annotations

from collections.abc import Sequence

from p2p.cli import run_cli


def main(argv: Sequence[str] | None = None) -> int:
    """Run the P2P CLI and return an integer process exit status."""
    return run_cli(argv)
