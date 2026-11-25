"""Optional terminal presentation helpers with plain-text fallback when non-TTY."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TextIO


def _use_rich(out: TextIO) -> bool:
    return bool(hasattr(out, "isatty") and out.isatty())


def render_table(
    *,
    title: str,
    headers: Sequence[str],
    rows: Sequence[Sequence[str]],
    out: TextIO,
) -> bool:
    """Render a Rich table to interactive TTY terminals when installed.

    Returns True if Rich rendered the table cleanly, or False when plain-text
    formatting should be used (always False for pipes, StringIO, and tests).
    """
    if not _use_rich(out):
        return False
    try:
        from rich.console import Console
        from rich.table import Table
    except ImportError:
        return False

    table = Table(title=title)
    for col in headers:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    Console(file=out, force_terminal=True).print(table)
    return True
