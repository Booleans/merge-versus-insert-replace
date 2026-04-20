"""Load SQL templates and render per-run statements with placeholder substitution."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from string import Template


SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "strategies"


def _load(scenario: str, strategy: str) -> Template:
    path = SQL_DIR / f"{scenario}_{strategy}.sql"
    return Template(path.read_text())


def render(
    *,
    scenario: str,
    strategy: str,
    target: str,
    source: str,
    t0: datetime,
    t1: datetime,
    partition_predicate: str = "",
) -> str:
    tmpl = _load(scenario, strategy)
    return tmpl.safe_substitute(
        target=target,
        source=source,
        t0=t0.strftime("%Y-%m-%d %H:%M:%S"),
        t1=t1.strftime("%Y-%m-%d %H:%M:%S"),
        partition_predicate=partition_predicate,
    )
