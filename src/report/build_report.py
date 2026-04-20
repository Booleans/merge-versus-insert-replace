"""Render benchmark results to Markdown + HTML for sharing."""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import SCENARIOS, STRATEGIES, BenchmarkConfig


REPORT_DIR = Path(__file__).resolve().parents[2] / "report"


def _load_summary(spark: SparkSession, cfg: BenchmarkConfig):
    return (
        spark.table(cfg.fq_results)
        .groupBy("scale", "scenario", "strategy", "compute_flavor")
        .agg(
            F.percentile_approx("wall_seconds", 0.5).alias("median_s"),
            F.min("wall_seconds").alias("min_s"),
            F.max("wall_seconds").alias("max_s"),
            F.count("*").alias("runs"),
            F.first("delta_metrics", ignorenulls=True).alias("sample_metrics"),
            F.first("dbr_version", ignorenulls=True).alias("dbr_version"),
        )
        .orderBy("scale", "scenario", "strategy", "compute_flavor")
    )


def _relative_to_merge(rows: list[dict]) -> list[dict]:
    """Add a 'rel_to_merge' column (median_s / merge_median_s) per (scale, scenario, compute)."""
    baseline: dict[tuple[str, str, str], float] = {}
    for r in rows:
        if r["strategy"] == "merge":
            baseline[(r["scale"], r["scenario"], r["compute_flavor"])] = r["median_s"]
    for r in rows:
        b = baseline.get((r["scale"], r["scenario"], r["compute_flavor"]))
        r["rel_to_merge"] = round(r["median_s"] / b, 2) if b and b > 0 else None
    return rows


def _bytes_from_metrics(metrics: dict | None) -> int:
    if not metrics:
        return 0
    for k in ("numOutputBytes", "numAddedBytes"):
        v = metrics.get(k)
        if v is not None:
            try:
                return int(v)
            except ValueError:
                return 0
    return 0


def _files_touched(metrics: dict | None) -> tuple[int, int]:
    if not metrics:
        return (0, 0)
    added = int(metrics.get("numTargetFilesAdded") or metrics.get("numAddedFiles") or 0)
    removed = int(metrics.get("numTargetFilesRemoved") or metrics.get("numRemovedFiles") or 0)
    return added, removed


def _md_table(rows: list[dict]) -> str:
    header = (
        "| scale | scenario | strategy | compute | median (s) | rel MERGE | files +/- | MB written | runs |"
    )
    sep = "|---|---|---|---|---:|---:|---|---:|---:|"
    lines = [header, sep]
    for r in rows:
        added, removed = _files_touched(r.get("sample_metrics"))
        mb = _bytes_from_metrics(r.get("sample_metrics")) // (1024 * 1024)
        lines.append(
            f"| {r['scale']} | {r['scenario']} | {r['strategy']} | {r['compute_flavor']} | "
            f"{r['median_s']:.2f} | {r.get('rel_to_merge')} | {added}/-{removed} | {mb} | {r['runs']} |"
        )
    return "\n".join(lines)


def _chart_per_scenario(rows: list[dict], scenario: str) -> go.Figure:
    scales = sorted({r["scale"] for r in rows if r["scenario"] == scenario})
    fig = go.Figure()
    for strat in STRATEGIES:
        ys = []
        for sc in scales:
            match = [r for r in rows if r["scenario"] == scenario and r["scale"] == sc and r["strategy"] == strat]
            ys.append(match[0]["median_s"] if match else None)
        fig.add_trace(go.Bar(name=strat, x=scales, y=ys))
    fig.update_layout(
        title=f"Scenario: {scenario} — median wall-clock by strategy",
        xaxis_title="Scale",
        yaxis_title="Median seconds",
        barmode="group",
        template="plotly_white",
    )
    return fig


FINDINGS_TEMPLATE = """# GPU-telemetry ingest benchmark — findings

## Summary (TODO: fill in)

- Which strategy won scenario 1 (full-window re-ingest):
- Which strategy won scenario 2 (partial overlap):
- Which strategy won scenario 3 (dynamic partition):
- Which strategy won scenario 4 (small delta into 1B rows):

## When to use each (decision matrix)

| Pattern                                    | Recommended         | Why                                                              |
|--------------------------------------------|---------------------|------------------------------------------------------------------|
| Re-ingest a bounded time window            | **REPLACE WHERE**   | Predicate-only DELETE + INSERT, no source join required          |
| Dynamic "partition" overwrite              | **REPLACE USING**   | Engine derives the partition-level DELETE from the source keys   |
| Upsert with arbitrary match condition      | **REPLACE ON** or MERGE | Most MERGE-like; use MERGE when you need WHEN NOT MATCHED logic |
| Small % of rows updated in a large table   | Depends — see data  | Usually MERGE + deletion vectors wins on file rewrites           |

## Caveats

- Results are from `{dbr}` on `{compute}`. Reruns against a different runtime or warehouse size will differ.
- All runs use liquid clustering on `(cloud, event_hour, gpu_uuid)`.
- OPTIMIZE is applied to the baseline; strategies are timed without any post-run optimize.
- Deletion vectors are enabled by default.

## Sources

- [INSERT (Databricks SQL)](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-dml-insert-into)
- [Selective overwrites with Delta Lake](https://docs.databricks.com/aws/en/delta/selective-overwrite)
- [MERGE INTO](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)
"""


def build(spark: SparkSession, cfg: BenchmarkConfig) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    summary_df = _load_summary(spark, cfg)
    rows = [r.asDict(recursive=True) for r in summary_df.collect()]
    rows = _relative_to_merge(rows)

    # Markdown
    md_parts = ["# MERGE vs INSERT REPLACE benchmark\n"]
    for scen in SCENARIOS:
        md_parts.append(f"\n## Scenario: {scen}\n")
        scen_rows = [r for r in rows if r["scenario"] == scen]
        md_parts.append(_md_table(scen_rows) if scen_rows else "_no data_")
    (REPORT_DIR / "results.md").write_text("\n".join(md_parts))

    # HTML (tables + charts)
    html_parts = [
        "<html><head><meta charset='utf-8'>",
        "<title>MERGE vs INSERT REPLACE — GPU-telemetry benchmark</title>",
        "<style>body{font-family:system-ui;margin:2rem}table{border-collapse:collapse}"
        "td,th{border:1px solid #ddd;padding:4px 8px}th{background:#f4f4f4}</style>",
        "</head><body><h1>MERGE vs INSERT REPLACE &mdash; GPU-telemetry benchmark</h1>",
    ]
    for scen in SCENARIOS:
        scen_rows = [r for r in rows if r["scenario"] == scen]
        html_parts.append(f"<h2>Scenario: {scen}</h2>")
        if not scen_rows:
            html_parts.append("<p><em>no data</em></p>")
            continue
        html_parts.append("<table><thead><tr>")
        for h in ("scale", "strategy", "compute", "median_s", "rel_merge", "files +/-", "MB", "runs"):
            html_parts.append(f"<th>{h}</th>")
        html_parts.append("</tr></thead><tbody>")
        for r in scen_rows:
            added, removed = _files_touched(r.get("sample_metrics"))
            mb = _bytes_from_metrics(r.get("sample_metrics")) // (1024 * 1024)
            html_parts.append(
                "<tr>"
                f"<td>{r['scale']}</td><td>{r['strategy']}</td><td>{r['compute_flavor']}</td>"
                f"<td>{r['median_s']:.2f}</td><td>{r.get('rel_to_merge')}</td>"
                f"<td>{added}/-{removed}</td><td>{mb}</td><td>{r['runs']}</td>"
                "</tr>"
            )
        html_parts.append("</tbody></table>")
        fig = _chart_per_scenario(rows, scen)
        html_parts.append(fig.to_html(full_html=False, include_plotlyjs="cdn"))
    html_parts.append("</body></html>")
    (REPORT_DIR / "results.html").write_text("\n".join(html_parts))

    dbr = rows[0].get("dbr_version", "unknown") if rows else "no-data"
    (REPORT_DIR / "findings_template.md").write_text(
        FINDINGS_TEMPLATE.format(dbr=dbr, compute=cfg.compute_flavor)
    )
