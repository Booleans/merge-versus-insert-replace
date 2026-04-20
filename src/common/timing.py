"""Wall-clock timing + Delta history metric capture."""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field

from pyspark.sql import SparkSession


DELTA_METRIC_KEYS: tuple[str, ...] = (
    "numTargetFilesAdded",
    "numTargetFilesRemoved",
    "numTargetRowsInserted",
    "numTargetRowsUpdated",
    "numTargetRowsDeleted",
    "numTargetRowsCopied",
    "numOutputRows",
    "numOutputBytes",
    "numSourceRows",
    "numAddedFiles",
    "numRemovedFiles",
    "numAddedBytes",
    "numRemovedBytes",
    "executionTimeMs",
    "scanTimeMs",
    "rewriteTimeMs",
)


@dataclass
class RunMetrics:
    wall_seconds: float = 0.0
    delta_metrics: dict[str, str] = field(default_factory=dict)
    operation: str = ""
    version: int = -1


@contextmanager
def timed():
    start = time.perf_counter()
    holder: dict[str, float] = {}
    try:
        yield holder
    finally:
        holder["wall_seconds"] = time.perf_counter() - start


def capture_last_history(spark: SparkSession, fq_table: str) -> RunMetrics:
    """Read the most recent entry from Delta history for fq_table."""
    row = (
        spark.sql(f"DESCRIBE HISTORY {fq_table} LIMIT 1")
        .select("version", "operation", "operationMetrics")
        .collect()[0]
    )
    op_metrics = {k: str(v) for k, v in (row["operationMetrics"] or {}).items() if k in DELTA_METRIC_KEYS}
    return RunMetrics(
        delta_metrics=op_metrics,
        operation=row["operation"],
        version=int(row["version"]),
    )
