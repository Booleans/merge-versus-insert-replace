"""Benchmark orchestrator.

For each (scenario, strategy, repeat) run:
  1. Deep-clone the baseline into a disposable target
  2. Execute the strategy SQL inside a wall-clock timer
  3. Read Delta history for operation metrics
  4. Assert row-count equivalence vs. the MERGE result for the same scenario
  5. Append a row to bench_results

All runs in a matrix are independent and use identical starting state.
"""

from __future__ import annotations

import itertools
import random
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.benchmark.scenarios import scenario_params
from src.benchmark.strategies import render
from src.common.config import STRATEGIES, BenchmarkConfig
from src.common.timing import capture_last_history, timed


RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("run_ts", TimestampType(), False),
        StructField("scale", StringType(), False),
        StructField("scenario", StringType(), False),
        StructField("strategy", StringType(), False),
        StructField("compute_flavor", StringType(), False),
        StructField("repeat_index", IntegerType(), False),
        StructField("wall_seconds", DoubleType(), False),
        StructField("delta_operation", StringType(), True),
        StructField("delta_version", LongType(), True),
        StructField("delta_metrics", MapType(StringType(), StringType()), True),
        StructField("dbr_version", StringType(), True),
        StructField("notes", StringType(), True),
    ]
)


def _deep_clone(spark: SparkSession, cfg: BenchmarkConfig, scenario: str, strategy: str) -> str:
    fq_clone = cfg.fq_run_target(scenario, strategy)
    spark.sql(f"DROP TABLE IF EXISTS {fq_clone}")
    spark.sql(
        f"CREATE TABLE {fq_clone} DEEP CLONE {cfg.fq_baseline}"
    )
    # Make sure layout is identical before running the strategy
    spark.sql(f"OPTIMIZE {fq_clone}")
    return fq_clone


def _dbr_version(spark: SparkSession) -> str:
    try:
        return spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown")
    except Exception:  # noqa: BLE001
        return "unknown"


def _row_count_check(
    spark: SparkSession, reference: str, candidate: str
) -> tuple[bool, int, int]:
    ref_rows = spark.table(reference).count()
    cand_rows = spark.table(candidate).count()
    return ref_rows == cand_rows, ref_rows, cand_rows


def run_matrix(
    spark: SparkSession,
    cfg: BenchmarkConfig,
    *,
    randomize_order: bool = True,
) -> None:
    """Run every (scenario, strategy, repeat) in cfg and append to bench_results."""
    run_id = str(uuid.uuid4())
    dbr = _dbr_version(spark)
    compute = cfg.compute_flavor

    combos = [
        (scen, strat, rep)
        for scen in cfg.scenarios
        for strat in cfg.strategies
        for rep in range(cfg.repeats)
    ]
    if randomize_order:
        random.seed(0xDEADBEEF)
        random.shuffle(combos)

    # Track the MERGE result per scenario so other strategies can be compared
    merge_reference: dict[str, str] = {}

    rows_to_append: list[dict] = []

    for scen, strat, rep in combos:
        fq_target = _deep_clone(spark, cfg, scen, f"{strat}_r{rep}")
        fq_source = cfg.fq_source(scen)
        params = scenario_params(scen, cfg)

        sql = render(
            scenario=scen,
            strategy=strat,
            target=fq_target,
            source=fq_source,
            t0=params.t0,
            t1=params.t1,
            partition_predicate=params.partition_predicate,
        )

        with timed() as holder:
            spark.sql(sql)
        wall = float(holder["wall_seconds"])

        history = capture_last_history(spark, fq_target)

        note = ""
        if strat == "merge" and rep == 0:
            merge_reference[scen] = fq_target
        elif strat != "merge" and scen in merge_reference and rep == 0:
            ok, ref_n, cand_n = _row_count_check(spark, merge_reference[scen], fq_target)
            if not ok:
                note = f"row-count mismatch vs MERGE: merge={ref_n} this={cand_n}"

        rows_to_append.append(
            {
                "run_id": run_id,
                "run_ts": datetime.now(timezone.utc),
                "scale": cfg.scale,
                "scenario": scen,
                "strategy": strat,
                "compute_flavor": compute,
                "repeat_index": rep,
                "wall_seconds": wall,
                "delta_operation": history.operation,
                "delta_version": history.version,
                "delta_metrics": history.delta_metrics,
                "dbr_version": dbr,
                "notes": note,
            }
        )

    df = spark.createDataFrame(rows_to_append, schema=RESULTS_SCHEMA)
    (df.write.format("delta").mode("append").saveAsTable(cfg.fq_results))


def summarize(spark: SparkSession, cfg: BenchmarkConfig) -> None:
    """Print a quick summary after a run — useful when invoked interactively."""
    summary = (
        spark.table(cfg.fq_results)
        .where(F.col("scale") == cfg.scale)
        .groupBy("scenario", "strategy", "compute_flavor")
        .agg(
            F.round(F.percentile_approx("wall_seconds", 0.5), 3).alias("median_seconds"),
            F.round(F.min("wall_seconds"), 3).alias("min_seconds"),
            F.round(F.max("wall_seconds"), 3).alias("max_seconds"),
            F.count("*").alias("runs"),
        )
        .orderBy("scenario", "strategy", "compute_flavor")
    )
    summary.show(truncate=False)


_ = STRATEGIES  # re-exported for callers that want the canonical order
_ = asdict  # imported above for future callers serializing config
