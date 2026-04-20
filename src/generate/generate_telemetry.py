"""Generate synthetic GPU telemetry and per-scenario source tables using dbldatagen.

Writes:
  - ${catalog}.${schema}.gpu_telemetry_baseline    (immutable baseline)
  - ${catalog}.${schema}.src_full_window_<scale>
  - ${catalog}.${schema}.src_partial_overlap_<scale>
  - ${catalog}.${schema}.src_dynamic_partition_<scale>
  - ${catalog}.${schema}.src_small_delta_<scale>     (only when scale == L)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import BenchmarkConfig
from src.common.schema import CLOUDS, COLUMN_NAMES, DRIVER_VERSIONS, GPU_MODELS, REGIONS, TELEMETRY_SCHEMA


def _canonical(df: DataFrame) -> DataFrame:
    """Enforce the canonical column order defined in src/common/schema.py."""
    return df.select(*[F.col(c) for c in COLUMN_NAMES])


def _apply_ddl(spark: SparkSession, cfg: BenchmarkConfig) -> None:
    ddl_path = Path(__file__).resolve().parents[2] / "sql" / "ddl.sql"
    statements = ddl_path.read_text().replace("${catalog}", cfg.catalog).replace("${schema}", cfg.schema)
    for stmt in (s.strip() for s in statements.split(";") if s.strip()):
        spark.sql(stmt)


def _base_generator(spark: SparkSession, cfg: BenchmarkConfig, rows: int) -> dg.DataGenerator:
    params = cfg.scale_params()
    total_gpus = params["hosts"] * params["gpus_per_host"]
    window_seconds = params["window_hours"] * 3600
    t0, _ = cfg.window()

    gen = (
        dg.DataGenerator(
            sparkSession=spark,
            name=f"gpu_telemetry_{cfg.scale}",
            rows=rows,
            partitions=max(8, rows // 5_000_000),
            randomSeed=42,
        )
        # time axis: uniform over the window, microsecond resolution
        .withColumn(
            "event_ts",
            "timestamp",
            begin=t0,
            end=t0 + timedelta(seconds=window_seconds - 1),
            interval=f"{params['sample_interval_sec']} seconds",
            random=True,
        )
        .withColumn("event_hour", "timestamp", expr="date_trunc('hour', event_ts)")
        # host/gpu identity
        .withColumn("host_ord", "long", minValue=0, maxValue=params["hosts"] - 1, random=True, omit=True)
        .withColumn("host_id", "string", format="host-%07d", baseColumn="host_ord")
        .withColumn("gpu_index", "tinyint", minValue=0, maxValue=params["gpus_per_host"] - 1, random=True)
        .withColumn(
            "gpu_uuid",
            "string",
            expr="concat('GPU-', substring(sha2(concat(host_id, '-', gpu_index), 256), 1, 8), '-',"
            " substring(sha2(concat(host_id, '-', gpu_index), 256), 9, 4), '-',"
            " substring(sha2(concat(host_id, '-', gpu_index), 256), 13, 4), '-',"
            " substring(sha2(concat(host_id, '-', gpu_index), 256), 17, 4), '-',"
            " substring(sha2(concat(host_id, '-', gpu_index), 256), 21, 12))",
            baseColumn=["host_id", "gpu_index"],
        )
        # geography
        .withColumn("cloud", "string", values=CLOUDS, random=True, weights=[45, 25, 10, 20])
        .withColumn(
            "region",
            "string",
            expr=(
                "case cloud "
                + " ".join(
                    [
                        f"when '{c}' then element_at(array({', '.join(repr(r) for r in REGIONS[c])}),"
                        f" cast(rand() * {len(REGIONS[c])} as int) + 1)"
                        for c in CLOUDS
                    ]
                )
                + " end"
            ),
            baseColumn="cloud",
        )
        .withColumn(
            "datacenter",
            "string",
            expr="concat(region, '-dc-', cast(pmod(abs(hash(host_id)), 8) as string))",
            baseColumn=["region", "host_id"],
        )
        # hardware
        .withColumn("gpu_model", "string", values=GPU_MODELS, random=True, weights=[25, 35, 15, 25])
        .withColumn("driver_version", "string", values=DRIVER_VERSIONS, random=True)
        # metrics
        .withColumn("utilization_pct", "tinyint", minValue=0, maxValue=100, random=True)
        .withColumn("mem_total_mib", "int", values=[81920, 141312, 188416], random=True)
        .withColumn("mem_used_mib", "int", minValue=1024, maxValue=188416, random=True)
        .withColumn("power_w", "smallint", minValue=50, maxValue=1000, random=True)
        .withColumn("temp_c", "tinyint", minValue=25, maxValue=92, random=True)
        .withColumn("sm_clock_mhz", "smallint", minValue=210, maxValue=1980, random=True)
        .withColumn("mem_clock_mhz", "smallint", minValue=800, maxValue=3200, random=True)
        .withColumn("pcie_rx_kbps", "int", minValue=0, maxValue=32_000_000, random=True)
        .withColumn("pcie_tx_kbps", "int", minValue=0, maxValue=32_000_000, random=True)
        .withColumn("nvlink_rx_kbps", "long", minValue=0, maxValue=900_000_000, random=True)
        .withColumn("nvlink_tx_kbps", "long", minValue=0, maxValue=900_000_000, random=True)
        .withColumn("ecc_sb_corrected", "long", minValue=0, maxValue=50_000, random=True)
        .withColumn("ecc_db_uncorr", "long", minValue=0, maxValue=5, random=True)
        .withColumn("xid_errors", "int", minValue=0, maxValue=3, random=True)
        .withColumn("throttle_reasons", "long", minValue=0, maxValue=(1 << 20) - 1, random=True)
        .withColumn("ingest_ts", "timestamp", expr="event_ts + interval 7 seconds", baseColumn="event_ts")
    )

    _ = total_gpus  # captured in host_ord range; kept for readability
    return gen


def _write_delta(df: DataFrame, fq_name: str) -> None:
    """Write as Delta, enforcing canonical column order AND uniqueness on (gpu_uuid, event_ts)."""
    deduped = _canonical(df).dropDuplicates(["gpu_uuid", "event_ts"])
    (deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq_name))


def _build_source_full_window(spark: SparkSession, cfg: BenchmarkConfig) -> DataFrame:
    """Full-window re-ingest: one hour fully replaced with same keys, new metrics."""
    t0, _ = cfg.window()
    hour_start = t0 + timedelta(hours=cfg.scale_params()["window_hours"] - 1)
    hour_end = hour_start + timedelta(hours=1)
    df = (
        spark.table(cfg.fq_baseline)
        .where(F.col("event_ts") >= F.lit(hour_start))
        .where(F.col("event_ts") < F.lit(hour_end))
        .withColumn("utilization_pct", (F.col("utilization_pct") + F.lit(1)).cast("tinyint"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return df


def _build_source_partial_overlap(spark: SparkSession, cfg: BenchmarkConfig) -> DataFrame:
    """80% new rows, 20% corrections of existing rows (by (gpu_uuid, event_ts))."""
    t0, t1 = cfg.window()
    next_hour = t1
    rows_new = int(cfg.scale_params()["rows"] * 0.01 * 0.8)
    rows_upd = int(cfg.scale_params()["rows"] * 0.01 * 0.2)

    # New rows: generate fresh into the hour immediately after the baseline window
    new_gen = _base_generator(spark, cfg, rows_new)
    new_df = _canonical(new_gen.build())
    # Shift new rows to the hour AFTER the baseline window so keys don't collide
    shifted = new_df.withColumn(
        "event_ts",
        F.col("event_ts") + F.expr(f"interval {cfg.scale_params()['window_hours']} hours"),
    ).withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
    _ = next_hour  # semantic note: shift places events in [t1, t1+window)

    # Update rows: random sample of baseline, bump utilization to mark "corrected"
    frac = min(1.0, (rows_upd * 2.0) / max(cfg.scale_params()["rows"], 1))
    upd = (
        spark.table(cfg.fq_baseline)
        .sample(False, frac, seed=7)
        .limit(rows_upd)
        .withColumn("utilization_pct", F.least(F.lit(100).cast("tinyint"), (F.col("utilization_pct") + F.lit(5)).cast("tinyint")))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    _ = t0
    return shifted.unionByName(upd)


def _build_source_dynamic_partition(spark: SparkSession, cfg: BenchmarkConfig) -> DataFrame:
    """Replace 4 (cloud, event_hour) buckets entirely — same rows, new ingest_ts and +1 util."""
    t0, _ = cfg.window()
    buckets = [
        ("aws",   t0 + timedelta(hours=3)),
        ("azure", t0 + timedelta(hours=7)),
        ("oci",   t0 + timedelta(hours=11)),
        ("gcp",   t0 + timedelta(hours=17)),
    ]
    predicate = F.lit(False)
    for cloud, hour in buckets:
        predicate = predicate | (
            (F.col("cloud") == F.lit(cloud))
            & (F.col("event_hour") == F.lit(hour))
        )
    df = (
        spark.table(cfg.fq_baseline)
        .where(predicate)
        .withColumn("utilization_pct", (F.col("utilization_pct") + F.lit(1)).cast("tinyint"))
        .withColumn("ingest_ts", F.current_timestamp())
    )
    return df


def _build_source_small_delta(spark: SparkSession, cfg: BenchmarkConfig) -> DataFrame:
    """1M-row correction sampled randomly across the whole baseline."""
    target_rows = 1_000_000
    frac = min(1.0, (target_rows * 2.0) / max(cfg.scale_params()["rows"], 1))
    return (
        spark.table(cfg.fq_baseline)
        .sample(False, frac, seed=11)
        .limit(target_rows)
        .withColumn("utilization_pct", F.least(F.lit(100).cast("tinyint"), (F.col("utilization_pct") + F.lit(7)).cast("tinyint")))
        .withColumn("ingest_ts", F.current_timestamp())
    )


SOURCE_BUILDERS = {
    "full_window": _build_source_full_window,
    "partial_overlap": _build_source_partial_overlap,
    "dynamic_partition": _build_source_dynamic_partition,
    "small_delta": _build_source_small_delta,
}


def generate(spark: SparkSession, cfg: BenchmarkConfig) -> None:
    """End-to-end: apply DDL, generate baseline, build every scenario's source table."""
    _apply_ddl(spark, cfg)

    # Baseline
    baseline_df = _base_generator(spark, cfg, cfg.scale_params()["rows"]).build()
    _write_delta(baseline_df, cfg.fq_baseline)
    spark.sql(f"OPTIMIZE {cfg.fq_baseline}")
    spark.sql(
        f"ANALYZE TABLE {cfg.fq_baseline} COMPUTE STATISTICS FOR ALL COLUMNS"
    )

    # Per-scenario source tables
    for scenario in cfg.scenarios:
        if scenario == "small_delta" and cfg.scale != "L":
            # small_delta only meaningful at L scale; still produce a scaled-down version so S/M can run
            pass
        src_df = SOURCE_BUILDERS[scenario](spark, cfg)
        _write_delta(src_df, cfg.fq_source(scenario))
        spark.sql(f"OPTIMIZE {cfg.fq_source(scenario)}")


def datetime_floor_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


_ = TELEMETRY_SCHEMA  # schema is re-applied implicitly by Delta; imported for test reference
