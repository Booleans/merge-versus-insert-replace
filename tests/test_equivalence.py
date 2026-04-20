"""Equivalence test: all four strategies must yield the same final row set.

Runs locally with a tiny dataset on DBR-connect / pyspark-Delta. Not a performance
check — just verifies the SQL renders and produces the correct result for each
scenario. Run with `pytest tests/` from a Databricks cluster or connected session.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from src.benchmark.run_benchmark import _deep_clone  # type: ignore[reportPrivateUsage]
from src.benchmark.scenarios import scenario_params
from src.benchmark.strategies import render
from src.common.config import STRATEGIES, BenchmarkConfig
from src.generate.generate_telemetry import generate


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("merge-versus-insert-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def cfg() -> BenchmarkConfig:
    return BenchmarkConfig(
        catalog="spark_catalog",
        schema="gpu_bench_test",
        scale="S",
        repeats=1,
        base_ts=datetime(2026, 4, 19, 0, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture(scope="module", autouse=True)
def prepare(spark, cfg):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    # Override rows for the test — keep it tiny
    from src.common import config as config_mod
    config_mod.SCALES["S"] = {
        "rows": 50_000,
        "hosts": 32,
        "gpus_per_host": 8,
        "window_hours": 6,
        "sample_interval_sec": 5,
    }
    generate(spark, cfg)


@pytest.mark.parametrize("scenario", ["full_window", "partial_overlap", "dynamic_partition"])
def test_strategies_equivalent(spark, cfg, scenario):
    params = scenario_params(scenario, cfg)
    targets: dict[str, str] = {}

    for strategy in STRATEGIES:
        fq = _deep_clone(spark, cfg, scenario, strategy)
        sql = render(
            scenario=scenario,
            strategy=strategy,
            target=fq,
            source=cfg.fq_source(scenario),
            t0=params.t0,
            t1=params.t1,
            partition_predicate=params.partition_predicate,
        )
        spark.sql(sql)
        targets[strategy] = fq

    ref = targets["merge"]
    for strategy, fq in targets.items():
        if strategy == "merge":
            continue
        # row-count equivalence
        ref_n = spark.table(ref).count()
        cand_n = spark.table(fq).count()
        assert ref_n == cand_n, f"{strategy}: count {cand_n} != merge {ref_n}"
        # key-set equivalence via anti-join
        diff = (
            spark.table(ref)
            .select("gpu_uuid", "event_ts")
            .exceptAll(spark.table(fq).select("gpu_uuid", "event_ts"))
            .count()
        )
        assert diff == 0, f"{strategy}: {diff} keys present in merge but missing"
