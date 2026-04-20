"""Benchmark configuration: scales, scenarios, strategies, table names."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


SCALES: dict[str, dict] = {
    "S": {
        "rows": 10_000_000,
        "hosts": 1_250,
        "gpus_per_host": 8,
        "window_hours": 24,
        "sample_interval_sec": 10,
    },
    "M": {
        "rows": 100_000_000,
        "hosts": 12_500,
        "gpus_per_host": 8,
        "window_hours": 24,
        "sample_interval_sec": 10,
    },
    "L": {
        "rows": 1_000_000_000,
        "hosts": 125_000,
        "gpus_per_host": 8,
        "window_hours": 24 * 7,
        "sample_interval_sec": 6,
    },
}

SCENARIOS: list[str] = [
    "full_window",
    "partial_overlap",
    "dynamic_partition",
    "small_delta",
]

STRATEGIES: list[str] = [
    "merge",
    "replace_where",
    "replace_using",
    "replace_on",
]

COMPUTE_FLAVORS: list[str] = ["serverless_sql", "photon_cluster"]


@dataclass
class BenchmarkConfig:
    catalog: str = "main"
    schema: str = "dgx_benchmark"
    target_table: str = "gpu_telemetry"
    baseline_table: str = "gpu_telemetry_baseline"
    results_table: str = "bench_results"
    scale: str = "S"
    repeats: int = 3
    compute_flavor: str = "serverless_sql"
    scenarios: list[str] = field(default_factory=lambda: list(SCENARIOS))
    strategies: list[str] = field(default_factory=lambda: list(STRATEGIES))
    base_ts: datetime = field(
        default_factory=lambda: datetime(2026, 4, 19, 0, 0, 0, tzinfo=timezone.utc)
    )

    @property
    def fq_target(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.target_table}"

    @property
    def fq_baseline(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.baseline_table}"

    @property
    def fq_results(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.results_table}"

    def fq_source(self, scenario: str) -> str:
        return f"{self.catalog}.{self.schema}.src_{scenario}_{self.scale}"

    def fq_run_target(self, scenario: str, strategy: str) -> str:
        return f"{self.catalog}.{self.schema}.tgt_{scenario}_{strategy}_{self.scale}"

    def scale_params(self) -> dict:
        return SCALES[self.scale]

    def window(self) -> tuple[datetime, datetime]:
        p = self.scale_params()
        return self.base_ts, self.base_ts + timedelta(hours=p["window_hours"])
