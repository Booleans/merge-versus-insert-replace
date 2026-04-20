"""Scenario-specific runtime parameters (time windows, partition lists)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.common.config import BenchmarkConfig


@dataclass
class ScenarioParams:
    t0: datetime
    t1: datetime
    partition_predicate: str = ""  # OR-of-AND form for REPLACE WHERE (tuple IN unsupported)


def scenario_params(scenario: str, cfg: BenchmarkConfig) -> ScenarioParams:
    base_t0, base_t1 = cfg.window()
    if scenario == "full_window":
        # the last hour of the baseline window
        t0 = base_t1 - timedelta(hours=1)
        t1 = base_t1
        return ScenarioParams(t0=t0, t1=t1)

    if scenario == "partial_overlap":
        # source contains: (a) 20% updates inside [base_t0, base_t1) and
        # (b) 80% fresh rows shifted by window_hours into [base_t1, base_t1+window_hours)
        window_h = cfg.scale_params()["window_hours"]
        t0 = base_t0
        t1 = base_t1 + timedelta(hours=window_h)
        return ScenarioParams(t0=t0, t1=t1)

    if scenario == "dynamic_partition":
        buckets = [
            ("aws",   base_t0 + timedelta(hours=3)),
            ("azure", base_t0 + timedelta(hours=7)),
            ("oci",   base_t0 + timedelta(hours=11)),
            ("gcp",   base_t0 + timedelta(hours=17)),
        ]
        predicate = " OR ".join(
            f"(cloud = '{c}' AND event_hour = TIMESTAMP'{h.strftime('%Y-%m-%d %H:%M:%S')}')"
            for c, h in buckets
        )
        return ScenarioParams(t0=base_t0, t1=base_t1, partition_predicate=predicate)

    if scenario == "small_delta":
        # source spans full baseline window (could be anywhere within it)
        return ScenarioParams(t0=base_t0, t1=base_t1)

    raise ValueError(f"unknown scenario: {scenario}")
