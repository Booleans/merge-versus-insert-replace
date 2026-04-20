"""GPU telemetry schema shared by generator, benchmark, and tests."""

from __future__ import annotations

from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


COLUMN_NAMES: list[str] = [
    "event_ts",
    "event_hour",
    "cloud",
    "region",
    "datacenter",
    "host_id",
    "gpu_uuid",
    "gpu_index",
    "gpu_model",
    "driver_version",
    "utilization_pct",
    "mem_used_mib",
    "mem_total_mib",
    "power_w",
    "temp_c",
    "sm_clock_mhz",
    "mem_clock_mhz",
    "pcie_rx_kbps",
    "pcie_tx_kbps",
    "nvlink_rx_kbps",
    "nvlink_tx_kbps",
    "ecc_sb_corrected",
    "ecc_db_uncorr",
    "xid_errors",
    "throttle_reasons",
    "ingest_ts",
]


TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("event_hour", TimestampType(), nullable=False),
        StructField("cloud", StringType(), nullable=False),
        StructField("region", StringType(), nullable=False),
        StructField("datacenter", StringType(), nullable=False),
        StructField("host_id", StringType(), nullable=False),
        StructField("gpu_uuid", StringType(), nullable=False),
        StructField("gpu_index", ByteType(), nullable=False),
        StructField("gpu_model", StringType(), nullable=False),
        StructField("driver_version", StringType(), nullable=True),
        StructField("utilization_pct", ByteType(), nullable=True),
        StructField("mem_used_mib", IntegerType(), nullable=True),
        StructField("mem_total_mib", IntegerType(), nullable=True),
        StructField("power_w", ShortType(), nullable=True),
        StructField("temp_c", ByteType(), nullable=True),
        StructField("sm_clock_mhz", ShortType(), nullable=True),
        StructField("mem_clock_mhz", ShortType(), nullable=True),
        StructField("pcie_rx_kbps", IntegerType(), nullable=True),
        StructField("pcie_tx_kbps", IntegerType(), nullable=True),
        StructField("nvlink_rx_kbps", LongType(), nullable=True),
        StructField("nvlink_tx_kbps", LongType(), nullable=True),
        StructField("ecc_sb_corrected", LongType(), nullable=True),
        StructField("ecc_db_uncorr", LongType(), nullable=True),
        StructField("xid_errors", IntegerType(), nullable=True),
        StructField("throttle_reasons", LongType(), nullable=True),
        StructField("ingest_ts", TimestampType(), nullable=False),
    ]
)


CLOUDS = ["aws", "azure", "oci", "gcp"]
REGIONS = {
    "aws": ["us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1"],
    "azure": ["eastus2", "westus3", "westeurope", "japaneast"],
    "oci": ["us-ashburn-1", "us-phoenix-1", "eu-frankfurt-1", "ap-tokyo-1"],
    "gcp": ["us-central1", "us-east4", "europe-west4", "asia-northeast1"],
}
GPU_MODELS = ["H100", "H200", "B100", "B200"]
DRIVER_VERSIONS = ["550.54.15", "550.90.07", "555.42.02", "560.28.03"]
