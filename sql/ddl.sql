-- DDL for the benchmark. Executed by src/generate/generate_telemetry.py.
-- Placeholders ${catalog} and ${schema} are substituted at runtime.

CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA  IF NOT EXISTS ${catalog}.${schema};

-- Baseline target table (deep-cloned per run to isolate strategies)
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.gpu_telemetry_baseline (
  event_ts         TIMESTAMP NOT NULL,
  event_hour       TIMESTAMP NOT NULL,
  cloud            STRING    NOT NULL,
  region           STRING    NOT NULL,
  datacenter       STRING    NOT NULL,
  host_id          STRING    NOT NULL,
  gpu_uuid         STRING    NOT NULL,
  gpu_index        TINYINT   NOT NULL,
  gpu_model        STRING    NOT NULL,
  driver_version   STRING,
  utilization_pct  TINYINT,
  mem_used_mib     INT,
  mem_total_mib    INT,
  power_w          SMALLINT,
  temp_c           TINYINT,
  sm_clock_mhz     SMALLINT,
  mem_clock_mhz    SMALLINT,
  pcie_rx_kbps     INT,
  pcie_tx_kbps     INT,
  nvlink_rx_kbps   BIGINT,
  nvlink_tx_kbps   BIGINT,
  ecc_sb_corrected BIGINT,
  ecc_db_uncorr    BIGINT,
  xid_errors       INT,
  throttle_reasons BIGINT,
  ingest_ts        TIMESTAMP NOT NULL
)
CLUSTER BY (cloud, event_hour, gpu_uuid)
TBLPROPERTIES (
  'delta.enableDeletionVectors'     = 'true',
  'delta.enableChangeDataFeed'      = 'false',
  'delta.feature.deletionVectors'   = 'supported',
  'delta.autoOptimize.optimizeWrite'= 'true',
  'delta.autoOptimize.autoCompact'  = 'true'
);

-- Results table
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.bench_results (
  run_id          STRING    NOT NULL,
  run_ts          TIMESTAMP NOT NULL,
  scale           STRING    NOT NULL,
  scenario        STRING    NOT NULL,
  strategy        STRING    NOT NULL,
  compute_flavor  STRING    NOT NULL,
  repeat_index    INT       NOT NULL,
  wall_seconds    DOUBLE    NOT NULL,
  delta_operation STRING,
  delta_version   BIGINT,
  delta_metrics   MAP<STRING, STRING>,
  dbr_version     STRING,
  notes           STRING
)
CLUSTER BY (scale, scenario, strategy);
