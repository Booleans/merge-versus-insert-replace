# MERGE vs INSERT REPLACE — GPU telemetry benchmark

A reproducible Databricks benchmark comparing four Delta-table write strategies for high-volume time-series ingest, built around a realistic multi-cloud GPU-telemetry workload (AWS / Azure / OCI / GCP).

- `MERGE INTO`                               — the baseline
- `INSERT ... REPLACE WHERE <predicate>`     — DBR 12.2 LTS+
- `INSERT ... REPLACE USING (cols)`          — DBR 16.3+ (liquid clustering support: 17.2+)
- `INSERT ... REPLACE ON <boolean>`          — DBR 17.1+

Four scenarios × four strategies × three scales (10M / 100M / 1B rows) × 3 repeats = **144 timed runs**.

## Headline results

Median wall-clock seconds on Databricks **serverless compute version 18.0** (≈ DBR 18.0 / Spark 4.1, backed the serverless job runtime as of Feb 2026), with liquid clustering enabled, deletion vectors on, Photon. Bold = winner; `×` = multiple of MERGE.

| Scenario          | Scale | MERGE | REPLACE WHERE | REPLACE USING | REPLACE ON |       Best          |
|-------------------|:---:  |------:|--------------:|--------------:|-----------:|---------------------|
| full_window       | S     | 4.55  | **2.26**      | 2.67          | 2.76       | REPLACE WHERE 0.50× |
| full_window       | M     | 3.72  | **2.07**      | 2.27          | 2.41       | REPLACE WHERE 0.56× |
| full_window       | L     | 3.96  | 2.18          | **2.15**      | 2.59       | REPLACE USING 0.54× |
| partial_overlap   | S     | 4.66  | **2.46**      | 3.25          | 3.00       | REPLACE WHERE 0.53× |
| partial_overlap   | M     | 4.30  | 2.29          | 2.53          | **1.87**   | REPLACE ON    0.43× |
| partial_overlap   | L     | 4.86  | **3.11**      | 3.90          | 3.85       | REPLACE WHERE 0.64× |
| dynamic_partition | S     | 4.29  | 2.44          | 2.46          | **2.35**   | REPLACE ON    0.55× |
| dynamic_partition | M     |**2.00**| 2.16         | 2.32          | 2.12       | MERGE (only inversion) |
| dynamic_partition | L     | 4.03  | **2.03**      | 2.40          | 2.15       | REPLACE WHERE 0.50× |
| small_delta       | S     | 4.71  | **2.08**      | 3.10          | 2.72       | REPLACE WHERE 0.44× |
| small_delta       | M     | 3.97  | 2.24          | 2.59          | **1.92**   | REPLACE ON    0.48× |
| small_delta       | L     | 5.60  | 2.66          | 2.95          | **2.62**   | REPLACE ON    0.47× |

**Takeaways**

1. For 11 of 12 (scale, scenario) pairs, an `INSERT REPLACE` variant beats `MERGE` by 30–60% wall-clock on the same data, same cluster, same layout.
2. `REPLACE WHERE` wins most time-window re-ingest patterns — the predicate is the cheapest to plan.
3. `REPLACE ON` wins mixed upsert/insert patterns (partial overlap, small delta into large table) — it has the same semantic power as MERGE but a tighter plan.
4. `REPLACE USING` is strongest when the key columns align with physical layout (liquid clustering here).
5. The only case where MERGE wins is `dynamic_partition` at M-scale — the source covers 4 buckets out of 96 and the key-based MERGE plan is narrower than REPLACE's rewrite. At L-scale, REPLACE retakes the lead by 2×.

Full 12-row table, all 48 per-scale rows, and an interactive chart per scenario live in [`results/results.md`](results/results.md) and [`results/results.html`](results/results.html).

## Scenarios

| # | Scenario              | Source shape                                         | Real-world analog                              |
|---|-----------------------|------------------------------------------------------|-----------------------------------------------|
| 1 | `full_window`         | 100% overlap — replace the last hour                 | Reprocess last hour after a pipeline bug      |
| 2 | `partial_overlap`     | 80% new + 20% corrections on `(gpu_uuid, event_ts)`  | Normal ingest with late-arriving corrections  |
| 3 | `dynamic_partition`   | Replace 4 `(cloud, event_hour)` buckets entirely     | Cross-cloud reconciliation or region backfill |
| 4 | `small_delta`         | 1M-row update into a 1B-row target                   | Bulk correction (metric recalibration, ECC)   |

## Data model

A single fact table `gpu_telemetry`, liquid-clustered by `(cloud, event_hour, gpu_uuid)` so every strategy plans against the same physical layout.

```sql
CREATE TABLE gpu_telemetry (
  event_ts         TIMESTAMP,
  event_hour       TIMESTAMP,     -- date_trunc('hour', event_ts)
  cloud            STRING,        -- aws | azure | oci | gcp
  region           STRING,
  datacenter       STRING,
  host_id          STRING,
  gpu_uuid         STRING,        -- deterministic hash of (host_id, gpu_index)
  gpu_index        TINYINT,       -- 0..7 per host
  gpu_model        STRING,        -- H100 | H200 | B100 | B200
  driver_version   STRING,
  utilization_pct  TINYINT,
  mem_used_mib     INT,  mem_total_mib   INT,
  power_w          SMALLINT, temp_c        TINYINT,
  sm_clock_mhz     SMALLINT, mem_clock_mhz SMALLINT,
  pcie_rx_kbps     INT,  pcie_tx_kbps    INT,
  nvlink_rx_kbps   BIGINT, nvlink_tx_kbps BIGINT,
  ecc_sb_corrected BIGINT, ecc_db_uncorr  BIGINT,
  xid_errors       INT,  throttle_reasons BIGINT,  -- NVML bitmask
  ingest_ts        TIMESTAMP
) CLUSTER BY (cloud, event_hour, gpu_uuid);
```

Uniqueness is enforced on `(gpu_uuid, event_ts)` before write so MERGE can match unambiguously — this matches correct telemetry pipeline semantics.

## Reproduce

### Prerequisites

- Databricks workspace with Unity Catalog. Minimum runtime is DBR 17.1 (for `REPLACE ON`); the committed numbers were collected on **serverless compute version 18.0** (≈ DBR 18.0 / Spark 4.1) — submitted jobs use `environment_key` with `client: "3"` to pin the Spark Connect client contract, while the server-side runtime auto-tracks the current serverless release.
- A catalog you can write to — defaults to `main`, override with `--var="catalog=<yours>"`.
- Databricks CLI ≥ 0.218, authenticated.
- If you hit the Terraform-checksum bug in `databricks bundle deploy` (embedded `terraform` binary with expired PGP signatures), fall back to the direct-API path in [`scripts/run_direct.sh`](scripts/run_direct.sh).

### One-command DAB path

```bash
databricks bundle deploy -t dev --var="catalog=<yours>"
databricks bundle run generate  --var="catalog=<yours>" --var="scale=S"
databricks bundle run benchmark --var="catalog=<yours>" --var="scale=S" --var="repeats=3"
databricks bundle run report    --var="catalog=<yours>"
# Repeat generate/benchmark for scale=M and scale=L.
```

### Direct-API fallback (no Terraform)

```bash
# Upload + submit one-shot jobs. Mirrors what the DAB would do.
scripts/run_direct.sh <databricks-profile> <catalog>
```

The script uploads the project, submits the generate/benchmark/report jobs as one-shot runs on serverless compute, and downloads the report files back to `report/`.

### Cost / time budget

Serverless SQL, 3 repeats, all 4 scenarios × 4 strategies:

| Scale | Rows  | Generate | Benchmark | Notes                                   |
|-------|-------|----------|-----------|-----------------------------------------|
| S     | 10 M  | ~3 min   | ~15 min   | Smoke test                              |
| M     | 100 M | ~6 min   | ~15 min   | Indicative production-like answer       |
| L     | 1 B   | ~20 min  | ~25 min   | Storage-heavy (deep clones × 48 runs)   |

## Layout

```
.
├── README.md
├── LICENSE
├── databricks.yml              DAB root
├── resources/
│   ├── jobs.yml                generate / benchmark / report jobs
│   └── warehouse.yml           Pro serverless SQL warehouse
├── src/
│   ├── common/                 schema, config, timing helpers
│   ├── generate/               dbldatagen baseline + scenario sources
│   ├── benchmark/              SQL rendering, scenarios, orchestrator
│   └── report/                 markdown + HTML + Plotly charts
├── sql/
│   ├── ddl.sql                 baseline + results tables (liquid clustered)
│   └── strategies/             16 templates — 4 scenarios × 4 strategies
├── notebooks/
│   ├── 01_generate.py
│   ├── 02_benchmark.py
│   └── 03_report.py
├── tests/
│   └── test_equivalence.py     all strategies must yield identical row sets
├── scripts/
│   └── run_direct.sh           DAB-free path for the Terraform-bug workaround
└── results/
    ├── results.md              this repo's committed run (serverless, S+M+L)
    └── results.html            interactive charts
```

## Fairness rules (read before citing these numbers)

- **Identical starting state.** Each (scenario, strategy, repeat) runs against a fresh `DEEP CLONE` of `gpu_telemetry_baseline`. No strategy ever sees another's output.
- **Identical physical layout.** Liquid clustering on `(cloud, event_hour, gpu_uuid)` applied once to the baseline and inherited by every clone.
- **Identical compute.** All runs in a batch use the same serverless client. DBR 17.1+.
- **Single SQL statement.** Each strategy is one SQL statement — no Python-side tricks MERGE couldn't match.
- **`OPTIMIZE` is excluded.** Applied once to the baseline before runs. Not counted in any strategy's time.
- **Randomized run order within a batch** to neutralize warm-cache effects.
- **Equivalence check.** After each non-MERGE run, row count and `(gpu_uuid, event_ts)` key set are compared against the MERGE result for the same scenario; mismatches are flagged in `bench_results.notes`.

## Known limitations & honest disclosures

- **Delta `operationMetrics` map is partially empty** — rendered as `0` / `0/-0` in the report tables for `MB written` and `files +/-` on some rows. Root cause: Spark Connect serialization of `Map<String,String>` loses some numeric fields when collected through `DESCRIBE HISTORY`. The wall-clock times are accurate; the Delta-metric capture needs to be rewritten to fetch each key individually. See [`src/common/timing.py`](src/common/timing.py).
- **`dbr_version` column in `bench_results` is `"unknown"` for every row.** The harness reads `spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")` which serverless intentionally hides. Version identification for this committed run is derived from the serverless compute release notes — **version 18.0 has been the active runtime since Feb 27 2026**. Re-runs on a classic cluster would populate the column.
- **Serverless compute only.** All measured numbers are from serverless job compute (not a SQL warehouse). A Photon all-purpose cluster or a pinned SQL warehouse will show different absolute numbers but similar relative patterns. `resources/jobs.yml` has an unused all-purpose cluster spec commented in for that path.
- **`dynamic_partition` MERGE SQL is simplified.** A literal "replace all rows in these buckets" via MERGE requires subqueries inside `WHEN MATCHED` conditions, which Delta doesn't support. The SQL file uses `WHEN MATCHED THEN UPDATE SET *` instead — a simplification that holds because our source for this scenario is a complete snapshot of the touched buckets (no dropped rows). This is flagged in [`sql/strategies/dynamic_partition_merge.sql`](sql/strategies/dynamic_partition_merge.sql).
- **`REPLACE WHERE` predicate grammar is limited.** Tuple `IN (...)` is not supported; `dynamic_partition_replace_where.sql` uses the OR-of-AND equivalent. See [the INSERT docs](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-dml-insert-into) for the full supported grammar.
- **dbldatagen collisions.** Random `event_ts` within a 10-second grid collides occasionally. Source tables are deduplicated on `(gpu_uuid, event_ts)` before write. Row counts per scale may be a few percent below the nominal 10M / 100M / 1B.

## When to use each (decision matrix)

| Pattern                                              | Recommended            | Why                                                                   |
|------------------------------------------------------|------------------------|-----------------------------------------------------------------------|
| Re-ingest a bounded time window (last hour, last day)| **REPLACE WHERE**      | Predicate-only DELETE, simplest plan, no source-join required         |
| Dynamic "partition" overwrite by key                 | **REPLACE USING / ON** | Engine derives the delete from source keys                            |
| Upsert with arbitrary match condition                | **REPLACE ON** or MERGE | REPLACE ON for simple replace; MERGE when you need `WHEN NOT MATCHED BY TARGET/SOURCE` logic |
| Small % of rows updated in a large table             | **REPLACE ON**         | Consistently beat MERGE in our tests, including at 1B-row L-scale     |

## Extending

- **Different keys / partitioning** → edit [`src/common/schema.py`](src/common/schema.py) and [`sql/ddl.sql`](sql/ddl.sql).
- **New scenario** → add SQL files `sql/strategies/<scenario>_<strategy>.sql` (one per strategy), a source builder in [`src/generate/generate_telemetry.py`](src/generate/generate_telemetry.py), parameters in [`src/benchmark/scenarios.py`](src/benchmark/scenarios.py), and include the name in `SCENARIOS` in [`src/common/config.py`](src/common/config.py).

## Sources

- [INSERT (Databricks SQL) — official syntax](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-dml-insert-into)
- [Selective overwrites with Delta Lake](https://docs.databricks.com/aws/en/delta/selective-overwrite)
- [MERGE INTO (Databricks SQL)](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)
- [SunnyData — New INSERT features: REPLACE ON and REPLACE USING](https://www.sunnydata.ai/blog/databricks-insert-replace-on-using-new-sql-features)

## License

MIT — see [LICENSE](LICENSE).
