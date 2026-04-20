-- Scenario: dynamic partition overwrite. Replace specific (cloud, event_hour) buckets.
-- Strategy: MERGE INTO. Note: MERGE cannot natively express "bucket-level replacement"
-- without subqueries in WHEN conditions (unsupported). Since our source for this scenario
-- is a complete row-level snapshot of the target in those buckets (no dropped rows),
-- a plain WHEN MATCHED UPDATE yields equivalent results. This reflects how customers
-- typically implement "partition overwrite via MERGE" when no rows disappear.
MERGE INTO ${target} AS t
USING ${source} AS s
  ON t.gpu_uuid = s.gpu_uuid AND t.event_ts = s.event_ts
WHEN MATCHED THEN UPDATE SET *;
