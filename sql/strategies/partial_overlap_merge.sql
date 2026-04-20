-- Scenario: partial-overlap upserts (80% new + 20% corrections).
-- Strategy: MERGE INTO (baseline)
MERGE INTO ${target} AS t
USING ${source} AS s
  ON t.gpu_uuid = s.gpu_uuid AND t.event_ts = s.event_ts
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
