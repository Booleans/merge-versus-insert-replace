-- Scenario: full-window re-ingest. 100% overlap with window [${t0}, ${t1}).
-- Strategy: MERGE INTO (baseline)
MERGE INTO ${target} AS t
USING ${source} AS s
  ON t.gpu_uuid = s.gpu_uuid AND t.event_ts = s.event_ts
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
