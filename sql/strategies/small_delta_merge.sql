-- Scenario: small delta (1M rows) into a large target (1B rows). Update-only.
-- Strategy: MERGE INTO (baseline)
MERGE INTO ${target} AS t
USING ${source} AS s
  ON t.gpu_uuid = s.gpu_uuid AND t.event_ts = s.event_ts
WHEN MATCHED THEN UPDATE SET *;
