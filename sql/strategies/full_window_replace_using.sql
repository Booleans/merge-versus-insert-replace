-- Scenario: full-window re-ingest.
-- Strategy: INSERT ... REPLACE USING (dynamic key overwrite)
INSERT INTO ${target}
  REPLACE USING (gpu_uuid, event_ts)
SELECT * FROM ${source};
