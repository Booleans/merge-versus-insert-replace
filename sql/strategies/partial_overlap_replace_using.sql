-- Scenario: partial-overlap upserts.
-- Strategy: INSERT ... REPLACE USING (dynamic key overwrite)
INSERT INTO ${target}
  REPLACE USING (gpu_uuid, event_ts)
SELECT * FROM ${source};
