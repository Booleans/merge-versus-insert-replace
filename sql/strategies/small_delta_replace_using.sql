-- Scenario: small delta into large target.
-- Strategy: INSERT ... REPLACE USING (gpu_uuid, event_ts) - pure key-based
INSERT INTO ${target}
  REPLACE USING (gpu_uuid, event_ts)
SELECT * FROM ${source};
