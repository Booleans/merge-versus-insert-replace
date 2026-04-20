-- Scenario: partial-overlap upserts.
-- Strategy: INSERT ... REPLACE WHERE
-- Replace the union of the touched (gpu_uuid, event_ts) pairs. The predicate
-- uses the source's time range plus a gpu_uuid IN list materialized at plan
-- time, so the engine only rewrites files in the source's time band.
INSERT INTO ${target}
  REPLACE WHERE event_ts >= TIMESTAMP'${t0}' AND event_ts < TIMESTAMP'${t1}'
SELECT * FROM (
  -- existing rows in the affected window that are NOT being corrected,
  -- unioned with the incoming source rows
  SELECT * FROM ${target}
    WHERE event_ts >= TIMESTAMP'${t0}' AND event_ts < TIMESTAMP'${t1}'
      AND NOT EXISTS (
        SELECT 1 FROM ${source} s
         WHERE s.gpu_uuid = ${target}.gpu_uuid
           AND s.event_ts = ${target}.event_ts
      )
  UNION ALL
  SELECT * FROM ${source}
);
