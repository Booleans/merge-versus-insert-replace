-- Scenario: full-window re-ingest.
-- Strategy: INSERT ... REPLACE WHERE (predicate-based time-window overwrite)
INSERT INTO ${target}
  REPLACE WHERE event_ts >= TIMESTAMP'${t0}' AND event_ts < TIMESTAMP'${t1}'
SELECT * FROM ${source};
