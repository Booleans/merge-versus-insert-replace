-- Scenario: small delta into large target.
-- Strategy: INSERT ... REPLACE WHERE within the source's time band.
-- Rewrites files in the band, preserving non-updated rows via UNION ALL.
INSERT INTO ${target}
  REPLACE WHERE event_ts >= TIMESTAMP'${t0}' AND event_ts < TIMESTAMP'${t1}'
SELECT * FROM (
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
