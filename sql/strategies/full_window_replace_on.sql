-- Scenario: full-window re-ingest.
-- Strategy: INSERT ... REPLACE ON (MERGE-like arbitrary predicate, DBR 17.1+)
INSERT INTO ${target} AS t
  REPLACE ON t.gpu_uuid <=> s.gpu_uuid AND t.event_ts <=> s.event_ts
  (SELECT * FROM ${source}) AS s;
