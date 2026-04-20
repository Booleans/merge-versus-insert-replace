-- Scenario: partial-overlap upserts.
-- Strategy: INSERT ... REPLACE ON (MERGE-like)
INSERT INTO ${target} AS t
  REPLACE ON t.gpu_uuid <=> s.gpu_uuid AND t.event_ts <=> s.event_ts
  (SELECT * FROM ${source}) AS s;
