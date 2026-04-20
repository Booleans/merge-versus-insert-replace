-- Scenario: small delta into large target.
-- Strategy: INSERT ... REPLACE ON (MERGE-like, most direct equivalent)
INSERT INTO ${target} AS t
  REPLACE ON t.gpu_uuid <=> s.gpu_uuid AND t.event_ts <=> s.event_ts
  (SELECT * FROM ${source}) AS s;
