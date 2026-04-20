-- Scenario: dynamic partition overwrite.
-- Strategy: INSERT ... REPLACE ON (bucket-level condition)
INSERT INTO ${target} AS t
  REPLACE ON t.cloud <=> s.cloud AND t.event_hour <=> s.event_hour
  (SELECT * FROM ${source}) AS s;
