-- Scenario: dynamic partition overwrite.
-- Strategy: INSERT ... REPLACE USING (cloud, event_hour) - natural fit
INSERT INTO ${target}
  REPLACE USING (cloud, event_hour)
SELECT * FROM ${source};
