-- Scenario: dynamic partition overwrite.
-- Strategy: INSERT ... REPLACE WHERE - OR-of-AND form because REPLACE WHERE
-- does not support tuple-IN predicates (only simple comparisons / IN with literals).
INSERT INTO ${target}
  REPLACE WHERE ${partition_predicate}
SELECT * FROM ${source};
