# MERGE vs INSERT REPLACE benchmark


## Scenario: full_window

| scale | scenario | strategy | compute | median (s) | rel MERGE | files +/- | MB written | runs |
|---|---|---|---|---:|---:|---|---:|---:|
| L | full_window | merge | serverless | 3.96 | 1.0 | 1/-0 | 0 | 3 |
| L | full_window | replace_on | serverless | 2.59 | 0.66 | 0/-0 | 14 | 3 |
| L | full_window | replace_using | serverless | 2.15 | 0.54 | 0/-0 | 14 | 3 |
| L | full_window | replace_where | serverless | 2.18 | 0.55 | 0/-0 | 14 | 3 |
| M | full_window | merge | serverless | 3.72 | 1.0 | 1/-0 | 0 | 3 |
| M | full_window | replace_on | serverless | 2.41 | 0.65 | 0/-0 | 1 | 3 |
| M | full_window | replace_using | serverless | 2.27 | 0.61 | 0/-0 | 1 | 3 |
| M | full_window | replace_where | serverless | 2.07 | 0.56 | 0/-0 | 1 | 3 |
| S | full_window | merge | serverless | 4.55 | 1.0 | 1/-0 | 0 | 3 |
| S | full_window | replace_on | serverless | 2.76 | 0.61 | 0/-0 | 0 | 3 |
| S | full_window | replace_using | serverless | 2.67 | 0.59 | 0/-0 | 0 | 3 |
| S | full_window | replace_where | serverless | 2.26 | 0.5 | 0/-0 | 0 | 3 |

## Scenario: partial_overlap

| scale | scenario | strategy | compute | median (s) | rel MERGE | files +/- | MB written | runs |
|---|---|---|---|---:|---:|---|---:|---:|
| L | partial_overlap | merge | serverless | 4.86 | 1.0 | 1/-0 | 0 | 3 |
| L | partial_overlap | replace_on | serverless | 3.85 | 0.79 | 0/-0 | 29 | 3 |
| L | partial_overlap | replace_using | serverless | 3.90 | 0.8 | 0/-0 | 29 | 3 |
| L | partial_overlap | replace_where | serverless | 3.11 | 0.64 | 0/-1 | 29 | 3 |
| M | partial_overlap | merge | serverless | 4.30 | 1.0 | 1/-0 | 0 | 3 |
| M | partial_overlap | replace_on | serverless | 1.87 | 0.43 | 0/-0 | 2 | 3 |
| M | partial_overlap | replace_using | serverless | 2.53 | 0.59 | 0/-0 | 2 | 3 |
| M | partial_overlap | replace_where | serverless | 2.29 | 0.53 | 0/-1 | 2 | 3 |
| S | partial_overlap | merge | serverless | 4.66 | 1.0 | 1/-0 | 0 | 3 |
| S | partial_overlap | replace_on | serverless | 3.00 | 0.64 | 0/-0 | 0 | 3 |
| S | partial_overlap | replace_using | serverless | 3.25 | 0.7 | 0/-0 | 0 | 3 |
| S | partial_overlap | replace_where | serverless | 2.46 | 0.53 | 0/-1 | 0 | 3 |

## Scenario: dynamic_partition

| scale | scenario | strategy | compute | median (s) | rel MERGE | files +/- | MB written | runs |
|---|---|---|---|---:|---:|---|---:|---:|
| L | dynamic_partition | merge | serverless | 4.03 | 1.0 | 1/-0 | 0 | 3 |
| L | dynamic_partition | replace_on | serverless | 2.15 | 0.53 | 0/-0 | 14 | 3 |
| L | dynamic_partition | replace_using | serverless | 2.40 | 0.59 | 0/-0 | 14 | 3 |
| L | dynamic_partition | replace_where | serverless | 2.03 | 0.5 | 0/-0 | 14 | 3 |
| M | dynamic_partition | merge | serverless | 2.00 | 1.0 | 0/-0 | 0 | 3 |
| M | dynamic_partition | replace_on | serverless | 2.12 | 1.06 | 0/-0 | 1 | 3 |
| M | dynamic_partition | replace_using | serverless | 2.32 | 1.16 | 0/-0 | 1 | 3 |
| M | dynamic_partition | replace_where | serverless | 2.16 | 1.08 | 0/-0 | 1 | 3 |
| S | dynamic_partition | merge | serverless | 4.29 | 1.0 | 1/-0 | 0 | 3 |
| S | dynamic_partition | replace_on | serverless | 2.35 | 0.55 | 0/-0 | 0 | 3 |
| S | dynamic_partition | replace_using | serverless | 2.46 | 0.57 | 0/-0 | 0 | 3 |
| S | dynamic_partition | replace_where | serverless | 2.44 | 0.57 | 0/-0 | 0 | 3 |

## Scenario: small_delta

| scale | scenario | strategy | compute | median (s) | rel MERGE | files +/- | MB written | runs |
|---|---|---|---|---:|---:|---|---:|---:|
| L | small_delta | merge | serverless | 5.60 | 1.0 | 1/-0 | 0 | 3 |
| L | small_delta | replace_on | serverless | 2.62 | 0.47 | 0/-0 | 14 | 3 |
| L | small_delta | replace_using | serverless | 2.95 | 0.53 | 0/-0 | 14 | 3 |
| L | small_delta | replace_where | serverless | 2.66 | 0.48 | 0/-1 | 14 | 3 |
| M | small_delta | merge | serverless | 3.97 | 1.0 | 1/-0 | 0 | 3 |
| M | small_delta | replace_on | serverless | 1.92 | 0.48 | 0/-0 | 1 | 3 |
| M | small_delta | replace_using | serverless | 2.59 | 0.65 | 0/-0 | 1 | 3 |
| M | small_delta | replace_where | serverless | 2.24 | 0.56 | 0/-0 | 16 | 3 |
| S | small_delta | merge | serverless | 4.71 | 1.0 | 1/-0 | 0 | 3 |
| S | small_delta | replace_on | serverless | 2.72 | 0.58 | 0/-0 | 0 | 3 |
| S | small_delta | replace_using | serverless | 3.10 | 0.66 | 0/-0 | 0 | 3 |
| S | small_delta | replace_where | serverless | 2.08 | 0.44 | 0/-1 | 0 | 3 |