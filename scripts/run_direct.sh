#!/usr/bin/env bash
# run_direct.sh — DAB-free reproduction path.
#
# Uses the Databricks CLI directly (workspace sync + jobs submit) when the
# `databricks bundle deploy` Terraform download fails (known PGP-signature bug
# on some CLI versions).
#
# Usage:
#   scripts/run_direct.sh <profile> <catalog> [scales]
#
#   <profile> — Databricks CLI profile (e.g. "dev" or "my-workspace")
#   <catalog> — Unity Catalog to write into (must already exist, you must have CREATE SCHEMA)
#   [scales]  — Space-separated list; default: "S M L"
#
# Example:
#   scripts/run_direct.sh dev main "S M"

set -euo pipefail

PROFILE="${1:?usage: $0 <profile> <catalog> [scales]}"
CATALOG="${2:?usage: $0 <profile> <catalog> [scales]}"
SCALES="${3:-S M L}"
SCHEMA="${SCHEMA:-gpu_bench}"
REPEATS="${REPEATS:-3}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

USER_EMAIL="$(databricks current-user me -p "$PROFILE" --output json | python3 -c 'import json,sys;print(json.load(sys.stdin)["emails"][0]["value"])')"
WS_ROOT="/Users/${USER_EMAIL}/merge-versus-insert"

log() { printf '\033[0;34m[run_direct]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[0;31m[run_direct]\033[0m %s\n' "$*" >&2; exit 1; }

log "Profile=$PROFILE  Catalog=$CATALOG  Schema=$SCHEMA  Scales=$SCALES"
log "Syncing $ROOT -> $WS_ROOT"
databricks sync "$ROOT" "$WS_ROOT" -p "$PROFILE" --full \
  --exclude '.git/**' --exclude '.claude/**' --exclude '/report/**' \
  --exclude 'scripts/**' --exclude '**/__pycache__/**' >&2

submit_notebook() {
  # $1 = run_name  $2 = notebook_basename  $3 = scale (or "-" for report)  $4 = compute_flavor
  local run_name="$1" nb="$2" scale="$3" flavor="$4"
  local base_params
  if [[ "$scale" == "-" ]]; then
    base_params="{\"catalog\":\"$CATALOG\",\"schema\":\"$SCHEMA\"}"
  else
    base_params="{\"catalog\":\"$CATALOG\",\"schema\":\"$SCHEMA\",\"scale\":\"$scale\",\"compute_flavor\":\"$flavor\",\"repeats\":\"$REPEATS\"}"
  fi
  local deps
  case "$nb" in
    01_generate) deps='["dbldatagen>=0.4.0"]';;
    03_report)   deps='["plotly>=5.20.0"]';;
    *)           deps='[]';;
  esac

  cat > "$TMP_DIR/submit.json" <<JSON
{
  "run_name": "$run_name",
  "tasks": [{
    "task_key": "${nb//_/-}",
    "notebook_task": {
      "notebook_path": "${WS_ROOT}/notebooks/${nb}",
      "base_parameters": $base_params
    },
    "environment_key": "default",
    "timeout_seconds": 14400
  }],
  "environments": [{
    "environment_key": "default",
    "spec": {"client": "3", "dependencies": $deps}
  }]
}
JSON
  local rid
  rid=$(databricks jobs submit -p "$PROFILE" --json "@$TMP_DIR/submit.json" --no-wait \
        | python3 -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
  log "Submitted $run_name run_id=$rid; polling (checks every 60s)..."
  while :; do
    local life result
    life=$(databricks jobs get-run "$rid" -p "$PROFILE" --output json \
           | python3 -c 'import json,sys;print(json.load(sys.stdin)["state"]["life_cycle_state"])')
    if [[ "$life" == "TERMINATED" || "$life" == "INTERNAL_ERROR" || "$life" == "SKIPPED" ]]; then
      result=$(databricks jobs get-run "$rid" -p "$PROFILE" --output json \
               | python3 -c 'import json,sys;print(json.load(sys.stdin)["state"].get("result_state",""))')
      log "$run_name -> $life / $result"
      [[ "$result" == "SUCCESS" ]] || fail "$run_name failed (see Databricks UI run $rid)"
      break
    fi
    sleep 60
  done
}

for scale in $SCALES; do
  submit_notebook "gpu-generate-$scale"  "01_generate"  "$scale" "serverless"
  submit_notebook "gpu-benchmark-$scale" "02_benchmark" "$scale" "serverless"
done

submit_notebook "gpu-report" "03_report" "-" "serverless"

log "Downloading report to $ROOT/report"
databricks workspace export-dir "${WS_ROOT}/report" "$ROOT/report" -p "$PROFILE" --overwrite >&2
log "Done. Open report/results.html for interactive view."
