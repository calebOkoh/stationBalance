#!/usr/bin/env bash
# Phase 1 -- drives the ingest Lambda through each source, in order.
#
# Every task is idempotent: /raw is immutable, so anything already landed is
# skipped rather than re-fetched (pipelines.md 1.1). Re-run this freely.
#
# Usage:  ./10_ingest.sh [task ...]     default: all of them
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

FUNCTION="$(tf_output ingestion ingest_function_name)"
TASKS=("$@")
if [[ ${#TASKS[@]} -eq 0 ]]; then
  # Order matters only in that `stations` supplies the go-live dates phase 3
  # needs; the rest are independent.
  TASKS=(stations station_info trips closures_bulk closures geo weather)
fi

for task in "${TASKS[@]}"; do
  echo
  echo "==> $task"
  OUT="$(mktemp)"
  # The weather and geo tasks run for minutes; the CLI's default read timeout
  # is 60s and would report a failure for a Lambda that is doing fine.
  ERR="$(aws lambda invoke \
    --function-name "$FUNCTION" \
    --cli-read-timeout 900 \
    --cli-binary-format raw-in-base64-out \
    --payload "{\"task\":\"$task\"}" \
    "$OUT" --output text --query 'FunctionError')"

  # A Lambda that raises still returns HTTP 200 with FunctionError set, so the
  # aws exit code alone would call a crashed invocation a success.
  if [[ "$ERR" != "None" ]]; then
    echo "!! $task failed ($ERR):"
    cat "$OUT"
    rm -f "$OUT"
    exit 1
  fi

  python3 -m json.tool < "$OUT"
  rm -f "$OUT"
done

echo
echo "Phase 1 done. Historical closures still need the one-time PGW pull:"
echo "  python3 scripts/pgw_backfill.py"
