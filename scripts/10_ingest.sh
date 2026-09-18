#!/usr/bin/env bash
# Downloads the historical archives, one ingest-Lambda task at a time.
#
# Every task is idempotent: /raw is immutable, so anything already landed is
# skipped rather than re-fetched (pipelines.md 1.1). Re-run this freely.
#
# Usage:  ./10_ingest.sh [task ...]     default: all of them
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

FUNCTION="$(tf_output ingestion ingest_function_name)"
TASKS=("$@")
if [[ ${#TASKS[@]} -eq 0 ]]; then
  # Independent of each other; `stations` first only because it is the
  # fastest check that the Lambda and its permissions work.
  TASKS=(stations trips weather)
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
echo "Downloads complete. Convert them to Parquet with:"
echo "  scripts/20_run_phase.sh land"
