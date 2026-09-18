#!/usr/bin/env bash
set -euo pipefail

# Stages the Lambda source into build/lambdas/<name>/ with the shared helper
# alongside it, so the deployment package is a flat directory Terraform can zip
# via `archive_file`.
#
# This is the whole build. There is no pip install, no wheel, no layer: every
# handler is standard library plus boto3, and boto3 ships in the Lambda
# runtime. That is a deliberate constraint: the function only downloads bytes
# and writes them to S3, and the raw -> Parquet conversion is a Spark job.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SRC="$PROJECT_ROOT/src/lambdas"
BUILD="$PROJECT_ROOT/build/lambdas"

FUNCTIONS=(ingest)

rm -rf "$BUILD"

for fn in "${FUNCTIONS[@]}"; do
  if [[ ! -f "$SRC/$fn/handler.py" ]]; then
    echo "!! missing $SRC/$fn/handler.py" >&2
    exit 1
  fi

  mkdir -p "$BUILD/$fn"
  cp "$SRC/$fn"/*.py "$BUILD/$fn/"
  cp "$SRC/common"/*.py "$BUILD/$fn/"

  # __pycache__ would make the zip hash unstable across machines, so every
  # apply would show a spurious source_code_hash diff.
  find "$BUILD/$fn" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

  echo "==> built $fn ($(find "$BUILD/$fn" -name '*.py' | wc -l | tr -d ' ') files)"
done

echo "Lambda packages staged in $BUILD"
