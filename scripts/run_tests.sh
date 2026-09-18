#!/usr/bin/env bash
set -euo pipefail

# Every test in the repo. No pytest dependency: each file is runnable on its
# own and reports PASS/FAIL per case, so this works in the container, on a
# laptop, and inside a Spark driver without three different invocations.
#
# test_recovery.py needs pyspark and SKIPS cleanly without it. That skip is not
# a pass: it is the gate README section 6 requires before phase 3 output can be
# trusted, and it has to run somewhere that has Spark.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

failures=0
for t in "$PROJECT_ROOT"/tests/test_*.py; do
  echo
  echo "=== $(basename "$t") ==="
  if ! python3 "$t"; then
    failures=$((failures + 1))
  fi
done

echo
if [[ $failures -gt 0 ]]; then
  echo "$failures test file(s) failed."
  exit 1
fi
echo "All test files passed."
