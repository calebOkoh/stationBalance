#!/usr/bin/env bash
# The phase-3 recovery GATE -- README section 6, "Recovery test (3.2 / 3.3)".
#
# tests/test_recovery.py injects synthetic van moves with known station pairs
# and timings, runs the ACTUAL 3.2/3.3 functions imported from
# pipelines/labels/job.py, and asserts exact recovery of every injected move.
# It also asserts that two trips either side of a quarter boundary at the same
# station produce NO event -- the specific failure a per-quarter pass injects.
#
# It runs HERE, on EMR Serverless, rather than on a laptop, for two reasons:
# the Spark version is the one phase 3 will actually use, and this container
# has no JVM. "That skip is not a pass" (README section 6) -- a skipped gate
# and a passed gate are different things, and only this script produces the
# second.
#
# The test exits non-zero on failure, so a failed gate surfaces as a FAILED job
# run. There is no extra assertion plumbing.
#
# Run it after `20_run_phase.sh conform` and BEFORE `20_run_phase.sh labels`.
#
# Usage:  ./25_run_gate.sh
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

MODEL_BUCKET="$(tf_output storage model_bucket)"
EMR_APP="$(tf_output pipeline emr_application_id)"
EMR_ROLE="$(tf_output pipeline emr_job_role_arn)"

CODE_URI="s3://$MODEL_BUCKET/code"

# 20_run_phase.sh syncs pipelines/ only. The gate needs the test itself, and it
# needs the job module it imports -- shipping a COPY of the emission logic is
# the one thing that would make this test meaningless.
echo "==> syncing gate code to $CODE_URI"
aws s3 sync "$PROJECT_ROOT/pipelines" "$CODE_URI/pipelines" --delete --exclude '__pycache__/*'
aws s3 cp "$PROJECT_ROOT/tests/test_recovery.py" "$CODE_URI/tests/test_recovery.py"

echo "==> submitting the phase-3 recovery gate"
JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$EMR_APP" \
  --execution-role-arn "$EMR_ROLE" \
  --name "gate-recovery-$(date -u +%Y%m%dT%H%M%SZ)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"$CODE_URI/tests/test_recovery.py\",
      \"sparkSubmitParameters\": \"--conf spark.archives= --py-files $CODE_URI/pipelines/labels/job.py,$CODE_URI/pipelines/lib.py --conf spark.driver.cores=4 --conf spark.driver.memory=12g --conf spark.driver.memoryOverhead=2g --conf spark.executor.cores=4 --conf spark.executor.memory=12g --conf spark.executor.memoryOverhead=2g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.sql.session.timeZone=America/New_York\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {\"logUri\": \"s3://$MODEL_BUCKET/emr-logs/\"}
    }
  }" \
  --query 'jobRunId' --output text)"

echo "    job run: $JOB_ID"
echo "==> waiting"

while true; do
  STATE="$(aws emr-serverless get-job-run \
    --application-id "$EMR_APP" --job-run-id "$JOB_ID" \
    --query 'jobRun.state' --output text)"
  printf '\r    %-12s' "$STATE"

  case "$STATE" in
    SUCCESS)
      echo
      echo "GATE PASSED. Phase 3 emission logic recovers every injected move."
      echo "Driver log (the per-test PASS lines):"
      echo "  s3://$MODEL_BUCKET/emr-logs/applications/$EMR_APP/jobs/$JOB_ID/SPARK_DRIVER/stdout.gz"
      exit 0 ;;
    FAILED|CANCELLED)
      echo
      echo "GATE FAILED. Do NOT trust phase 3 output (README section 6)."
      aws emr-serverless get-job-run \
        --application-id "$EMR_APP" --job-run-id "$JOB_ID" \
        --query 'jobRun.stateDetails' --output text
      echo "Driver logs: s3://$MODEL_BUCKET/emr-logs/applications/$EMR_APP/jobs/$JOB_ID/"
      exit 1 ;;
  esac
  sleep 15
done
