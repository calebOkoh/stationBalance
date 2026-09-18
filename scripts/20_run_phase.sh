#!/usr/bin/env bash
# Phases 1-5 -- submits one EMR Serverless job run and waits for it.
#
# One phase per invocation, deliberately. `labels` is the expensive stage and
# the training flow routes a failed baseline check (6.1) back to `features`:
# fusing them would re-run the global bike_id sort on every feature experiment
# (.claude/decisions.md).
#
# Sizing is passed EXPLICITLY. Without it Spark runs on the image defaults with
# dynamic allocation on, keeps asking for executors past the application's
# maximumCapacity, and every job summary is decorated with
# ApplicationMaxCapacityExceededException warnings that look like failures and
# are not. One driver plus two executors at 4 vCPU / 14 GB per container is
# 12 vCPU and 42 GB -- inside both the application cap and the account's
# 16 vCPU EMR Serverless quota (L-D05C8A75).
#
# Usage:  ./20_run_phase.sh land|conform|labels|features|assembly
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

PHASE="${1:-}"
case "$PHASE" in
  land|conform|labels|features|assembly) ;;
  *) echo "usage: $0 land|conform|labels|features|assembly" >&2; exit 1 ;;
esac

DATA_BUCKET="$(tf_output storage data_bucket)"
MODEL_BUCKET="$(tf_output storage model_bucket)"
GLUE_DB="$(tf_output storage glue_database)"
EMR_APP="$(tf_output pipeline emr_application_id)"
EMR_ROLE="$(tf_output pipeline emr_job_role_arn)"

CODE_URI="s3://$MODEL_BUCKET/code"

# Sync on every run rather than only when something changed: the whole tree is
# a few hundred KB, and a stale job script that silently runs last week's logic
# is far more expensive than the upload.
echo "==> syncing job code to $CODE_URI"
aws s3 sync "$PROJECT_ROOT/pipelines" "$CODE_URI/pipelines" --delete --exclude '__pycache__/*'
# features.json is written by the storage layer from features.yaml via
# yamldecode, so nothing here needs a YAML parser.

echo "==> submitting phase '$PHASE'"
JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$EMR_APP" \
  --execution-role-arn "$EMR_ROLE" \
  --name "$PHASE-$(date -u +%Y%m%dT%H%M%SZ)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"$CODE_URI/pipelines/$PHASE/job.py\",
      \"entryPointArguments\": [
        \"--data-bucket\", \"$DATA_BUCKET\",
        \"--model-bucket\", \"$MODEL_BUCKET\",
        \"--glue-database\", \"$GLUE_DB\",
        \"--features\", \"$CODE_URI/features.json\"
      ],
      \"sparkSubmitParameters\": \"--conf spark.archives= --py-files $CODE_URI/pipelines/calendarfeat.py,$CODE_URI/pipelines/lib.py --conf spark.driver.cores=4 --conf spark.driver.memory=12g --conf spark.driver.memoryOverhead=2g --conf spark.executor.cores=4 --conf spark.executor.memory=12g --conf spark.executor.memoryOverhead=2g --conf spark.executor.instances=2 --conf spark.dynamicAllocation.enabled=false --conf spark.sql.session.timeZone=America/New_York --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {\"logUri\": \"s3://$MODEL_BUCKET/emr-logs/\"}
    }
  }" \
  --query 'jobRunId' --output text)"

echo "    job run: $JOB_ID"
echo "==> waiting (ctrl-c is safe; the job keeps running)"

while true; do
  STATE="$(aws emr-serverless get-job-run \
    --application-id "$EMR_APP" --job-run-id "$JOB_ID" \
    --query 'jobRun.state' --output text)"
  printf '\r    %-12s' "$STATE"

  case "$STATE" in
    SUCCESS)
      echo; echo "Phase '$PHASE' succeeded."
      exit 0 ;;
    FAILED|CANCELLED)
      echo
      aws emr-serverless get-job-run \
        --application-id "$EMR_APP" --job-run-id "$JOB_ID" \
        --query 'jobRun.stateDetails' --output text
      echo "Driver logs: s3://$MODEL_BUCKET/emr-logs/applications/$EMR_APP/jobs/$JOB_ID/"
      exit 1 ;;
  esac
  sleep 15
done
