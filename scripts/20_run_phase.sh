#!/usr/bin/env bash
# Phases 2-5 -- submits one EMR Serverless job run and waits for it.
#
# One phase per invocation, deliberately. Phases 2-3 and 4-5 are separate jobs
# because step 3.1 is the expensive stage and the training flow routes a failed
# baseline check (6.1) back to phase 4: fusing them would re-run the global
# bike_id sort on every feature experiment (.claude/decisions.md).
#
# Usage:  ./20_run_phase.sh conform|labels|features|assembly
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

PHASE="${1:-}"
case "$PHASE" in
  conform|labels|features|assembly) ;;
  *) echo "usage: $0 conform|labels|features|assembly" >&2; exit 1 ;;
esac

RAW_BUCKET="$(tf_output lake raw_bucket)"
GOLD_BUCKET="$(tf_output lake gold_bucket)"
GLUE_DB="$(tf_output lake glue_database)"
EMR_APP="$(tf_output pipeline emr_application_id)"
EMR_ROLE="$(tf_output pipeline emr_job_role_arn)"

CODE_URI="s3://$GOLD_BUCKET/code"

# Sync on every run rather than only when something changed: the whole tree is
# a few hundred KB, and a stale job script that silently runs last week's logic
# is far more expensive than the upload.
echo "==> syncing job code to $CODE_URI"
aws s3 sync "$PROJECT_ROOT/pipelines" "$CODE_URI/pipelines" --delete --exclude '__pycache__/*'
# features.json is written by the lake layer from features.yaml via
# yamldecode, so nothing here needs a YAML parser.
# calendarfeat.py is shared with the inference Lambda -- the SAME file, not a
# copy, which is what makes the 4.6 train/serve contract hold.
aws s3 cp "$PROJECT_ROOT/src/lambdas/common/calendarfeat.py" "$CODE_URI/calendarfeat.py"

echo "==> submitting phase '$PHASE'"
JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$EMR_APP" \
  --execution-role-arn "$EMR_ROLE" \
  --name "$PHASE-$(date -u +%Y%m%dT%H%M%SZ)" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"$CODE_URI/pipelines/$PHASE/job.py\",
      \"entryPointArguments\": [
        \"--raw-bucket\", \"$RAW_BUCKET\",
        \"--gold-bucket\", \"$GOLD_BUCKET\",
        \"--glue-database\", \"$GLUE_DB\",
        \"--features\", \"$CODE_URI/features.json\"
      ],
      \"sparkSubmitParameters\": \"--conf spark.archives= --py-files $CODE_URI/calendarfeat.py,$CODE_URI/pipelines/lib.py --conf spark.sql.session.timeZone=America/New_York --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory\"
    }
  }" \
  --configuration-overrides "{
    \"monitoringConfiguration\": {
      \"s3MonitoringConfiguration\": {\"logUri\": \"s3://$GOLD_BUCKET/emr-logs/\"}
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
      echo "Driver logs: s3://$GOLD_BUCKET/emr-logs/applications/$EMR_APP/jobs/$JOB_ID/"
      exit 1 ;;
  esac
  sleep 15
done
