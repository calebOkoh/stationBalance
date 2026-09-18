#!/usr/bin/env bash
# Confirms the deploy actually landed, before anyone spends an hour on a phase
# that was going to fail at the first S3 write.
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

require aws
require terraform

echo "==> identity"
aws sts get-caller-identity --output table

DATA_BUCKET="$(tf_output storage data_bucket)"
MODEL_BUCKET="$(tf_output storage model_bucket)"
EMR_APP="$(tf_output pipeline emr_application_id)"
INGEST="$(tf_output ingestion ingest_function_name)"

echo
echo "==> buckets"
aws s3 ls "s3://$DATA_BUCKET/"  >/dev/null && echo "  data  $DATA_BUCKET  OK"
aws s3 ls "s3://$MODEL_BUCKET/" >/dev/null && echo "  model $MODEL_BUCKET  OK"

echo
echo "==> feature contract published"
if aws s3 ls "s3://$MODEL_BUCKET/code/features.json" >/dev/null 2>&1; then
  echo "  features.json OK"
else
  echo "  !! missing — re-run infra/deploy-storage.sh"
fi

echo
echo "==> EMR Serverless application"
aws emr-serverless get-application --application-id "$EMR_APP" \
  --query 'application.{id:applicationId,state:state,release:releaseLabel}' --output table

echo
echo "==> ingest function"
aws lambda get-function-configuration --function-name "$INGEST" \
  --query '{name:FunctionName,runtime:Runtime,timeout:Timeout,memory:MemorySize}' --output table

echo
echo "==> what has been downloaded so far"
for prefix in trips stations weather; do
  # `aws s3 ls` exits 1 on an empty prefix, which is the normal state before
  # the first ingest run -- so it must not be allowed to trip `set -e`.
  n="$(aws s3 ls "s3://$DATA_BUCKET/raw/$prefix/" --recursive 2>/dev/null | wc -l | tr -d ' ' || true)"
  printf '  raw/%-10s %s objects\n' "$prefix" "${n:-0}"
done

echo
echo "==> confirm nothing is scheduled"
# There should be no schedules at all. Anything listed here is a live-data
# collector that does not belong in this project.
n="$(aws scheduler list-schedules --query 'length(Schedules)' --output text 2>/dev/null || echo 0)"
if [[ "$n" == "0" ]]; then
  echo "  no EventBridge schedules — correct"
else
  echo "  !! $n schedule(s) exist; this project should have none"
  aws scheduler list-schedules --query 'Schedules[].Name' --output text
fi
