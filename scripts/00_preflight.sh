#!/usr/bin/env bash
# Confirms the deploy actually landed, before anyone spends an hour on a phase
# that was going to fail at the first S3 write.
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

require aws
require terraform

echo "==> identity"
aws sts get-caller-identity --output table

RAW_BUCKET="$(tf_output lake raw_bucket)"
GOLD_BUCKET="$(tf_output lake gold_bucket)"
EMR_APP="$(tf_output pipeline emr_application_id)"
POLLER="$(tf_output ingestion poller_function_name)"

echo
echo "==> buckets"
aws s3 ls "s3://$RAW_BUCKET/"  >/dev/null && echo "  raw  $RAW_BUCKET  OK"
aws s3 ls "s3://$GOLD_BUCKET/" >/dev/null && echo "  gold $GOLD_BUCKET  OK"

echo
echo "==> EMR Serverless application"
aws emr-serverless get-application --application-id "$EMR_APP" \
  --query 'application.{id:applicationId,state:state,release:releaseLabel}' --output table

echo
echo "==> is the poller actually collecting?"
# The single most important check here. The poller gating nothing in this
# delivery is exactly why a silent failure would go unnoticed for weeks, and
# its data cannot be backfilled.
TODAY="$(date -u +%Y-%m-%d)"
COUNT="$(aws s3 ls "s3://$RAW_BUCKET/raw/station_status/dt=$TODAY/" --recursive 2>/dev/null | wc -l | tr -d ' ')"
echo "  objects landed today ($TODAY): $COUNT"
if [[ "$COUNT" -eq 0 ]]; then
  echo "  !! nothing landed today. Check the schedule and the function:"
  echo "     aws lambda invoke --function-name $POLLER /dev/stdout"
  echo "     aws logs tail /aws/lambda/$POLLER --since 1h"
else
  echo "  poller is healthy (expect ~288/day at the 5-minute interval)"
fi

echo
echo "==> model bundle present?"
if aws s3 ls "s3://$GOLD_BUCKET/models/current/model_net_flow.txt" >/dev/null 2>&1; then
  echo "  yes -- Pipeline 2 can run"
else
  echo "  no -- expected until phase 6 has run. The API will return 503."
fi
