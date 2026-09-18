#!/usr/bin/env bash
# Step 6.8 -- unpacks a finished training job's artifacts into the bundle
# location the inference Lambda reads, then registers the version.
#
# "The inference pipeline must load exactly these. Versioned together, since a
# model and its preprocessing are one unit" (pipelines.md 6.8). Publishing is a
# separate, explicit step from training so a worse model never reaches the API
# just because its job happened to finish.
#
# Usage:  ./40_publish_model.sh <training-job-name>
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

JOB_NAME="${1:-}"
[[ -n "$JOB_NAME" ]] || { echo "usage: $0 <training-job-name>" >&2; exit 1; }

GOLD_BUCKET="$(tf_output lake gold_bucket)"
GROUP="$(tf_output pipeline model_package_group)"

ARTIFACT="s3://$GOLD_BUCKET/models/$JOB_NAME/output/model.tar.gz"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo "==> fetching $ARTIFACT"
aws s3 cp "$ARTIFACT" "$STAGE/model.tar.gz"
tar -xzf "$STAGE/model.tar.gz" -C "$STAGE"

# The gate. Every one of these is something the inference Lambda opens by name
# on its first invocation, so a partial bundle would surface as an hourly
# CloudWatch error rather than here.
REQUIRED=(model_net_flow.txt model_is_empty.txt features.json serving_context.json metrics.json attribution.json)
for f in "${REQUIRED[@]}"; do
  if [[ ! -f "$STAGE/$f" ]]; then
    echo "!! bundle is missing $f -- refusing to publish" >&2
    exit 1
  fi
done

echo "==> metrics for this candidate"
python3 -m json.tool < "$STAGE/metrics.json"
echo
read -r -p "Publish this model to models/current/? [y/N] " ok
[[ "$ok" == "y" || "$ok" == "Y" ]] || { echo "Not published."; exit 0; }

# Keep the immutable copy under the job name, then point `current` at it. The
# versioned copy is what makes a rollback a copy rather than a retrain.
echo "==> publishing"
for f in "${REQUIRED[@]}"; do
  aws s3 cp "$STAGE/$f" "s3://$GOLD_BUCKET/models/versions/$JOB_NAME/$f"
  aws s3 cp "$STAGE/$f" "s3://$GOLD_BUCKET/models/current/$f"
done

echo "==> registering in the model package group"
aws sagemaker create-model-package \
  --model-package-group-name "$GROUP" \
  --model-package-description "station-balance $JOB_NAME" \
  --model-approval-status Approved \
  --inference-specification "{
    \"Containers\": [{
      \"Image\": \"683313688378.dkr.ecr.$AWS_REGION.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3\",
      \"ModelDataUrl\": \"$ARTIFACT\"
    }],
    \"SupportedContentTypes\": [\"application/json\"],
    \"SupportedResponseMIMETypes\": [\"application/json\"]
  }" \
  --query 'ModelPackageArn' --output text

cat <<NEXT

Published to s3://$GOLD_BUCKET/models/current/

Pipeline 2 can now run. If its schedules are still disabled, turn them on:

  cd infra && ./deploy-serving.sh -var enable_inference=true

Then the first cube lands within the hour, or immediately with:

  aws lambda invoke --function-name station-balance-inference \\
    --cli-read-timeout 900 /dev/stdout
NEXT
