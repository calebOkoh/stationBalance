#!/usr/bin/env bash
# Step 6.8 -- unpacks a finished training job's artifacts, checks the bundle is
# complete, and registers the version in the SageMaker Model Registry.
#
# "Versioned together, since a model and its preprocessing are one unit"
# (pipelines.md 6.8). Publishing is a separate, explicit step from training so
# a worse model never becomes the current one just because its job finished.
#
# Usage:  ./40_publish_model.sh <training-job-name>
source "$(dirname "${BASH_SOURCE[0]}")/_env.sh"

# --attribution-only publishes a VARIANT: it is versioned and registered, but
# it does NOT become models/current/. A no-lag run predicts worse by
# construction (it exists to measure total rather than direct effect), so the
# one thing that must never happen is it silently becoming the served bundle.
ATTRIBUTION_ONLY=0
ARGS=()
for a in "$@"; do
  case "$a" in
    --attribution-only) ATTRIBUTION_ONLY=1 ;;
    *) ARGS+=("$a") ;;
  esac
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

JOB_NAME="${1:-}"
[[ -n "$JOB_NAME" ]] || { echo "usage: $0 [--attribution-only] <training-job-name>" >&2; exit 1; }

MODEL_BUCKET="$(tf_output storage model_bucket)"
GROUP="$(tf_output pipeline model_package_group)"

ARTIFACT="s3://$MODEL_BUCKET/models/$JOB_NAME/output/model.tar.gz"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

echo "==> fetching $ARTIFACT"
aws s3 cp "$ARTIFACT" "$STAGE/model.tar.gz"
tar -xzf "$STAGE/model.tar.gz" -C "$STAGE"

# The gate. A model without its feature contract, its metrics, or its
# attribution is not a deliverable -- the attribution IS the research output
# (pipelines.md 6.7), not an extra.
REQUIRED=(model_net_flow.txt model_is_empty.txt features.json metrics.json attribution.json)
for f in "${REQUIRED[@]}"; do
  if [[ ! -f "$STAGE/$f" ]]; then
    echo "!! bundle is missing $f -- refusing to publish" >&2
    exit 1
  fi
done

echo "==> metrics for this candidate"
python3 -m json.tool < "$STAGE/metrics.json"
echo

# Cross-check the flag against the bundle rather than trusting the caller. The
# variant records what it dropped in metrics.json, so publishing one as the
# deliverable is caught here instead of at the point where someone reads
# models/current/ and gets the deliberately-worse model.
# A bundle is a VARIANT if it dropped feature groups OR was trained on an
# objective other than the deliverable's l1. The second half matters: an L2 run
# on the full feature set drops nothing, so a dropped-groups-only test would
# wave it through and let it overwrite models/current/.
read -r DROPPED OBJECTIVE <<<"$(python3 -c "
import json, sys
v = json.load(open(sys.argv[1])).get('variant', {})
print(','.join(v.get('dropped_feature_groups', [])) or '-', v.get('objective', 'l1'))
" "$STAGE/metrics.json")"
[[ "$DROPPED" == "-" ]] && DROPPED=""

IS_VARIANT=0
REASONS=()
[[ -n "$DROPPED" ]] && { IS_VARIANT=1; REASONS+=("dropped [$DROPPED]"); }
[[ "$OBJECTIVE" != "l1" ]] && { IS_VARIANT=1; REASONS+=("objective=$OBJECTIVE"); }
REASON="$(IFS=', '; echo "${REASONS[*]-}")"

if [[ "$IS_VARIANT" == "1" && "$ATTRIBUTION_ONLY" != "1" ]]; then
  echo "!! this bundle is an attribution variant ($REASON), not the deliverable." >&2
  echo "   Re-run with --attribution-only." >&2
  exit 1
fi
if [[ "$IS_VARIANT" != "1" && "$ATTRIBUTION_ONLY" == "1" ]]; then
  echo "!! --attribution-only given, but this bundle is the deliverable shape" >&2
  echo "   (no dropped groups, objective=l1)." >&2
  exit 1
fi

if [[ "$ATTRIBUTION_ONLY" == "1" ]]; then
  echo "VARIANT ($REASON). It will be versioned and registered as"
  echo "PendingManualApproval, and will NOT be copied to models/current/."
  read -r -p "Register this attribution variant? [y/N] " ok
else
  read -r -p "Publish this model to models/current/? [y/N] " ok
fi
[[ "$ok" == "y" || "$ok" == "Y" ]] || { echo "Not published."; exit 0; }

# Keep the immutable copy under the job name, then point `current` at it. The
# versioned copy is what makes a rollback a copy rather than a retrain.
echo "==> publishing"
for f in "${REQUIRED[@]}"; do
  aws s3 cp "$STAGE/$f" "s3://$MODEL_BUCKET/models/versions/$JOB_NAME/$f"
  [[ "$ATTRIBUTION_ONLY" == "1" ]] || \
    aws s3 cp "$STAGE/$f" "s3://$MODEL_BUCKET/models/current/$f"
done

echo "==> registering in the model package group"
if [[ "$ATTRIBUTION_ONLY" == "1" ]]; then
  DESCRIPTION="station-balance $JOB_NAME -- ATTRIBUTION VARIANT ($REASON); do not serve"
  APPROVAL=PendingManualApproval
else
  DESCRIPTION="station-balance $JOB_NAME"
  APPROVAL=Approved
fi

aws sagemaker create-model-package \
  --model-package-group-name "$GROUP" \
  --model-package-description "$DESCRIPTION" \
  --model-approval-status "$APPROVAL" \
  --inference-specification "{
    \"Containers\": [{
      \"Image\": \"683313688378.dkr.ecr.$AWS_REGION.amazonaws.com/sagemaker-scikit-learn:1.2-1-cpu-py3\",
      \"ModelDataUrl\": \"$ARTIFACT\"
    }],
    \"SupportedContentTypes\": [\"application/json\"],
    \"SupportedResponseMIMETypes\": [\"application/json\"]
  }" \
  --query 'ModelPackageArn' --output text

if [[ "$ATTRIBUTION_ONLY" == "1" ]]; then
cat <<NEXT

Versioned at s3://$MODEL_BUCKET/models/versions/$JOB_NAME/
models/current/ is UNCHANGED -- this is an attribution variant ($REASON).
Registered as PendingManualApproval in the Model Registry group: $GROUP
NEXT
else
cat <<NEXT

Published to s3://$MODEL_BUCKET/models/current/
Registered in the SageMaker Model Registry group: $GROUP

That is the deliverable. Read the research output with:

  aws s3 cp s3://$MODEL_BUCKET/models/current/attribution.json - | python3 -m json.tool
  aws s3 cp s3://$MODEL_BUCKET/models/current/metrics.json - | python3 -m json.tool

Nothing serves this model. What would consume it is drawn in
docs/live_inference.drawio and is not built.
NEXT
fi
