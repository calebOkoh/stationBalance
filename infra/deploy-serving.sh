#!/usr/bin/env bash
set -euo pipefail

# Deploys Pipeline 2: the inference Lambda, the status refresher, the DynamoDB
# cube they write, and the HTTP API plus CloudFront in front of it.
#
# The two schedules are DISABLED until a model bundle exists. Apply this layer
# whenever you like -- the endpoint comes up immediately and returns 503 until
# there is something to serve -- then re-apply with -var enable_inference=true
# once scripts/40_publish_model.sh has run.
#
# Requires deploy-lake.sh to have run.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/serving"

# Override by exporting AWS_PROFILE yourself. The profile must be able to
# create IAM roles AND attach policies to them -- the deploy fails at the first
# aws_iam_role_policy otherwise.
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

echo "==> building Lambda packages"
"$PROJECT_ROOT/scripts/build_lambdas.sh"

echo "==> terraform init (serving)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (serving)"
terraform -chdir="$LAYER_DIR" apply -input=false "$@"

echo
echo "Serving layer up:"
terraform -chdir="$LAYER_DIR" output public_url
echo
if [[ "$(terraform -chdir="$LAYER_DIR" output -raw schedules_enabled)" == "false" ]]; then
  echo "Pipeline 2 schedules are DISABLED (no model bundle yet)."
  echo "After scripts/40_publish_model.sh, re-run:"
  echo "  ./deploy-serving.sh -var enable_inference=true"
fi
