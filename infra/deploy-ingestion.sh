#!/usr/bin/env bash
set -euo pipefail

# Deploys the collectors and turns them on.
#
# This is the layer that makes the project start collecting data. The
# station_status poller begins on apply and never stops: there is no published
# archive of dock occupancy, so every day it is not running is validation data
# that cannot be recovered (pipelines.md 0.5).
#
# Requires deploy-lake.sh to have run.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/ingestion"

# Override by exporting AWS_PROFILE yourself. The profile must be able to
# create IAM roles AND attach policies to them -- the deploy fails at the first
# aws_iam_role_policy otherwise.
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

# Lambda packages are staged before the plan, because Terraform hashes the zip
# at plan time to decide whether the function needs updating.
echo "==> building Lambda packages"
"$PROJECT_ROOT/scripts/build_lambdas.sh"

echo "==> terraform init (ingestion)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (ingestion)"
terraform -chdir="$LAYER_DIR" apply -input=false "$@"

echo
echo "Collectors live. The poller is now writing to /raw/station_status/."
echo "Kick off the historical backfill with:  scripts/10_ingest.sh"
