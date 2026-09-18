#!/usr/bin/env bash
set -euo pipefail

# Deploys the ingest Lambda.
#
# Nothing starts running when this applies. The function downloads published
# archives and is invoked by hand through scripts/10_ingest.sh -- there is no
# schedule, because there is no live data in this project.
#
# Requires deploy-storage.sh to have run.

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
echo "Ingest function deployed. Download the archives with:"
echo "  scripts/10_ingest.sh"
