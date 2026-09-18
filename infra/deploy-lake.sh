#!/usr/bin/env bash
set -euo pipefail

# Deploys the storage foundation: the two S3 zones, the read-only Glue catalog
# over them, the Athena workgroup that runs the QA gates, and the monthly
# budget.
#
# Run this FIRST. Every other layer looks its buckets up by name, so they fail
# at plan time until this one exists.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/lake"

# Override by exporting AWS_PROFILE yourself. The profile must be able to
# create IAM roles AND attach policies to them -- the deploy fails at the first
# aws_iam_role_policy otherwise.
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

echo "==> terraform init (lake)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (lake)"
terraform -chdir="$LAYER_DIR" apply -input=false "$@"

echo
echo "Lake ready:"
terraform -chdir="$LAYER_DIR" output
