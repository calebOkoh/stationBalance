#!/usr/bin/env bash
set -euo pipefail

# Deploys the batch compute: the EMR Serverless application for phases 2-5, the
# SageMaker role for phase 6, and the model package group that is the handoff
# to Pipeline 2.
#
# Nothing here runs on a schedule. Pipeline 1 is attended -- ordering comes
# from the numbered scripts in scripts/, not from an orchestrator.
#
# Requires deploy-lake.sh to have run.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/pipeline"

# Override by exporting AWS_PROFILE yourself. The profile must be able to
# create IAM roles AND attach policies to them -- the deploy fails at the first
# aws_iam_role_policy otherwise.
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

echo "==> terraform init (pipeline)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (pipeline)"
terraform -chdir="$LAYER_DIR" apply -input=false "$@"

echo
echo "Pipeline compute ready. Phases are run by hand, in order:"
echo "  scripts/10_ingest.sh          phase 1   (Lambda)"
echo "  scripts/20_run_phase.sh conform|labels|features|assembly   (EMR Serverless)"
echo "  scripts/30_train.sh           phase 6   (SageMaker)"
echo "  scripts/40_publish_model.sh   step 6.8  (bundle -> s3, enables Pipeline 2)"
