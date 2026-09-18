#!/usr/bin/env bash
set -euo pipefail

# Deploys the batch compute: the EMR Serverless application for phases 2-5, the
# SageMaker role for phase 6, and the model package group that is the handoff
# the trained model is registered into.
#
# Nothing here runs on a schedule. Pipeline 1 is attended -- ordering comes
# from the numbered scripts in scripts/, not from an orchestrator.
#
# Requires deploy-storage.sh to have run.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/pipeline"

# Resolves AWS_PROFILE and populates TF_ARGS with everything else. The profile
# name lives in your environment or infra/deploy.env -- never in this file.
PREFLIGHT_LAYERS=pipeline
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh" "$@"

echo "==> terraform init (pipeline)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (pipeline)"
terraform -chdir="$LAYER_DIR" apply -input=false "${TF_ARGS[@]}"

echo
echo "Pipeline compute ready. Phases are run by hand, in order:"
echo "  scripts/10_ingest.sh          download the archives  (Lambda)"
echo "  scripts/20_run_phase.sh land|conform|labels|features|assembly   (EMR Serverless)"
echo "  scripts/30_train.sh           phase 6                (SageMaker)"
echo "  scripts/40_publish_model.sh   step 6.8, register the model"
