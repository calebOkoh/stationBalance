#!/usr/bin/env bash
set -euo pipefail

# Deploys the storage foundation: the two S3 buckets, the read-only Glue
# catalog over them, the Athena workgroup that runs the QA gates, and the
# monthly budget.
#
# Run this FIRST. Every other layer looks its buckets up by name, so they fail
# at plan time until this one exists.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAYER_DIR="$SCRIPT_DIR/storage"

# Resolves AWS_PROFILE and populates TF_ARGS with everything else. The profile
# name lives in your environment or infra/deploy.env -- never in this file.
PREFLIGHT_LAYERS=storage
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh" "$@"

echo "==> terraform init (storage)"
terraform -chdir="$LAYER_DIR" init -input=false

echo "==> terraform apply (storage)"
terraform -chdir="$LAYER_DIR" apply -input=false "${TF_ARGS[@]}"

echo
echo "Storage ready:"
terraform -chdir="$LAYER_DIR" output
