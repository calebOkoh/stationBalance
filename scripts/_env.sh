#!/usr/bin/env bash
# Shared environment for the numbered scripts. Sourced, never executed.
#
# Every value is read from Terraform outputs rather than hardcoded, so there is
# no second place for a bucket name to live and drift.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INFRA_DIR="$PROJECT_ROOT/infra"

# Same single source as the deploy scripts: AWS_PROFILE from the environment
# or infra/deploy.env, never hardcoded here.
if [[ -z "${AWS_PROFILE:-}" && -f "$INFRA_DIR/deploy.env" ]]; then
  set -a; source "$INFRA_DIR/deploy.env"; set +a
fi
if [[ -z "${AWS_PROFILE:-}" ]]; then
  echo "!! No AWS profile set. Export AWS_PROFILE or write infra/deploy.env" >&2
  echo "   (see infra/deploy.env.example)" >&2
  exit 1
fi
export AWS_PROFILE
export AWS_REGION="${AWS_REGION:-us-east-1}"

tf_output() {
  local layer="$1" name="$2"
  terraform -chdir="$INFRA_DIR/$layer" output -raw "$name" 2>/dev/null || {
    echo "!! could not read output '$name' from layer '$layer'." >&2
    echo "   Has infra/deploy-$layer.sh been run?" >&2
    exit 1
  }
}

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "!! $1 is required but not installed" >&2; exit 1; }
}
