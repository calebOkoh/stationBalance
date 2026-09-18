#!/usr/bin/env bash
set -euo pipefail

# Tears every layer down, in reverse dependency order.
#
# The S3 buckets are versioned and Terraform will refuse to delete them while
# objects remain. That is deliberate: raw/ is the downloaded archive set and
# training/ holds the splits every reported number traces back to. Empty the
# buckets by hand first if you genuinely mean it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

echo "This destroys the station-balance infrastructure in:"
aws sts get-caller-identity --query 'Account' --output text
echo
read -r -p "Type the account number to confirm: " confirm
if [[ "$confirm" != "$(aws sts get-caller-identity --query 'Account' --output text)" ]]; then
  echo "Aborted." >&2
  exit 1
fi

for layer in pipeline ingestion storage; do
  echo "==> destroying $layer"
  terraform -chdir="$SCRIPT_DIR/$layer" destroy -input=false "$@"
done
