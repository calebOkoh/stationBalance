#!/usr/bin/env bash
set -euo pipefail

# Checks the resolved identity can actually do what the layers require, BEFORE
# any apply starts.
#
# The IAM requirement is derived from the layer's own .tf files, not hardcoded
# here. A blanket "you need IAM rights" is wrong: the storage layer creates no
# IAM at all, and blocking it on a permission it never uses is a false
# negative that costs a real deployment.
#
# Usage:  preflight-credentials.sh [layer ...]     default: all three

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAYERS=("$@")
[[ ${#LAYERS[@]} -gt 0 ]] || LAYERS=(storage ingestion pipeline)

PROFILE="${AWS_PROFILE:-}"
echo "==> checking credentials (AWS_PROFILE=${PROFILE:-<unset>})"

if [[ -n "$PROFILE" ]] && ! aws configure list-profiles 2>/dev/null | grep -qx "$PROFILE"; then
  cat >&2 <<MSG

!! AWS profile '$PROFILE' is not configured on this machine.

   Profiles found:
$(aws configure list-profiles 2>/dev/null | sed 's/^/     /')

   Add it with:  aws configure --profile $PROFILE

MSG
  exit 1
fi

if ! IDENTITY="$(aws sts get-caller-identity --output json 2>&1)"; then
  cat >&2 <<MSG

!! could not authenticate with profile '${PROFILE:-<default chain>}':

$(echo "$IDENTITY" | sed 's/^/   /')

   The profile exists but has no credentials. Supply them with:
     aws configure --profile ${PROFILE:-<name>}

MSG
  exit 1
fi

ARN="$(echo "$IDENTITY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Arn"])')"
echo "    $ARN"

# Which of the requested layers actually create IAM? Read it off the source.
NEEDS_IAM=()
for layer in "${LAYERS[@]}"; do
  if grep -qlE '^resource "aws_iam_(role_policy|role_policy_attachment)"' \
       "$INFRA_DIR/$layer"/*.tf 2>/dev/null; then
    NEEDS_IAM+=("$layer")
  fi
done

if [[ ${#NEEDS_IAM[@]} -eq 0 ]]; then
  echo "    no IAM policy changes in: ${LAYERS[*]}"
  exit 0
fi

DECISIONS="$(aws iam simulate-principal-policy \
  --policy-source-arn "$ARN" \
  --action-names iam:CreateRole iam:PutRolePolicy iam:AttachRolePolicy iam:PassRole \
  --query 'EvaluationResults[].join(`=`, [EvalActionName, EvalDecision])' \
  --output text 2>/dev/null || true)"

if [[ -z "$DECISIONS" ]]; then
  # Simulation itself needs iam:SimulatePrincipalPolicy. Not being able to run
  # it is not evidence of failure -- proceed and let the apply speak.
  echo "    (could not simulate IAM permissions; continuing)"
  exit 0
fi

BLOCKED=""
for d in $DECISIONS; do
  [[ "${d##*=}" == "allowed" ]] || BLOCKED="$BLOCKED ${d%%=*}"
done

if [[ -n "$BLOCKED" ]]; then
  OK_LAYERS=()
  for layer in "${LAYERS[@]}"; do
    printf '%s\n' "${NEEDS_IAM[@]}" | grep -qx "$layer" || OK_LAYERS+=("$layer")
  done

  cat >&2 <<MSG

!! '$ARN' cannot perform:$BLOCKED

   Blocked layers:   ${NEEDS_IAM[*]}
   Unaffected:       ${OK_LAYERS[*]:-(none)}

   These layers create roles and then attach policies to them. Without the
   attach, the roles exist but grant nothing, and Lambda / EMR Serverless /
   SageMaker cannot assume anything useful -- so the apply is stopped here
   rather than leaving half-built roles behind.

MSG
  if [[ ${#OK_LAYERS[@]} -gt 0 ]]; then
    cat >&2 <<MSG
   To deploy only what this identity CAN do:
$(printf '     ./deploy-%s.sh\n' "${OK_LAYERS[@]}")

MSG
  fi
  cat >&2 <<MSG
   To deploy everything, use an identity with those permissions:
     ./deploy-all.sh --profile <admin-profile>

MSG
  exit 1
fi

echo "    IAM: can create and attach role policies (needed by: ${NEEDS_IAM[*]})"
