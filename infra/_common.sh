#!/usr/bin/env bash
# Shared setup for every deploy script. Sourced, never executed.
#
# Resolves the AWS profile from exactly ONE place, so the name is never
# hardcoded in a script, a provider block, or a Terraform variable. It lives
# either in your environment or in infra/deploy.env, and nowhere else.
#
# Resolution order:
#   1. --profile NAME   (or --profile=NAME) on the command line
#   2. AWS_PROFILE      already exported in your shell
#   3. AWS_PROFILE=...  in infra/deploy.env   (gitignored; copy deploy.env.example)
#
# There is deliberately NO default. A default is how a stale name ends up
# baked into half a dozen files and fails at provider init months later.
#
# Note also that no Terraform provider block sets `profile`. The AWS provider's
# `profile` argument takes precedence over AWS_PROFILE, so setting it there
# would silently override whatever is resolved here.

INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Command-line args that are NOT --profile, forwarded to terraform.
TF_ARGS=()

_resolve_profile() {
  local args=("$@") i=0

  while [[ $i -lt ${#args[@]} ]]; do
    case "${args[$i]}" in
      --profile)
        i=$((i + 1))
        [[ $i -lt ${#args[@]} ]] || { echo "!! --profile needs a value" >&2; exit 1; }
        AWS_PROFILE="${args[$i]}"
        ;;
      --profile=*)
        AWS_PROFILE="${args[$i]#--profile=}"
        ;;
      *)
        TF_ARGS+=("${args[$i]}")
        ;;
    esac
    i=$((i + 1))
  done

  # deploy.env is the persistent answer, so the profile is typed once and not
  # once per invocation. It never overrides an explicit choice.
  if [[ -z "${AWS_PROFILE:-}" && -f "$INFRA_DIR/deploy.env" ]]; then
    # shellcheck disable=SC1091
    set -a; source "$INFRA_DIR/deploy.env"; set +a
  fi

  if [[ -z "${AWS_PROFILE:-}" ]]; then
    cat >&2 <<MSG

!! No AWS profile set. Pick one of:

   ./$(basename "$0") --profile <name>
   AWS_PROFILE=<name> ./$(basename "$0")
   echo 'AWS_PROFILE=<name>' > infra/deploy.env     # persists across runs

   Profiles configured here:
$(aws configure list-profiles 2>/dev/null | sed 's/^/     /' || echo "     (none)")

MSG
    exit 1
  fi

  export AWS_PROFILE
}

_resolve_profile "$@"

# Credentials are checked here rather than in deploy-all.sh so that running a
# single layer directly gets the same guard. The guard variable stops
# deploy-all re-running it once per child script.
if [[ -z "${STATION_BALANCE_PREFLIGHT_DONE:-}" ]]; then
  "$INFRA_DIR/preflight-credentials.sh" ${PREFLIGHT_LAYERS:-}
  export STATION_BALANCE_PREFLIGHT_DONE=1
fi
