#!/usr/bin/env bash
set -euo pipefail

# Deploys every layer, in dependency order.
#
# What this gets you: somewhere to put data, a Lambda that can download the
# archives, EMR Serverless and SageMaker ready to run, and a model registry to
# publish into.
#
# What it does NOT get you is anything running. There is no schedule in this
# project and nothing reads a live feed. Every phase is driven by hand from
# scripts/, in order, and this prints that order at the end.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Resolves AWS_PROFILE and populates TF_ARGS with everything else. The profile
# name lives in your environment or infra/deploy.env -- never in this file.
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh" "$@"

# Storage goes first because the other two resolve its buckets by name. The
# remaining two are independent of each other.
for layer in storage ingestion pipeline; do
  echo
  echo "############################################################"
  echo "# $layer"
  echo "############################################################"
  "$SCRIPT_DIR/deploy-$layer.sh" --profile "$AWS_PROFILE" -auto-approve "${TF_ARGS[@]}"
done

cat <<'NEXT'

############################################################
# Deployed. Nothing is running — that is intentional.
############################################################

  scripts/00_preflight.sh                  check the deploy landed
  scripts/10_ingest.sh                     download trips, stations, weather
  scripts/20_run_phase.sh land             raw -> Parquet
  scripts/20_run_phase.sh conform          normalise, filter, dedupe
  scripts/20_run_phase.sh labels           bike_id trajectories -> net_flow
  scripts/20_run_phase.sh features         weather + calendar + lags
  scripts/20_run_phase.sh assembly         wide table, chronological split
  scripts/30_train.sh                      train, evaluate, attribute
  scripts/40_publish_model.sh <job-name>   register the model

NEXT
