#!/usr/bin/env bash
set -euo pipefail

# Deploys every layer, in dependency order, in one go.
#
# What you get when this finishes:
#   * the lake, catalogued and cost-capped
#   * the station_status poller running every 5 minutes, from this moment
#   * the daily station_information and closure snapshots scheduled
#   * EMR Serverless and the SageMaker role ready to run phases 2-6
#   * a live public API endpoint, serving 503 until a model exists
#
# What you do NOT get is a trained model. Pipeline 1 is attended by design
# (.claude/decisions.md cut Step Functions because "every run is attended"),
# so the phases are driven by the numbered scripts in scripts/ afterwards.
# This script prints that running order at the end.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-coa-dev}"

# The lake goes first because every other layer resolves its buckets by name.
# The remaining three are independent of each other.
for layer in lake ingestion pipeline serving; do
  echo
  echo "############################################################"
  echo "# $layer"
  echo "############################################################"
  "$SCRIPT_DIR/deploy-$layer.sh" -auto-approve "$@"
done

cat <<'NEXT'

############################################################
# Deployed. Ingestion has started.
############################################################

The poller is already writing to /raw/station_status/ and will keep doing so.
Everything below is attended -- run it when you are ready to watch it.

  scripts/00_preflight.sh                  check the deploy landed
  scripts/10_ingest.sh                     phase 1: archives, weather, geo
  scripts/pgw_backfill.py                  step 1.6: the one-time PGW pull
  scripts/20_run_phase.sh conform          phase 2
  scripts/20_run_phase.sh labels           phase 3   <- the expensive one
  scripts/20_run_phase.sh features         phase 4
  scripts/20_run_phase.sh assembly         phase 5
  scripts/30_train.sh                      phase 6
  scripts/40_publish_model.sh              step 6.8, then enable Pipeline 2

NEXT
