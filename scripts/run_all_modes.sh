#!/usr/bin/env bash
# run_all_modes.sh — Build and run all three TaxiTopology variants sequentially,
# then produce a comparison table.
#
# Usage:
#   bash scripts/run_all_modes.sh [--duration-s N]
#
# Output:
#   ~/ra-stream-metrics/local/taxi/metrics.csv

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DURATION_S="${DURATION_S:-120}"
for arg in "$@"; do
  case "$arg" in
    --duration-s) shift; DURATION_S="$1" ;;
  esac
done

export METRICS_PATH="${METRICS_PATH:-$HOME/ra-stream-metrics/local/taxi}"
mkdir -p "$METRICS_PATH"

JAR="$REPO_ROOT/target/ra-stream-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR" ]; then
  echo "Building..."
  mvn -q package -DskipTests
fi

run_mode() {
  local mode="$1"
  echo ""
  echo "============================================================"
  echo "  Running variant: $mode  (duration=${DURATION_S}s)"
  echo "============================================================"
  java -jar "$JAR" --mode "$mode" --duration-s "$DURATION_S"
}

run_mode "basic"
run_mode "rastream"
run_mode "rastream-ft"

echo ""
echo "============================================================"
echo "  All variants complete. Metrics: $METRICS_PATH/metrics.csv"
echo "  Generating comparison table..."
echo "============================================================"
python3 "$REPO_ROOT/scripts/compare_metrics.py" "$METRICS_PATH/metrics.csv"
