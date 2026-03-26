#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export METRICS_PATH="${METRICS_PATH:-$HOME/ra-stream-metrics/local}"
mkdir -p "$METRICS_PATH"

export RA_RUN_ID="run_$(date +%Y%m%d_%H%M%S)"
export RA_PROFILE="${RA_PROFILE:-paper_fluctuating}"
export RA_WARMUP_SEC="${RA_WARMUP_SEC:-30}"
export RA_STAGE_SEC="${RA_STAGE_SEC:-60}"

run_mode() {
  local mode="$1"
  echo "Running mode=$mode"
  mvn -q exec:java \
    -Dexec.mainClass="com.rastream.topology.TopologyRunner" \
    -Dexec.classpathScope=runtime \
    -DRA_SCHEDULER_MODE="$mode" \
    -DRA_RUN_ID="$RA_RUN_ID" \
    -DRA_PROFILE="$RA_PROFILE" \
    -DRA_WARMUP_SEC="$RA_WARMUP_SEC" \
    -DRA_STAGE_SEC="$RA_STAGE_SEC"
}

run_mode "RASTREAM"
run_mode "BASELINE_ROUND_ROBIN"
run_mode "BASELINE_ANTI_LOCALITY_WORST_FIT"

echo "Mode runs complete. Check: $METRICS_PATH/master.csv"
