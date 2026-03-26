#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p results/raw-data results/plots results/analysis
mvn -q -DskipTests compile

# 🔥 Ultra-dense + ultra-fast
DURATION=5
RATES=$(seq 1000 50 7000)   # VERY dense (every 50 → super smooth curves)
REPEATS=1
PARALLEL=10                 # increase if CPU allows

CONFIGS=(BasicProcessor EvenScheduler RaStream RaStreamFT RaStreamEnhanced)

run_job() {
  rep=$1
  cfg=$2
  rate=$3

  echo "[RUN] cfg=$cfg rate=$rate"

  mvn -q -DskipTests exec:java \
    -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
    -Dexec.args="$rate $DURATION ${cfg}" \
    >> results/raw-data/${cfg}_rate${rate}.log 2>&1
}

export -f run_job

# Quick warmup
mvn -q -DskipTests exec:java \
  -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
  -Dexec.args="2000 5 warmup" >/dev/null 2>&1

# Generate jobs
for cfg in "${CONFIGS[@]}"; do
  for rate in $RATES; do
    echo "1 $cfg $rate"
  done
done | xargs -n 3 -P $PARALLEL bash -c 'run_job "$@"' _

echo "[DONE] Ultra-dense 5s runs completed."
