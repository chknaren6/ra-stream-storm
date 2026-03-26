#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results/raw-data results/plots results/analysis
mvn -q -DskipTests compile

DURATION=30                     # 30 sec per run
RATES=$(seq 1000 100 7000)     # VERY dense (every 100 → smooth curve)
REPEATS=1                      # only 1 pass for speed
PARALLEL=8                     # adjust based on CPU (try 6–10)

CONFIGS=(BasicProcessor EvenScheduler RaStream RaStreamFT RaStreamEnhanced)

run_job() {
  rep=$1
  cfg=$2
  rate=$3

  echo "[RUN] rep=$rep cfg=$cfg rate=$rate"

  mvn -q -DskipTests exec:java \
    -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
    -Dexec.args="$rate $DURATION ${cfg}_rep${rep}" \
    >> results/raw-data/${cfg}_rate${rate}.log 2>&1
}

export -f run_job

mvn -q -DskipTests exec:java \
  -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
  -Dexec.args="2000 15 warmup" >/dev/null 2>&1

for rep in $(seq 1 $REPEATS); do
  for cfg in "${CONFIGS[@]}"; do
    for rate in $RATES; do
      echo "$rep $cfg $rate"
    done
  done
done | xargs -n 3 -P $PARALLEL bash -c 'run_job "$@"' _

echo "[DONE] Ultra-fast dense run completed."