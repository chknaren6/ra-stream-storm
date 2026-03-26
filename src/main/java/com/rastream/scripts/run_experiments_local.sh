#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-dense}"  # dense | full
mkdir -p results/raw-data results/plots results/analysis
mvn -q -DskipTests compile

if [[ "$MODE" == "dense" ]]; then
  DURATION=600  # 10 min each run
  RATES=(1000 2000 3000 4000 5000 6000 7000)
  REPEATS=3     # 3 repetitions for confidence/error bars
elif [[ "$MODE" == "full" ]]; then
  DURATION=3600
  RATES=(1000 2000 3000 4000 5000 6000 7000)
  REPEATS=3
else
  echo "Usage: $0 [dense|full]"
  exit 1
fi

CONFIGS=(BasicProcessor EvenScheduler RaStream RaStreamFT RaStreamEnhanced)

for rep in $(seq 1 $REPEATS); do
  for cfg in "${CONFIGS[@]}"; do
    for rate in "${RATES[@]}"; do
      echo "[RUN] rep=$rep cfg=$cfg rate=$rate duration=${DURATION}s"
      mvn -q -DskipTests exec:java \
        -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
        -Dexec.args="$rate $DURATION ${cfg}_rep${rep}"
    done
  done
done

echo "[DONE] Dense runs finished."