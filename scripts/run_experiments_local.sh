#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-dense}"   # smoke | quick | dense | full
mkdir -p results/raw-data results/plots results/analysis
mvn -q -DskipTests compile

case "$MODE" in
  smoke)
    DURATION=60
    RATES=(1000)
    REPEATS=1
    ;;
  quick)
    DURATION=180
    RATES=(1000 3000)
    REPEATS=1
    ;;
  dense)
    DURATION=300
    RATES=(1000 2000 3000 4000 5000 6000 7000)
    REPEATS=2
    ;;
  full)
    DURATION=3600
    RATES=(1000 2000 3000 4000 5000 6000 7000)
    REPEATS=3
    ;;
  *)
    echo "Usage: $0 [smoke|quick|dense|full]"
    exit 1
    ;;
esac

CONFIGS=(BasicProcessor EvenScheduler RaStream RaStreamFT RaStreamEnhanced)

for rep in $(seq 1 $REPEATS); do
  for cfg in "${CONFIGS[@]}"; do
    for rate in "${RATES[@]}"; do
      echo "[RUN] mode=$MODE rep=$rep cfg=$cfg rate=$rate duration=${DURATION}s"
      mvn -q -DskipTests exec:java \
        -Dexec.mainClass="com.rastream.experiments.StableStreamExperiment" \
        -Dexec.args="$rate $DURATION ${cfg}_rep${rep}"
    done
  done
done

echo "[DONE] mode=$MODE"
