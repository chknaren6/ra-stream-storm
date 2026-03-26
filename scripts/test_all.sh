#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

export METRICS_PATH="${METRICS_PATH:-$HOME/ra-stream-metrics/local}"
mkdir -p "$METRICS_PATH"

echo "[1/7] Clean build"
mvn -q -DskipTests clean package

echo "[2/7] Unit tests"
mvn -q test

echo "[3/7] Start MetricsAggregator in background"
mvn -q exec:java \
  -Dexec.mainClass="com.rastream.metrics.MetricsAggregator" \
  -Dexec.classpathScope=compile > /tmp/ras_aggregator.log 2>&1 &
AGG_PID=$!
sleep 3
echo "Aggregator PID=$AGG_PID"

echo "[4/7] Seed one node CSV row"
cat > "$METRICS_PATH/node-test.csv" << 'EOF'
timestamp,node,scheduler,baseline,run_id,stream_profile,cluster_size,topology,stream_rate_tps,avg_latency_ms,max_latency_ms,min_latency_ms,throughput_tps,tuple_count,avg_cpu_pct,avg_mem_pct,avg_io_pct,avg_net_rx_bps,avg_net_tx_bps,avg_net_util_pct,stage,notes
2026-03-25 12:00:00,node-test,RASTREAM,none,run-test,profile-test,5,TaxiNYC,3000,10.5,20.0,5.0,2800,10000,65,55,20,1000000,800000,2.0,test,seed
EOF
sleep 12

echo "[5/7] Validate master.csv exists + has data"
MASTER="$METRICS_PATH/master.csv"
test -f "$MASTER"
LINES=$(wc -l < "$MASTER")
if [ "$LINES" -lt 2 ]; then
  echo "master.csv did not receive rows"
  cat /tmp/ras_aggregator.log || true
  kill $AGG_PID || true
  exit 1
fi
echo "master.csv lines=$LINES"

echo "[6/7] Duplicate protection check"
LINES1=$(wc -l < "$MASTER")
sleep 12
LINES2=$(wc -l < "$MASTER")
if [ "$LINES2" -ne "$LINES1" ]; then
  echo "duplicate append detected! before=$LINES1 after=$LINES2"
  kill $AGG_PID || true
  exit 1
fi
echo "No duplicate append confirmed."

echo "[7/7] Stop aggregator"
kill $AGG_PID
sleep 1 || true
echo "All tests passed."
