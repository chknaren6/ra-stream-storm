#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export METRICS_PATH="${METRICS_PATH:-$HOME/ra-stream-metrics/local}"
mkdir -p "$METRICS_PATH"

mvn -q exec:java \
  -Dexec.mainClass="com.rastream.metrics.MetricsAggregator" \
  -Dexec.classpathScope=compile
