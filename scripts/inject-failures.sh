#!/usr/bin/env bash
set -euo pipefail
# Replace with actual worker kill commands in your environment
sleep $((15*60)); echo "[FAILURE] kill 1 worker"
sleep $((15*60)); echo "[FAILURE] kill 2 workers"
sleep $((15*60)); echo "[FAILURE] kill 1 worker"
