run_mode () {
  local mode="$1"
  echo "=== Running mode: $mode ==="
  mvn -q exec:java \
    -Dexec.mainClass="com.rastream.topology.TopologyRunner" \
    -Dexec.classpathScope=runtime \
    -DRA_SCHEDULER_MODE="$mode" \
    -DRA_RUN_ID="$RA_RUN_ID" \
    -DRA_PROFILE="$RA_PROFILE" \
    -DRA_WARMUP_SEC="$RA_WARMUP_SEC" \
    -DRA_STAGE_SEC="$RA_STAGE_SEC"
}
