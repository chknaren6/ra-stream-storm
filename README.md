# ra-stream-storm

RA-Stream: Adaptive stream-processing topology on Apache Storm.

## Overview

**TaxiTopology** processes NYC taxi trip data through a 5-stage pipeline:

| Stage | Component | Description |
|-------|-----------|-------------|
| 1 | TaxiSpout | Reads CSV (or built-in synthetic data) and emits trip tuples |
| 2 | ValidatorBolt | Filters invalid fares/distances with adaptive batching |
| 3 | AggregateBolt | 10-second tumbling window aggregation per pickup zone |
| 4 | AnomalyBolt | Detects anomalous fares / long-distance trips |
| 5 | OutputBolt | Measures end-to-end latency and throughput |

## Three Variants

| Mode | CLI flag | Description |
|------|----------|-------------|
| Basic | `--mode basic` | Single spout/bolt, no optimizations, no fault tolerance |
| RA-Stream | `--mode rastream` *(default)* | Backpressure + SmartBatcher optimizations |
| RA-Stream FT | `--mode rastream-ft` | RA-Stream + Storm reliability (ack/fail/retry/dead-letter) |

## Prerequisites

- Java 11+
- Maven 3.6+
- No external data required — synthetic data is generated automatically.

To use real NYC taxi data set `CSV_PATH` to a CSV with columns:
`VendorID,pickup_datetime,pickup_zone,dropoff_zone,fare_amount,total_amount,trip_distance,passenger_count`

## Build

```bash
mvn package -DskipTests
```

## Run

### All three variants (recommended)

```bash
bash scripts/run_all_modes.sh
```

### Individual variants

```bash
# Variant 1 — Basic
java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode basic --duration-s 120

# Variant 2 — RA-Stream (default)
java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode rastream --duration-s 120

# Variant 3 — RA-Stream with Fault Tolerance
java -jar target/ra-stream-1.0-SNAPSHOT.jar --mode rastream-ft --duration-s 120
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CSV_PATH` | *(none — uses synthetic data)* | Path to real taxi CSV |
| `METRICS_PATH` | `~/ra-stream-metrics/local/taxi` | Directory for metrics CSV |

## Metrics

Metrics are written to `${METRICS_PATH}/metrics.csv` after each run.

Columns: `timestamp, scheduler, topology, stream_rate_tps, avg_latency_ms, max_latency_ms, min_latency_ms, throughput_tps, tuple_count, node_count, avg_cpu_pct, avg_mem_pct, stage, notes`

### Compare variants

```bash
python3 scripts/compare_metrics.py
# or specify a path:
python3 scripts/compare_metrics.py /path/to/metrics.csv
# include raw rows:
python3 scripts/compare_metrics.py --raw
```

## Fault Tolerance (Variant 3)

- **Storm reliability**: spout emits with message IDs; each bolt acks/fails every tuple.
- **Retry with backoff**: on `fail()`, the spout re-enqueues the tuple with exponential backoff (100 ms × 2^retry).
- **Dead-letter**: after 3 retries the tuple is discarded and counted as `deadLettered`.
- **FastLogger**: binary audit log written to `/tmp/taxi-recovery.log`.
- **FailureDetector**: heartbeat monitor triggers `RecoveryManager` on worker timeout.
- **AckTracker**: per-stream bitmap for window-level completion tracking.

Spout prints a metrics line every 5 000 tuples:
```
[TaxiSpout] emitted=5000 acked=4980 failed=12 retried=11 deadLettered=1
```

## Project Structure

```
src/main/java/com/rastream/
  topology/         # TaxiTopology, TopologyRunner, …
  faulttolerance/   # AckTracker, FailureDetector, FastLogger, RecoveryManager
  optimizations/    # BackPressureController, SmartBatcher, TupleRouter, ZeroCopyTransfer
  metrics/          # LatencyTracker, ThroughputTracker, MetricsCSVWriter, …
  scheduler/        # EvenScheduler, RaStreamScheduler, RaStreamFTScheduler
scripts/
  run_all_modes.sh       # Run all 3 variants sequentially
  compare_metrics.py     # Print comparison table from metrics CSV
```
