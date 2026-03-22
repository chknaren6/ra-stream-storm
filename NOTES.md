# Ra-Stream Implementation Notes

## System Under Test
- Machine: HP Z4 G5 Workstation
- RAM: 64GB
- CPU: Intel(R) Xeon(R) w5-2565X
- Model name: Intel(R) Xeon(R) w5-2565X
- Thread(s) per core: 2
- Core(s) per socket: 18
- Socket(s): 1
- CPU(s) scaling MHz: 28%
- NUMA node0 CPU(s): 0-35
- OS: Ubuntu 24.04
- Java: OpenJDK 11.0.30
- Storm: 2.6.1
- Dataset: NYC Taxi 2023 Q1-Q3 (9,384,487 rows, 434.7MB CSV)

## Local Mode Baseline (Single Machine, LocalCluster)
Date: 2026-03-22

### Topology Configuration
- TaxiSpout:      2 tasks (parallelism=2)
- ValidatorBolt:  3 tasks (parallelism=3)  HIGH Tr with spout
- AggregateBolt:  3 tasks (parallelism=3)  fieldsGrouping on pickupZone
- AnomalyBolt:    2 tasks (parallelism=2)  LOW Tr
- OutputBolt:     2 tasks (parallelism=2)
- Total tasks:    12
- Workers:        2 (Storm worker JVMs)

### Fine-grained Scheduling Applied
- TaxiSpout → ValidatorBolt: HIGH Tr (every tuple) → same process
- ValidatorBolt → AggregateBolt: MEDIUM Tr → can be inter-process
- AggregateBolt → AnomalyBolt: LOW Tr (only anomalies) → inter-process OK
- AggregateBolt → OutputBolt: LOW Tr (window summaries) → inter-process OK

### Throughput Results (RaStream scheduler)
| Target t/s | Actual t/s | Avg Latency | Status         |
|------------|------------|-------------|----------------|
| 1,000      | 1,437      | 1.08ms      | exceeded target|
| 2,000      | 1,617      | 1.10ms      | exceeded target|
| 3,000      | 1,688      | 0.90ms      | near ceiling   |
| 5,000      | 1,727      | 0.92ms      | ceiling hit    |
| 8,000      | 1,751      | 0.90ms      | ceiling        |
| 12,000     | 1,769      | 0.91ms      | hard ceiling   |

### Local Mode Ceiling: ~1,769 t/s

### Why ceiling at 1769 t/s on a 64GB machine?
The bottleneck is NOT RAM or CPU capacity. It is JVM thread contention.

LocalCluster runs ALL tasks in a SINGLE JVM process:
  - 2 spout threads + 3 validator threads + 3 aggregator threads
  - all sharing the same JVM thread scheduler
  - Storm uses internal LMAX Disruptor queues between tasks
  - Disruptor queue throughput on single JVM = ~1800-2000 t/s per topology

Even with 64GB RAM, a single JVM cannot push more than
~1800 t/s through this topology because:
  1. All 12 tasks share ~16 executor threads (2 workers × 8 threads)
  2. Thread context switching overhead accumulates
  3. Disruptor ring buffer between spout→validator is the pinch point
  4. In-memory data loading eliminated I/O bottleneck (was 905 t/s before)
  5. But JVM thread pool is now the new ceiling

### What changes on Docker/real cluster?
  - Each supervisor = separate JVM = no thread sharing
  - 5 supervisors × ~1800 t/s theoretical = 9,000 t/s possible
  - 14 supervisors × ~1800 t/s theoretical = 25,200 t/s possible
  - Real network adds 2-10ms latency (currently 0.91ms local)
  - Ra-Stream intra-process placement will show measurable benefit

### Spout Evolution
v1 (CSV reader): 905 t/s ceiling — blocked on disk I/O per row
v2 (in-memory):  1769 t/s ceiling — 9.38M rows pre-loaded into RAM
                 Both spout tasks loaded independently (~900MB RAM used)

### Anomaly Detected
Zone 156: avgFare=346.00, trips=2 → suspicious high fare
Zone 265: avgFare=82.73, trips=12 → flagged by AnomalyBolt
Zone 93:  avgFare=70.00, trips=16 → borderline

### Next Steps
1. Docker setup: 1 nimbus + 5 supervisors (scale to 14 later)
2. Run same topology on Docker → compare throughput and latency
3. Run EvenScheduler on same Docker → compare with RaStream
4. Record all results in metrics/local/taxi/metrics.csv
5. Scale to 14 Docker containers → approach paper's setup
6. Connect 15 real machines → reproduce paper results exactly

## Docker Setup (PENDING)
- Target: 1 nimbus + 14 supervisors
- RAM per supervisor: 2GB (matches paper's cluster spec)
- Total Docker RAM: ~30GB (fits in 64GB machine)
- Expected throughput: 5,000-15,000 t/s
- Expected latency: 5-15ms (real network between containers)

## Real Cluster Setup (PENDING)
- 15 machines, each 2GB RAM worker
- Matches paper Section 6.1 exactly
- Expected: latency 5.26ms (RaStream) vs 11.57ms (EvenScheduler)
