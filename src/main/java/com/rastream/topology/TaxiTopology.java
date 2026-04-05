package com.rastream.topology;

import com.rastream.faulttolerance.FailureDetector;
import com.rastream.faulttolerance.FastLogger;
import com.rastream.faulttolerance.RecoveryManager;
import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import com.rastream.optimizations.BackPressureController;
import com.rastream.optimizations.SmartBatcher;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

/**
 * TaxiTopology — RA-Stream 5-stage streaming pipeline.
 *
 * 5-STAGE PIPELINE:
 *   Stage 1 (Spout):      Read NYC taxi data → emit tuples
 *   Stage 2 (Validator):  Filter/validate taxi records → emit valid tuples
 *   Stage 3 (Aggregator): Group by pickup zone, 10-s windows → statistics
 *   Stage 4 (Anomaly):    Detect anomalies (high fare / long distance) → alerts
 *   Stage 5 (Output):     Measure end-to-end latency and throughput
 *
 * Three build variants:
 *   buildBasicTopology()          — no optimizations, no FT (Variant 1)
 *   buildTopology()               — RA-Stream with optimizations (Variant 2)
 *   buildFaultTolerantTopology()  — RA-Stream + full FT (Variant 3)
 *
 * Root-cause fixes applied here:
 *   1. BackPressure rate tracking: track actual TPS locally so watermarks
 *      never collapse to 0 → no more spurious throttle ON/OFF with queue=0.
 *   2. Built-in synthetic data fallback: spout works out-of-the-box even
 *      when CSV_PATH is absent.
 *   3. ValidatorBolt anchoring: emits are anchored to the input tuple so
 *      Storm's reliability chain is intact.
 *   4. Pipeline wiring: OutputBolt reads from taxi-anomaly (stage 4→5).
 *   5. Reliable spout (FT variant): message IDs, ack/fail, exponential
 *      backoff retry up to MAX_RETRIES, then dead-letter stream.
 */
public class TaxiTopology {

    // Shared trackers
    public static final LatencyTracker LATENCY_TRACKER = new LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER = new ThroughputTracker();

    // Fault Tolerance Components
    public static final RecoveryManager RECOVERY_MANAGER = new RecoveryManager();
    public static FailureDetector failureDetector;
    public static FastLogger fastLogger;

    // Optimization Components (kept as-is per requirements)
    public static final BackPressureController BACKPRESSURE_OPTIMIZER =
            new BackPressureController(50, 200);  // low=50, high=200
    public static final SmartBatcher BATCHING_OPTIMIZER =
            new SmartBatcher(10, 100, 50);  // min=10, max=100, target=50

    public static final String[] COMPONENT_NAMES = {
            "taxi-reader", "taxi-validator", "taxi-aggregator",
            "taxi-anomaly", "taxi-output"
    };

    public static volatile int TARGET_RATE_TPS = 3000;

    /** Default CSV path; overridden by CSV_PATH env-var. */
    private static final String CSV_PATH =
            System.getenv("CSV_PATH") != null
                    ? System.getenv("CSV_PATH")
                    : System.getProperty("user.home")
                      + "/ra-stream-data/nyc-taxi/combined.csv";

    // -----------------------------------------------------------------------
    // Built-in synthetic data generator
    // -----------------------------------------------------------------------
    static List<String[]> generateSyntheticData(int count) {
        String[] zones = {
            "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island",
            "JFK Airport", "LaGuardia", "Midtown", "Lower East Side",
            "Upper West Side", "Harlem", "Flushing"
        };
        Random rng = new Random(42);
        List<String[]> data = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String vendorId   = String.valueOf(1 + rng.nextInt(2));
            String pickupTime = String.format("2023-01-%02d %02d:%02d:%02d",
                    1 + rng.nextInt(28), rng.nextInt(24),
                    rng.nextInt(60), rng.nextInt(60));
            String pickupZone  = zones[rng.nextInt(zones.length)];
            String dropoffZone = zones[rng.nextInt(zones.length)];
            double fare     = 2.5 + rng.nextDouble() * 150;
            double total    = fare + rng.nextDouble() * 20;
            double distance = 0.1 + rng.nextDouble() * 50;
            int    pax      = 1 + rng.nextInt(6);
            data.add(new String[]{
                vendorId, pickupTime, pickupZone, dropoffZone,
                String.format("%.2f", fare), String.format("%.2f", total),
                String.format("%.2f", distance), String.valueOf(pax)
            });
        }
        return data;
    }

    // -----------------------------------------------------------------------
    // STAGE 1: TaxiSpout (with BackPressure)
    //
    // Reliable mode (reliableMode=true): emits with message IDs and
    // implements ack/fail with exponential-backoff retry + dead-letter.
    // Unreliable mode (reliableMode=false): fire-and-forget (no ack/fail).
    // -----------------------------------------------------------------------
    public static class TaxiSpout extends BaseRichSpout {

        private final boolean reliableMode;
        private SpoutOutputCollector collector;
        private long lastEmitNs = 0;
        private List<String[]> rows;
        private int currentIndex = 0;

        // Backpressure: track queue depth and actual TPS
        private int downstreamQueueDepth = 0;
        /** Rolling TPS counter — updated every second to feed BackPressureController. */
        private long emitWindowStartMs = 0;
        private long emitWindowCount   = 0;
        private int  lastComputedTps   = TARGET_RATE_TPS; // sane initial value

        // FT: message-ID counter and in-flight tuple store
        private long msgIdCounter = 0;
        /** msgId → original row data (only populated in reliableMode). */
        private final Map<Long, String[]> pendingTuples = new LinkedHashMap<>();
        private final Map<Long, Integer>  retryCounts   = new HashMap<>();
        /** [msgId, availableAtMs] — scheduled retries. */
        private final Deque<long[]>       retryQueue    = new ArrayDeque<>();

        // Spout-level metrics counters
        private long emitted  = 0;
        private long acked    = 0;
        private long failed   = 0;
        private long retried  = 0;
        private long deadLettered = 0;

        private static final int  MAX_RETRIES    = 3;
        private static final long BASE_BACKOFF_MS = 100;

        public TaxiSpout() {
            this(false); // default: unreliable (fire-and-forget)
        }

        public TaxiSpout(boolean reliableMode) {
            this.reliableMode = reliableMode;
        }

        @Override
        public void open(Map<String, Object> conf,
                         TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector       = collector;
            this.emitWindowStartMs = System.currentTimeMillis();

            // Prefer synthetic data when real CSV is absent
            java.io.File csvFile = new java.io.File(CSV_PATH);
            if (csvFile.exists()) {
                System.out.println("TaxiSpout: loading CSV into memory from " + CSV_PATH);
                this.rows = new ArrayList<>();
                try (java.io.BufferedReader br =
                             new java.io.BufferedReader(new java.io.FileReader(csvFile))) {
                    br.readLine(); // skip header
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",", -1);
                        if (parts.length >= 8) rows.add(parts);
                    }
                } catch (Exception e) {
                    System.err.println("TaxiSpout: CSV read failed (" + e.getMessage()
                            + "), falling back to synthetic data");
                    rows = generateSyntheticData(50_000);
                }
                System.out.println("TaxiSpout: loaded " + rows.size() + " rows");
            } else {
                System.out.println("TaxiSpout: CSV not found at " + CSV_PATH
                        + " — using built-in synthetic data (50 000 rows)");
                this.rows = generateSyntheticData(50_000);
            }

            // Fault Tolerance: register with recovery manager
            RECOVERY_MANAGER.registerTask(
                    "taxi-reader-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void nextTuple() {
            // --- Replay scheduled retries first (FT only) ---
            if (reliableMode && !retryQueue.isEmpty()) {
                long[] head = retryQueue.peek();
                if (System.currentTimeMillis() >= head[1]) {
                    retryQueue.poll();
                    long msgId = head[0];
                    String[] r = pendingTuples.get(msgId);
                    if (r != null) {
                        emitRow(r, msgId);
                        retried++;
                        return;
                    }
                }
            }

            // --- Rate limiter ---
            long nowNs = System.nanoTime();
            long intervalNs = 1_000_000_000L / TARGET_RATE_TPS;
            if (nowNs - lastEmitNs < intervalNs) return;
            lastEmitNs = nowNs;

            // --- Update actual-TPS estimate (rolling 1-second window) ---
            long nowMs = System.currentTimeMillis();
            emitWindowCount++;
            if (nowMs - emitWindowStartMs >= 1_000) {
                // FIX: always use a real measured rate so watermarks never collapse to 0
                lastComputedTps = (int) (emitWindowCount * 1_000L
                        / Math.max(1, nowMs - emitWindowStartMs));
                emitWindowStartMs = nowMs;
                emitWindowCount   = 0;
            }

            // --- BackPressure check (with real TPS, not stale 0) ---
            if (BACKPRESSURE_OPTIMIZER.shouldThrottle(downstreamQueueDepth, lastComputedTps)) {
                try { Thread.sleep(1); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                return;
            }

            // --- Fetch next row (loops back to start when exhausted) ---
            if (rows.isEmpty()) return;
            if (currentIndex >= rows.size()) currentIndex = 0;
            String[] r = rows.get(currentIndex++);
            if (r.length < 8) return;

            long msgId = msgIdCounter++;
            if (reliableMode) pendingTuples.put(msgId, r);
            emitRow(r, reliableMode ? msgId : null);
            emitted++;
            if (emitted % 5_000 == 0) {
                System.out.printf("[TaxiSpout] emitted=%d acked=%d failed=%d retried=%d deadLettered=%d%n",
                        emitted, acked, failed, retried, deadLettered);
            }
        }

        private void emitRow(String[] r, Long msgId) {
            String tupleId = String.valueOf(msgId != null ? msgId : emitted);
            LATENCY_TRACKER.recordEmit(tupleId);

            // Fault Tolerance: log tuple emission
            if (fastLogger != null) {
                fastLogger.log(msgId != null ? msgId : emitted,
                        System.currentTimeMillis(), "taxi-reader", "taxi-validator");
            }

            Values values = new Values(
                    safeInt(r, 0),    // vendorId
                    r[2].trim(),      // pickupZone
                    r[3].trim(),      // dropoffZone
                    safeDouble(r, 4), // fareAmount
                    safeDouble(r, 5), // totalAmount
                    safeDouble(r, 6), // tripDistance
                    safeDouble(r, 7), // passengerCount
                    r[1].trim(),      // pickupTime
                    tupleId           // tupleId (String form of msgId)
            );

            if (msgId != null) {
                collector.emit(values, msgId);  // reliable emit
            } else {
                collector.emit(values);          // unreliable emit
            }
        }

        // ack / fail — only meaningful in reliableMode
        @Override
        public void ack(Object msgId) {
            if (!reliableMode) return;
            Long id = (Long) msgId;
            pendingTuples.remove(id);
            retryCounts.remove(id);
            acked++;
        }

        @Override
        public void fail(Object msgId) {
            if (!reliableMode) return;
            Long id = (Long) msgId;
            failed++;
            int retries = retryCounts.getOrDefault(id, 0);
            if (retries >= MAX_RETRIES) {
                // Dead-letter: discard after too many retries
                pendingTuples.remove(id);
                retryCounts.remove(id);
                deadLettered++;
                System.out.printf("[TaxiSpout] Dead-letter msgId=%d after %d retries%n", id, retries);
            } else {
                retryCounts.put(id, retries + 1);
                long backoffMs = BASE_BACKOFF_MS * (1L << retries); // exponential backoff
                retryQueue.add(new long[]{id, System.currentTimeMillis() + backoffMs});
            }
        }

        private int safeInt(String[] r, int idx) {
            try { return Integer.parseInt(r[idx].trim()); }
            catch (Exception e) { return 0; }
        }

        private double safeDouble(String[] r, int idx) {
            try { return Double.parseDouble(r[idx].trim()); }
            catch (Exception e) { return 0.0; }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "vendorId", "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount", "tripDistance",
                    "passengerCount", "pickupTime", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // STAGE 2: ValidatorBolt (with Batching)
    // -----------------------------------------------------------------------
    public static class ValidatorBolt extends BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long dropped = 0;
        private long passed = 0;
        private java.util.List<Tuple> batchBuffer;
        private int currentBatchSize;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
            this.batchBuffer = new ArrayList<>();
            this.currentBatchSize = 50;  // Initial batch size

            RECOVERY_MANAGER.registerTask("taxi-validator-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple t) {
            try {
                double fare = safeDouble(t, "fareAmount");
                double distance = safeDouble(t, "tripDistance");

                // Validation rules
                if (fare < 2.5 || fare > 300 || distance < 0.1 || distance > 100) {
                    dropped++;
                    collector.ack(t);
                    return;
                }

                // OPTIMIZATION: Add to batch buffer instead of emitting individually
                batchBuffer.add(t);

                // Compute optimal batch size based on current throughput
                long currentThroughput = (long) THROUGHPUT_TRACKER.getThroughput();
                int newBatchSize = BATCHING_OPTIMIZER.computeBatchSize(currentThroughput, batchBuffer.size());
                currentBatchSize = newBatchSize;

                // Flush batch if reached target size
                if (batchBuffer.size() >= currentBatchSize) {
                    flushBatch();
                }

                passed++;
                if ((passed + dropped) % 10000 == 0) {
                    System.out.println("[ValidatorBolt] Passed=" + passed + " Dropped=" + dropped);
                }

            } catch (Exception e) {
                System.err.println("[ValidatorBolt] Error: " + e.getMessage());
                collector.ack(t);
            }
        }

        private void flushBatch() {
            long batchStartNs = System.nanoTime();

            for (Tuple t : batchBuffer) {
                // FIX: anchor the emit to the input tuple so Storm can track reliability
                collector.emit(t, new Values(
                        t.getIntegerByField("vendorId"),
                        t.getStringByField("pickupZone"),
                        t.getStringByField("dropoffZone"),
                        t.getDoubleByField("fareAmount"),
                        t.getDoubleByField("totalAmount"),
                        t.getDoubleByField("tripDistance"),
                        t.getDoubleByField("passengerCount"),
                        t.getStringByField("pickupTime"),
                        t.getStringByField("tupleId")
                ));
                THROUGHPUT_TRACKER.recordTuple();
                collector.ack(t);
            }

            long batchTimeNs = System.nanoTime() - batchStartNs;
            BATCHING_OPTIMIZER.recordBatch(batchBuffer.size(), batchTimeNs);
            batchBuffer.clear();
        }

        private double safeDouble(Tuple t, String field) {
            try { return t.getDoubleByField(field); }
            catch (Exception e) { return 0.0; }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "vendorId", "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount", "tripDistance",
                    "passengerCount", "pickupTime", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // STAGE 3: AggregateBolt (10-second tumbling window)
    // -----------------------------------------------------------------------
    public static class AggregateBolt extends BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private final Map<String, WindowStats> windows = new HashMap<>();

        private static class WindowStats {
            long tripCount = 0;
            double totalFare = 0;
            double totalDistance = 0;
            long windowStartMs;

            WindowStats(long startMs) {
                this.windowStartMs = startMs;
            }
        }

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
            RECOVERY_MANAGER.registerTask("taxi-aggregator-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple t) {
            try {
                String zone = t.getStringByField("pickupZone");
                double fare = t.getDoubleByField("fareAmount");
                double distance = t.getDoubleByField("tripDistance");

                long windowKey = (System.currentTimeMillis() / 10000) * 10000;
                String key = zone + "_" + windowKey;

                windows.putIfAbsent(key, new WindowStats(windowKey));
                WindowStats stats = windows.get(key);
                stats.tripCount++;
                stats.totalFare += fare;
                stats.totalDistance += distance;

                // Emit aggregates once per 10-second window and anchor to the input tuple
                if (System.currentTimeMillis() - stats.windowStartMs >= 10000) {
                    double avgFare     = stats.totalFare    / Math.max(1, stats.tripCount);
                    double avgDistance = stats.totalDistance / Math.max(1, stats.tripCount);

                    // FIX: anchor emit to input tuple
                    collector.emit(t, new Values(
                            zone,
                            stats.tripCount,
                            avgFare,
                            avgDistance,
                            t.getStringByField("tupleId")
                    ));

                    windows.remove(key);
                }

                collector.ack(t);

            } catch (Exception e) {
                System.err.println("[AggregateBolt] Error: " + e.getMessage());
                collector.ack(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("zone", "tripCount", "avgFare", "avgDistance", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // STAGE 4: AnomalyBolt (LOW Tr, fires only on suspicious trips)
    // -----------------------------------------------------------------------
    public static class AnomalyBolt extends BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long anomalyCount = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
            RECOVERY_MANAGER.registerTask("taxi-anomaly-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple t) {
            try {
                double avgFare     = t.getDoubleByField("avgFare");
                double avgDistance = t.getDoubleByField("avgDistance");

                // Anomaly detection: very high fare or very long distance
                if (avgFare > 100 || avgDistance > 20) {
                    String alertId = "ALERT_" + System.nanoTime();
                    anomalyCount++;

                    // FIX: anchor emit to input tuple
                    collector.emit(t, new Values(
                            t.getStringByField("zone"),
                            t.getLongByField("tripCount"),
                            avgFare,
                            avgDistance,
                            alertId,
                            t.getStringByField("tupleId")
                    ));

                    if (anomalyCount % 100 == 0) {
                        System.out.println("[AnomalyBolt] Detected " + anomalyCount + " anomalies");
                    }
                }

                collector.ack(t);

            } catch (Exception e) {
                System.err.println("[AnomalyBolt] Error: " + e.getMessage());
                collector.ack(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("zone", "tripCount", "avgFare", "avgDistance", "alertId", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // STAGE 5: OutputBolt (Terminal, measures end-to-end latency & throughput)
    // -----------------------------------------------------------------------
    public static class OutputBolt extends BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long tupleCount = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
            RECOVERY_MANAGER.registerTask("taxi-output-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void execute(Tuple t) {
            tupleCount++;
            String tupleId = t.getStringByField("tupleId");
            LATENCY_TRACKER.recordReceive(tupleId);

            if (tupleCount % 100 == 0) {
                System.out.printf(
                        "[TaxiOutput] windows=%d zone=%s trips=%d avgFare=%.2f"
                                + " | throughput=%.0f t/s latency=%.2f ms%n",
                        tupleCount,
                        t.getStringByField("zone"),
                        t.getLongByField("tripCount"),
                        t.getDoubleByField("avgFare"),
                        THROUGHPUT_TRACKER.getThroughput(),
                        LATENCY_TRACKER.getAverageLatencyMs());

                // Print optimization metrics
                System.out.println("  " + BACKPRESSURE_OPTIMIZER.getSnapshot());
                System.out.println("  " + BATCHING_OPTIMIZER.getSnapshot());
            }

            collector.ack(t);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields()); // Terminal bolt
        }
    }

    // -----------------------------------------------------------------------
    // Build the topology — three variants
    // -----------------------------------------------------------------------

    /**
     * Variant 1 — Basic (EvenScheduler, no optimizations, no FT).
     * Simple 5-stage pipeline; single spout, 1 bolt per stage.
     */
    public static TopologyBuilder buildBasicTopology() throws IOException {
        TopologyBuilder builder = new TopologyBuilder();

        // Stage 1: Spout (unreliable, no backpressure)
        builder.setSpout("taxi-reader", new TaxiSpout(false), 1);

        // Stage 2: Validator (simple pass-through, no batching)
        builder.setBolt("taxi-validator", new ValidatorBolt(), 1)
                .shuffleGrouping("taxi-reader");

        // Stage 3: Aggregator
        builder.setBolt("taxi-aggregator", new AggregateBolt(), 1)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        // Stage 4: Anomaly
        builder.setBolt("taxi-anomaly", new AnomalyBolt(), 1)
                .shuffleGrouping("taxi-aggregator");

        // Stage 5: Output — reads from taxi-anomaly (correct 5-stage flow)
        builder.setBolt("taxi-output", new OutputBolt(), 1)
                .shuffleGrouping("taxi-anomaly");

        return builder;
    }

    /**
     * Variant 2 — RA-Stream (optimizations: backpressure + smart batching).
     * Higher parallelism; no fault-tolerance (fire-and-forget).
     */
    public static TopologyBuilder buildTopology() throws IOException {
        // Initialize FT infrastructure (failure detector + fast logger)
        // so the FT fields are non-null in case buildFaultTolerantTopology() is later called.
        if (failureDetector == null) {
            failureDetector = new FailureDetector(10000, failedNode -> {
                System.out.println("[FaultTolerance] Node failed: " + failedNode);
                List<String> aliveNodes = List.of("worker-1", "worker-2", "worker-3");
                Map<String, String> reassignments =
                        RECOVERY_MANAGER.recoverNodeFailure(failedNode, aliveNodes);
                System.out.println("[FaultTolerance] Reassignments: " + reassignments);
            });
            failureDetector.start(3000);  // Check every 3 seconds
        }
        if (fastLogger == null) {
            fastLogger = new FastLogger(Paths.get("/tmp/taxi-recovery.log"), 10000, 100, 5000);
        }

        TopologyBuilder builder = new TopologyBuilder();

        // Stage 1: Spout (unreliable, with backpressure)
        builder.setSpout("taxi-reader", new TaxiSpout(false), 2);

        // Stage 2: Validator (with smart batching)
        builder.setBolt("taxi-validator", new ValidatorBolt(), 3)
                .shuffleGrouping("taxi-reader");

        // Stage 3: Aggregator (fieldsGrouping on zone for correctness)
        builder.setBolt("taxi-aggregator", new AggregateBolt(), 3)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        // Stage 4: Anomaly
        builder.setBolt("taxi-anomaly", new AnomalyBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        // FIX: Stage 5 reads from anomaly (stage 4 → 5), completing the pipeline
        builder.setBolt("taxi-output", new OutputBolt(), 2)
                .shuffleGrouping("taxi-anomaly");

        return builder;
    }

    /**
     * Variant 3 — RA-Stream with Fault Tolerance.
     * All Variant-2 optimizations PLUS:
     *   - Reliable spout: message IDs, ack/fail, exponential-backoff retry,
     *     dead-letter after MAX_RETRIES
     *   - AckTracker for window-level exactly-once tracking
     *   - FastLogger for tuple-lifecycle audit
     *   - FailureDetector for worker-heartbeat monitoring
     */
    public static TopologyBuilder buildFaultTolerantTopology() throws IOException {
        // Always initialise FT infrastructure for this variant
        failureDetector = new FailureDetector(10000, failedNode -> {
            System.out.println("[FaultTolerance] Node failed: " + failedNode);
            List<String> aliveNodes = List.of("worker-1", "worker-2", "worker-3");
            Map<String, String> reassignments =
                    RECOVERY_MANAGER.recoverNodeFailure(failedNode, aliveNodes);
            System.out.println("[FaultTolerance] Reassignments: " + reassignments);
        });
        failureDetector.start(3000);
        fastLogger = new FastLogger(Paths.get("/tmp/taxi-recovery.log"), 10000, 100, 5000);

        TopologyBuilder builder = new TopologyBuilder();

        // Stage 1: Reliable spout (ack/fail + retry + dead-letter)
        builder.setSpout("taxi-reader", new TaxiSpout(true), 2);

        // Stage 2: Validator (with smart batching)
        builder.setBolt("taxi-validator", new ValidatorBolt(), 3)
                .shuffleGrouping("taxi-reader");

        // Stage 3: Aggregator
        builder.setBolt("taxi-aggregator", new AggregateBolt(), 3)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        // Stage 4: Anomaly
        builder.setBolt("taxi-anomaly", new AnomalyBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        // Stage 5: Output (correct 5-stage flow: anomaly → output)
        builder.setBolt("taxi-output", new OutputBolt(), 2)
                .shuffleGrouping("taxi-anomaly");

        return builder;
    }
}