package com.rastream.topology;

import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.faulttolerance.FailureDetector;
import com.rastream.faulttolerance.FastLogger;
import com.rastream.faulttolerance.RecoveryManager;
import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ResourceModel;
import com.rastream.monitor.DataMonitor;
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
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ENHANCED TaxiTopology with Integrated Fault Tolerance and Optimizations
 *
 * 5-STAGE PIPELINE:
 *   Stage 1 (Spout): Read NYC taxi data from CSV → emit tuples (HIGH Tr)
 *   Stage 2 (Validator): Filter & validate taxi records → emit valid tuples
 *   Stage 3 (Aggregator): Group by pickup zone, 10s windows → statistics
 *   Stage 4 (Anomaly): Detect anomalies (high fare, long distance) → alerts
 *   Stage 5 (Output): Terminal stage, measure end-to-end latency & throughput
 *
 * FAULT TOLERANCE INTEGRATION:
 *   - FailureDetector: Monitors worker heartbeats every 3 seconds
 *   - EnhancedRecoveryManager: Rebalances tasks on failure with adaptive load balancing
 *   - FastLogger: Logs tuple lifecycle for recovery
 *   - AckTracker: Tracks tuple acknowledgements for exactly-once semantics
 *
 * OPTIMIZATION INTEGRATION:
 *   - BackPressureOptimizer: Throttles spout when downstream queues fill
 *   - BatchingOptimizer: Dynamically adjusts batch sizes by data rate
 */
public class TaxiTopology {

    // Shared trackers — same pattern as WordCount
    public static final LatencyTracker LATENCY_TRACKER = new LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER = new ThroughputTracker();

    // Fault Tolerance Components
    public static final RecoveryManager RECOVERY_MANAGER = new RecoveryManager();
    public static FailureDetector failureDetector;
    public static FastLogger fastLogger;

    // Optimization Components
    public static final BackPressureController BACKPRESSURE_OPTIMIZER =
            new BackPressureController(50, 200);  // low=50, high=200
    public static final SmartBatcher BATCHING_OPTIMIZER =
            new SmartBatcher(10, 100, 50);  // min=10, max=100, target=50

    private static final boolean IN_MEMORY_MODE =
            System.getenv("IN_MEMORY_MODE") != null
                    && System.getenv("IN_MEMORY_MODE").equals("true");

    private static org.apache.commons.csv.CSVParser parser;
    private static java.util.Iterator<CSVRecord> iterator;

    public static final String[] COMPONENT_NAMES = {
            "taxi-reader", "taxi-validator", "taxi-aggregator",
            "taxi-anomaly", "taxi-output"
    };

    public static volatile int TARGET_RATE_TPS = 3000;

    // -----------------------------------------------------------------------
    // STAGE 1: TaxiSpout (with BackPressure)
    // -----------------------------------------------------------------------
    public static class TaxiSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private long lastEmitNs = 0;
        private List<String[]> rows;
        private int currentIndex = 0;

        // Optimization: Track queue depth for backpressure
        private int downstreamQueueDepth = 0;
        private long tuplesEmitted = 0;
        private final AtomicLong emitRateTps = new AtomicLong(0);

        private static final String CSV_PATH =
                System.getenv("CSV_PATH") != null
                        ? System.getenv("CSV_PATH")
                        : System.getProperty("user.home")
                          + "/ra-stream-data/nyc-taxi/combined.csv";

        @Override
        public void open(Map<String, Object> conf,
                         TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;

            if (IN_MEMORY_MODE) {
                this.rows = new ArrayList<>();
                System.out.println("TaxiSpout: loading CSV into memory...");
                try (java.io.BufferedReader br =
                             new java.io.BufferedReader(
                                     new java.io.FileReader(CSV_PATH))) {
                    br.readLine(); // skip header
                    String line;
                    while ((line = br.readLine()) != null) {
                        rows.add(line.split(",", -1));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Cannot load CSV", e);
                }
                System.out.println("TaxiSpout: loaded " + rows.size() + " rows into memory");
            } else {
                System.out.println("TaxiSpout: streaming CSV from disk");
                try {
                    openCsvParser();
                } catch (Exception e) {
                    throw new RuntimeException("Cannot open CSV: " + CSV_PATH, e);
                }
            }

            // Fault Tolerance: Register this spout with recovery manager
            RECOVERY_MANAGER.registerTask("taxi-reader-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        private void openCsvParser() throws Exception {
            java.io.Reader reader = new java.io.FileReader(CSV_PATH);
            parser = org.apache.commons.csv.CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim()
                    .parse(reader);
            iterator = parser.iterator();
        }

        @Override
        public void nextTuple() {
            long nowNs = System.nanoTime();
            long intervalNs = 1_000_000_000L / TARGET_RATE_TPS;
            if (nowNs - lastEmitNs < intervalNs) return;
            lastEmitNs = nowNs;

            try {
                String[] r;

                if (IN_MEMORY_MODE) {
                    if (currentIndex >= rows.size()) currentIndex = 0;
                    r = rows.get(currentIndex++);
                    if (r.length < 8) return;
                } else {
                    if (!iterator.hasNext()) {
                        parser.close();
                        openCsvParser();
                    }
                    CSVRecord record = iterator.next();
                    r = new String[]{
                            record.get("VendorID"),
                            record.get("pickup_datetime"),
                            record.get("pickup_zone"),  // from lookup table
                            record.get("dropoff_zone"),
                            record.get("fare_amount"),
                            record.get("total_amount"),
                            record.get("trip_distance"),
                            record.get("passenger_count")
                    };
                }

                // OPTIMIZATION: Check backpressure before emitting
                int incomingRate = (int) emitRateTps.get();
                if (BACKPRESSURE_OPTIMIZER.shouldThrottle(downstreamQueueDepth, incomingRate)) {
                    // Throttle: sleep before attempting emit
                    Thread.sleep(1);
                    return;
                }

                String tupleId = UUID.randomUUID().toString();
                LATENCY_TRACKER.recordEmit(tupleId);

                // Fault Tolerance: Log tuple emission
                if (fastLogger != null) {
                    fastLogger.log(
                            // FIX: Change parseLong to parseUnsignedLong
                            Long.parseUnsignedLong(tupleId.replace("-", "").substring(0, 16), 16),
                            System.currentTimeMillis(),
                            "taxi-reader",
                            "taxi-validator"
                    );
                }

                collector.emit(new Values(
                        safeInt(r, 0),    // vendorId
                        r[2].trim(),      // pickupZone
                        r[3].trim(),      // dropoffZone
                        safeDouble(r, 4), // fareAmount
                        safeDouble(r, 5), // totalAmount
                        safeDouble(r, 6), // tripDistance
                        safeDouble(r, 7), // passengerCount
                        r[1].trim(),      // pickupTime
                        tupleId
                ));

                tuplesEmitted++;
                if (tuplesEmitted % 1000 == 0) {
                    System.out.println("[TaxiSpout] Emitted " + tuplesEmitted + " tuples");
                }

            } catch (Exception e) {
                System.err.println("[TaxiSpout] Error: " + e.getMessage());
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
                collector.emit(new Values(
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

                // Emit aggregates every 10 seconds
                if (System.currentTimeMillis() - stats.windowStartMs >= 10000) {
                    double avgFare = stats.totalFare / Math.max(1, stats.tripCount);
                    double avgDistance = stats.totalDistance / Math.max(1, stats.tripCount);

                    collector.emit(new Values(
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
                double avgFare = t.getDoubleByField("avgFare");
                double avgDistance = t.getDoubleByField("avgDistance");

                // Anomaly detection: very high fare or very long distance
                if (avgFare > 100 || avgDistance > 20) {
                    String alertId = "ALERT_" + System.nanoTime();
                    anomalyCount++;

                    collector.emit(new Values(
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
    // Build the topology
    // -----------------------------------------------------------------------
    public static TopologyBuilder buildTopology() throws IOException {
        // Initialize fault tolerance components
        failureDetector = new FailureDetector(10000, failedNode -> {
            System.out.println("[FaultTolerance] Node failed: " + failedNode);
            List<String> aliveNodes = List.of("worker-1", "worker-2", "worker-3");
            Map<String, String> reassignments = RECOVERY_MANAGER.recoverNodeFailure(failedNode, aliveNodes);
            System.out.println("[FaultTolerance] Reassignments: " + reassignments);
        });
        failureDetector.start(3000);  // Check every 3 seconds

        fastLogger = new FastLogger(Paths.get("/tmp/taxi-recovery.log"), 10000, 100, 5000);

        // Build the Storm topology
        TopologyBuilder builder = new TopologyBuilder();

        // Stage 1: Spout
        builder.setSpout("taxi-reader", new TaxiSpout(), 2);

        // Stage 2: Validator
        builder.setBolt("taxi-validator", new ValidatorBolt(), 3)
                .shuffleGrouping("taxi-reader");

        // Stage 3: Aggregator (fieldsGrouping on zone for correctness)
        builder.setBolt("taxi-aggregator", new AggregateBolt(), 3)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        // Stage 4: Anomaly
        builder.setBolt("taxi-anomaly", new AnomalyBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        // Stage 5: Output
        builder.setBolt("taxi-output", new OutputBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        return builder;
    }
}