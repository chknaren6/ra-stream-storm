package com.rastream.topology;

import com.rastream.faulttolerance.FailureDetector;
import com.rastream.faulttolerance.FastLogger;
import com.rastream.faulttolerance.RecoveryManager;
import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import com.rastream.optimizations.BackPressureController;
import com.rastream.optimizations.SmartBatcher;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.*;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TaxiTopology {

    public static final LatencyTracker LATENCY_TRACKER = new LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER = new ThroughputTracker();

    public static final RecoveryManager RECOVERY_MANAGER = new RecoveryManager();
    public static FailureDetector failureDetector;
    public static FastLogger fastLogger;

    public static final BackPressureController BACKPRESSURE_OPTIMIZER =
            new BackPressureController(50, 200);
    public static final SmartBatcher BATCHING_OPTIMIZER =
            new SmartBatcher(10, 100, 50);

    private static org.apache.commons.csv.CSVParser parser;
    private static Iterator<CSVRecord> iterator;

    public static AtomicLong TOTAL_PROCESSED = new AtomicLong(0);
    public static AtomicLong TOTAL_FAILED = new AtomicLong(0);
    public static AtomicLong TOTAL_RETRIED = new AtomicLong(0);
    public static AtomicLong TOTAL_DROPPED = new AtomicLong(0);
    public static volatile int TARGET_RATE_TPS = 3000;


    // ============================
    // STAGE 1: SPOUT (FIXED FT)
    // ============================
    public static class TaxiSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private long lastEmitNs = 0;

        private Map<String, Values> pendingTuples = new HashMap<>();
        private Map<String, Integer> retryCount = new HashMap<>();
        private int retryLimit = 3;

        private List<String[]> rows;
        private int index = 0;

        private static final String CSV_PATH =
                System.getProperty("user.home") + "/ra-stream-data/nyc-taxi/combined.csv";

        @Override
        public void open(Map<String, Object> conf, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;

            rows = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(CSV_PATH))) {
                br.readLine();
                String line;
                while ((line = br.readLine()) != null) {
                    rows.add(line.split(",", -1));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            RECOVERY_MANAGER.registerTask("spout-" + context.getThisTaskId(),
                    "worker-" + context.getThisWorkerPort());
        }

        @Override
        public void nextTuple() {
            long now = System.nanoTime();
            if (now - lastEmitNs < 1_000_000_000L / TARGET_RATE_TPS) return;
            lastEmitNs = now;

            try {
                if (index >= rows.size()) index = 0;
                String[] r = rows.get(index++);

                String tupleId = UUID.randomUUID().toString();

                Values values = new Values(
                        Integer.parseInt(r[0]),
                        r[2], r[3],
                        Double.parseDouble(r[4]),
                        Double.parseDouble(r[5]),
                        Double.parseDouble(r[6]),
                        Double.parseDouble(r[7]),
                        r[1],
                        tupleId
                );

                pendingTuples.put(tupleId, values);

                LATENCY_TRACKER.recordEmit(tupleId);

                collector.emit(values, tupleId); // 🔥 FT enabled

            } catch (Exception ignored) {}
        }

        @Override
        public void ack(Object msgId) {
            String id = (String) msgId;
            pendingTuples.remove(id);
            retryCount.remove(id);
        }

        @Override
        public void fail(Object msgId) {
            String id = (String) msgId;

            int retries = retryCount.getOrDefault(id, 0);

            TOTAL_FAILED.incrementAndGet();

            if (retries < retryLimit) {
                retryCount.put(id, retries + 1);
                TOTAL_RETRIED.incrementAndGet();

                collector.emit(pendingTuples.get(id), id);
            } else {
                TOTAL_DROPPED.incrementAndGet();

                pendingTuples.remove(id);
                retryCount.remove(id);
                System.out.println("[Spout] Dropped " + id);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("vendorId","pickupZone","dropoffZone",
                    "fareAmount","totalAmount","tripDistance",
                    "passengerCount","pickupTime","tupleId"));
        }
    }

    // ============================
    // STAGE 2: VALIDATOR
    // ============================
    public static class ValidatorBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(Tuple t) {

            try {
                if (Math.random() < 0.05) {
                    TOTAL_FAILED.incrementAndGet();
                    collector.fail(t);   // 🔥 trigger Storm retry
                    return;
                }

                double fare = t.getDoubleByField("fareAmount");
                double dist = t.getDoubleByField("tripDistance");

                if (fare < 2.5 || fare > 300 || dist < 0.1 || dist > 100) {
                    collector.ack(t);
                    return;
                }

                collector.emit(t, t.getValues()); // 🔥 anchored
                collector.ack(t);

            } catch (Exception e) {
                collector.fail(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("vendorId","pickupZone","dropoffZone",
                    "fareAmount","totalAmount","tripDistance",
                    "passengerCount","pickupTime","tupleId"));
        }
    }

    // ============================
    // STAGE 3: AGGREGATOR (FT)
    // ============================
    public static class AggregateBolt extends BaseRichBolt {

        private OutputCollector collector;
        private Map<String, Double> revenue = new HashMap<>();
        private Set<String> processed = new HashSet<>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector col) {
            this.collector = col;
            loadCheckpoint();
        }

        @Override
        public void execute(Tuple t) {
            try {
                String id = t.getStringByField("tupleId");

                if (processed.contains(id)) {
                    collector.ack(t);
                    return;
                }

                processed.add(id);

                String zone = t.getStringByField("pickupZone");
                double fare = t.getDoubleByField("fareAmount");

                revenue.put(zone, revenue.getOrDefault(zone,0.0)+fare);

                collector.emit(t,new Values(zone,1L,
                        revenue.get(zone),0.0,id));

                if (processed.size() % 5000 == 0) saveCheckpoint();

                collector.ack(t);

            } catch (Exception e) {
                collector.fail(t);
            }
        }

        private void saveCheckpoint() {
            try (ObjectOutputStream oos =
                         new ObjectOutputStream(new FileOutputStream("/tmp/agg.chk"))) {
                oos.writeObject(revenue);
            } catch (Exception ignored) {}
        }

        private void loadCheckpoint() {
            try (ObjectInputStream ois =
                         new ObjectInputStream(new FileInputStream("/tmp/agg.chk"))) {
                revenue = (Map<String, Double>) ois.readObject();
            } catch (Exception ignored) {}
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("zone","tripCount","avgFare","avgDistance","tupleId"));
        }
    }

    // ============================
    // STAGE 4: ANOMALY
    // ============================
    public static class AnomalyBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(Tuple t) {
            try {
                double fare = t.getDoubleByField("avgFare");

                if (fare > 100) {
                    collector.emit(t,new Values(
                            t.getStringByField("zone"),
                            t.getLongByField("tripCount"),
                            fare,0.0,
                            "ALERT",
                            t.getStringByField("tupleId")
                    ));
                }

                collector.ack(t);

            } catch (Exception e) {
                collector.fail(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("zone","tripCount","avgFare","avgDistance","alertId","tupleId"));
        }
    }

    // ============================
    // STAGE 5: OUTPUT
    // ============================
    public static class OutputBolt extends BaseRichBolt {

        private OutputCollector collector;
        private long tupleCount = 0;
        private PrintWriter writer;

        private double getCpuUsage() {
            try {
                com.sun.management.OperatingSystemMXBean osBean =
                        (com.sun.management.OperatingSystemMXBean)
                                java.lang.management.ManagementFactory.getOperatingSystemMXBean();

                double cpu = osBean.getSystemCpuLoad();
                if (cpu < 0) return 0.0;

                return cpu * 100;

            } catch (Exception e) {
                return 0.0;
            }
        }

        private double getMemoryUsage() {
            Runtime runtime = Runtime.getRuntime();
            return (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0);
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector col) {
            this.collector = col;

            try {
                File file = new File("/home/karthik_hadoop/ra-stream-metrics/local/taxi/metricsFt.csv");
                boolean exists = file.exists();

                writer = new PrintWriter(new FileWriter(file, true));

                if (!exists) {
                    writer.println("timestamp,zone,tripCount,avgFare,throughput,latency");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void execute(Tuple t) {

            tupleCount++;

            String zone = t.getStringByField("zone");
            long trips = t.getLongByField("tripCount");
            double avgFare = t.getDoubleByField("avgFare");
            String tupleId = t.getStringByField("tupleId");

            LATENCY_TRACKER.recordReceive(tupleId);
            double latency = LATENCY_TRACKER.getAverageLatencyMs();
            double throughput = THROUGHPUT_TRACKER.getThroughput();

            TOTAL_PROCESSED.incrementAndGet();

            double cpu = getCpuUsage();
            double mem = getMemoryUsage();

            System.out.println(
                    "[OUTPUT] zone=" + zone +
                            " trips=" + trips +
                            " avgFare=" + avgFare +
                            " throughput=" + throughput +
                            " latency=" + latency +
                            " cpu=" + cpu +
                            " mem=" + mem +
                            " failed=" + TOTAL_FAILED.get() +
                            " retried=" + TOTAL_RETRIED.get()
            );

            writer.println(System.currentTimeMillis() + "," +
                    zone + "," + trips + "," +
                    avgFare + "," + throughput + "," + latency + "," +
                    cpu + "," + mem + "," +
                    TOTAL_PROCESSED.get() + "," +
                    TOTAL_FAILED.get() + "," +
                    TOTAL_RETRIED.get() + "," +
                    TOTAL_DROPPED.get());

            writer.flush();

            THROUGHPUT_TRACKER.recordTuple();
            collector.ack(t);
        }

        @Override
        public void cleanup() {
            if (writer != null) writer.close();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {}
    }
    // ============================
    // BUILD TOPOLOGY
    // ============================
    public static TopologyBuilder buildTopology() throws IOException {

        failureDetector = new FailureDetector(10000, failedNode -> {
            System.out.println("[FT] Node failed: " + failedNode);

            List<String> alive = List.of("worker-1","worker-2");
            Map<String,String> map =
                    RECOVERY_MANAGER.recoverNodeFailure(failedNode, alive);

            map.forEach((t,w)-> System.out.println("[FT] "+t+" -> "+w));
        });

        failureDetector.start(3000);

        fastLogger = new FastLogger(Paths.get("/tmp/taxi.log"),10000,100,5000);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("taxi-reader", new TaxiSpout(),2);

        builder.setBolt("taxi-validator", new ValidatorBolt(),2)
                .shuffleGrouping("taxi-reader");

        builder.setBolt("taxi-aggregator", new AggregateBolt(),2)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        builder.setBolt("taxi-anomaly", new AnomalyBolt(),2)
                .shuffleGrouping("taxi-aggregator");

        builder.setBolt("taxi-output", new OutputBolt(),1)
                .shuffleGrouping("taxi-anomaly"); // 🔥 FIXED

        return builder;
    }
}