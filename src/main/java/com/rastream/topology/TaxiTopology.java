package com.rastream.topology;

import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.FileReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TaxiTopology {

    // Shared trackers — same pattern as WordCount
    public static final LatencyTracker LATENCY_TRACKER
            = new LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER
            = new ThroughputTracker();

    private static final boolean IN_MEMORY_MODE =
            System.getenv("IN_MEMORY_MODE") != null
                    && System.getenv("IN_MEMORY_MODE").equals("true");

    private static org.apache.commons.csv.CSVParser parser;
    private static java.util.Iterator<org.apache.commons.csv.CSVRecord> iterator;
    private List<String[]> rows;
    private int currentIndex = 0;

    public static final String[] COMPONENT_NAMES = {"taxi-reader", "taxi-validator", "taxi-aggregator", "taxi-anomaly", "taxi-output"};
    // Target emit rate — change this to control load
    // Start at 3000, increase to find your system's limit
    public static volatile int TARGET_RATE_TPS = 3000;

    public static class TaxiSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private long lastEmitNs = 0;

        // Pre-loaded in memory — eliminates I/O bottleneck
        private List<String[]> rows;
        private int currentIndex = 0;



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
                // Load all rows into memory (for local mode)
                this.rows = new java.util.ArrayList<>();
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
                System.out.println("TaxiSpout: loaded "
                        + rows.size() + " rows into memory");
            } else {
                // Stream mode — read row by row (for Docker/cluster)
                System.out.println("TaxiSpout: streaming CSV from disk");
                try {
                    openCsvParser();
                } catch (Exception e) {
                    throw new RuntimeException("Cannot open CSV: "
                            + CSV_PATH, e);
                }
            }
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
                    org.apache.commons.csv.CSVRecord record =
                            iterator.next();
                    r = new String[]{
                            record.get("VendorID"),
                            record.get("tpep_pickup_datetime"),
                            record.get("PULocationID"),
                            record.get("DOLocationID"),
                            record.get("fare_amount"),
                            record.get("total_amount"),
                            record.get("trip_distance"),
                            record.get("passenger_count")
                    };
                }

                String tupleId = java.util.UUID.randomUUID().toString();
                LATENCY_TRACKER.recordEmit(tupleId);

                collector.emit(new Values(
                        safeInt(r, 0),
                        r[2].trim(),
                        r[3].trim(),
                        safeDouble(r, 4),
                        safeDouble(r, 5),
                        safeDouble(r, 6),
                        safeDouble(r, 7),
                        r[1].trim(),
                        tupleId
                ));
            } catch (Exception e) {
                // skip malformed row
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
        public void declareOutputFields(
                OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "vendorId", "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount", "tripDistance",
                    "passengerCount", "pickupTime", "tupleId"));
        }
    }
    // ValidatorBolt — HIGH Tr with TaxiSpout
    // Ra-Stream places these TWO in same process
    // This is where fine-grained scheduling saves the most
    public static class ValidatorBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long dropped = 0;
        private long passed  = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple t) {
            double fare     = t.getDoubleByField("fareAmount");
            double distance = t.getDoubleByField("tripDistance");
            String zone     = t.getStringByField("pickupZone");

            // Drop invalid rows
            // These are the 71K null rows from our analysis
            boolean valid = fare > 0
                    && fare < 500          // no crazy fares
                    && distance >= 0       // no negative distance
                    && zone != null
                    && !zone.isEmpty();

            if (!valid) {
                dropped++;
                collector.ack(t);
                return;
            }

            passed++;
            // Pass all fields through plus validation flag
            collector.emit(new Values(
                    t.getStringByField("pickupZone"),
                    t.getStringByField("dropoffZone"),
                    fare,
                    t.getDoubleByField("totalAmount"),
                    distance,
                    t.getDoubleByField("passengerCount"),
                    t.getStringByField("pickupTime"),
                    t.getStringByField("tupleId")
            ));
            THROUGHPUT_TRACKER.recordTuple();
            collector.ack(t);
        }

        @Override
        public void declareOutputFields(
                OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount",
                    "tripDistance", "passengerCount",
                    "pickupTime", "tupleId"));
        }
    }
    // AggregateBolt — groups trips by pickup zone
    // Uses fieldsGrouping so same zone always hits same task
    // This is critical for correct aggregation
    public static class AggregateBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;

        // In-memory aggregation per zone
        private final Map<String, double[]> zoneStats
                = new java.util.HashMap<>();
        // double[] = [tripCount, totalFare, totalDistance]

        private long windowStart = System.currentTimeMillis();
        private static final long WINDOW_MS = 10_000; // 10 second window

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple t) {
            String zone  = t.getStringByField("pickupZone");
            double fare  = t.getDoubleByField("fareAmount");
            double dist  = t.getDoubleByField("tripDistance");

            // Accumulate stats for this zone
            zoneStats.computeIfAbsent(zone,
                    k -> new double[]{0, 0, 0});
            double[] stats = zoneStats.get(zone);
            stats[0]++;         // trip count
            stats[1] += fare;   // total fare
            stats[2] += dist;   // total distance

            // Emit window summary every 10 seconds
            long now = System.currentTimeMillis();
            if (now - windowStart >= WINDOW_MS) {
                for (Map.Entry<String, double[]> e
                        : zoneStats.entrySet()) {
                    double[] s = e.getValue();
                    double avgFare = s[0] > 0 ? s[1]/s[0] : 0;
                    double avgDist = s[0] > 0 ? s[2]/s[0] : 0;
                    collector.emit(new Values(
                            e.getKey(),     // zone
                            (long) s[0],    // trip count
                            s[1],           // total fare
                            avgFare,        // avg fare
                            avgDist,        // avg distance
                            t.getStringByField("tupleId")
                    ));
                }
                zoneStats.clear();
                windowStart = now;
            }
            collector.ack(t);
        }

        @Override
        public void declareOutputFields(
                OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "zone", "tripCount", "totalFare",
                    "avgFare", "avgDistance", "tupleId"));
        }
    }
    // AnomalyBolt — detects suspicious trips
    // LOW Tr — only fires on anomalies (rare)
    // Inter-process is acceptable here
    public static class AnomalyBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long anomalyCount = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple t) {
            double avgFare = t.getDoubleByField("avgFare");
            double avgDist = t.getDoubleByField("avgDistance");
            long   trips   = t.getLongByField("tripCount");

            // Flag anomalies:
            // High fare + short distance = suspicious
            // Very high trip count = possible data issue
            boolean anomaly =
                    (avgFare > 80 && avgDist < 1.0)
                            || (trips > 10000)
                            || (avgFare <= 0 && trips > 0);

            if (anomaly) {
                anomalyCount++;
                collector.emit(new Values(
                        t.getStringByField("zone"),
                        trips, avgFare, avgDist,
                        "ANOMALY-" + anomalyCount,
                        t.getStringByField("tupleId")
                ));
            }
            collector.ack(t);
        }

        @Override
        public void declareOutputFields(
                OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "zone", "tripCount", "avgFare",
                    "avgDistance", "alertId", "tupleId"));
        }
    }

    // OutputBolt — measures latency and throughput
    public static class OutputBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long tupleCount = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple t) {
            tupleCount++;

            // Record latency — tupleId flows from spout through aggregator
            String tupleId = t.getStringByField("tupleId");
            LATENCY_TRACKER.recordReceive(tupleId);

            if (tupleCount % 100 == 0) {
                System.out.printf(
                        "[Taxi Output] windows=%d zone=%s " +
                                "trips=%d avgFare=%.2f " +
                                "| raw_throughput=%.0f t/s latency=%.2fms%n",
                        tupleCount,
                        t.getStringByField("zone"),
                        t.getLongByField("tripCount"),
                        t.getDoubleByField("avgFare"),
                        THROUGHPUT_TRACKER.getThroughput(),
                        LATENCY_TRACKER.getAverageLatencyMs());
            }
            collector.ack(t);
        }

        @Override
        public void declareOutputFields(
                OutputFieldsDeclarer d) {
            d.declare(new Fields());
        }
    }
    // Build topology — parallelism matches paper Figure 2 style
    // Spout(2) → Validator(3) → Aggregator(3) → Anomaly(2) → Output(2)
    //
    // Fine-grained scheduling will pair:
    //   Spout task 1 + Validator task 1 → same process (HIGH Tr)
    //   Spout task 2 + Validator task 2 → same process (HIGH Tr)
    //   Aggregator + Anomaly → inter-process OK (LOW Tr)
    public static TopologyBuilder buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        // v1: reader spout — 2 tasks
        builder.setSpout("taxi-reader",
                new TaxiSpout(), 2);

        // v2: validator — 3 tasks, HIGH Tr with spout
        // shuffleGrouping = spread load across validator tasks
        builder.setBolt("taxi-validator",
                        new ValidatorBolt(), 3)
                .shuffleGrouping("taxi-reader");

        // v3: aggregator — 3 tasks
        // fieldsGrouping on pickupZone = same zone → same task
        // This is REQUIRED for correct aggregation
        builder.setBolt("taxi-aggregator",
                        new AggregateBolt(), 3)
                .fieldsGrouping("taxi-validator",
                        new Fields("pickupZone"));

        // v4: anomaly detector — 2 tasks, LOW Tr
        builder.setBolt("taxi-anomaly",
                        new AnomalyBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        // v5: output — 2 tasks
        builder.setBolt("taxi-output",
                        new OutputBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        return builder;
    }
}