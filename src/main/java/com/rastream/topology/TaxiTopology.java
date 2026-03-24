package com.rastream.topology;

import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ResourceModel;
import com.rastream.monitor.DataMonitor;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class TaxiTopology {

    // Shared trackers — same pattern as WordCount
    public static final LatencyTracker LATENCY_TRACKER
            = new LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER
            = new ThroughputTracker();

    private static final boolean IN_MEMORY_MODE =
            System.getenv("IN_MEMORY_MODE") != null
                    && System.getenv("IN_MEMORY_MODE").equals("true");

    // Static CSV parser state for streaming mode
    private static org.apache.commons.csv.CSVParser parser;
    private static java.util.Iterator<org.apache.commons.csv.CSVRecord> iterator;

    public static final String[] COMPONENT_NAMES = {
            "taxi-reader", "taxi-validator", "taxi-aggregator",
            "taxi-anomaly", "taxi-output"
    };

    // Target emit rate — change this to control load
    public static volatile int TARGET_RATE_TPS = 3000;

    // -----------------------------------------------------------------------
    // Spout
    // -----------------------------------------------------------------------
    public static class TaxiSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private long lastEmitNs = 0;
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
                System.out.println("TaxiSpout: streaming CSV from disk");
                try {
                    openCsvParser();
                } catch (Exception e) {
                    throw new RuntimeException("Cannot open CSV: " + CSV_PATH, e);
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
                    org.apache.commons.csv.CSVRecord record = iterator.next();
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
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "vendorId", "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount", "tripDistance",
                    "passengerCount", "pickupTime", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // ValidatorBolt — HIGH Tr with TaxiSpout
    // Ra-Stream places these TWO in same process (fine-grained scheduling)
    // -----------------------------------------------------------------------
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

            boolean valid = fare > 0
                    && fare < 500
                    && distance >= 0
                    && zone != null
                    && !zone.isEmpty();

            if (!valid) {
                dropped++;
                collector.ack(t);
                return;
            }

            passed++;
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
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "pickupZone", "dropoffZone",
                    "fareAmount", "totalAmount",
                    "tripDistance", "passengerCount",
                    "pickupTime", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // AggregateBolt — groups trips by pickup zone (10-second tumbling window)
    // -----------------------------------------------------------------------
    public static class AggregateBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private final Map<String, double[]> zoneStats = new java.util.HashMap<>();
        private long windowStart = System.currentTimeMillis();
        private static final long WINDOW_MS = 10_000;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector col) {
            this.collector = col;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple t) {
            String zone = t.getStringByField("pickupZone");
            double fare = t.getDoubleByField("fareAmount");
            double dist = t.getDoubleByField("tripDistance");

            zoneStats.computeIfAbsent(zone, k -> new double[]{0, 0, 0});
            double[] stats = zoneStats.get(zone);
            stats[0]++;
            stats[1] += fare;
            stats[2] += dist;

            long now = System.currentTimeMillis();
            if (now - windowStart >= WINDOW_MS) {
                for (Map.Entry<String, double[]> e : zoneStats.entrySet()) {
                    double[] s = e.getValue();
                    double avgFare = s[0] > 0 ? s[1] / s[0] : 0;
                    double avgDist = s[0] > 0 ? s[2] / s[0] : 0;
                    collector.emit(new Values(
                            e.getKey(),
                            (long) s[0],
                            s[1],
                            avgFare,
                            avgDist,
                            t.getStringByField("tupleId")
                    ));
                }
                zoneStats.clear();
                windowStart = now;
            }
            collector.ack(t);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "zone", "tripCount", "totalFare",
                    "avgFare", "avgDistance", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // AnomalyBolt — LOW Tr, fires only on suspicious trips
    // -----------------------------------------------------------------------
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
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "zone", "tripCount", "avgFare",
                    "avgDistance", "alertId", "tupleId"));
        }
    }

    // -----------------------------------------------------------------------
    // OutputBolt — measures end-to-end latency and throughput
    // -----------------------------------------------------------------------
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
            String tupleId = t.getStringByField("tupleId");
            LATENCY_TRACKER.recordReceive(tupleId);

            if (tupleCount % 100 == 0) {
                System.out.printf(
                        "[Taxi Output] windows=%d zone=%s trips=%d avgFare=%.2f"
                                + " | throughput=%.0f t/s latency=%.2fms%n",
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
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields()); // terminal bolt
        }
    }

    // -----------------------------------------------------------------------
    // buildTopology() — Taxi-only, DataMonitor wired to the Taxi DAG
    //
    // Topology shape (matches paper Figure 8 style):
    //   TaxiSpout(2) → ValidatorBolt(3) → AggregateBolt(3)
    //                                           ├→ AnomalyBolt(2)
    //                                           └→ OutputBolt(2)
    //
    // Fine-grained co-location intent:
    //   TaxiSpout task k + ValidatorBolt task k → same process (HIGH Tr)
    //   AggregateBolt → AnomalyBolt / OutputBolt → inter-process OK (LOW Tr)
    // -----------------------------------------------------------------------
    public static TopologyBuilder buildTopology() {

        // --- Build the Taxi DAG for DataMonitor ---
        // Vertex IDs match COMPONENT_NAMES order:
        //   v1 = taxi-reader, v2 = taxi-validator, v3 = taxi-aggregator,
        //   v4 = taxi-anomaly, v5 = taxi-output
        StreamApplication taxiApp = new StreamApplication("TaxiNYC-DAG");

        // Tasks per vertex (parallelism hints match Storm config below)
        Task reader1 = new Task(1, 0, "taxi-reader");
        Task reader2 = new Task(1, 1, "taxi-reader");

        Task validator1 = new Task(2, 0, "taxi-validator");
        Task validator2 = new Task(2, 1, "taxi-validator");
        Task validator3 = new Task(2, 2, "taxi-validator");

        Task aggregator1 = new Task(3, 0, "taxi-aggregator");
        Task aggregator2 = new Task(3, 1, "taxi-aggregator");
        Task aggregator3 = new Task(3, 2, "taxi-aggregator");

        Task anomaly1 = new Task(4, 0, "taxi-anomaly");
        Task anomaly2 = new Task(4, 1, "taxi-anomaly");

        Task output1 = new Task(5, 0, "taxi-output");
        Task output2 = new Task(5, 1, "taxi-output");

        // Add all tasks to the DAG
        for (Task t : new Task[]{
                reader1, reader2,
                validator1, validator2, validator3,
                aggregator1, aggregator2, aggregator3,
                anomaly1, anomaly2,
                output1, output2}) {
            taxiApp.addTask(t);
        }

        // Add edges (reader → validator, HIGH Tr expected)
        for (Task r : new Task[]{reader1, reader2}) {
            for (Task v : new Task[]{validator1, validator2, validator3}) {
                taxiApp.addEdge(new Edge(r, v));
            }
        }
        // validator → aggregator
        for (Task v : new Task[]{validator1, validator2, validator3}) {
            for (Task a : new Task[]{aggregator1, aggregator2, aggregator3}) {
                taxiApp.addEdge(new Edge(v, a));
            }
        }
        // aggregator → anomaly (LOW Tr) and → output
        for (Task a : new Task[]{aggregator1, aggregator2, aggregator3}) {
            for (Task an : new Task[]{anomaly1, anomaly2}) {
                taxiApp.addEdge(new Edge(a, an));
            }
            for (Task o : new Task[]{output1, output2}) {
                taxiApp.addEdge(new Edge(a, o));
            }
        }

        // --- Wire DataMonitor to the Taxi DAG ---
        CommunicationModel commModel = new CommunicationModel();
        ResourceModel resModel = new ResourceModel();
        // DataMonitor is registered as a Storm IMetricsConsumer in the Config
        // (see TopologyRunner / ClusterTopologyRunner). We construct it here
        // so the reference is available; Storm will call prepare() on it.
        DataMonitor monitor = new DataMonitor(taxiApp, commModel, resModel);

        // --- Build the Storm topology ---
        TopologyBuilder builder = new TopologyBuilder();

        // v1: reader spout — 2 tasks
        builder.setSpout("taxi-reader", new TaxiSpout(), 2);

        // v2: validator — 3 tasks, HIGH Tr with spout
        builder.setBolt("taxi-validator", new ValidatorBolt(), 3)
                .shuffleGrouping("taxi-reader");

        // v3: aggregator — 3 tasks
        // fieldsGrouping on pickupZone ensures same zone → same task (correctness)
        builder.setBolt("taxi-aggregator", new AggregateBolt(), 3)
                .fieldsGrouping("taxi-validator", new Fields("pickupZone"));

        // v4: anomaly detector — 2 tasks, LOW Tr
        builder.setBolt("taxi-anomaly", new AnomalyBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        // v5: output — 2 tasks
        builder.setBolt("taxi-output", new OutputBolt(), 2)
                .shuffleGrouping("taxi-aggregator");

        return builder;
    }
}