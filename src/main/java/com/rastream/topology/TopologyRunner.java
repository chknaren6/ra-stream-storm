package com.rastream.topology;

import com.rastream.metrics.MetricsCSVWriter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * TopologyRunner — entry point for running TaxiTopology in LocalCluster.
 *
 * Usage:
 *   java -jar ra-stream.jar [--mode basic|rastream|rastream-ft] [--duration-s N]
 *
 * Variants:
 *   basic        — Variant 1: simple 5-stage pipeline, no optimizations, no FT.
 *   rastream     — Variant 2 (default): RA-Stream with BackPressure + SmartBatcher.
 *   rastream-ft  — Variant 3: RA-Stream + full fault tolerance (ack/fail/retry).
 *
 * Metrics are written to:
 *   ${METRICS_PATH}/metrics.csv   (env var)
 *   or  ~/ra-stream-metrics/local/taxi/metrics.csv  (default)
 */
public class TopologyRunner {

    public static void main(String[] args) throws Exception {
        String mode       = "rastream";  // default variant
        int    durationS  = 200;         // total run time in seconds

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":
                    if (i + 1 < args.length) mode = args[++i];
                    break;
                case "--duration-s":
                    if (i + 1 < args.length) durationS = Integer.parseInt(args[++i]);
                    break;
                default:
                    break;
            }
        }

        System.out.println("=== Starting Taxi Topology ===");
        System.out.println("Mode     : " + mode);
        System.out.println("Duration : " + durationS + " s");
        System.out.println("Dataset  : NYC Taxi (built-in synthetic data or CSV_PATH)");
        System.out.println("Rate     : " + TaxiTopology.TARGET_RATE_TPS + " t/s initial");

        runTaxi(mode, durationS);
    }

    public static void runTaxi(String mode, int totalDurationS) throws Exception {
        String metricsBase = System.getenv("METRICS_PATH") != null
                ? System.getenv("METRICS_PATH")
                : System.getProperty("user.home") + "/ra-stream-metrics/local/taxi";

        new java.io.File(metricsBase).mkdirs();
        String metricsFile = metricsBase + "/metrics.csv";
        MetricsCSVWriter csv = new MetricsCSVWriter(metricsFile);
        csv.open();

        // Select topology variant
        TopologyBuilder builder;
        String schedulerLabel;
        switch (mode) {
            case "basic":
                builder        = TaxiTopology.buildBasicTopology();
                schedulerLabel = "Basic";
                break;
            case "rastream-ft":
                builder        = TaxiTopology.buildFaultTolerantTopology();
                schedulerLabel = "RaStreamFT";
                break;
            default: // "rastream"
                builder        = TaxiTopology.buildTopology();
                schedulerLabel = "RaStream";
                break;
        }

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("taxi-" + mode, conf, builder.createTopology());

        System.out.println("Topology submitted. Warming up 10s...\n");
        Thread.sleep(10_000);

        // Ramp up the emit rate in steps and record metrics at each step
        int[] rates    = {1000, 2000, 3000, 5000, 8000};
        int   stepS    = Math.max(15, (totalDurationS - 10) / rates.length);

        for (int rate : rates) {
            TaxiTopology.TARGET_RATE_TPS = rate;
            System.out.printf("%n>>> Rate → %d t/s  (running %d s)...%n", rate, stepS);

            Thread.sleep((long) stepS * 1_000);

            // Print snapshot
            TaxiTopology.LATENCY_TRACKER.printReport();
            TaxiTopology.THROUGHPUT_TRACKER.printReport();

            // Write to CSV
            csv.writeRow(
                    schedulerLabel,
                    "TaxiNYC",
                    rate,
                    TaxiTopology.LATENCY_TRACKER.getAverageLatencyMs(),
                    TaxiTopology.LATENCY_TRACKER.getMaxLatencyMs(),
                    TaxiTopology.LATENCY_TRACKER.getMinLatencyMs(),
                    TaxiTopology.THROUGHPUT_TRACKER.getThroughput(),
                    TaxiTopology.THROUGHPUT_TRACKER.getTupleCount(),
                    1,
                    0.0,
                    0.0,
                    "local",
                    "mode=" + mode + " ramp_rate=" + rate
            );
        }

        csv.close();
        cluster.killTopology("taxi-" + mode);
        cluster.close();

        System.out.println("\n=== Done. Metrics written to ===");
        System.out.println(metricsFile);
    }

    // Backward-compatible helper (called by tests / old scripts)
    public static void runTaxi() throws Exception {
        runTaxi("rastream", 200);
    }
}