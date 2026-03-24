package com.rastream.topology;

import com.rastream.metrics.MetricsCSVWriter;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyRunner {

    public static void main(String[] args) throws Exception {

        runTaxi();


    }

    public static void runTaxi() throws Exception {
        System.out.println("=== Starting Taxi Topology ===");
        System.out.println("Dataset: NYC Taxi 2023 Q1 (9.38M rows)");
        System.out.println("Starting rate: "
                + TaxiTopology.TARGET_RATE_TPS + " t/s");

        String metricsBase = System.getenv("METRICS_PATH") != null
                ? System.getenv("METRICS_PATH")
                : System.getProperty("user.home") + "/ra-stream-metrics/local/taxi";

        new java.io.File(metricsBase).mkdirs();
        MetricsCSVWriter csv = new MetricsCSVWriter(metricsBase + "/metrics.csv");

        csv.open();

        TopologyBuilder builder = TaxiTopology.buildTopology();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("taxi-rastream",
                conf, builder.createTopology());

        System.out.println("Topology submitted. Warming up 10s...\n");
        Thread.sleep(10_000);

        // Ramp up rate every 30 seconds
        // This will find your system's saturation point
        int[] rates = {1000, 2000, 3000, 5000, 8000, 12000};

        for (int rate : rates) {
            TaxiTopology.TARGET_RATE_TPS = rate;
            System.out.println("\n>>> Changing rate to "
                    + rate + " t/s — running 30s...");

            Thread.sleep(30_000);

            // Print snapshot
            TaxiTopology.LATENCY_TRACKER.printReport();
            TaxiTopology.THROUGHPUT_TRACKER.printReport();

            // Write to CSV
            csv.writeRow(
                    "RaStream",
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
                    "ramp_up_rate=" + rate
            );
        }

        csv.close();
        cluster.killTopology("taxi-rastream");
        cluster.close();

        System.out.println("\n=== Done. Check metrics at ===");
        System.out.println(System.getProperty("user.home")
                + "/ra-stream-metrics/local/taxi/metrics.csv");
    }
}