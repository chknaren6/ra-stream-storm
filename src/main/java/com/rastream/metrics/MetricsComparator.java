package com.rastream.metrics;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * MetricsComparator - Compares RaStream vs EvenScheduler metrics
 * and identifies performance degradation.
 */
public class MetricsComparator {

    static class MetricRow {
        String timestamp;
        String scheduler;
        int streamRate;
        double avgLatency;
        double maxLatency;
        double throughput;
        long tupleCount;
        String notes;
    }

    public static void main(String[] args) throws Exception {
        String metricsPath = System.getenv("METRICS_PATH") != null
                ? System.getenv("METRICS_PATH")
                : System.getProperty("user.home") + "/ra-stream-metrics/local/taxi";

        String raStreamFile = metricsPath + "/metrics.csv";
        String evenSchedulerFile = metricsPath + "/metrics_even.csv";

        System.out.println("=== Taxi Topology Metrics Comparison ===\n");

        List<MetricRow> raStreamMetrics = parseMetrics(raStreamFile);
        List<MetricRow> evenMetrics = parseMetrics(evenSchedulerFile);

        if (raStreamMetrics.isEmpty() || evenMetrics.isEmpty()) {
            System.out.println("ERROR: Could not load metrics from files");
            return;
        }

        System.out.println("Metrics loaded: RaStream=" + raStreamMetrics.size()
                + ", EvenScheduler=" + evenMetrics.size());
        System.out.println("\n=== Performance Analysis ===\n");

        Map<Integer, MetricRow> raByRate = new HashMap<>();
        Map<Integer, MetricRow> evenByRate = new HashMap<>();

        for (MetricRow r : raStreamMetrics) {
            raByRate.put(r.streamRate, r);
        }
        for (MetricRow r : evenMetrics) {
            evenByRate.put(r.streamRate, r);
        }

        System.out.println(String.format("%-12s | %-12s | %-12s | %-12s | %-12s",
                "Stream Rate", "RaStream Lat", "Even Lat", "Degradation", "Throughput"));
        System.out.println(String.format("%-12s | %-12s | %-12s | %-12s | %-12s",
                "(tps)", "(ms)", "(ms)", "(% worse)", "RaStream vs Even"));
        System.out.println("---------|---------|---------|---------|---------|");

        double totalDegradation = 0.0;
        int count = 0;

        for (int rate : raByRate.keySet()) {
            MetricRow ra = raByRate.get(rate);
            MetricRow even = evenByRate.get(rate);

            if (even != null) {
                double latencyDegradation = ((even.avgLatency - ra.avgLatency) / ra.avgLatency) * 100;
                double throughputDiff = ra.throughput - even.throughput;

                System.out.println(String.format("%-12d | %-12.2f | %-12.2f | %-12.1f%% | %.0f vs %.0f",
                        rate, ra.avgLatency, even.avgLatency, latencyDegradation,
                        ra.throughput, even.throughput));

                totalDegradation += latencyDegradation;
                count++;
            }
        }

        System.out.println("\n=== Summary ===");
        if (count > 0) {
            double avgDegradation = totalDegradation / count;
            System.out.printf("Average latency degradation (EvenScheduler vs RaStream): %.1f%%%n", avgDegradation);

            if (avgDegradation > 0) {
                System.out.println("✓ EvenScheduler shows expected worse performance (higher latency)");
            } else {
                System.out.println("✗ EvenScheduler unexpectedly performs better - check system state");
            }
        }
    }

    static List<MetricRow> parseMetrics(String filePath) throws IOException {
        List<MetricRow> rows = new ArrayList<>();
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            System.out.println("File not found: " + filePath);
            return rows;
        }

        List<String> lines = Files.readAllLines(path);
        for (int i = 1; i < lines.size(); i++) { // Skip header
            String[] parts = lines.get(i).split(",");
            if (parts.length >= 13) {
                MetricRow row = new MetricRow();
                row.timestamp = parts[0];
                row.scheduler = parts[1];
                row.streamRate = Integer.parseInt(parts[3]);
                row.avgLatency = Double.parseDouble(parts[4]);
                row.maxLatency = Double.parseDouble(parts[5]);
                row.throughput = Double.parseDouble(parts[7]);
                row.tupleCount = Long.parseLong(parts[8]);
                row.notes = parts.length > 13 ? parts[13] : "";
                rows.add(row);
            }
        }
        return rows;
    }
}