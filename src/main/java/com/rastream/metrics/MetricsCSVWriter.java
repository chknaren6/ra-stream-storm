package com.rastream.metrics;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MetricsCSVWriter {

    private final String filePath;
    private PrintWriter writer;

    private static final String HEADER =
            "timestamp,scheduler,topology,stream_rate_tps," +
                    "avg_latency_ms,max_latency_ms,min_latency_ms," +
                    "throughput_tps,tuple_count,node_count," +
                    "avg_cpu_pct,avg_mem_pct,stage,notes";

    public MetricsCSVWriter(String filePath) {
        this.filePath = filePath;
    }

    public void open() throws IOException {
        java.io.File f = new java.io.File(filePath);
        // Create parent directories if they don't exist
        f.getParentFile().mkdirs();
        writer = new PrintWriter(new FileWriter(filePath, true));
        // Write header only if file is new or empty
        if (f.length() == 0) {
            writer.println(HEADER);
            writer.flush();
        }
        System.out.println("MetricsCSVWriter opened: " + filePath);
    }

    // Write one metrics row directly with all values
    public void writeRow(
            String scheduler,
            String topology,
            int    streamRateTps,
            double avgLatencyMs,
            double maxLatencyMs,
            double minLatencyMs,
            double throughputTps,
            long   tupleCount,
            int    nodeCount,
            double avgCpuPct,
            double avgMemPct,
            String stage,
            String notes) {

        String timestamp = LocalDateTime.now().format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        writer.printf("%s,%s,%s,%d,%.2f,%.2f,%.2f,%.2f,%d,%d,%.1f,%.1f,%s,%s%n",
                timestamp, scheduler, topology, streamRateTps,
                avgLatencyMs, maxLatencyMs, minLatencyMs,
                throughputTps, tupleCount, nodeCount,
                avgCpuPct, avgMemPct, stage, notes);
        writer.flush();
    }

    public void close() {
        if (writer != null) {
            writer.close();
            System.out.println("MetricsCSVWriter closed: " + filePath);
        }
    }
}