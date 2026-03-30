package com.rastream.metrics;

import java.io.*;
import java.nio.file.*;

public class MetricsCollector implements Closeable {
    private final FileWriter fw;

    public MetricsCollector(String csvPath) throws IOException {
        Path p = Paths.get(csvPath);
        Files.createDirectories(p.getParent());
        fw = new FileWriter(p.toFile());
        fw.write("time_sec,config,data_rate,avg_ms,p50_ms,p95_ms,p99_ms,tps,cpu,used_mem,max_mem,active_nodes,recovery_ms,data_loss\n");
    }

    public synchronized void writeRow(long t, String config, int rate,
                                      double avg, double p50, double p95, double p99,
                                      double tps, double cpu, long usedMem, long maxMem,
                                      int activeNodes, long recoveryMs, long dataLoss) throws IOException {
        fw.write(String.join(",",
                String.valueOf(t), config, String.valueOf(rate),
                String.valueOf(avg), String.valueOf(p50), String.valueOf(p95), String.valueOf(p99),
                String.valueOf(tps), String.valueOf(cpu),
                String.valueOf(usedMem), String.valueOf(maxMem),
                String.valueOf(activeNodes), String.valueOf(recoveryMs), String.valueOf(dataLoss)) + "\n");
        fw.flush();
    }

    @Override public void close() throws IOException { fw.close(); }
}