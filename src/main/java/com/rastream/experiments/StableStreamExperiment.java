package com.rastream.experiments;

import com.rastream.baseline.BasicStreamProcessor;
import com.rastream.metrics.*;

public class StableStreamExperiment {
    public static void main(String[] args) throws Exception {
        int rate = args.length > 0 ? Integer.parseInt(args[0]) : 3000;
        int durationSec = args.length > 1 ? Integer.parseInt(args[1]) : 3600;
        String config = args.length > 2 ? args[2] : "BasicProcessor";

        BasicStreamProcessor processor = new BasicStreamProcessor(200_000, 16);
        ResourceMonitor resource = new ResourceMonitor();
        MetricsCollector collector = new MetricsCollector("results/raw-data/stable_" + config + "_" + rate + ".csv");

        processor.start();
        long start = System.currentTimeMillis();
        long nextSample = start + 10_000;

        long tupleId = 1;
        while (System.currentTimeMillis() - start < durationSec * 1000L) {
            long secStart = System.currentTimeMillis();
            for (int i = 0; i < rate; i++) {
                processor.submit(new BasicStreamProcessor.Tuple(tupleId++, System.nanoTime(), payload(128)), 5);
            }
            long elapsed = System.currentTimeMillis() - secStart;
            if (elapsed < 1000) Thread.sleep(1000 - elapsed);

            if (System.currentTimeMillis() >= nextSample) {
                long t = (System.currentTimeMillis() - start) / 1000;
                var lat = processor.latencySnapshot();
                var th = processor.throughputSnapshot();
                var rs = resource.snapshot();
                collector.writeRow(t, config, rate, lat.avgMs, lat.p50Ms, lat.p95Ms, lat.p99Ms,
                        th.tuplesPerSec, rs.cpuPercent, rs.usedMemoryBytes, rs.maxMemoryBytes, 1, 0, 0);
                nextSample += 10_000;
            }
        }

        collector.close();
        processor.stop();
    }

    private static byte[] payload(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i % 127);
        return b;
    }
}