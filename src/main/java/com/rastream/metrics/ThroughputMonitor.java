package com.rastream.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class ThroughputMonitor {
    private final AtomicLong processed = new AtomicLong(0);
    private volatile long startMs = 0L;
    private volatile boolean started = false;

    public void start() {
        this.startMs = System.currentTimeMillis();
        this.started = true;
    }

    public void stop() {
        this.started = false;
    }

    public void markProcessed(long n) {
        if (n > 0) processed.addAndGet(n);
    }

    public Snapshot snapshot() {
        long now = System.currentTimeMillis();
        long elapsed = Math.max(1L, now - startMs);
        double tps = started ? (processed.get() * 1000.0 / elapsed) : 0.0;
        return new Snapshot(processed.get(), tps);
    }

    public static class Snapshot {
        public final long totalProcessed;
        public final double tuplesPerSec;

        public Snapshot(long totalProcessed, double tuplesPerSec) {
            this.totalProcessed = totalProcessed;
            this.tuplesPerSec = tuplesPerSec;
        }
    }
}
