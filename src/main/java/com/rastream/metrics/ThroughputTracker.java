package com.rastream.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class ThroughputTracker {

    private final AtomicLong tupleCount = new AtomicLong(0);
    private final long startTimeMs      = System.currentTimeMillis();

    // Call every time a tuple is fully processed
    public void recordTuple() {
        tupleCount.incrementAndGet();
    }

    // Tuples per second since start
    public double getThroughput() {
        long elapsed = System.currentTimeMillis() - startTimeMs;
        if (elapsed == 0) return 0.0;
        return (tupleCount.get() * 1000.0) / elapsed;
    }

    public long getTupleCount() { return tupleCount.get(); }

    public void printReport() {
        System.out.println("\n=== Throughput Report ===");
        System.out.println("Total tuples     : " + getTupleCount());
        System.out.println("Throughput (t/s) : "
                + String.format("%.2f", getThroughput()));
        System.out.println("Runtime (s)      : "
                + (System.currentTimeMillis() - startTimeMs) / 1000);
    }
}