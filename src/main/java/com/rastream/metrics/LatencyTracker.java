package com.rastream.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyTracker {

    // Stores emit timestamp for each tuple ID
    // Key = tupleId, Value = emit time in ms
    private final ConcurrentHashMap<String, Long> emitTimes
            = new ConcurrentHashMap<>();

    // Running stats
    private final AtomicLong totalLatency   = new AtomicLong(0);
    private final AtomicLong tupleCount     = new AtomicLong(0);
    private volatile long    maxLatency     = 0;
    private volatile long    minLatency     = Long.MAX_VALUE;

    // Call this when spout emits a tuple
    public void recordEmit(String tupleId) {
        emitTimes.put(tupleId, System.currentTimeMillis());
    }

    // Call this when output bolt receives the tuple
    public void recordReceive(String tupleId) {
        Long emitTime = emitTimes.remove(tupleId);
        if (emitTime == null) return;

        long latency = System.currentTimeMillis() - emitTime;
        totalLatency.addAndGet(latency);
        tupleCount.incrementAndGet();

        // Update min/max
        synchronized (this) {
            if (latency > maxLatency) maxLatency = latency;
            if (latency < minLatency) minLatency = latency;
        }
    }

    public double getAverageLatencyMs() {
        long count = tupleCount.get();
        if (count == 0) return 0.0;
        return (double) totalLatency.get() / count;
    }

    public long getMaxLatencyMs() { return maxLatency; }
    public long getMinLatencyMs() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }
    public long getTupleCount()   { return tupleCount.get(); }

    public void printReport() {
        System.out.println("\n=== Latency Report ===");
        System.out.println("Tuples processed : " + getTupleCount());
        System.out.println("Avg latency (ms) : "
                + String.format("%.2f", getAverageLatencyMs()));
        System.out.println("Min latency (ms) : " + getMinLatencyMs());
        System.out.println("Max latency (ms) : " + getMaxLatencyMs());
    }
}