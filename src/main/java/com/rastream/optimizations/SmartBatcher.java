package com.rastream.optimizations;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive Batching Optimizer with Rate-Aware Tuning
 *
 * ALGORITHM: Rate-Driven Adaptive Batching
 * - Computes optimal batch size based on data rate
 * - Minimizes CPU overhead while maintaining low latency
 * - Handles burst traffic intelligently
 */
public class SmartBatcher {

    private final int minBatch;
    private final int maxBatch;
    private final int targetBatch;

    // Metrics
    private final AtomicLong batchesProcessed = new AtomicLong(0);
    private final AtomicLong tuplesProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchProcessingTimeNs = new AtomicLong(0);
    private final AtomicInteger currentBatchSize = new AtomicInteger(0);

    public SmartBatcher (int minBatch, int maxBatch, int targetBatch) {
        this.minBatch = minBatch;
        this.maxBatch = maxBatch;
        this.targetBatch = targetBatch;
        this.currentBatchSize.set(targetBatch);
    }

    /**
     * MAIN ALGORITHM: Rate-Driven Adaptive Batching
     *
     * INPUT: dataRate (tuples/sec), queueDepth (current tuples buffered)
     * OUTPUT: batchSize (tuples to process per batch)
     *
     * ALGORITHM STEPS:
     * 1. Compute batch size as linear interpolation of data rate
     * 2. Clamp to [minBatch, maxBatch] range
     * 3. Adjust based on queue depth (burst traffic handling)
     * 4. Update current batch size metric
     */
    public int computeBatchSize(double dataRate, int queueDepth) {
        // Step 1: Base batch size from data rate
        // At 1000 tps → minBatch
        // At 7000 tps → maxBatch
        // Linear interpolation between
        int baseBatchSize;
        if (dataRate <= 1000) {
            baseBatchSize = minBatch;
        } else if (dataRate >= 7000) {
            baseBatchSize = maxBatch;
        } else {
            // Linear interpolation
            double ratio = (dataRate - 1000.0) / 6000.0;
            baseBatchSize = (int) (minBatch + ratio * (maxBatch - minBatch));
        }

        // Step 2: Clamp to valid range
        baseBatchSize = Math.max(minBatch, Math.min(maxBatch, baseBatchSize));

        // Step 3: Adjust based on queue depth (burst handling)
        // If queue is growing rapidly, use larger batches to catch up
        // If queue is shrinking, use smaller batches for responsiveness
        int adjustedBatchSize = baseBatchSize;

        if (queueDepth > 1000) {
            // Heavy load: increase batch size by 20%
            adjustedBatchSize = (int) (baseBatchSize * 1.2);
        } else if (queueDepth > 500) {
            // Medium load: increase by 10%
            adjustedBatchSize = (int) (baseBatchSize * 1.1);
        } else if (queueDepth < 50) {
            // Light load: decrease by 10% for lower latency
            adjustedBatchSize = (int) (baseBatchSize * 0.9);
        }

        adjustedBatchSize = Math.max(minBatch, Math.min(maxBatch, adjustedBatchSize));

        currentBatchSize.set(adjustedBatchSize);
        return adjustedBatchSize;
    }

    /**
     * Record batch processing metrics
     */
    public void recordBatch(int tupleCount, long processingTimeNs) {
        batchesProcessed.incrementAndGet();
        tuplesProcessed.addAndGet(tupleCount);
        totalBatchProcessingTimeNs.addAndGet(processingTimeNs);
    }

    /**
     * Get batching metrics
     */
    public long getBatchesProcessed() {
        return batchesProcessed.get();
    }

    public long getTuplesProcessed() {
        return tuplesProcessed.get();
    }

    public double getAverageBatchSize() {
        long batches = batchesProcessed.get();
        if (batches == 0) return minBatch;
        return (double) tuplesProcessed.get() / batches;
    }

    public double getAverageBatchTimeMs() {
        long batches = batchesProcessed.get();
        if (batches == 0) return 0;
        return totalBatchProcessingTimeNs.get() / (batches * 1_000_000.0);
    }

    public int getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    /**
     * Get summary snapshot
     */
    public BatcherSnapshot getSnapshot() {
        return new BatcherSnapshot(
                currentBatchSize.get(),
                batchesProcessed.get(),
                tuplesProcessed.get(),
                getAverageBatchSize(),
                getAverageBatchTimeMs()
        );
    }

    public static class BatcherSnapshot {
        public final int currentBatchSize;
        public final long batchesProcessed;
        public final long tuplesProcessed;
        public final double avgBatchSize;
        public final double avgBatchTimeMs;

        public BatcherSnapshot(int batchSize, long batches, long tuples, double avgSize, double avgTime) {
            this.currentBatchSize = batchSize;
            this.batchesProcessed = batches;
            this.tuplesProcessed = tuples;
            this.avgBatchSize = avgSize;
            this.avgBatchTimeMs = avgTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "[Batching] currentBatch=%d, processed=%d batches (%d tuples), " +
                            "avgSize=%.1f, avgTimeMs=%.3f",
                    currentBatchSize, batchesProcessed, tuplesProcessed,
                    avgBatchSize, avgBatchTimeMs
            );
        }
    }
}