package com.rastream.optimizations;

public class SmartBatcher {
    private final int minBatch;
    private final int maxBatch;

    public SmartBatcher(int minBatch, int maxBatch) {
        this.minBatch = minBatch;
        this.maxBatch = maxBatch;
    }

    public int batchSize(double rate) {
        if (rate <= 1000) return minBatch;
        if (rate >= 7000) return maxBatch;
        return (int) (minBatch + (rate - 1000.0) * (maxBatch - minBatch) / 6000.0);
    }
}