package com.rastream.monitor;

import com.rastream.dag.StreamApplication;

import java.util.HashMap;
import java.util.Map;

public class EdgeRateCalculator {

    private final StreamApplication app;
    private final Map<String, Long> previousEmitCounts;

    public EdgeRateCalculator(StreamApplication app) {
        this.app = app;
        this.previousEmitCounts = new HashMap<>();
    }

    public double calculateEdgeRate(String sourceTaskId, String targetTaskId,
                                    long emitCountSource, double sampleIntervalSec) {
        long previousCount = previousEmitCounts.getOrDefault(sourceTaskId, 0L);
        long delta = emitCountSource - previousCount;
        if (sampleIntervalSec <= 0) return 0.0;
        return Math.max(0.0, (double) delta / sampleIntervalSec);
    }

    public void updatePreviousEmitCounts(TaskMetricsTracker tracker) {
        for (Map.Entry<String, Long> entry : tracker.getEmitCounts().entrySet()) {
            String stormKey = entry.getKey();
            long count = entry.getValue();
            String internalTaskId = tracker.getInternalTaskIdByStormKey(stormKey);
            if (internalTaskId != null) {
                previousEmitCounts.put(internalTaskId, count);
            }
        }
    }

    public long getPreviousEmitCount(String taskId) {
        return previousEmitCounts.getOrDefault(taskId, 0L);
    }

    public void reset() {
        // no-op: previousEmitCounts intentionally preserved across windows
    }
}