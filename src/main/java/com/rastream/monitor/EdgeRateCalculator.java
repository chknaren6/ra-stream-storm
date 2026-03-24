package com.rastream.monitor;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;
import com.rastream.dag.StreamApplication;

import java.util.HashMap;
import java.util.Map;

/**
 * Calculates Tr(v_i,k, v_j,l) — tuple transmission rates from Equation 2
 *
 * Based on per-task emission counts from the stat window.
 * Emission counts → transmission rates using topology connectivity.
 */
public class EdgeRateCalculator {

    private final StreamApplication app;

    // Per-sample tracking: emitCount before and after this window
    // Key = taskId, Value = previous emit count
    private final Map<String, Long> previousEmitCounts;

    // Current window's delta emissions per task
    // Key = taskId, Value = tuples emitted in this window
    private Map<String, Long> windowEmissions;

    public EdgeRateCalculator(StreamApplication app) {
        this.app = app;
        this.previousEmitCounts = new HashMap<>();
        this.windowEmissions = new HashMap<>();
    }

    /**
     * Calculate tuple transmission rate for a single edge
     * Called during the stat window sampling phase
     *
     * @param sourceTaskId  e.g. "v1_t0"
     * @param targetTaskId  e.g. "v2_t1"
     * @param emitCountSource  cumulative emit count from source
     * @param sampleIntervalSec  time elapsed in seconds (e.g., 1.0 for 1-second micro-samples)
     * @return transmission rate in tuples/second
     */
    public double calculateEdgeRate(String sourceTaskId, String targetTaskId,
                                    long emitCountSource, double sampleIntervalSec) {
        // Get prior emit count (default 0 if first sample)
        long previousCount = previousEmitCounts.getOrDefault(sourceTaskId, 0L);

        // Delta = tuples emitted by source in this sample period
        long delta = emitCountSource - previousCount;

        // Rate = delta / interval (tuples per second)
        if (sampleIntervalSec <= 0) return 0.0;
        double rate = (double) delta / sampleIntervalSec;

        // Clamp to non-negative (should always be true, but safety)
        return Math.max(rate, 0.0);
    }

    /**
     * Update the previous emit count after sampling
     * Call this after all edges have been sampled
     */
    public void updatePreviousEmitCounts(TaskMetricsTracker tracker) {
        for (Map.Entry<String, Long> entry : tracker.getEmitCounts().entrySet()) {
            String taskKey = entry.getKey();
            long count = entry.getValue();

            // Extract internal task ID from "component-taskId"
            String internalTaskId = extractInternalTaskId(taskKey);
            previousEmitCounts.put(internalTaskId, count);
        }
    }

    /**
     * Convert from Storm's "component-taskId" format to internal "v1_t0" format
     * This is a simple example — you may need to customize for your topology
     */
    private String extractInternalTaskId(String stormTaskKey) {
        // Format: "componentName-taskId"
        String[] parts = stormTaskKey.split("-");
        if (parts.length >= 2) {
            String componentName = parts[0];
            int taskId = Integer.parseInt(parts[1]);

            // Map component to vertex ID (customize per topology)
            int vertexId = mapComponentToVertex(componentName);
            return "v" + vertexId + "_t" + taskId;
        }
        return stormTaskKey;
    }

    /**
     * Maps Storm component names to our vertex IDs
     * Customize this based on your topology structure
     *
     * Example: WordCount topology
     *   reader → vertex 1
     *   split → vertex 2
     *   count → vertex 3
     *   output → vertex 4
     */
    private int mapComponentToVertex(String componentName) {
        switch (componentName.toLowerCase()) {
            case "reader": case "spout": return 1;
            case "split": case "validator": return 2;
            case "count": case "aggregator": return 3;
            case "output": case "taxi-output": return 4;
            default: return 1; // fallback
        }
    }

    /**
     * Get the previous emit count for a source task
     */
    public long getPreviousEmitCount(String taskId) {
        return previousEmitCounts.getOrDefault(taskId, 0L);
    }

    /**
     * Reset for next window
     */
    public void reset() {
        windowEmissions.clear();
    }
}