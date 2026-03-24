package com.rastream.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

/**
 * Tracks per-task metrics from Storm's IMetricsConsumer.
 * Called by DataMonitor.handleDataPoints() for each task during the stat window.
 *
 * Maps taskId (e.g., "v1_t0") → emission count
 */
public class TaskMetricsTracker {

    /**
     * Key = taskId from Storm (componentName + taskId)
     *       e.g. "reader-1", "split-2", etc.
     * Value = cumulative tuple count emitted
     */
    private final ConcurrentHashMap<String, Long> emitCounts;

    /**
     * Maps Storm's componentName (e.g. "reader", "split")
     * to our internal Task representation (v1_t0)
     * This is populated during topology construction.
     */
    private final ConcurrentHashMap<String, String> componentTaskMap;

    public TaskMetricsTracker() {
        this.emitCounts = new ConcurrentHashMap<>();
        this.componentTaskMap = new ConcurrentHashMap<>();
    }

    /**
     * Register a mapping from Storm component to internal task ID
     * Call this during topology builder setup
     *
     * @param stormComponentName e.g. "reader" (from topologyBuilder.setSpout("reader", ...))
     * @param internalTaskId     e.g. "v1_t0" (from our Task class)
     */
    public void registerComponentTask(String stormComponentName, String internalTaskId) {
        componentTaskMap.put(stormComponentName, internalTaskId);
    }

    /**
     * Called by DataMonitor.handleDataPoints() for each task metric
     * Extracts the __emit-count and stores it
     */
    public void recordEmitCount(String stormComponentName, int taskId, long emitCount) {
        String key = stormComponentName + "-" + taskId;
        emitCounts.put(key, emitCount);
    }

    /**
     * Get the emit count for a specific task
     * Returns 0 if never seen before
     */
    public long getEmitCount(String stormComponentName, int taskId) {
        String key = stormComponentName + "-" + taskId;
        return emitCounts.getOrDefault(key, 0L);
    }

    /**
     * Map from Storm's component+taskId to our internal Task notation
     * e.g. ("reader", 0) → "v1_t0"
     */
    public String getInternalTaskId(String stormComponentName, int taskId) {
        // For now, simple mapping based on component name
        // In a full implementation, this could query a topology registry
        String component = componentTaskMap.get(stormComponentName);
        if (component == null) {
            // Fallback: assume component maps to component-taskId
            return stormComponentName + "_" + taskId;
        }
        return component + "_" + taskId;
    }

    /**
     * Get all recorded emit counts
     */
    public Map<String, Long> getEmitCounts() {
        return new ConcurrentHashMap<>(emitCounts);
    }

    /**
     * Reset all counters for the next stat window
     */
    public void reset() {
        emitCounts.clear();
    }
}