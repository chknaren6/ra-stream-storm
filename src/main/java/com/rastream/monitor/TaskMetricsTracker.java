package com.rastream.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskMetricsTracker {

    private final ConcurrentHashMap<String, Long> emitCounts;

    // component -> vertexId (e.g., taxi-reader -> 1)
    private final ConcurrentHashMap<String, Integer> componentVertexMap;

    // stormKey(component-taskId) -> internalId(vX_tY)
    private final ConcurrentHashMap<String, String> stormTaskToInternal;

    public TaskMetricsTracker() {
        this.emitCounts = new ConcurrentHashMap<>();
        this.componentVertexMap = new ConcurrentHashMap<>();
        this.stormTaskToInternal = new ConcurrentHashMap<>();
    }

    public void registerComponentVertex(String componentName, int vertexId) {
        componentVertexMap.put(componentName, vertexId);
    }

    public void registerStormTask(String componentName, int stormTaskId) {
        Integer vertexId = componentVertexMap.get(componentName);
        if (vertexId == null) return;
        // deterministic mapping: v{vertex}_t{stormTaskId}
        String internal = "v" + vertexId + "_t" + stormTaskId;
        stormTaskToInternal.put(componentName + "-" + stormTaskId, internal);
    }

    public void recordEmitCount(String stormComponentName, int taskId, long emitCount) {
        String key = stormComponentName + "-" + taskId;
        emitCounts.put(key, emitCount);
        // lazy register if not already known
        if (!stormTaskToInternal.containsKey(key)) {
            registerStormTask(stormComponentName, taskId);
        }
    }

    public long getEmitCount(String stormComponentName, int taskId) {
        String key = stormComponentName + "-" + taskId;
        return emitCounts.getOrDefault(key, 0L);
    }

    public String getInternalTaskId(String stormComponentName, int taskId) {
        String key = stormComponentName + "-" + taskId;
        return stormTaskToInternal.get(key);
    }

    public String getInternalTaskIdByStormKey(String stormTaskKey) {
        return stormTaskToInternal.get(stormTaskKey);
    }

    public Map<String, Long> getEmitCounts() {
        return new ConcurrentHashMap<>(emitCounts);
    }

    public Map<String, String> getStormTaskToInternalMap() {
        return new ConcurrentHashMap<>(stormTaskToInternal);
    }

    public void reset() {
        emitCounts.clear();
    }
}