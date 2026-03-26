package com.rastream.faulttolerance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RecoveryManager {
    private final Map<String, String> taskToNode = new ConcurrentHashMap<>();
    private final Map<String, Integer> queueSizeByNode = new ConcurrentHashMap<>();

    public void registerTask(String taskId, String nodeId) { taskToNode.put(taskId, nodeId); }
    public void updateQueueSize(String nodeId, int size) { queueSizeByNode.put(nodeId, size); }

    public Map<String, String> recoverNodeFailure(String failedNode, List<String> aliveNodes) {
        Map<String, String> reassignments = new LinkedHashMap<>();
        for (var e : taskToNode.entrySet()) {
            if (failedNode.equals(e.getValue())) {
                String target = aliveNodes.stream()
                        .min(Comparator.comparingInt(n -> queueSizeByNode.getOrDefault(n, Integer.MAX_VALUE / 4)))
                        .orElseThrow(() -> new IllegalStateException("No alive nodes"));
                taskToNode.put(e.getKey(), target);
                reassignments.put(e.getKey(), target);
            }
        }
        return reassignments;
    }
}