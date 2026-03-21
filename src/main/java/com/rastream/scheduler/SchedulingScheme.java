package com.rastream.scheduler;

import com.rastream.dag.Task;
import com.rastream.model.ComputeNode;
import com.rastream.partitioning.Subgraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulingScheme {

    // Maps each subgraph to the node it was assigned to
    // Key = subgraph id, Value = compute node
    private final Map<Integer, ComputeNode> subgraphToNode;

    // Fine-grained: which tasks share the same process
    // Each inner list = one process group (same thread bucket)
    private final List<List<Task>> processGroups;

    public SchedulingScheme() {
        this.subgraphToNode = new HashMap<>();
        this.processGroups  = new ArrayList<>();
    }
    // Record that subgraph s was placed on node cn
    // Called during coarse-grained scheduling
    public void assignSubgraphToNode(Subgraph s, ComputeNode cn) {
        subgraphToNode.put(s.getId(), cn);
    }

    // Record a pair of tasks that share the same process
    // Called during fine-grained scheduling
    public void addProcessGroup(List<Task> group) {
        processGroups.add(group);
    }

    // Find which node a specific task ended up on
    // Walks through subgraph assignments to find it
    public ComputeNode getNodeForTask(Task task,
                                      List<Subgraph> subgraphs) {
        for (Subgraph s : subgraphs) {
            if (s.containsTask(task)) {
                return subgraphToNode.get(s.getId());
            }
        }
        return null;
    }

    public Map<Integer, ComputeNode> getSubgraphToNode() {
        return subgraphToNode;
    }

    public List<List<Task>> getProcessGroups() {
        return processGroups;
    }

    public void printSummary(List<Subgraph> subgraphs) {
        System.out.println("\n--- Scheduling Scheme Summary ---");
        for (Subgraph s : subgraphs) {
            ComputeNode cn = subgraphToNode.get(s.getId());
            String nodeName = (cn != null) ? cn.getId() : "UNASSIGNED";
            System.out.println("  Subgraph " + s.getId()
                    + " → " + nodeName
                    + " | tasks: " + s.getTasks());
        }
        System.out.println("  Process groups (same thread):");
        for (List<Task> group : processGroups) {
            System.out.println("    " + group);
        }
    }
}