package com.rastream.scheduler;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;
import com.rastream.model.ComputeNode;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.Subgraph;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class FineGrainedScheduler {

    // Eq 15 weights gamma=0.50, delta=0.35
    private final double gamma;
    private final double delta;
    private final ResourceModel resourceModel;

    public FineGrainedScheduler(ResourceModel resourceModel) {
        this.gamma         = 0.50;
        this.delta         = 0.35;
        this.resourceModel = resourceModel;
    }
    // coarse-grained scheduling
    // Find the compute node with highest fitness for this subgraph
    // Fitness = Eq 14: f_fit = f_de * S_R
    private ComputeNode selectBestNode(Subgraph s,
                                       List<ComputeNode> nodes) {
        ComputeNode best        = null;
        double      bestFitness = Double.MIN_VALUE;

        for (ComputeNode cn : nodes) {
            if (!cn.isActive()) continue;

            // Eq 14: fitness factor
            double fitness = cn.computeFitness(
                    s, resourceModel, gamma, delta);

            if (fitness > bestFitness) {
                bestFitness = fitness;
                best        = cn;
            }
        }
        return best;
    }
    //  fine-grained scheduling
    // Group tasks by communication intensity
    // Highest Tr(v_i,k, v_j,l) pairs go into same process
    private List<List<Task>> buildProcessGroups(Subgraph s,
                                                List<Edge> allEdges) {
        List<List<Task>> groups    = new ArrayList<>();
        List<Task>       remaining = new ArrayList<>(s.getTasks());

        // Find all edges internal to this subgraph
        // sorted by Tr descending — heaviest pairs first
        List<Edge> internalEdges = new ArrayList<>();
        for (Edge e : allEdges) {
            if (s.containsTask(e.getSource()) &&
                    s.containsTask(e.getTarget())) {
                internalEdges.add(e);
            }
        }
        internalEdges.sort(
                Comparator.comparingDouble(
                        Edge::getTupleTransmissionRate).reversed());

        // Place highest-Tr pair in same process, remove from pool
        for (Edge e : internalEdges) {
            Task src = e.getSource();
            Task tgt = e.getTarget();

            boolean srcAvailable = remaining.stream()
                    .anyMatch(t -> t.getId().equals(src.getId()));
            boolean tgtAvailable = remaining.stream()
                    .anyMatch(t -> t.getId().equals(tgt.getId()));

            if (srcAvailable && tgtAvailable) {
                List<Task> group = new ArrayList<>();
                group.add(src);
                group.add(tgt);
                groups.add(group);

                // Remove from remaining pool
                remaining.removeIf(t ->
                        t.getId().equals(src.getId()) ||
                                t.getId().equals(tgt.getId()));
            }
        }

        // Any tasks not paired get their own process group
        for (Task t : remaining) {
            List<Task> solo = new ArrayList<>();
            solo.add(t);
            groups.add(solo);
        }

        return groups;
    }
    // Algorithm 3 — full fine-grained scheduling
    // Input:  RS (resource scaling result), compute nodes, edges
    // Output: SchedulingScheme SS
    public SchedulingScheme schedule(List<Subgraph> RS,
                                     List<ComputeNode> nodes,
                                     List<Edge> allEdges) {
        SchedulingScheme SS = new SchedulingScheme();

        for (Subgraph s : RS) {

            // --- Coarse-grained: pick best node  ---
            ComputeNode bestNode = selectBestNode(s, nodes);

            if (bestNode == null) {
                System.out.println("WARNING: No node available "
                        + "for subgraph " + s.getId());
                continue;
            }

            // Assign subgraph to node and deduct resources
            SS.assignSubgraphToNode(s, bestNode);
            bestNode.assignSubgraph(s, resourceModel);

            System.out.println("Subgraph " + s.getId()
                    + " → " + bestNode.getId()
                    + " (fitness=" + String.format("%.4f",
                    bestNode.computeFitness(
                            s, resourceModel, gamma, delta))
                    + ")");

            // --- Fine-grained: place pairs in same process  ---
            List<List<Task>> groups =
                    buildProcessGroups(s, allEdges);

            for (List<Task> group : groups) {
                SS.addProcessGroup(group);
            }
        }

        return SS;
    }
}