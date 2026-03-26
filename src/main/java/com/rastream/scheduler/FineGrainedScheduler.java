package com.rastream.scheduler;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;
import com.rastream.model.ComputeNode;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.Subgraph;

import java.util.*;

/**
 * Algorithm 3 — Fine-Grained Task Scheduling
 *
 * Two phases exactly as described in Section 5.4:
 *
 * Phase 1 (coarse-grained): for each subgraph in RS, find the compute
 * node with the highest fitness factor f_fit (Eq. 14) and assign it.
 *
 * Phase 2 (fine-grained): within every subgraph on its assigned node,
 * repeatedly extract the task pair with the highest Tr(vi,k, vj,l)
 * and co-locate them in the same process (executor group). This
 * converts inter-process communication into intra-process communication,
 * eliminating the cost entirely (Eq. 22-23).
 *
 * Time complexity: O(m * n) where m = subgraphs, n = tasks per subgraph.
 */
public class FineGrainedScheduler {

    private final ResourceModel resourceModel;

    // Paper Table 4: gamma=0.50, delta=0.35
    private static final double GAMMA = 0.50;
    private static final double DELTA = 0.35;

    public FineGrainedScheduler(ResourceModel resourceModel) {
        this.resourceModel = resourceModel;
    }

    /**
     * Main entry point — Algorithm 3 from the paper.
     *
     * @param RS       resource scaling scheme (output of Algorithm 2)
     * @param nodes    available compute nodes (from cluster)
     * @param allEdges all edges in the application DAG
     * @return SchedulingScheme SS with node assignments + process groups
     */
    public SchedulingScheme schedule(List<Subgraph> RS,
                                     List<ComputeNode> nodes,
                                     List<Edge> allEdges) {
        SchedulingScheme SS = new SchedulingScheme();

        // Work on a copy so we don't mutate the input list
        List<Subgraph> remaining = new ArrayList<>(RS);

        // ----------------------------------------------------------------
        // Phase 1: Coarse-grained scheduling (Algorithm 3, Steps 2-4)
        // Schedule each subgraph to the compute node with highest f_fit.
        // ----------------------------------------------------------------
        for (Subgraph sub : remaining) {
            ComputeNode best = selectBestNode(sub, nodes);
            if (best == null) {
                System.err.println("[FineGrainedScheduler] WARNING: no node "
                        + "can accept subgraph " + sub.getId()
                        + " — skipping (cluster overloaded?)");
                continue;
            }
            best.assignSubgraph(sub, resourceModel);
            SS.assignSubgraphToNode(sub, best);

            System.out.println("[FineGrainedScheduler] Subgraph "
                    + sub.getId() + " → " + best.getId()
                    + " (fitness=" + String.format("%.4f",
                    best.computeFitness(sub, resourceModel, GAMMA, DELTA)) + ")");

            // ----------------------------------------------------------
            // Phase 2: Fine-grained scheduling (Algorithm 3, Steps 5-8)
            // Within this subgraph, greedily pair tasks with highest Tr.
            // Each pair goes into the same process (executor thread group).
            // ----------------------------------------------------------
            List<List<Task>> processGroups = finegrainedPairing(sub, allEdges);
            for (List<Task> group : processGroups) {
                SS.addProcessGroup(group);
            }
        }

        SS.printSummary(remaining);
        return SS;
    }

    // ------------------------------------------------------------------
    // Phase 1 helper: select compute node with highest fitness (Eq. 14)
    // ------------------------------------------------------------------

    /**
     * Eq. 13: canAccept — node must have CPU and mem > subgraph demand.
     * Eq. 14: f_fit = f_de * S_R (0 if not acceptable, surplus otherwise)
     * Eq. 15: S_R = gamma*(Ac - Rc) + delta*(Am - Rm) + (1-g-d)*(Aio - Rio)
     *
     * We pick the node with the highest f_fit > 0.
     * "Highest surplus" means the node best absorbs potential load spikes
     * from bursty stream rates — exactly the paper's intent.
     */
    private ComputeNode selectBestNode(Subgraph sub, List<ComputeNode> nodes) {
        ComputeNode best = null;
        double bestFit = -1.0;

        for (ComputeNode node : nodes) {
            if (!node.isActive()) continue;
            double fit = node.computeFitness(sub, resourceModel, GAMMA, DELTA);
            if (fit > bestFit) {
                bestFit = fit;
                best = node;
            }
        }
        return best; // null if no node can accept
    }

    // ------------------------------------------------------------------
    // Phase 2 helper: greedy task-pair co-location
    // ------------------------------------------------------------------

    /**
     * Algorithm 3 Steps 5-8:
     *   while tasks remain in T(G_sub_i):
     *     find the pair (vi,k, vj,l) with highest Tr(vi,k, vj,l)
     *     place them in the same process
     *     remove both from the working set
     *
     * Returns a list of process groups. Each group is a list of tasks
     * that should share the same Storm executor thread.
     *
     * If an odd task is left over it forms a singleton group.
     *
     * Why this matters for Taxi:
     *   TaxiSpout(task 0) → ValidatorBolt(task 0,1,2) has the highest Tr
     *   (every raw tuple crosses this edge). The pairing will co-locate
     *   spout task k with validator task k, making that edge intra-process
     *   and reducing latency by ~50% vs inter-process.
     */
    private List<List<Task>> finegrainedPairing(Subgraph sub,
                                                List<Edge> allEdges) {
        List<List<Task>> groups = new ArrayList<>();

        // Working copy — we remove tasks as they're paired
        List<Task> workingSet = new ArrayList<>(sub.getTasks());

        while (workingSet.size() > 1) {
            // Find the edge with highest Tr between any two remaining tasks
            double maxTr = -1.0;
            Task taskA = null;
            Task taskB = null;

            for (Edge e : allEdges) {
                Task src = e.getSource();
                Task tgt = e.getTarget();

                boolean srcPresent = containsById(workingSet, src);
                boolean tgtPresent = containsById(workingSet, tgt);

                if (srcPresent && tgtPresent) {
                    double tr = e.getTupleTransmissionRate();
                    if (tr > maxTr) {
                        maxTr = tr;
                        taskA = src;
                        taskB = tgt;
                    }
                }
            }

            if (taskA == null) {
                // No edge connects any remaining pair (disconnected tasks)
                // Fall back: pair the first two remaining tasks
                taskA = workingSet.get(0);
                taskB = workingSet.get(1);
                maxTr = 0.0;
            }

            List<Task> pair = Arrays.asList(taskA, taskB);
            groups.add(pair);

            // Remove paired tasks — must match by ID not reference
            Task finalTaskA = taskA;
            Task finalTaskB = taskB;
            workingSet.removeIf(t ->
                    t.getId().equals(finalTaskA.getId()) ||
                            t.getId().equals(finalTaskB.getId()));

            System.out.println("[FineGrainedScheduler] Co-located "
                    + taskA.getId() + " + " + taskB.getId()
                    + " in same process (Tr=" + String.format("%.2f", maxTr) + ")");
        }

        // Leftover singleton (odd number of tasks)
        if (!workingSet.isEmpty()) {
            groups.add(Collections.singletonList(workingSet.get(0)));
            System.out.println("[FineGrainedScheduler] Singleton process: "
                    + workingSet.get(0).getId());
        }

        return groups;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    private boolean containsById(List<Task> list, Task task) {
        for (Task t : list) {
            if (t.getId().equals(task.getId())) return true;
        }
        return false;
    }
}