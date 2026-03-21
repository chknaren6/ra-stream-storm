package com.rastream.model;

import com.rastream.dag.Task;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ResourceScaler {

    // T_under = 0.60, T_over = 0.75
    private final double tUnder;
    private final double tOver;
    private final ResourceModel resourceModel;

    public ResourceScaler(ResourceModel resourceModel) {
        this.tUnder        = 0.60;
        this.tOver         = 0.75;
        this.resourceModel = resourceModel;
    }

    // Eq 21
    // True if R < T_under (wasteful) or R > T_over (overloaded)
    private boolean needsAdjustment(Subgraph s) {
        double R = resourceModel.computeSubgraphResourceDemand(
                s.getTasks());
        return (R < tUnder) || (R > tOver);
    }

    private boolean isUnderloaded(Subgraph s) {
        double R = resourceModel.computeSubgraphResourceDemand(
                s.getTasks());
        return R < tUnder;
    }

    private boolean isOverloaded(Subgraph s) {
        double R = resourceModel.computeSubgraphResourceDemand(
                s.getTasks());
        return R > tOver;
    }
    // Try to find a subgraph to merge with target
    // Merge is valid if combined resource demand <= T_over
    private Subgraph findMergeCandidate(Subgraph target,
                                        List<Subgraph> available) {
        double targetR = resourceModel
                .computeSubgraphResourceDemand(target.getTasks());

        // Binary search substitute: find best fit candidate
        // whose combined demand stays under T_over
        Subgraph best = null;
        double bestCombined = Double.MAX_VALUE;

        for (Subgraph candidate : available) {
            if (candidate.getId() == target.getId()) continue;
            double candidateR = resourceModel
                    .computeSubgraphResourceDemand(candidate.getTasks());
            double combined = targetR + candidateR;
            if (combined <= tOver && combined < bestCombined) {
                best = candidate;
                bestCombined = combined;
            }
        }
        return best;
    }

    // Merge two subgraphs into one
    private Subgraph mergeSubgraphs(Subgraph a, Subgraph b) {
        Subgraph merged = new Subgraph(a.getId());
        for (Task t : a.getTasks()) merged.addTask(t);
        for (Task t : b.getTasks()) merged.addTask(t);
        return merged;
    }

    // Find task with minimal impact on source subgraph weight
    // minimal impact = task involved in fewest internal edges
    private Task findMinImpactTask(Subgraph source,
                                   List<com.rastream.dag.Edge> allEdges) {
        Task minTask   = null;
        int  minEdges  = Integer.MAX_VALUE;

        for (Task t : source.getTasks()) {
            // Count how many internal edges this task participates in
            int edgeCount = 0;
            for (com.rastream.dag.Edge e : source.getInternalEdges()) {
                if (e.getSource().getId().equals(t.getId()) ||
                        e.getTarget().getId().equals(t.getId())) {
                    edgeCount++;
                }
            }
            if (edgeCount < minEdges) {
                minEdges = edgeCount;
                minTask  = t;
            }
        }
        return minTask;
    }

    // Find a subgraph that can absorb one more task
    private Subgraph findTaskAdjustCandidate(Subgraph target,
                                             List<Subgraph> available) {
        for (Subgraph candidate : available) {
            if (candidate.getId() == target.getId()) continue;
            double R = resourceModel.computeSubgraphResourceDemand(
                    candidate.getTasks());
            // Must have room below T_over
            if (R < tOver) return candidate;
        }
        return null;
    }

    // Algorithm 2 — full resource scaling
    // Input:  partition scheme X from Algorithm 1
    // Output: resource scaling scheme RS
    //         number of subgraphs in RS = minimum nodes needed
    public List<Subgraph> scale(PartitionScheme X,
                                List<com.rastream.dag.Edge> allEdges) {

        // Working list — sorted ascending by resource demand
        List<Subgraph> working = new ArrayList<>(
                X.getSubgraphs());
        working.sort(Comparator.comparingDouble(s ->
                resourceModel.computeSubgraphResourceDemand(
                        s.getTasks())));

        // Output list RS
        List<Subgraph> RS = new ArrayList<>();

        int i = 0;
        while (i < working.size()) {
            Subgraph current = working.get(i);
            double R = resourceModel.computeSubgraphResourceDemand(
                    current.getTasks());

            //  UNDERLOADED branch
            if (R < tUnder) {
                Subgraph mergeTarget =
                        findMergeCandidate(current, working);

                if (mergeTarget != null) {
                    //  merge and add to RS
                    Subgraph merged =
                            mergeSubgraphs(current, mergeTarget);
                    merged.recomputeInternalEdges(allEdges);
                    RS.add(merged);
                    working.remove(mergeTarget);
                    System.out.println("Merged subgraph "
                            + current.getId() + " with "
                            + mergeTarget.getId());
                } else {
                    // move tasks in until above T_under
                    while (isUnderloaded(current)
                            && current.getTaskCount() > 1) {
                        Subgraph donor =
                                findTaskAdjustCandidate(current, working);
                        if (donor == null) break;
                        Task taskToMove =
                                findMinImpactTask(donor, allEdges);
                        if (taskToMove == null) break;
                        donor.removeTask(taskToMove);
                        current.addTask(taskToMove);
                        current.recomputeInternalEdges(allEdges);
                        System.out.println("Moved task "
                                + taskToMove.getId()
                                + " into subgraph " + current.getId());
                    }
                    RS.add(current);
                }

                // OVERLOADED branch
            } else if (R > tOver) {
                while (isOverloaded(current)
                        && current.getTaskCount() > 1) {
                    Subgraph receiver =
                            findTaskAdjustCandidate(current, working);
                    if (receiver == null) break;
                    Task taskToMove =
                            findMinImpactTask(current, allEdges);
                    if (taskToMove == null) break;
                    current.removeTask(taskToMove);
                    receiver.addTask(taskToMove);
                    current.recomputeInternalEdges(allEdges);
                    System.out.println("Moved task "
                            + taskToMove.getId()
                            + " out of subgraph " + current.getId());
                }
                RS.add(current);

                // JUST RIGHT branch
            } else {
                RS.add(current);
            }
            i++;
        }

        System.out.println("Resource scaling done: "
                + RS.size() + " compute nodes needed");
        return RS;
    }

    public double getTUnder() { return tUnder; }
    public double getTOver()  { return tOver; }
}
