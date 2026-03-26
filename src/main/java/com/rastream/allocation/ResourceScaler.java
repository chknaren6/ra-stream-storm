package com.rastream.allocation;

import com.rastream.dag.Task;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ResourceScaler {

    // Paper thresholds (Table 4)
    private final double tUnder;
    private final double tOver;
    private final ResourceModel resourceModel;

    public ResourceScaler(ResourceModel resourceModel) {
        this.tUnder        = 0.60;
        this.tOver         = 0.75;
        this.resourceModel = resourceModel;
    }

    private boolean isUnderloaded(Subgraph s) {
        double r = resourceModel.computeSubgraphResourceDemand(s.getTasks());
        return r < tUnder;
    }

    private boolean isOverloaded(Subgraph s) {
        double r = resourceModel.computeSubgraphResourceDemand(s.getTasks());
        return r > tOver;
    }

    /**
     * Merge candidate for underloaded subgraph:
     * choose candidate with max utilization that still keeps merged <= tOver.
     */
    private Subgraph findMergeCandidate(Subgraph target, List<Subgraph> available) {
        double targetR = resourceModel.computeSubgraphResourceDemand(target.getTasks());

        Subgraph best = null;
        double bestCombined = -1.0;

        for (Subgraph candidate : available) {
            if (candidate.getId() == target.getId()) continue;
            double candidateR = resourceModel.computeSubgraphResourceDemand(candidate.getTasks());
            double combined = targetR + candidateR;
            if (combined <= tOver && combined > bestCombined) {
                best = candidate;
                bestCombined = combined;
            }
        }
        return best;
    }

    private Subgraph mergeSubgraphs(Subgraph a, Subgraph b) {
        Subgraph merged = new Subgraph(a.getId());
        for (Task t : a.getTasks()) merged.addTask(t);
        for (Task t : b.getTasks()) merged.addTask(t);
        return merged;
    }

    /**
     * Minimal impact = fewest internal edges touched.
     */
    private Task findMinImpactTask(Subgraph source) {
        Task minTask = null;
        int minEdges = Integer.MAX_VALUE;

        for (Task t : source.getTasks()) {
            int edgeCount = 0;
            for (com.rastream.dag.Edge e : source.getInternalEdges()) {
                if (e.getSource().getId().equals(t.getId()) ||
                        e.getTarget().getId().equals(t.getId())) {
                    edgeCount++;
                }
            }
            if (edgeCount < minEdges) {
                minEdges = edgeCount;
                minTask = t;
            }
        }
        return minTask;
    }

    /**
     * For underloaded target, pick a donor (not target, >1 task).
     * Prefer the donor with highest current resource demand to converge quickly.
     */
    private Subgraph findDonorCandidateForUnderloaded(Subgraph target, List<Subgraph> available) {
        Subgraph best = null;
        double bestR = -1.0;
        for (Subgraph donor : available) {
            if (donor.getId() == target.getId()) continue;
            if (donor.getTaskCount() <= 1) continue;
            double r = resourceModel.computeSubgraphResourceDemand(donor.getTasks());
            if (r > bestR) {
                best = donor;
                bestR = r;
            }
        }
        return best;
    }

    /**
     * For overloaded source, find receiver where receiverR + taskR <= tOver.
     * Prefer receiver with highest current utilization that still fits.
     */
    private Subgraph findReceiverCandidateForOverloaded(Subgraph source,
                                                        Task taskToMove,
                                                        List<Subgraph> available) {
        double taskR = resourceModel.computeTaskResourceUtilization(taskToMove);

        Subgraph best = null;
        double bestReceiverR = -1.0;

        for (Subgraph receiver : available) {
            if (receiver.getId() == source.getId()) continue;
            double receiverR = resourceModel.computeSubgraphResourceDemand(receiver.getTasks());
            if (receiverR + taskR <= tOver && receiverR > bestReceiverR) {
                best = receiver;
                bestReceiverR = receiverR;
            }
        }
        return best;
    }

    /**
     * Algorithm 2 — resource scaling.
     */
    public List<Subgraph> scale(PartitionScheme x,
                                List<com.rastream.dag.Edge> allEdges) {

        List<Subgraph> working = new ArrayList<>(x.getSubgraphs());
        working.sort(Comparator.comparingDouble(s ->
                resourceModel.computeSubgraphResourceDemand(s.getTasks())));

        List<Subgraph> rs = new ArrayList<>();

        int i = 0;
        while (i < working.size()) {
            Subgraph current = working.get(i);
            double r = resourceModel.computeSubgraphResourceDemand(current.getTasks());

            // UNDERLOADED
            if (r < tUnder) {
                Subgraph mergeTarget = findMergeCandidate(current, working);
                if (mergeTarget != null) {
                    Subgraph merged = mergeSubgraphs(current, mergeTarget);
                    merged.recomputeInternalEdges(allEdges);

                    rs.add(merged);
                    working.remove(mergeTarget);

                    System.out.println("[ResourceScaler] Merged subgraph "
                            + current.getId() + " with " + mergeTarget.getId());
                } else {
                    // Pull tasks from donors until >= tUnder or no valid move
                    while (isUnderloaded(current) && current.getTaskCount() >= 1) {
                        Subgraph donor = findDonorCandidateForUnderloaded(current, working);
                        if (donor == null) break;

                        Task taskToMove = findMinImpactTask(donor);
                        if (taskToMove == null) break;

                        donor.removeTask(taskToMove);
                        current.addTask(taskToMove);

                        donor.recomputeInternalEdges(allEdges);
                        current.recomputeInternalEdges(allEdges);

                        System.out.println("[ResourceScaler] Moved task "
                                + taskToMove.getId()
                                + " donor=" + donor.getId()
                                + " -> current=" + current.getId());

                        if (donor.getTaskCount() == 0) break;
                    }
                    rs.add(current);
                }

                // OVERLOADED
            } else if (r > tOver) {
                while (isOverloaded(current) && current.getTaskCount() > 1) {
                    Task taskToMove = findMinImpactTask(current);
                    if (taskToMove == null) break;

                    Subgraph receiver = findReceiverCandidateForOverloaded(current, taskToMove, working);
                    if (receiver == null) break;

                    current.removeTask(taskToMove);
                    receiver.addTask(taskToMove);

                    current.recomputeInternalEdges(allEdges);
                    receiver.recomputeInternalEdges(allEdges);

                    System.out.println("[ResourceScaler] Moved task "
                            + taskToMove.getId()
                            + " current=" + current.getId()
                            + " -> receiver=" + receiver.getId());
                }
                rs.add(current);

                // JUST RIGHT
            } else {
                rs.add(current);
            }

            i++;
        }

        System.out.println("[ResourceScaler] Done. Compute nodes needed = " + rs.size());
        return rs;
    }

    public double getTUnder() { return tUnder; }
    public double getTOver()  { return tOver; }
}