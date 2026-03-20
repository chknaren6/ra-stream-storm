package com.rastream.partitioning;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;

import java.util.ArrayList;
import java.util.List;

public class PartitionScheme {
    // The k subgraphs — one per compute node
    private final List<Subgraph> subgraphs;

    // All edges in the application — needed to classify
    // each edge as internal or cut after every move
    private final List<Edge> allEdges;

    public PartitionScheme(List<Edge> allEdges) {
        this.subgraphs = new ArrayList<>();
        this.allEdges = allEdges;
    }

    public void addSubgraph(Subgraph s) {
        subgraphs.add(s);
    }

    // Deep copy — needed by simulated annealing to
    // try a neighbor solution without destroying current one
    public PartitionScheme deepCopy() {
        PartitionScheme copy = new PartitionScheme(allEdges);
        for (Subgraph s : subgraphs) {
            Subgraph newSub = new Subgraph(s.getId());
            for (Task t : s.getTasks()) {
                newSub.addTask(t);
            }
            newSub.recomputeInternalEdges(allEdges);
            copy.addSubgraph(newSub);
        }
        return copy;
    }

    // E_cut(G) — edges whose endpoints are in DIFFERENT subgraphs
    // These are the expensive cross-node edges we want to MINIMIZE
    public List<Edge> getCutEdges() {
        List<Edge> cutEdges = new ArrayList<>();
        for (Edge e : allEdges) {
            Subgraph srcSub = findSubgraphForTask(e.getSource());
            Subgraph tgtSub = findSubgraphForTask(e.getTarget());
            if (srcSub != null && tgtSub != null
                    && srcSub.getId() != tgtSub.getId()) {
                cutEdges.add(e);
            }
        }
        return cutEdges;
    }

    // W_cut(G) — total weight of cut edges (Equation 9)
    public double getCutEdgeWeight() {
        double sum = 0.0;
        for (Edge e : getCutEdges()) {
            sum += e.getTupleTransmissionRate();
        }
        return sum;
    }

    // Find which subgraph a task currently lives in
    public Subgraph findSubgraphForTask(Task task) {
        for (Subgraph s : subgraphs) {
            if (s.containsTask(task)) return s;
        }
        return null;
    }

    // Refresh ALL subgraph internal edges after any move
    public void recomputeAllInternalEdges() {
        for (Subgraph s : subgraphs) {
            s.recomputeInternalEdges(allEdges);
        }
    }

    public List<Subgraph> getSubgraphs() { return subgraphs; }
    public int getSubgraphCount()        { return subgraphs.size(); }

}
