package com.rastream.partitioning;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;

import java.util.ArrayList;
import java.util.List;

public class Subgraph {

    // Unique id for this subgraph
    private final int id;

    // Tasks assigned to this subgraph — T(G_sub_i) in paper
    private final List<Task> tasks;

    // Edges whose BOTH endpoints are inside this subgraph
    // These are the cheap intra-node edges — W_int in paper
    private final List<Edge> internalEdges;

    public Subgraph(int id) {
        this.id = id;
        this.tasks = new ArrayList<>();
        this.internalEdges = new ArrayList<>();
    }
    public void addTask(Task task) {
        tasks.add(task);
    }

    public void removeTask(Task task) {
        tasks.removeIf(t -> t.getId().equals(task.getId()));
    }

    public boolean containsTask(Task task) {
        return tasks.stream()
                .anyMatch(t -> t.getId().equals(task.getId()));
    }

    // Recompute which edges are internal after tasks change
    // An edge is internal if BOTH source and target are in this subgraph
    public void recomputeInternalEdges(List<Edge> allEdges) {
        internalEdges.clear();
        for (Edge e : allEdges) {
            boolean sourceHere = containsTask(e.getSource());
            boolean targetHere = containsTask(e.getTarget());
            if (sourceHere && targetHere) {
                internalEdges.add(e);
            }
        }
    }

    // W_int(G_sub_i) — sum of internal edge weights
    // This is what we want to MAXIMIZE in Equation 9
    public double getInternalWeight() {
        double sum = 0.0;
        for (Edge e : internalEdges) {
            sum += e.getTupleTransmissionRate();
        }
        return sum;
    }

    public int getId()                    { return id; }
    public List<Task> getTasks()          { return tasks; }
    public List<Edge> getInternalEdges()  { return internalEdges; }
    public int getTaskCount()             { return tasks.size(); }

    @Override
    public String toString() {
        return "Subgraph" + id + tasks;
    }

}
