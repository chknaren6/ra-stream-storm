package com.rastream.model;

import com.rastream.dag.Edge;
import com.rastream.dag.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommunicationModel {
    // Stores raw transmission rate samples per edge
    // Key = "sourceId->targetId" e.g. "v1_t1->v2_t1"
    // Value = list of rates observed during the stat window
    private final Map<String, List<Double>> rateHistory;

    public CommunicationModel() {
        this.rateHistory = new HashMap<>();
    }

    // Build the map key from two tasks
    private String edgeKey(Task source, Task target) {
        return source.getId() + "->" + target.getId();
    }
    // Called by DataMonitor at each tick during stat window st
    // Records one sample of tuple rate between two tasks
    public void recordSample(Task source, Task target, double rate) {
        String key = edgeKey(source, target);
        rateHistory
                .computeIfAbsent(key, k -> new ArrayList<>())
                .add(rate);
    }

    // Equation 1 from the paper:
    // E_st = (integral of E_t dt - max(E_t) - min(E_t)) / (tc - to)
    //
    // In discrete form: sum all samples, subtract max and min,
    // divide by number of samples (approximates the time window)
    public double computeAverageRate(Task source, Task target) {
        String key = edgeKey(source, target);
        List<Double> samples = rateHistory.getOrDefault(key, new ArrayList<>());

        // Need at least 3 samples to drop max and min meaningfully
        if (samples.size() < 3) {
            if (samples.isEmpty()) return 0.0;
            double sum = 0.0;
            for (double s : samples) sum += s;
            return sum / samples.size();
        }

        double sum = 0.0;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;

        for (double s : samples) {
            sum += s;
            if (s > max) max = s;
            if (s < min) min = s;
        }

        // This matches Eq.1 — drop outliers, average the rest
        return (sum - max - min) / samples.size();
    }

    // Equation 2 from the paper:
    // Tr(v_i,k, v_j,l) = 0 if no transmission, else E_st
    //
    // Updates the edge's weight with the computed average
    public void updateEdgeRate(Edge edge) {
        double rate = computeAverageRate(
                edge.getSource(),
                edge.getTarget()
        );
        // Equation 2: if rate is effectively 0, no communication
        edge.setTupleTransmissionRate(Math.max(rate, 0.0));
    }

    // Updates ALL edges in the application at once
    public void updateAllEdges(List<Edge> edges) {
        for (Edge e : edges) {
            updateEdgeRate(e);
        }
    }

    // Clear history after each stat window
    // Ready for the next window's samples
    public void resetWindow() {
        rateHistory.clear();
    }

    public Map<String, List<Double>> getRateHistory() {
        return rateHistory;
    }

}
