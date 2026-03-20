package com.rastream.partitioning;

import java.util.List;

public class ObjectiveFunction {
    // epsilon — weight balancing r vs sigma_W
    // Paper says: if all Tr are equal, set epsilon=1
    // Decrease epsilon as Tr differences increase
    // Table 4 in paper uses 0.70
    private final double epsilon;

    public ObjectiveFunction(double epsilon) {
        this.epsilon = epsilon;
    }

    // Equation 16: variance of internal weight sums across subgraphs
    // sigma_W = sqrt( (1/k) * sum( (W_int_i - avg_W_int)^2 ) )
    // LOW variance = balanced subgraphs = good
    public double computeVariance(PartitionScheme scheme) {
        List<Subgraph> subs = scheme.getSubgraphs();
        int k = subs.size();
        if (k == 0) return 0.0;

        // First pass: compute average internal weight
        double avgWeight = 0.0;
        for (Subgraph s : subs) {
            avgWeight += s.getInternalWeight();
        }
        avgWeight /= k;

        // Second pass: compute variance (Equation 16)
        double sumSquaredDiff = 0.0;
        for (Subgraph s : subs) {
            double diff = s.getInternalWeight() - avgWeight;
            sumSquaredDiff += diff * diff;
        }

        return Math.sqrt(sumSquaredDiff / k);
    }

    // Equation 18: r = W_cut / (W_cut + W_int_avg)
    // Ratio of cut edge weight to total weight
    // LOW r = fewer expensive cross-node edges = good
    public double computeCutRatio(PartitionScheme scheme) {
        double wCut = scheme.getCutEdgeWeight();

        // Sum of ALL internal weights across all subgraphs
        double wInt = 0.0;
        for (Subgraph s : scheme.getSubgraphs()) {
            wInt += s.getInternalWeight();
        }

        double total = wCut + wInt;
        if (total == 0.0) return 0.0;

        // Equation 18
        return wCut / total;
    }


    // Equation 19: f(x) = epsilon * r + (1 - epsilon) * sigma_W
    // This is the single number simulated annealing minimizes
    // LOWER f(x) = better partition scheme
    public double compute(PartitionScheme scheme) {
        double r      = computeCutRatio(scheme);
        double sigmaW = computeVariance(scheme);

        // Equation 19
        return (epsilon * r) + ((1.0 - epsilon) * sigmaW);
    }

    public double getEpsilon() { return epsilon; }

}
