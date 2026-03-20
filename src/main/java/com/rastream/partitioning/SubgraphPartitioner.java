package com.rastream.partitioning;

import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SubgraphPartitioner {

    // Algorithm 1 inputs — match paper Table 4
    private final int maxIter;      // max_iter inner loop
    private final double T0;        // initial temperature
    private final double Tf;        // final temperature
    private final double zeta;      // cooling rate ζ = 0.99
    private final double epsilon;   // objective weight ε = 0.70

    private final ObjectiveFunction objectiveFunction;
    private final Random random;

    // Paper's parameter values from Table 4
    public SubgraphPartitioner() {
        this.maxIter  = 100;
        this.T0       = 100.0;
        this.Tf       = 0.1;
        this.zeta     = 0.99;
        this.epsilon  = 0.70;
        this.objectiveFunction = new ObjectiveFunction(epsilon);
        this.random   = new Random();
    }

    // Allow custom parameters for experimentation
    public SubgraphPartitioner(int maxIter, double T0,
                               double Tf, double zeta,
                               double epsilon) {
        this.maxIter  = maxIter;
        this.T0       = T0;
        this.Tf       = Tf;
        this.zeta     = zeta;
        this.epsilon  = epsilon;
        this.objectiveFunction = new ObjectiveFunction(epsilon);
        this.random   = new Random();
    }

    // Build starting partition: distribute tasks round-robin
    // across k subgraphs — this is x0 in Algorithm 1 line 2
    private PartitionScheme buildInitialPartition(
            StreamApplication app, int k) {

        PartitionScheme scheme =
                new PartitionScheme(app.getEdges());

        // Create k empty subgraphs
        for (int i = 0; i < k; i++) {
            scheme.addSubgraph(new Subgraph(i));
        }

        // Assign tasks round-robin across subgraphs
        List<Task> allTasks =
                new ArrayList<>(app.getTasks().values());
        for (int i = 0; i < allTasks.size(); i++) {
            int subgraphIndex = i % k;
            scheme.getSubgraphs()
                    .get(subgraphIndex)
                    .addTask(allTasks.get(i));
        }

        // Compute initial internal edges
        scheme.recomputeAllInternalEdges();
        return scheme;
    }

    // Algorithm 1 line 9: generate neighbor by moving
    // one random task to a different random subgraph
    private PartitionScheme generateNeighbor(PartitionScheme current) {
        PartitionScheme neighbor = current.deepCopy();
        List<Subgraph> subs = neighbor.getSubgraphs();

        // Pick a random subgraph that has more than 1 task
        // (never empty a subgraph completely)
        List<Subgraph> nonEmpty = new ArrayList<>();
        for (Subgraph s : subs) {
            if (s.getTaskCount() > 1) nonEmpty.add(s);
        }
        if (nonEmpty.isEmpty()) return neighbor;

        Subgraph fromSub =
                nonEmpty.get(random.nextInt(nonEmpty.size()));

        // Pick a random task from that subgraph
        List<Task> tasks = fromSub.getTasks();
        Task taskToMove  = tasks.get(random.nextInt(tasks.size()));

        // Pick a different random subgraph to move it to
        List<Subgraph> others = new ArrayList<>(subs);
        others.remove(fromSub);
        Subgraph toSub = others.get(random.nextInt(others.size()));

        // Execute the move
        fromSub.removeTask(taskToMove);
        toSub.addTask(taskToMove);

        // Recompute internal edges after the move
        neighbor.recomputeAllInternalEdges();
        return neighbor;
    }

    // Algorithm 1 — full simulated annealing loop
    // Input:  app (G), k (number of subgraphs = number of nodes)
    // Output: best PartitionScheme X found
    public PartitionScheme partition(StreamApplication app, int k) {

        // Lines 1-4: initialize
        PartitionScheme X  = buildInitialPartition(app, k);
        double fX          = objectiveFunction.compute(X);

        // Line 5: set initial temperature
        double T           = T0;

        System.out.println("Starting partitioning: k=" + k
                + " tasks=" + app.getTaskCount()
                + " initial f(x)=" + String.format("%.4f", fX));

        // Lines 7-20: outer cooling loop
        while (T > Tf) {
            int currentIter = 0;

            // Lines 8-18: inner iteration loop
            while (currentIter < maxIter) {

                // Line 9: generate neighbor solution
                PartitionScheme xi = generateNeighbor(X);
                double fXi = objectiveFunction.compute(xi);

                // Lines 11-16: accept or reject
                if (fXi <= fX) {
                    // Better solution — always accept (line 12)
                    X  = xi;
                    fX = fXi;
                } else {
                    // Worse solution — accept with probability p_i
                    // Equation 20: p_i = exp((f(xi) - f(X)) / T)
                    // Note: fXi > fX so exponent is positive/negative
                    double pi = Math.exp((fX - fXi) / T);
                    if (random.nextDouble() < pi) {
                        X  = xi;
                        fX = fXi;
                    }
                }
                currentIter++;
            }

            // Line 19: cool down — T = zeta * T
            T *= zeta;
        }

        System.out.println("Partitioning done: final f(x)="
                + String.format("%.4f", fX));
        return X;
    }

}
