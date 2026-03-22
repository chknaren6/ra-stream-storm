package com.rastream.partitioning;

import com.rastream.dag.Edge;
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
        this.maxIter  = 200;
        this.T0       = 100.0;
        this.Tf       = 0.1;
        this.zeta     = 0.99;
        this.epsilon  = 0.60;
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
    // Build initial partition using topology-aware assignment
// Tasks connected by edges start in the same subgraph
// This gives simulated annealing a much better starting point
    private PartitionScheme buildInitialPartition(
            StreamApplication app, int k) {

        PartitionScheme scheme = new PartitionScheme(app.getEdges());
        for (int i = 0; i < k; i++) {
            scheme.addSubgraph(new Subgraph(i));
        }

        List<Task> allTasks = new ArrayList<>(app.getTasks().values());
        List<Edge> allEdges = app.getEdges();

        // Sort tasks so connected tasks get assigned together
        // Strategy: assign task, then immediately assign its neighbors
        java.util.Set<String> assigned = new java.util.HashSet<>();
        int subgraphIndex = 0;
        int tasksPerSubgraph = (int) Math.ceil(
                (double) allTasks.size() / k);

        for (Task seed : allTasks) {
            if (assigned.contains(seed.getId())) continue;

            // Assign seed task
            Subgraph current = scheme.getSubgraphs().get(subgraphIndex);
            current.addTask(seed);
            assigned.add(seed.getId());

            // Assign directly connected neighbors to same subgraph
            for (Edge e : allEdges) {
                if (current.getTaskCount() >= tasksPerSubgraph) break;

                Task neighbor = null;
                if (e.getSource().getId().equals(seed.getId())
                        && !assigned.contains(e.getTarget().getId())) {
                    neighbor = e.getTarget();
                } else if (e.getTarget().getId().equals(seed.getId())
                        && !assigned.contains(e.getSource().getId())) {
                    neighbor = e.getSource();
                }

                if (neighbor != null) {
                    current.addTask(neighbor);
                    assigned.add(neighbor.getId());
                }
            }

            // Move to next subgraph when current is full
            if (current.getTaskCount() >= tasksPerSubgraph) {
                subgraphIndex = Math.min(subgraphIndex + 1, k - 1);
            }
        }

        scheme.recomputeAllInternalEdges();

        for (Subgraph s : scheme.getSubgraphs()) {
            System.out.println("  " + s + " internalWeight="
                    + s.getInternalWeight());
        }


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
        PartitionScheme current = buildInitialPartition(app, k);
        double fCurrent = objectiveFunction.compute(current);

        // Track BEST solution separately — never lose it
        PartitionScheme best = current.deepCopy();
        double fBest = fCurrent;

        double T = T0;

        System.out.println("Starting partitioning: k=" + k
                + " tasks=" + app.getTaskCount()
                + " initial f(x)=" + String.format("%.4f", fCurrent));

        while (T > Tf) {
            int currentIter = 0;

            while (currentIter < maxIter) {

                // Generate neighbor
                PartitionScheme neighbor = generateNeighbor(current);
                double fNeighbor = objectiveFunction.compute(neighbor);

                if (fNeighbor <= fCurrent) {
                    // Better than current — always accept
                    current  = neighbor;
                    fCurrent = fNeighbor;

                    // Also update best if this is the best ever seen
                    if (fNeighbor < fBest) {
                        best  = neighbor.deepCopy();
                        fBest = fNeighbor;
                    }
                } else {
                    // Worse than current — accept with probability
                    // Equation 20: pi = exp((fCurrent - fNeighbor) / T)
                    double pi = Math.exp((fCurrent - fNeighbor) / T);
                    if (random.nextDouble() < pi) {
                        // Accept worse solution to escape local optimum
                        // but DO NOT update best
                        current  = neighbor;
                        fCurrent = fNeighbor;
                    }
                }
                currentIter++;
            }

            // Cool down
            T *= zeta;
        }

        System.out.println("Partitioning done: final f(x)="
                + String.format("%.4f", fBest));

        // Return BEST found, not current
        return best;
    }

}
