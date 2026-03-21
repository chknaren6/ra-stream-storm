package com.rastream.utils;

import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.model.ResourceModel;
import com.rastream.allocation.ResourceScaler;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;
import com.rastream.partitioning.SubgraphPartitioner;
import java.util.List;

public class TestRunner {

    public static void main(String[] args) {
        testPhase2();
        System.out.println("----------------------------");
        testPhase3();
    }

    public static void testPhase2() {
        System.out.println("=== Testing Phase 2: Subgraph Partitioning ===");

        StreamApplication wc = new StreamApplication("WordCount");

        Task v1t1 = new Task(1, 1, "reader");
        Task v1t2 = new Task(1, 2, "reader");
        Task v2t1 = new Task(2, 1, "split");
        Task v2t2 = new Task(2, 2, "split");
        Task v3t1 = new Task(3, 1, "count");
        Task v3t2 = new Task(3, 2, "count");

        for (Task t : List.of(v1t1, v1t2, v2t1, v2t2, v3t1, v3t2))
            wc.addTask(t);

        // use the new connect() with rate directly
        wc.connect(v1t1, v2t1, 100.0);
        wc.connect(v1t2, v2t2, 120.0);
        wc.connect(v2t1, v3t1, 80.0);
        wc.connect(v2t2, v3t2, 90.0);

        // debug: confirm edges have non-zero rates
        System.out.println("\n--- Debug: edges ---");
        for (Edge e : wc.getEdges()) {
            System.out.println("  " + e);
        }

        SubgraphPartitioner partitioner = new SubgraphPartitioner();
        PartitionScheme scheme = partitioner.partition(wc, 2);

        // debug: confirm internal edges are found
        System.out.println("\n--- Debug: subgraphs after partitioning ---");
        for (Subgraph s : scheme.getSubgraphs()) {
            System.out.println("  " + s);
            System.out.println("  internal edges: "
                    + s.getInternalEdges().size());
        }

        System.out.println("\n--- Result ---");
        for (Subgraph s : scheme.getSubgraphs()) {
            System.out.println(s + " internalWeight="
                    + String.format("%.2f", s.getInternalWeight()));
        }
    }

    public static void testPhase3() {
        System.out.println("\n=== Testing Phase 3: Resource Scaling ===");

        StreamApplication wc = new StreamApplication("WordCount");
        Task v1t1 = new Task(1, 1, "reader");
        Task v1t2 = new Task(1, 2, "reader");
        Task v2t1 = new Task(2, 1, "split");
        Task v2t2 = new Task(2, 2, "split");
        Task v3t1 = new Task(3, 1, "count");
        Task v3t2 = new Task(3, 2, "count");

        for (Task t : List.of(v1t1, v1t2, v2t1, v2t2, v3t1, v3t2))
            wc.addTask(t);

        // use the new connect() with rate directly
        wc.connect(v1t1, v2t1, 100.0);
        wc.connect(v1t2, v2t2, 120.0);
        wc.connect(v2t1, v3t1, 80.0);
        wc.connect(v2t2, v3t2, 90.0);

        SubgraphPartitioner partitioner = new SubgraphPartitioner();
        PartitionScheme scheme = partitioner.partition(wc, 2);

        ResourceModel rm = new ResourceModel();
        rm.updateNodeUtilization(0.65, 0.50, 0.30);
        for (Task t : wc.getTasks().values()) {
            rm.recordTupleCount(t, 1000L);
        }

        ResourceScaler scaler = new ResourceScaler(rm);
        List<Subgraph> RS = scaler.scale(scheme, wc.getEdges());

        System.out.println("\n--- Resource Scaling Result ---");
        System.out.println("Minimum compute nodes needed: " + RS.size());
        for (Subgraph s : RS) {
            System.out.println("  Subgraph " + s.getId()
                    + " tasks=" + s.getTaskCount()
                    + " demand=" + String.format("%.4f",
                    rm.computeSubgraphResourceDemand(s.getTasks())));
        }
    }
}