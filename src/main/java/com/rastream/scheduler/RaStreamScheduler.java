package com.rastream.scheduler;

import com.rastream.allocation.ResourceScaler;
import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ComputeNode;
import com.rastream.model.ResourceModel;
import com.rastream.monitor.DataMonitor;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;
import com.rastream.partitioning.SubgraphPartitioner;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.*;

import java.util.*;
import java.util.stream.Collectors;   // ← ADDED for older Java compatibility

public class RaStreamScheduler implements IScheduler {

    private SubgraphPartitioner partitioner;
    private ResourceModel resourceModel;
    private ResourceScaler resourceScaler;
    private FineGrainedScheduler fineScheduler;
    private CommunicationModel commModel;
    private DataMonitor monitor;

    @Override
    public Map config() {
        return new HashMap<>();
    }

    @Override
    public void prepare(Map<String, Object> conf, StormMetricsRegistry metricsRegistry) {
        this.resourceModel = new ResourceModel();
        this.commModel = new CommunicationModel();
        this.partitioner = new SubgraphPartitioner();
        this.resourceScaler = new ResourceScaler(resourceModel);
        this.fineScheduler = new FineGrainedScheduler(resourceModel);
        System.out.println("✅ RaStreamScheduler ready (full paper implementation)");
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topo : cluster.needsSchedulingTopologies()) {
            scheduleOneTopology(topo, cluster);
        }
    }

    private void scheduleOneTopology(TopologyDetails topo, Cluster cluster) {
        StreamApplication app = buildDAG(topo);

        if (monitor == null) {
            monitor = new DataMonitor(app, commModel, resourceModel);
        }

        int k = Math.min(cluster.getSupervisors().size(), 14);
        PartitionScheme X = partitioner.partition(app, k);

        List<Subgraph> RS = resourceScaler.scale(X, app.getEdges());

        List<ComputeNode> nodes = buildComputeNodes(cluster);

        SchedulingScheme SS = fineScheduler.schedule(RS, nodes, app.getEdges());

        assignToStorm(SS, RS, cluster, topo);
    }

    private StreamApplication buildDAG(TopologyDetails topo) {
        // Create with required name (matches your current constructor)
        StreamApplication app = new StreamApplication("TaxiNYC-DAG");

        // Detect real parallelism from Storm topology
        Map<String, Integer> compParallelism = new HashMap<>();
        Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
        for (ExecutorDetails exec : execToComp.keySet()) {
            String comp = execToComp.get(exec);
            compParallelism.merge(comp, 1, Integer::sum);
        }

        // Map component names to vertexId (paper-style vertex numbering)
        String[] components = {"taxi-reader", "taxi-validator", "taxi-aggregator", "taxi-anomaly", "taxi-output"};
        int[] vertexIds = {1, 2, 3, 4, 5};  // v1 = reader, v2 = validator, etc.

        Map<String, List<Task>> compToTasks = new HashMap<>();

        for (int i = 0; i < components.length; i++) {
            String c = components[i];
            int vertexId = vertexIds[i];
            int par = compParallelism.getOrDefault(c, c.equals("taxi-reader") ? 2 : 3);

            List<Task> tasksList = new ArrayList<>();
            for (int taskIdx = 0; taskIdx < par; taskIdx++) {
                // Matches your current Task constructor: (vertexId, taskId, functionName)
                Task t = new Task(vertexId, taskIdx, c);
                app.addTask(t);                    // automatically groups by vertexId
                tasksList.add(t);
            }
            compToTasks.put(c, tasksList);
        }

        // === Add edges (exactly as before – high Tr on reader→validator) ===
        List<Task> readers = compToTasks.get("taxi-reader");
        List<Task> validators = compToTasks.get("taxi-validator");
        for (Task r : readers) {
            for (Task v : validators) {
                app.addEdge(new Edge(r, v));
            }
        }

        List<Task> aggregators = compToTasks.get("taxi-aggregator");
        for (Task v : validators) {
            for (Task a : aggregators) {
                app.addEdge(new Edge(v, a));
            }
        }

        List<Task> anomalies = compToTasks.get("taxi-anomaly");
        List<Task> outputs = compToTasks.get("taxi-output");
        for (Task a : aggregators) {
            for (Task an : anomalies) app.addEdge(new Edge(a, an));
            for (Task o : outputs)   app.addEdge(new Edge(a, o));
        }

        System.out.println("DAG built: " + app.getTaskCount() + " tasks, "
                + app.getEdges().size() + " edges, "
                + app.getVertexCount() + " vertices");
        return app;
    }

    private List<ComputeNode> buildComputeNodes(Cluster cluster) {
        List<ComputeNode> nodes = new ArrayList<>();
        for (SupervisorDetails sup : cluster.getSupervisors().values()) {
            // Explicit variables to avoid any IDE literal parsing confusion
            double cpu = 0.85;
            double mem = 0.80;
            double io  = 0.75;
            nodes.add(new ComputeNode(sup.getId(), cpu, mem, io));  // ← 4 args, fixed
        }
        return nodes;
    }

    private void assignToStorm(SchedulingScheme SS, List<Subgraph> RS, Cluster cluster, TopologyDetails topo) {
        Map<WorkerSlot, List<ExecutorDetails>> slotMap = new HashMap<>();

        for (List<Task> group : SS.getProcessGroups()) {
            List<WorkerSlot> available = cluster.getAvailableSlots();
            if (available.isEmpty()) continue;
            WorkerSlot slot = available.get(0);

            List<ExecutorDetails> exes = cluster.getUnassignedExecutors(topo).stream()
                    .filter(e -> {
                        String comp = topo.getExecutorToComponent().get(e);
                        return comp != null && group.stream().anyMatch(t ->
                                t.getId().startsWith(comp.split("_")[0]));
                    })
                    .collect(Collectors.toList());   // ← FIXED: no .toList() on Java 8/11

            slotMap.computeIfAbsent(slot, k -> new ArrayList<>()).addAll(exes);
        }

        // Explicit 3-argument call + explicit list (fixes "Expected 3 arguments")
        for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : slotMap.entrySet()) {
            List<ExecutorDetails> executorsToAssign = new ArrayList<>(entry.getValue());
            cluster.assign(entry.getKey(), topo.getId(), executorsToAssign);  // ← 3 args
        }

        System.out.println("✅ Ra-Stream assigned " + RS.size() + " subgraphs with fine-grained co-location");
    }

    @Override
    public void cleanup() {
        if (monitor != null) monitor.cleanup();
    }
}