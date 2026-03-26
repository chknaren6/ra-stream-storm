package com.rastream.scheduler;

import com.rastream.allocation.ResourceScaler;
import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ComputeNode;
import com.rastream.model.ResourceModel;
import com.rastream.monitor.DataMonitor;
import com.rastream.monitor.SchedulingController;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;
import com.rastream.partitioning.SubgraphPartitioner;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Ra-Stream IScheduler — full paper implementation.
 *
 * What was missing before:
 *   1. SchedulingController was created but never started or consumed.
 *   2. The rescheduling feedback loop (Figure 7) was not wired.
 *   3. FineGrainedScheduler was referenced but not implemented.
 *
 * This version:
 *   - Starts the SchedulingController after the first topology submission.
 *   - On every subsequent schedule() call, checks isRescheduleReady()
 *     and uses the new subgraph plan if one is available.
 *   - Wires DataMonitor ↔ SchedulingController as per Section 5.1.
 */
public class RaStreamScheduler implements IScheduler {

    private SubgraphPartitioner partitioner;
    private ResourceModel resourceModel;
    private ResourceScaler resourceScaler;
    private FineGrainedScheduler fineScheduler;
    private CommunicationModel commModel;
    private DataMonitor monitor;
    private SchedulingController schedulingController;

    // Cache the last computed subgraph list so reschedule-only calls
    // (from SchedulingController) don't redo the partition from scratch
    private volatile List<Subgraph> currentSubgraphs = null;

    @Override
    public Map config() {
        return new HashMap<>();
    }

    @Override
    public void prepare(Map<String, Object> conf,
                        StormMetricsRegistry metricsRegistry) {
        this.resourceModel  = new ResourceModel();
        this.commModel      = new CommunicationModel();
        this.partitioner    = new SubgraphPartitioner();
        this.resourceScaler = new ResourceScaler(resourceModel);
        this.fineScheduler  = new FineGrainedScheduler(resourceModel);
        System.out.println("[RaStreamScheduler] Ready (full paper implementation)");
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        for (TopologyDetails topo : cluster.needsSchedulingTopologies()) {
            scheduleOneTopology(topo, cluster);
        }
    }

    // ------------------------------------------------------------------
    // Per-topology scheduling (called on initial submit AND on reschedule)
    // ------------------------------------------------------------------

    private void scheduleOneTopology(TopologyDetails topo, Cluster cluster) {
        StreamApplication app = buildDAG(topo);

        // ----------------------------------------------------------------
        // First time: set up DataMonitor + SchedulingController
        // ----------------------------------------------------------------
        if (monitor == null) {
            monitor = new DataMonitor(app, commModel, resourceModel);

            int clusterSize = Math.min(cluster.getSupervisors().size(), 14);
            schedulingController = new SchedulingController(
                    app, commModel, resourceModel,
                    partitioner, resourceScaler, clusterSize);

            // Wire them together (Section 5.1 / Figure 7)
            monitor.setSchedulingController(schedulingController);
            schedulingController.start();

            System.out.println("[RaStreamScheduler] DataMonitor + "
                    + "SchedulingController started. Cluster size=" + clusterSize);
        }

        // ----------------------------------------------------------------
        // Check if SchedulingController has produced a new plan (Figure 7)
        // If yes, use it directly instead of re-running Algorithm 1+2.
        // ----------------------------------------------------------------
        List<Subgraph> RS;

        if (schedulingController.isRescheduleReady()) {
            RS = schedulingController.consumeLatestSubgraphs();
            System.out.println("[RaStreamScheduler] Using rescheduled plan from "
                    + "SchedulingController (" + RS.size() + " subgraphs)");
        } else if (currentSubgraphs == null) {
            // Initial scheduling: run Algorithm 1 + Algorithm 2
            int k = Math.min(cluster.getSupervisors().size(), 14);
            PartitionScheme X = partitioner.partition(app, k);
            RS = resourceScaler.scale(X, app.getEdges());
            currentSubgraphs = RS;
            System.out.println("[RaStreamScheduler] Initial scheduling: "
                    + RS.size() + " subgraphs from "
                    + app.getTaskCount() + " tasks");
        } else {
            // Nothing to do — topology is already scheduled
            return;
        }

        currentSubgraphs = RS;

        // ----------------------------------------------------------------
        // Algorithm 3: Fine-grained scheduling
        // ----------------------------------------------------------------
        List<ComputeNode> nodes = buildComputeNodes(cluster);
        SchedulingScheme SS = fineScheduler.schedule(RS, nodes, app.getEdges());

        // ----------------------------------------------------------------
        // Apply to Storm cluster
        // ----------------------------------------------------------------
        assignToStorm(SS, RS, cluster, topo);
    }

    // ------------------------------------------------------------------
    // DAG builder — maps Storm topology to our Task/Edge model
    // ------------------------------------------------------------------

    private StreamApplication buildDAG(TopologyDetails topo) {
        StreamApplication app = new StreamApplication("TaxiNYC-DAG");

        // Count actual parallelism from Storm
        Map<String, Integer> compParallelism = new HashMap<>();
        for (ExecutorDetails exec : topo.getExecutorToComponent().keySet()) {
            String comp = topo.getExecutorToComponent().get(exec);
            compParallelism.merge(comp, 1, Integer::sum);
        }

        // Taxi topology: 5 vertices, matching COMPONENT_NAMES order
        String[] components = {
                "taxi-reader", "taxi-validator", "taxi-aggregator",
                "taxi-anomaly",  "taxi-output"
        };
        int[] vertexIds = {1, 2, 3, 4, 5};
        // Default parallelism: reader=2, validator=3, aggregator=3, anomaly=2, output=2
        int[] defaults  = {2, 3, 3, 2, 2};

        Map<String, List<Task>> compToTasks = new LinkedHashMap<>();

        for (int i = 0; i < components.length; i++) {
            String c  = components[i];
            int vid   = vertexIds[i];
            int par   = compParallelism.getOrDefault(c, defaults[i]);
            List<Task> tasks = new ArrayList<>();
            for (int k = 0; k < par; k++) {
                Task t = new Task(vid, k, c);
                app.addTask(t);
                tasks.add(t);
            }
            compToTasks.put(c, tasks);
        }

        // Wire edges exactly as TaxiTopology.buildTopology() does
        // reader → validator (HIGH Tr — fine-grained co-location target)
        for (Task r : compToTasks.get("taxi-reader")) {
            for (Task v : compToTasks.get("taxi-validator")) {
                app.addEdge(new com.rastream.dag.Edge(r, v));
            }
        }
        // validator → aggregator
        for (Task v : compToTasks.get("taxi-validator")) {
            for (Task a : compToTasks.get("taxi-aggregator")) {
                app.addEdge(new com.rastream.dag.Edge(v, a));
            }
        }
        // aggregator → anomaly (LOW Tr) + aggregator → output
        for (Task a : compToTasks.get("taxi-aggregator")) {
            for (Task an : compToTasks.get("taxi-anomaly")) {
                app.addEdge(new com.rastream.dag.Edge(a, an));
            }
            for (Task o : compToTasks.get("taxi-output")) {
                app.addEdge(new com.rastream.dag.Edge(a, o));
            }
        }

        System.out.println("[RaStreamScheduler] DAG: "
                + app.getTaskCount() + " tasks, "
                + app.getEdges().size() + " edges, "
                + app.getVertexCount() + " vertices");
        return app;
    }

    // ------------------------------------------------------------------
    // Build ComputeNode list from Storm supervisor info
    // ------------------------------------------------------------------

    private List<ComputeNode> buildComputeNodes(Cluster cluster) {
        List<ComputeNode> nodes = new ArrayList<>();
        for (SupervisorDetails sup : cluster.getSupervisors().values()) {
            // Paper Table 3: each supervisor has dual-core, 2GB RAM, 100Mbps
            // We represent available resources as fractions [0,1]
            // In a real deployment these come from /proc via ProcfsMetricsReader
            nodes.add(new ComputeNode(sup.getId(), 0.85, 0.80, 0.75));
        }
        return nodes;
    }

    // ------------------------------------------------------------------
    // Apply SchedulingScheme to Storm cluster
    // ------------------------------------------------------------------

    /**
     * Maps process groups from SS to Storm WorkerSlots.
     *
     * Each process group (from fine-grained co-location) is assigned to
     * a slot on the node that owns its subgraph. Storm executors whose
     * component name matches the group's tasks are placed on that slot.
     *
     * NOTE: Storm's own executor-to-slot assignment is coarse at the
     * worker level — true thread-level co-location is achieved via
     * Storm's executor threading model (multiple executors in one worker).
     */
    private void assignToStorm(SchedulingScheme SS,
                               List<Subgraph> RS,
                               Cluster cluster,
                               TopologyDetails topo) {
        Map<WorkerSlot, List<ExecutorDetails>> slotMap = new HashMap<>();

        List<ExecutorDetails> unassigned =
                new ArrayList<>(cluster.getUnassignedExecutors(topo));
        List<WorkerSlot> available = new ArrayList<>(cluster.getAvailableSlots());

        if (available.isEmpty()) {
            System.err.println("[RaStreamScheduler] No available slots — "
                    + "cannot assign topology");
            return;
        }

        // For each process group, find matching executors and pack them
        // onto the same slot (= same JVM worker = lowest IPC overhead)
        int slotIdx = 0;
        for (List<Task> group : SS.getProcessGroups()) {
            if (unassigned.isEmpty()) break;
            if (slotIdx >= available.size()) slotIdx = 0;

            WorkerSlot slot = available.get(slotIdx);
            Set<String> groupComponentNames = new HashSet<>();
            for (Task t : group) {
                groupComponentNames.add(t.getFunctionName()); // e.g. "taxi-reader"
            }

            // Collect executors whose component is in this group
            List<ExecutorDetails> toAssign = new ArrayList<>();
            Iterator<ExecutorDetails> it = unassigned.iterator();
            while (it.hasNext()) {
                ExecutorDetails exec = it.next();
                String comp = topo.getExecutorToComponent().get(exec);
                if (comp != null && groupComponentNames.contains(comp)) {
                    toAssign.add(exec);
                    it.remove();
                }
            }

            if (!toAssign.isEmpty()) {
                slotMap.computeIfAbsent(slot, k -> new ArrayList<>())
                        .addAll(toAssign);
                slotIdx++;
            }
        }

        // Any remaining unassigned executors go to the first available slot
        if (!unassigned.isEmpty() && !available.isEmpty()) {
            WorkerSlot fallback = available.get(0);
            slotMap.computeIfAbsent(fallback, k -> new ArrayList<>())
                    .addAll(unassigned);
            System.out.println("[RaStreamScheduler] "
                    + unassigned.size()
                    + " executors assigned to fallback slot");
        }

        // Commit to Storm
        int assigned = 0;
        for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry
                : slotMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                cluster.assign(entry.getKey(),
                        topo.getId(),
                        entry.getValue());
                assigned += entry.getValue().size();
            }
        }

        System.out.println("[RaStreamScheduler] Assigned " + assigned
                + " executors across " + slotMap.size() + " slots, "
                + RS.size() + " subgraphs.");
    }

    @Override
    public void cleanup() {
        if (schedulingController != null) schedulingController.stop();
        if (monitor != null) monitor.cleanup();
    }
}