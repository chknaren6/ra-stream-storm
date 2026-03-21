package com.rastream.scheduler;

import com.rastream.allocation.ResourceScaler;
import com.rastream.model.ComputeNode;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.Subgraph;
import com.rastream.partitioning.SubgraphPartitioner;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RaStreamScheduler implements IScheduler {

    private SubgraphPartitioner  partitioner;
    private ResourceModel        resourceModel;
    private ResourceScaler       resourceScaler;
    private FineGrainedScheduler fineScheduler;

    // config() returns Map — this is what Storm calls to get
    @Override
    public Map config() {
        return new HashMap();
    }

    @Override
    public void prepare(Map<String, Object> conf,
                        StormMetricsRegistry metricsRegistry) {
        this.resourceModel   = new ResourceModel();
        this.partitioner     = new SubgraphPartitioner();
        this.resourceScaler  = new ResourceScaler(resourceModel);
        this.fineScheduler   = new FineGrainedScheduler(resourceModel);
        System.out.println("RaStreamScheduler initialized");
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Collection<TopologyDetails> toSchedule =
                cluster.needsSchedulingTopologies();

        if (toSchedule.isEmpty()) return;

        List<WorkerSlot> availableSlots =
                new ArrayList<>(cluster.getAvailableSlots());

        if (availableSlots.isEmpty()) {
            System.out.println("RaStreamScheduler: no available slots");
            return;
        }

        List<ComputeNode> computeNodes = buildComputeNodes(cluster);

        for (TopologyDetails topology : toSchedule) {
            System.out.println("RaStreamScheduler: scheduling "
                    + topology.getName());
            scheduleTopology(topology, cluster,
                    computeNodes, availableSlots);
        }
    }

    private void scheduleTopology(TopologyDetails topology,
                                  Cluster cluster,
                                  List<ComputeNode> computeNodes,
                                  List<WorkerSlot> slots) {
        List<ExecutorDetails> executors = new ArrayList<>(
                cluster.getUnassignedExecutors(topology));

        if (executors.isEmpty()) return;

        int numNodes = Math.min(computeNodes.size(), slots.size());
        if (numNodes == 0) return;

        int exePerSlot = Math.max(1, executors.size() / numNodes);
        int exeIndex   = 0;

        for (int i = 0; i < numNodes && exeIndex < executors.size(); i++) {
            WorkerSlot slot = slots.get(i);
            List<ExecutorDetails> slotExecutors = new ArrayList<>();

            int end = (i == numNodes - 1)
                    ? executors.size()
                    : Math.min(exeIndex + exePerSlot, executors.size());

            while (exeIndex < end) {
                slotExecutors.add(executors.get(exeIndex++));
            }

            if (!slotExecutors.isEmpty()) {
                cluster.assign(slot, topology.getId(), slotExecutors);
                System.out.println("  Assigned " + slotExecutors.size()
                        + " executors to " + slot);
            }
        }
    }

    private List<ComputeNode> buildComputeNodes(Cluster cluster) {
        List<ComputeNode> nodes = new ArrayList<>();
        for (SupervisorDetails supervisor :
                cluster.getSupervisors().values()) {
            nodes.add(new ComputeNode(
                    supervisor.getId(), 0.70, 0.70, 0.70));
        }
        return nodes;
    }

    @Override
    public void cleanup() {
        System.out.println("RaStreamScheduler cleanup");
    }
}