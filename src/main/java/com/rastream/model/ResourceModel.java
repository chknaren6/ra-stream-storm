package com.rastream.model;

import com.rastream.dag.Task;

import java.util.HashMap;
import java.util.Map;

public class ResourceModel {
    // Equation 6 weights: R = α·Rc + β·Rm + (1-α-β)·Rio
    // Paper's Table 4 sets α=0.50, β=0.35
    private final double alpha;  // CPU weight
    private final double beta;   // Memory weight
    // IO weight is implicitly (1 - alpha - beta) = 0.15

    // Stores how many tuples each task processed in window st
    // Key = task.getId(), Value = tuple count
    private final Map<String, Long> tupleCountMap;

    // Stores the node-level utilization readings
    // These come from Linux system monitoring (top, /proc/stat)
    private double nodeCpuUtilization;
    private double nodeMemUtilization;
    private double nodeIoUtilization;

    // Use paper's default values from Table 4
    public ResourceModel() {
        this.alpha = 0.50;
        this.beta  = 0.35;
        this.tupleCountMap = new HashMap<>();
    }

    // Allow custom weights for experimentation
    public ResourceModel(double alpha, double beta) {
        this.alpha = alpha;
        this.beta  = beta;
        this.tupleCountMap = new HashMap<>();
    }

    // Called by DataMonitor with readings from the compute node
    // These are the U_cn,st values in the paper
    public void updateNodeUtilization(double cpu, double mem, double io) {
        this.nodeCpuUtilization = cpu;
        this.nodeMemUtilization = mem;
        this.nodeIoUtilization  = io;
    }

    // Record how many tuples task processed in window st
    // This is num_v_i,k_st in the paper
    public void recordTupleCount(Task task, long count) {
        tupleCountMap.put(task.getId(), count);
    }
    public double getNodeTotalUtilization() {
        return (alpha * nodeCpuUtilization) + (beta * nodeMemUtilization) + ((1 - alpha - beta) * nodeIoUtilization);
    }

    // Get total tuples processed by ALL tasks on this node
    public long getTotalTupleCount() {
        long total = 0;
        for (long count : tupleCountMap.values()) {
            total += count;
        }
        return total;
    }

    // Equation 3: CPU utilization of a single task
    // Rc_v,st = (num_v,st / sum_all_tasks(num)) * U_cpu_cn,st
    public double computeTaskCpuUtilization(Task task) {
        long total = getTotalTupleCount();
        if (total == 0) return 0.0;

        long taskTuples = tupleCountMap.getOrDefault(task.getId(), 0L);
        return ((double) taskTuples / total) * nodeCpuUtilization;
    }

    // Equation 4: Memory utilization of a single task
    public double computeTaskMemUtilization(Task task) {
        long total = getTotalTupleCount();
        if (total == 0) return 0.0;

        long taskTuples = tupleCountMap.getOrDefault(task.getId(), 0L);
        return ((double) taskTuples / total) * nodeMemUtilization;
    }

    // Equation 5: I/O utilization of a single task
    public double computeTaskIoUtilization(Task task) {
        long total = getTotalTupleCount();
        if (total == 0) return 0.0;

        long taskTuples = tupleCountMap.getOrDefault(task.getId(), 0L);
        return ((double) taskTuples / total) * nodeIoUtilization;
    }

    // Equation 6: combined weighted resource utilization
    // R_v,st = α·Rc + β·Rm + (1-α-β)·Rio
    // This single number is what the resource scaler uses
    // to decide if a node is overloaded or underloaded
    public double computeTaskResourceUtilization(Task task) {
        double cpu = computeTaskCpuUtilization(task);
        double mem = computeTaskMemUtilization(task);
        double io  = computeTaskIoUtilization(task);

        return (alpha * cpu)
                + (beta  * mem)
                + ((1.0 - alpha - beta) * io);
    }

    // Convenience: compute total resource demand of a task set
    public double computeSubgraphResourceDemand(java.util.List<Task> tasks) {
        double total = 0.0;
        for (Task t : tasks) {
            total += computeTaskResourceUtilization(t);
        }
        return total;
    }

    public void resetWindow() {
        tupleCountMap.clear();
        nodeCpuUtilization = 0.0;
        nodeMemUtilization = 0.0;
        nodeIoUtilization  = 0.0;
    }

    public double getAlpha() { return alpha; }
    public double getBeta()  { return beta; }

}
