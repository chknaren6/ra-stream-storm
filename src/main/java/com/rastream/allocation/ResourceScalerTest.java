package com.rastream.allocation;

import com.rastream.dag.Task;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;

class ResourceScalerTest {

    @Test
    void scale_shouldReturnNonEmptyAndRespectThresholdFlow() {
        ResourceModel rm = new ResourceModel();
        rm.updateNodeUtilization(0.8, 0.7, 0.6);

        Task t1 = new Task(1, 0, "a");
        Task t2 = new Task(1, 1, "a");
        Task t3 = new Task(2, 0, "b");
        Task t4 = new Task(2, 1, "b");

        for (Task t : List.of(t1, t2, t3, t4)) rm.recordTupleCount(t, 1000);

        Subgraph s1 = new Subgraph(1); s1.addTask(t1); s1.addTask(t2);
        Subgraph s2 = new Subgraph(2); s2.addTask(t3); s2.addTask(t4);

        PartitionScheme x = new PartitionScheme(new ArrayList<>());
        x.addSubgraph(s1);
        x.addSubgraph(s2);

        ResourceScaler scaler = new ResourceScaler(rm);
        List<Subgraph> out = scaler.scale(x, new ArrayList<>());

        assertNotNull(out);
        assertFalse(out.isEmpty());
    }
}