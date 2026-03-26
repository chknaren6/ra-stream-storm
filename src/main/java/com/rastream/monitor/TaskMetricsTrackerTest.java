package com.rastream.monitor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TaskMetricsTrackerTest {

    @Test
    void deterministicMapping_shouldGenerateInternalTaskId() {
        TaskMetricsTracker tracker = new TaskMetricsTracker();
        tracker.registerComponentVertex("taxi-reader", 1);
        tracker.recordEmitCount("taxi-reader", 3, 100L);

        String internal = tracker.getInternalTaskId("taxi-reader", 3);
        assertEquals("v1_t3", internal);
    }
}