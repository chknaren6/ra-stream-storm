package com.rastream.monitor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProcfsMetricsReaderTest {

    @Test
    void systemMetrics_shouldBeLength3AndNonNegative() {
        ProcfsMetricsReader r = new ProcfsMetricsReader();
        double[] m = r.readSystemMetrics();
        assertEquals(3, m.length);
        assertTrue(m[0] >= 0); assertTrue(m[1] >= 0); assertTrue(m[2] >= 0);
    }

    @Test
    void networkMetrics_shouldBeLength3AndNonNegative() {
        ProcfsMetricsReader r = new ProcfsMetricsReader();
        double[] n = r.readNetworkMetrics();
        assertEquals(3, n.length);
        assertTrue(n[0] >= 0); assertTrue(n[1] >= 0); assertTrue(n[2] >= 0);
    }
}