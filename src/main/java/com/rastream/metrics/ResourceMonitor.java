package com.rastream.metrics;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

public class ResourceMonitor {
    private final OperatingSystemMXBean osBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final Runtime rt = Runtime.getRuntime();

    public Snapshot snapshot() {
        double cpu = Math.max(0, osBean.getProcessCpuLoad() * 100.0);
        long usedMem = rt.totalMemory() - rt.freeMemory();
        long maxMem = rt.maxMemory();
        return new Snapshot(cpu, usedMem, maxMem);
    }

    public static class Snapshot {
        public final double cpuPercent;
        public final long usedMemoryBytes;
        public final long maxMemoryBytes;
        public Snapshot(double cpuPercent, long usedMemoryBytes, long maxMemoryBytes) {
            this.cpuPercent = cpuPercent; this.usedMemoryBytes = usedMemoryBytes; this.maxMemoryBytes = maxMemoryBytes;
        }
    }
}