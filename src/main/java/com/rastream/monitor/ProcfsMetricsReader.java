package com.rastream.monitor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reads real system resource metrics from Linux /proc filesystem.
 *
 * Returns:
 * - readSystemMetrics(): [cpuUtil, memUtil, ioUtil]
 * - readNetworkMetrics(): [rxBytesPerSec, txBytesPerSec, netUtil]
 */
public class ProcfsMetricsReader {

    static final String PROC_STAT      = "/proc/stat";
    static final String PROC_MEMINFO   = "/proc/meminfo";
    static final String PROC_DISKSTATS = "/proc/diskstats";
    static final String PROC_NET_DEV   = "/proc/net/dev";

    private final boolean procAvailable;

    // CPU cache
    private long[] lastCpuTicks = null; // user,nice,system,idle,iowait,irq,softirq

    // IO cache
    private long[] lastIoBytes = null; // read,write

    // NET cache
    private long[] lastNetBytes = null; // rx,tx

    // shared timestamp cache for delta-based metrics
    private long lastSnapshotMs = 0L;
    private long lastNetSnapshotMs = 0L;

    // last good snapshots
    private final AtomicReference<double[]> lastGoodSystemMetrics =
            new AtomicReference<>(new double[]{0.0, 0.0, 0.0});

    private final AtomicReference<double[]> lastGoodNetworkMetrics =
            new AtomicReference<>(new double[]{0.0, 0.0, 0.0});

    public ProcfsMetricsReader() {
        this.procAvailable = Files.exists(Paths.get(PROC_STAT))
                && Files.exists(Paths.get(PROC_MEMINFO))
                && Files.exists(Paths.get(PROC_DISKSTATS));
        if (!procAvailable) {
            System.out.println("[ProcfsMetricsReader] /proc not fully available. Returning zeros.");
        }
    }

    /**
     * @return [cpuUtil, memUtil, ioUtil] in [0,1]
     */
    public double[] readSystemMetrics() {
        if (!procAvailable) return new double[]{0.0, 0.0, 0.0};
        try {
            double cpu = readCpuUtilization();
            double mem = readMemoryUtilization();
            double io = readIoUtilization();

            double[] v = new double[]{cpu, mem, io};
            lastGoodSystemMetrics.set(v);
            return v;
        } catch (Exception e) {
            System.err.println("[ProcfsMetricsReader] System read error: " + e.getMessage());
            return lastGoodSystemMetrics.get();
        }
    }

    /**
     * @return [rxBytesPerSec, txBytesPerSec, netUtil(0..1)]
     */
    public double[] readNetworkMetrics() {
        if (!Files.exists(Paths.get(PROC_NET_DEV))) {
            return new double[]{0.0, 0.0, 0.0};
        }
        try {
            long nowMs = System.currentTimeMillis();
            long[] cur = parseNetDevBytes();

            if (lastNetBytes == null || lastNetSnapshotMs == 0) {
                lastNetBytes = cur;
                lastNetSnapshotMs = nowMs;
                return new double[]{0.0, 0.0, 0.0};
            }

            long elapsedMs = nowMs - lastNetSnapshotMs;
            if (elapsedMs <= 0) return new double[]{0.0, 0.0, 0.0};

            long rxDelta = cur[0] - lastNetBytes[0];
            long txDelta = cur[1] - lastNetBytes[1];

            double rxBps = Math.max(0.0, rxDelta / (elapsedMs / 1000.0));
            double txBps = Math.max(0.0, txDelta / (elapsedMs / 1000.0));

            // Normalize against 1Gbps link budget (125 MB/s)
            // adjust this reference if your NIC differs
            double refBytesPerSec = 125_000_000.0;
            double util = Math.min(1.0, Math.max(0.0, (rxBps + txBps) / refBytesPerSec));

            lastNetBytes = cur;
            lastNetSnapshotMs = nowMs;

            double[] v = new double[]{rxBps, txBps, util};
            lastGoodNetworkMetrics.set(v);
            return v;

        } catch (Exception e) {
            System.err.println("[ProcfsMetricsReader] Network read error: " + e.getMessage());
            return lastGoodNetworkMetrics.get();
        }
    }

    // ---------------- CPU ----------------

    double readCpuUtilization() throws IOException {
        long[] ticks = parseCpuTicks();

        if (lastCpuTicks == null) {
            lastCpuTicks = ticks;
            return 0.0;
        }

        long userDelta    = ticks[0] - lastCpuTicks[0];
        long niceDelta    = ticks[1] - lastCpuTicks[1];
        long systemDelta  = ticks[2] - lastCpuTicks[2];
        long idleDelta    = ticks[3] - lastCpuTicks[3];
        long iowaitDelta  = ticks[4] - lastCpuTicks[4];
        long irqDelta     = ticks[5] - lastCpuTicks[5];
        long softirqDelta = ticks[6] - lastCpuTicks[6];

        lastCpuTicks = ticks;

        long active = userDelta + niceDelta + systemDelta + irqDelta + softirqDelta;
        long total = active + idleDelta + iowaitDelta;

        if (total <= 0) return 0.0;
        return Math.min(1.0, Math.max(0.0, (double) active / total));
    }

    private long[] parseCpuTicks() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(PROC_STAT))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("cpu ")) {
                    String[] parts = line.trim().split("\\s+");
                    long[] ticks = new long[7];
                    for (int i = 0; i < 7; i++) {
                        ticks[i] = Long.parseLong(parts[i + 1]);
                    }
                    return ticks;
                }
            }
        }
        throw new IOException("Cannot find cpu aggregate line in " + PROC_STAT);
    }

    // ---------------- MEM ----------------

    double readMemoryUtilization() throws IOException {
        long memTotalKb = -1;
        long memAvailableKb = -1;

        try (BufferedReader br = new BufferedReader(new FileReader(PROC_MEMINFO))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("MemTotal:")) {
                    memTotalKb = parseKbValue(line);
                } else if (line.startsWith("MemAvailable:")) {
                    memAvailableKb = parseKbValue(line);
                }
                if (memTotalKb > 0 && memAvailableKb >= 0) break;
            }
        }

        if (memTotalKb <= 0) return 0.0;
        if (memAvailableKb < 0) memAvailableKb = 0;

        long usedKb = memTotalKb - memAvailableKb;
        return Math.min(1.0, Math.max(0.0, (double) usedKb / memTotalKb));
    }

    private long parseKbValue(String line) {
        String[] p = line.trim().split("\\s+");
        if (p.length >= 2) {
            try { return Long.parseLong(p[1]); } catch (Exception ignored) {}
        }
        return 0;
    }

    // ---------------- IO ----------------

    double readIoUtilization() throws IOException {
        long nowMs = System.currentTimeMillis();
        long[] currentIo = parseDiskstats();

        if (lastIoBytes == null || lastSnapshotMs == 0) {
            lastIoBytes = currentIo;
            lastSnapshotMs = nowMs;
            return 0.0;
        }

        long readDelta = currentIo[0] - lastIoBytes[0];
        long writeDelta = currentIo[1] - lastIoBytes[1];
        long elapsedMs = nowMs - lastSnapshotMs;

        lastIoBytes = currentIo;
        lastSnapshotMs = nowMs;

        if (elapsedMs <= 0) return 0.0;

        long totalBytes = readDelta + writeDelta;
        double bytesPerSec = totalBytes / (elapsedMs / 1000.0);

        // normalize using 100MB/s reference
        double util = bytesPerSec / 100_000_000.0;
        return Math.min(1.0, Math.max(0.0, util));
    }

    private long[] parseDiskstats() throws IOException {
        long totalReadBytes = 0L;
        long totalWriteBytes = 0L;

        try (BufferedReader br = new BufferedReader(new FileReader(PROC_DISKSTATS))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.trim().split("\\s+");
                if (p.length < 10) continue;

                String devName = p[2];
                if (!isPhysicalDisk(devName)) continue;

                try {
                    long sectorsRead = Long.parseLong(p[5]);
                    long sectorsWritten = Long.parseLong(p[9]);
                    totalReadBytes += sectorsRead * 512L;
                    totalWriteBytes += sectorsWritten * 512L;
                } catch (NumberFormatException ignored) {
                }
            }
        }

        return new long[]{totalReadBytes, totalWriteBytes};
    }

    private boolean isPhysicalDisk(String name) {
        if (name == null || name.isEmpty()) return false;
        if (name.matches("sd[a-z]")) return true;
        if (name.matches("vd[a-z]")) return true;
        if (name.matches("hd[a-z]")) return true;
        if (name.matches("xvd[a-z]")) return true;
        if (name.matches("nvme\\d+n\\d+")) return true;
        return false;
    }

    // ---------------- NET ----------------

    /**
     * @return [totalRxBytes, totalTxBytes] for non-loopback interfaces.
     */
    private long[] parseNetDevBytes() throws IOException {
        long rx = 0L;
        long tx = 0L;

        try (BufferedReader br = new BufferedReader(new FileReader(PROC_NET_DEV))) {
            String line;
            int lineNo = 0;
            while ((line = br.readLine()) != null) {
                lineNo++;
                if (lineNo <= 2) continue; // skip headers

                String[] split = line.trim().split(":");
                if (split.length != 2) continue;

                String iface = split[0].trim();
                if (iface.equals("lo")) continue; // skip loopback

                String[] cols = split[1].trim().split("\\s+");
                // format: receive bytes is cols[0], transmit bytes is cols[8]
                if (cols.length < 9) continue;

                try {
                    long ifaceRx = Long.parseLong(cols[0]);
                    long ifaceTx = Long.parseLong(cols[8]);
                    rx += ifaceRx;
                    tx += ifaceTx;
                } catch (NumberFormatException ignored) {
                }
            }
        }

        return new long[]{rx, tx};
    }

    public boolean isProcAvailable() {
        return procAvailable;
    }
}