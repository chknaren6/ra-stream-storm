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
 * Implements the node-level utilization readings used in Equations 3-6:
 *   U_cpu_cn,st  — from /proc/stat
 *   U_mem_cn,st  — from /proc/meminfo
 *   U_io_cn,st   — from /proc/diskstats
 *
 * Returns double[3] = { cpuUtil, memUtil, ioUtil } in range [0.0, 1.0]
 * On non-Linux systems (no /proc), returns {0.0, 0.0, 0.0} gracefully.
 *
 * Thread-safe: uses AtomicReference for the cached snapshot so multiple
 * threads calling readSystemMetrics() never race on the file handles.
 */
public class ProcfsMetricsReader {

    // /proc paths — package-private for unit testing with mocks
    static final String PROC_STAT     = "/proc/stat";
    static final String PROC_MEMINFO  = "/proc/meminfo";
    static final String PROC_DISKSTATS = "/proc/diskstats";

    // Whether this JVM is running on a Linux host with /proc available
    private final boolean procAvailable;

    // Cache: last raw CPU tick snapshot for delta calculation
    // long[7] = { user, nice, system, idle, iowait, irq, softirq }
    private long[] lastCpuTicks = null;

    // Cache: last raw I/O byte snapshot for delta calculation
    // long[2] = { totalReadBytes, totalWriteBytes }
    private long[] lastIoBytes = null;

    // Last snapshot timestamp (ms) — used to scale I/O bytes → utilization
    private long lastSnapshotMs = 0;

    // Cached result from the most recent read — returned if /proc read fails
    private final AtomicReference<double[]> lastGoodMetrics =
            new AtomicReference<>(new double[]{0.0, 0.0, 0.0});

    public ProcfsMetricsReader() {
        this.procAvailable = Files.exists(Paths.get(PROC_STAT));
        if (!procAvailable) {
            System.out.println("[ProcfsMetricsReader] /proc not available "
                    + "(non-Linux?). Will return 0.0 for all metrics.");
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Read current system metrics.
     *
     * @return double[3]: [0] = CPU utilization  (0.0–1.0)
     *                    [1] = Memory utilization (0.0–1.0)
     *                    [2] = I/O utilization    (0.0–1.0, clamped)
     */
    public double[] readSystemMetrics() {
        if (!procAvailable) {
            return new double[]{0.0, 0.0, 0.0};
        }
        try {
            double cpu = readCpuUtilization();
            double mem = readMemoryUtilization();
            double io  = readIoUtilization();

            double[] result = {cpu, mem, io};
            lastGoodMetrics.set(result);
            return result;

        } catch (Exception e) {
            System.err.println("[ProcfsMetricsReader] Read error: " + e.getMessage());
            // Return last known-good values rather than crashing the window
            return lastGoodMetrics.get();
        }
    }

    // -----------------------------------------------------------------------
    // CPU — /proc/stat
    // -----------------------------------------------------------------------

    /**
     * Parses /proc/stat "cpu" aggregate line.
     *
     * Format: cpu  user nice system idle iowait irq softirq [steal guest]
     * All values are cumulative jiffies since boot.
     *
     * Utilization = (active_delta) / (total_delta)
     * where active = user + nice + system + irq + softirq
     *       idle   = idle + iowait
     *
     * Returns value in [0.0, 1.0].
     */
    double readCpuUtilization() throws IOException {
        long[] ticks = parseCpuTicks();

        if (lastCpuTicks == null) {
            // First call — no delta yet, snapshot and return 0
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
        long total  = active + idleDelta + iowaitDelta;

        if (total <= 0) return 0.0;
        return Math.min(1.0, (double) active / total);
    }

    private long[] parseCpuTicks() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(PROC_STAT))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("cpu ")) {   // aggregate line (space after "cpu")
                    String[] parts = line.trim().split("\\s+");
                    // parts[0]="cpu", parts[1]=user, [2]=nice, [3]=system,
                    // [4]=idle, [5]=iowait, [6]=irq, [7]=softirq
                    long[] ticks = new long[7];
                    for (int i = 0; i < 7; i++) {
                        ticks[i] = Long.parseLong(parts[i + 1]);
                    }
                    return ticks;
                }
            }
        }
        throw new IOException("Could not find 'cpu ' line in " + PROC_STAT);
    }

    // -----------------------------------------------------------------------
    // Memory — /proc/meminfo
    // -----------------------------------------------------------------------

    /**
     * Parses /proc/meminfo for MemTotal and MemAvailable.
     *
     * utilization = (MemTotal - MemAvailable) / MemTotal
     *
     * Returns value in [0.0, 1.0].
     */
    double readMemoryUtilization() throws IOException {
        long memTotalKb     = -1;
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
        return Math.min(1.0, (double) usedKb / memTotalKb);
    }

    /** Extract the numeric kB value from a /proc/meminfo line like "MemTotal: 2048000 kB" */
    private long parseKbValue(String line) {
        String[] parts = line.trim().split("\\s+");
        // parts[0] = "MemTotal:", parts[1] = "2048000", parts[2] = "kB"
        if (parts.length >= 2) {
            try { return Long.parseLong(parts[1]); }
            catch (NumberFormatException e) { /* fall through */ }
        }
        return 0;
    }

    // -----------------------------------------------------------------------
    // I/O — /proc/diskstats
    // -----------------------------------------------------------------------

    /**
     * Parses /proc/diskstats to sum read + write sectors across all physical disks.
     *
     * /proc/diskstats columns (kernel 2.6+):
     *   major minor name reads_completed ... sectors_read ... writes_completed ... sectors_written ...
     *   Column indices (0-based):  0=major, 1=minor, 2=name,
     *   3=reads_completed, 5=sectors_read, 7=writes_completed, 9=sectors_written
     *
     * We track delta sectors * 512 bytes = bytes transferred, then normalise
     * by elapsed time to get bytes/sec. We cap at a reference bandwidth
     * (100 MB/s = typical spinning disk) to produce a [0.0, 1.0] utilization.
     *
     * Only physical disk devices are counted (name matches sd*, vd*, hd*, nvme*).
     */
    double readIoUtilization() throws IOException {
        long nowMs = System.currentTimeMillis();
        long[] currentIo = parseDiskstats();

        if (lastIoBytes == null || lastSnapshotMs == 0) {
            lastIoBytes     = currentIo;
            lastSnapshotMs  = nowMs;
            return 0.0;
        }

        long readDelta  = currentIo[0] - lastIoBytes[0];
        long writeDelta = currentIo[1] - lastIoBytes[1];
        long elapsedMs  = nowMs - lastSnapshotMs;

        lastIoBytes    = currentIo;
        lastSnapshotMs = nowMs;

        if (elapsedMs <= 0) return 0.0;

        // Total bytes transferred in the window
        long totalBytes = readDelta + writeDelta;

        // Reference: 100 MB/s = typical spinning disk full-speed
        // Normalise: util = bytesPerSec / 100_000_000
        double bytesPerSec = (double) totalBytes / (elapsedMs / 1000.0);
        double util = bytesPerSec / 100_000_000.0;

        return Math.min(1.0, Math.max(0.0, util));
    }

    /**
     * Returns long[2] = { total_read_bytes, total_write_bytes } across all physical disks.
     * Sectors are multiplied by 512 to get bytes.
     */
    private long[] parseDiskstats() throws IOException {
        long totalReadBytes  = 0;
        long totalWriteBytes = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(PROC_DISKSTATS))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.trim().split("\\s+");
                // Need at least 10 columns
                if (p.length < 10) continue;

                String devName = p[2];

                // Only count physical block devices: sda, vda, hda, nvme0n1, xvda, etc.
                // Skip partitions (sda1, sda2), loop devices, ram disks
                if (!isPhysicalDisk(devName)) continue;

                try {
                    long sectorsRead    = Long.parseLong(p[5]);
                    long sectorsWritten = Long.parseLong(p[9]);
                    totalReadBytes  += sectorsRead    * 512L;
                    totalWriteBytes += sectorsWritten * 512L;
                } catch (NumberFormatException e) {
                    // Malformed line — skip
                }
            }
        }

        return new long[]{totalReadBytes, totalWriteBytes};
    }

    /**
     * Returns true for physical block devices we want to track.
     * Excludes partitions (sda1), loop (loop0), ram (ram0), dm-* (LVM).
     */
    private boolean isPhysicalDisk(String name) {
        if (name == null || name.isEmpty()) return false;
        // Match: sda, sdb, vda, hda, xvda, nvme0n1 — but NOT sda1, nvme0n1p1
        if (name.matches("sd[a-z]"))            return true;  // SCSI/SATA
        if (name.matches("vd[a-z]"))            return true;  // VirtIO
        if (name.matches("hd[a-z]"))            return true;  // IDE
        if (name.matches("xvd[a-z]"))           return true;  // Xen VBD
        if (name.matches("nvme\\d+n\\d+"))      return true;  // NVMe (no partition suffix)
        return false;
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    /** @return true if running on a Linux system with /proc available */
    public boolean isProcAvailable() {
        return procAvailable;
    }
}