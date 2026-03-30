package com.rastream.metrics;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * MetricsAggregator — runs on Nimbus.
 *
 * Polls all node CSVs under METRICS_DIR and appends unseen rows to master.csv.
 * Keeps persistent seen-lines state to avoid duplicates.
 *
 * Expected per-node schema (recommended):
 * timestamp,node,scheduler,baseline,run_id,stream_profile,cluster_size,topology,stream_rate_tps,
 * avg_latency_ms,max_latency_ms,min_latency_ms,throughput_tps,tuple_count,
 * avg_cpu_pct,avg_mem_pct,avg_io_pct,avg_net_rx_bps,avg_net_tx_bps,avg_net_util_pct,stage,notes
 */
public class MetricsAggregator {

    private static final int POLL_INTERVAL_SEC = 10;
    private static final String METRICS_DIR = System.getenv("METRICS_PATH") != null
            ? System.getenv("METRICS_PATH") : "/metrics";
    private static final String MASTER_CSV = METRICS_DIR + "/master.csv";

    // Master header (canonical)
    private static final String HEADER =
            "timestamp,node,scheduler,baseline,run_id,stream_profile,cluster_size,topology,stream_rate_tps,"
                    + "avg_latency_ms,max_latency_ms,min_latency_ms,throughput_tps,tuple_count,"
                    + "avg_cpu_pct,avg_mem_pct,avg_io_pct,avg_net_rx_bps,avg_net_tx_bps,avg_net_util_pct,stage,notes";

    // Persist poll state across invocations
    private static final Map<String, Integer> seenLines = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("[MetricsAggregator] Starting. Metrics dir: " + METRICS_DIR);
        new File(METRICS_DIR).mkdirs();
        ensureHeader();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                MetricsAggregator::pollAndMerge,
                0, POLL_INTERVAL_SEC, TimeUnit.SECONDS);

        Thread.currentThread().join();
    }

    private static void ensureHeader() throws IOException {
        File f = new File(MASTER_CSV);
        if (!f.exists() || f.length() == 0) {
            try (PrintWriter pw = new PrintWriter(new FileWriter(MASTER_CSV))) {
                pw.println(HEADER);
            }
        }
    }

    private static void pollAndMerge() {
        try {
            File dir = new File(METRICS_DIR);
            File[] csvFiles = dir.listFiles((d, name) ->
                    name.endsWith(".csv") && !name.equals("master.csv"));

            if (csvFiles == null || csvFiles.length == 0) return;

            List<String> newRows = new ArrayList<>();

            // aggregate summary
            double totalLatency = 0.0;
            double totalThroughput = 0.0;
            double totalCpu = 0.0;
            double totalMem = 0.0;
            double totalIo = 0.0;
            double totalNetUtil = 0.0;
            int sampleCount = 0;

            for (File csv : csvFiles) {
                String fileKey = csv.getName();
                int skip = seenLines.getOrDefault(fileKey, 1); // 1 => header consumed

                try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
                    String line;
                    int lineNum = 0;

                    String headerLine = br.readLine();
                    lineNum++;
                    if (headerLine == null) {
                        seenLines.put(fileKey, lineNum);
                        continue;
                    }

                    List<String> headers = parseCsvLine(headerLine);
                    Map<String, Integer> idx = indexMap(headers);

                    while ((line = br.readLine()) != null) {
                        lineNum++;
                        if (lineNum <= skip) continue;

                        List<String> cols = parseCsvLine(line);
                        if (cols.isEmpty()) continue;

                        // normalize row to canonical master schema
                        String normalized = normalizeToMasterRow(cols, idx, fileKey.replace(".csv", ""));
                        newRows.add(normalized);

                        // aggregate if parsable
                        totalLatency += parseDoubleByName(cols, idx, "avg_latency_ms");
                        totalThroughput += parseDoubleByName(cols, idx, "throughput_tps");
                        totalCpu += parseDoubleByName(cols, idx, "avg_cpu_pct");
                        totalMem += parseDoubleByName(cols, idx, "avg_mem_pct");
                        totalIo += parseDoubleByName(cols, idx, "avg_io_pct");
                        totalNetUtil += parseDoubleByName(cols, idx, "avg_net_util_pct");
                        sampleCount++;
                    }

                    seenLines.put(fileKey, lineNum);

                } catch (IOException e) {
                    System.err.println("[MetricsAggregator] Cannot read " + csv.getName() + ": " + e.getMessage());
                }
            }

            if (!newRows.isEmpty()) {
                try (PrintWriter pw = new PrintWriter(new FileWriter(MASTER_CSV, true))) {
                    for (String row : newRows) {
                        pw.println(row);
                    }
                }
                System.out.println("[MetricsAggregator] Appended " + newRows.size() + " rows to master.csv");
            }

            if (sampleCount > 0) {
                System.out.printf("[MetricsAggregator] CLUSTER SUMMARY: samples=%d avg_latency=%.2fms total_throughput=%.2f t/s avg_cpu=%.2f%% avg_mem=%.2f%% avg_io=%.2f%% avg_net=%.2f%%%n",
                        sampleCount,
                        totalLatency / sampleCount,
                        totalThroughput,
                        totalCpu / sampleCount,
                        totalMem / sampleCount,
                        totalIo / sampleCount,
                        totalNetUtil / sampleCount);
            }

        } catch (Exception e) {
            System.err.println("[MetricsAggregator] Poll error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> indexMap(List<String> headers) {
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            idx.put(headers.get(i).trim(), i);
        }
        return idx;
    }

    private static double parseDoubleByName(List<String> cols, Map<String, Integer> idx, String name) {
        Integer i = idx.get(name);
        if (i == null || i < 0 || i >= cols.size()) return 0.0;
        try {
            return Double.parseDouble(cols.get(i));
        } catch (Exception ignored) {
            return 0.0;
        }
    }

    private static String valueOr(List<String> cols, Map<String, Integer> idx, String name, String def) {
        Integer i = idx.get(name);
        if (i == null || i < 0 || i >= cols.size()) return def;
        String v = cols.get(i);
        return v == null || v.isEmpty() ? def : v;
    }

    private static String normalizeToMasterRow(List<String> cols, Map<String, Integer> idx, String nodeNameFallback) {
        // canonical order:
        // timestamp,node,scheduler,baseline,run_id,stream_profile,cluster_size,topology,stream_rate_tps,
        // avg_latency_ms,max_latency_ms,min_latency_ms,throughput_tps,tuple_count,
        // avg_cpu_pct,avg_mem_pct,avg_io_pct,avg_net_rx_bps,avg_net_tx_bps,avg_net_util_pct,stage,notes

        String timestamp       = valueOr(cols, idx, "timestamp", "");
        String node            = valueOr(cols, idx, "node", nodeNameFallback);
        String scheduler       = valueOr(cols, idx, "scheduler", "unknown");
        String baseline        = valueOr(cols, idx, "baseline", "none");
        String runId           = valueOr(cols, idx, "run_id", "na");
        String profile         = valueOr(cols, idx, "stream_profile", "default");
        String clusterSize     = valueOr(cols, idx, "cluster_size", "0");
        String topology        = valueOr(cols, idx, "topology", "unknown");
        String streamRate      = valueOr(cols, idx, "stream_rate_tps", "0");
        String avgLatency      = valueOr(cols, idx, "avg_latency_ms", "0");
        String maxLatency      = valueOr(cols, idx, "max_latency_ms", "0");
        String minLatency      = valueOr(cols, idx, "min_latency_ms", "0");
        String throughput      = valueOr(cols, idx, "throughput_tps", "0");
        String tupleCount      = valueOr(cols, idx, "tuple_count", "0");
        String avgCpu          = valueOr(cols, idx, "avg_cpu_pct", "0");
        String avgMem          = valueOr(cols, idx, "avg_mem_pct", "0");
        String avgIo           = valueOr(cols, idx, "avg_io_pct", "0");
        String netRx           = valueOr(cols, idx, "avg_net_rx_bps", "0");
        String netTx           = valueOr(cols, idx, "avg_net_tx_bps", "0");
        String netUtil         = valueOr(cols, idx, "avg_net_util_pct", "0");
        String stage           = valueOr(cols, idx, "stage", "");
        String notes           = valueOr(cols, idx, "notes", "");

        return String.join(",",
                escapeCsv(timestamp), escapeCsv(node), escapeCsv(scheduler), escapeCsv(baseline),
                escapeCsv(runId), escapeCsv(profile), escapeCsv(clusterSize), escapeCsv(topology),
                escapeCsv(streamRate), escapeCsv(avgLatency), escapeCsv(maxLatency), escapeCsv(minLatency),
                escapeCsv(throughput), escapeCsv(tupleCount), escapeCsv(avgCpu), escapeCsv(avgMem),
                escapeCsv(avgIo), escapeCsv(netRx), escapeCsv(netTx), escapeCsv(netUtil),
                escapeCsv(stage), escapeCsv(notes));
    }

    // minimal CSV parser that supports quoted commas
    private static List<String> parseCsvLine(String line) {
        List<String> out = new ArrayList<>();
        if (line == null) return out;
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    private static String escapeCsv(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"") || s.contains("\n")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }
}