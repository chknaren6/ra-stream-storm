package com.rastream.monitor;

import com.rastream.allocation.ResourceScaler;
import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ResourceModel;
import com.rastream.partitioning.PartitionScheme;
import com.rastream.partitioning.Subgraph;
import com.rastream.partitioning.SubgraphPartitioner;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements the Figure 7 "Scheduling trigger" feedback loop from the paper.
 *
 * Every RECHECK_INTERVAL_SEC seconds it checks:
 *   1. Has any edge's tuple rate changed by > RATE_CHANGE_THRESHOLD (30%)?
 *      → Re-run SubgraphPartitioner (Algorithm 1) + ResourceScaler (Algorithm 2)
 *   2. Has overall node utilization drifted by > UTILIZATION_DRIFT_THRESHOLD (20%)?
 *      → Re-run ResourceScaler (Algorithm 2) only
 *
 * When a trigger fires, the new PartitionScheme / scaled subgraph list is
 * stored and exposed via getLatestSubgraphs() so RaStreamScheduler can pick
 * it up on its next scheduling cycle.
 *
 * NOTE: full live Storm re-assignment (cluster.assign()) is handled by
 * RaStreamScheduler.schedule() — SchedulingController only computes *what*
 * the new assignment should be and signals that a reschedule is needed.
 */
public class SchedulingController {

    // -------------------------------------------------------------------
    // Thresholds (paper Section 5.1)
    // -------------------------------------------------------------------

    /** Fractional rate change that triggers re-partitioning (30%). */
    private static final double RATE_CHANGE_THRESHOLD = 0.30;

    /** Fractional utilization drift that triggers re-scaling (20%). */
    private static final double UTILIZATION_DRIFT_THRESHOLD = 0.20;

    /** How often the controller checks metrics (seconds). */
    private static final int RECHECK_INTERVAL_SEC = 30;

    // -------------------------------------------------------------------
    // Dependencies injected at construction
    // -------------------------------------------------------------------

    private final StreamApplication      app;
    private final CommunicationModel     commModel;
    private final ResourceModel          resModel;
    private final SubgraphPartitioner    partitioner;
    private final ResourceScaler         resourceScaler;
    private final int                    clusterNodeCount;

    // -------------------------------------------------------------------
    // Internal state
    // -------------------------------------------------------------------

    // Baseline edge rates captured after the first warm-up window
    // key = edge.toString(), value = baseline Tr
    private final java.util.concurrent.ConcurrentHashMap<String, Double> baselineRates =
            new java.util.concurrent.ConcurrentHashMap<>();

    // Baseline utilization captured after first warm-up window
    private volatile double baselineUtilization = -1.0;

    // Latest scaled subgraph list produced by the last reschedule
    private final AtomicReference<List<Subgraph>> latestSubgraphs =
            new AtomicReference<>(null);

    // Flag: set to true when a new plan is ready for RaStreamScheduler to consume
    private final AtomicBoolean rescheduleReady = new AtomicBoolean(false);

    // How many reschedule events have fired
    private final AtomicLong rescheduleCount = new AtomicLong(0);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "scheduling-controller");
                t.setDaemon(true);
                return t;
            });

    private ScheduledFuture<?> checkTask;

    // -------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------

    public SchedulingController(StreamApplication app,
                                CommunicationModel commModel,
                                ResourceModel resModel,
                                SubgraphPartitioner partitioner,
                                ResourceScaler resourceScaler,
                                int clusterNodeCount) {
        this.app              = app;
        this.commModel        = commModel;
        this.resModel         = resModel;
        this.partitioner      = partitioner;
        this.resourceScaler   = resourceScaler;
        this.clusterNodeCount = clusterNodeCount;
    }

    // -------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------

    /** Start the periodic check loop. Call once after topology is submitted. */
    public void start() {
        checkTask = scheduler.scheduleAtFixedRate(
                this::checkAndRescheduleIfNeeded,
                RECHECK_INTERVAL_SEC,
                RECHECK_INTERVAL_SEC,
                TimeUnit.SECONDS
        );
        System.out.println("[SchedulingController] Started. "
                + "Check interval=" + RECHECK_INTERVAL_SEC + "s, "
                + "rate threshold=" + (RATE_CHANGE_THRESHOLD * 100) + "%, "
                + "util threshold=" + (UTILIZATION_DRIFT_THRESHOLD * 100) + "%");
    }

    /** Stop the controller cleanly. */
    public void stop() {
        if (checkTask != null) checkTask.cancel(false);
        scheduler.shutdown();
        System.out.println("[SchedulingController] Stopped after "
                + rescheduleCount.get() + " reschedules.");
    }

    // -------------------------------------------------------------------
    // Main check loop
    // -------------------------------------------------------------------

    /**
     * Fired every RECHECK_INTERVAL_SEC seconds.
     *
     * Decision tree (Figure 7):
     *   baseline not set → capture baseline, return
     *   rate change > 30% → full re-partition + re-scale
     *   util drift > 20%  → re-scale only
     *   otherwise         → no action
     */
    private void checkAndRescheduleIfNeeded() {
        try {
            // --- Step 1: capture baseline on first run ---
            if (baselineUtilization < 0) {
                captureBaseline();
                System.out.println("[SchedulingController] Baseline captured. "
                        + "util=" + String.format("%.3f", baselineUtilization)
                        + "  edges=" + baselineRates.size());
                return;
            }

            // --- Step 2: measure current state ---
            double currentUtil   = resModel.getNodeTotalUtilization();
            double maxRateChange = computeMaxRateChange();

            System.out.printf("[SchedulingController] Check #%d: "
                            + "maxRateChange=%.1f%%  utilDrift=%.1f%%%n",
                    rescheduleCount.get() + 1,
                    maxRateChange * 100,
                    Math.abs(currentUtil - baselineUtilization) * 100);

            // --- Step 3: decide what to do ---
            if (maxRateChange > RATE_CHANGE_THRESHOLD) {
                System.out.println("[SchedulingController] Rate change exceeded "
                        + (RATE_CHANGE_THRESHOLD * 100) + "% — full re-partition + re-scale");
                runFullReschedule();

            } else if (Math.abs(currentUtil - baselineUtilization)
                    > UTILIZATION_DRIFT_THRESHOLD) {
                System.out.println("[SchedulingController] Utilization drift exceeded "
                        + (UTILIZATION_DRIFT_THRESHOLD * 100) + "% — re-scale only");
                runRescaleOnly();

            } else {
                System.out.println("[SchedulingController] No action needed.");
            }

        } catch (Exception e) {
            System.err.println("[SchedulingController] Check error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // -------------------------------------------------------------------
    // Reschedule actions
    // -------------------------------------------------------------------

    /**
     * Full reschedule: re-partition (Algorithm 1) → re-scale (Algorithm 2).
     * Used when data stream rate has shifted significantly.
     */
    private void runFullReschedule() {
        long start = System.currentTimeMillis();

        // Algorithm 1 — re-partition with current edge weights
        PartitionScheme newScheme = partitioner.partition(app, clusterNodeCount);

        // Algorithm 2 — scale subgraphs to fit utilization thresholds
        List<Subgraph> newSubgraphs =
                resourceScaler.scale(newScheme, app.getEdges());

        // Publish result
        latestSubgraphs.set(newSubgraphs);
        rescheduleReady.set(true);
        long count = rescheduleCount.incrementAndGet();

        // Reset baseline to current state
        captureBaseline();

        System.out.printf("[SchedulingController] Full reschedule #%d done in %dms. "
                        + "New subgraph count=%d%n",
                count,
                System.currentTimeMillis() - start,
                newSubgraphs.size());
    }

    /**
     * Rescale only: re-run Algorithm 2 on the existing partition.
     * Used when utilization drifts but stream rates are stable.
     */
    private void runRescaleOnly() {
        long start = System.currentTimeMillis();

        // Re-use last partition scheme — just re-scale it
        // We need a current partition; if none exists, fall back to full reschedule
        List<Subgraph> current = latestSubgraphs.get();
        if (current == null) {
            System.out.println("[SchedulingController] No existing subgraphs — "
                    + "falling back to full reschedule.");
            runFullReschedule();
            return;
        }

        // Wrap current subgraphs back into a PartitionScheme for the scaler
        com.rastream.partitioning.PartitionScheme tempScheme =
                new com.rastream.partitioning.PartitionScheme(app.getEdges());
        for (Subgraph s : current) {
            tempScheme.addSubgraph(s);
        }

        List<Subgraph> rescaled =
                resourceScaler.scale(tempScheme, app.getEdges());

        latestSubgraphs.set(rescaled);
        rescheduleReady.set(true);
        long count = rescheduleCount.incrementAndGet();

        // Reset baseline utilization
        baselineUtilization = resModel.getNodeTotalUtilization();

        System.out.printf("[SchedulingController] Re-scale #%d done in %dms. "
                        + "Subgraph count=%d%n",
                count,
                System.currentTimeMillis() - start,
                rescaled.size());
    }

    // -------------------------------------------------------------------
    // Baseline helpers
    // -------------------------------------------------------------------

    private void captureBaseline() {
        // Snapshot current utilization
        baselineUtilization = resModel.getNodeTotalUtilization();

        // Snapshot current edge Tr values
        baselineRates.clear();
        for (Edge edge : app.getEdges()) {
            baselineRates.put(
                    edge.getSource().getId() + "->" + edge.getTarget().getId(),
                    edge.getTupleTransmissionRate());
        }
    }

    /**
     * Returns the maximum fractional rate change across all edges since baseline.
     *
     * fractionalChange = |current - baseline| / max(baseline, 1.0)
     *
     * Using max(baseline, 1.0) avoids division-by-zero when baseline is 0
     * (e.g., at startup before any tuples flow).
     */
    private double computeMaxRateChange() {
        double maxChange = 0.0;
        for (Edge edge : app.getEdges()) {
            String key = edge.getSource().getId() + "->" + edge.getTarget().getId();
            double baseline = baselineRates.getOrDefault(key, 0.0);
            double current  = edge.getTupleTransmissionRate();

            double denominator = Math.max(baseline, 1.0);
            double change = Math.abs(current - baseline) / denominator;
            if (change > maxChange) maxChange = change;
        }
        return maxChange;
    }

    // -------------------------------------------------------------------
    // Public accessors — consumed by RaStreamScheduler
    // -------------------------------------------------------------------

    /**
     * Returns true if a new scheduling plan is ready to be applied.
     * Call consumeLatestSubgraphs() to retrieve it and clear the flag.
     */
    public boolean isRescheduleReady() {
        return rescheduleReady.get();
    }

    /**
     * Retrieves the latest scaled subgraph list and clears the ready flag.
     * Returns null if no reschedule has fired yet.
     */
    public List<Subgraph> consumeLatestSubgraphs() {
        rescheduleReady.set(false);
        return latestSubgraphs.get();
    }

    /** How many reschedule events have fired since start. */
    public long getRescheduleCount() {
        return rescheduleCount.get();
    }
}