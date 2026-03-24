package com.rastream.monitor;

import com.rastream.dag.Edge;
import com.rastream.dag.StreamApplication;
import com.rastream.dag.Task;
import com.rastream.model.CommunicationModel;
import com.rastream.model.ResourceModel;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors Storm topology metrics and updates communication/resource models.
 *
 * Changes from previous version:
 *   1. ProcfsMetricsReader replaces the hardcoded (0.65, 0.60, 0.55) placeholder.
 *      Every stat window it reads real CPU/mem/IO from /proc and calls
 *      resModel.updateNodeUtilization() with live values (Equations 3-6).
 *
 *   2. SchedulingController is optional. Wire it in via setSchedulingController()
 *      before the topology starts. Its own internal timer fires every 30s
 *      independently of this class's 5s window timer.
 */
public class DataMonitor implements IMetricsConsumer {

    private final CommunicationModel commModel;
    private final ResourceModel      resModel;
    private final StreamApplication  app;

    private final TaskMetricsTracker  metricsTracker;
    private final EdgeRateCalculator  rateCalc;
    private final ProcfsMetricsReader metricsReader;   // NEW — real /proc metrics

    // Optional: dynamic rescheduling feedback loop (Figure 7)
    private volatile SchedulingController schedulingController = null;

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);

    private static final int STAT_WINDOW_SEC = 5;  // Paper Table 4

    private final AtomicLong windowCount  = new AtomicLong(0);
    private final AtomicLong edgesUpdated = new AtomicLong(0);

    // -------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------

    public DataMonitor(StreamApplication app,
                       CommunicationModel comm,
                       ResourceModel res) {
        this.app            = app;
        this.commModel      = comm;
        this.resModel       = res;
        this.metricsTracker = new TaskMetricsTracker();
        this.rateCalc       = new EdgeRateCalculator(app);
        this.metricsReader  = new ProcfsMetricsReader();
    }

    /**
     * Wire in the SchedulingController after construction.
     * The controller starts its own 30-second timer independently.
     *
     * Example (in TopologyRunner or RaStreamScheduler):
     *   SchedulingController ctrl = new SchedulingController(
     *       app, commModel, resModel, partitioner, scaler, clusterSize);
     *   monitor.setSchedulingController(ctrl);
     *   ctrl.start();
     */
    public void setSchedulingController(SchedulingController controller) {
        this.schedulingController = controller;
        System.out.println("[DataMonitor] SchedulingController registered.");
    }

    // -------------------------------------------------------------------
    // IMetricsConsumer lifecycle
    // -------------------------------------------------------------------

    @Override
    public void prepare(Map<String, Object> conf,
                        Object registrationArgument,
                        TopologyContext context,
                        IErrorReporter errorReporter) {
        scheduler.scheduleAtFixedRate(
                this::sampleWindow,
                STAT_WINDOW_SEC,
                STAT_WINDOW_SEC,
                TimeUnit.SECONDS
        );
        System.out.println("[DataMonitor] Started."
                + "  app="   + app.getName()
                + "  edges=" + app.getEdges().size()
                + "  /proc=" + metricsReader.isProcAvailable());
    }

    /**
     * Storm calls this ~every second for every task.
     * Collects __emit-count into TaskMetricsTracker for the current window.
     *
     * Uses taskInfo.srcComponentId — correct Storm 2.x field (not srcComponentName).
     */
    @Override
    public void handleDataPoints(TaskInfo taskInfo,
                                 Collection<DataPoint> dataPoints) {
        String componentId = taskInfo.srcComponentId;
        int    taskId      = taskInfo.srcTaskId;

        for (DataPoint dp : dataPoints) {
            if ("__emit-count".equals(dp.name)
                    && dp.value instanceof Number) {

                long emitCount = ((Number) dp.value).longValue();
                metricsTracker.recordEmitCount(componentId, taskId, emitCount);

                if (emitCount > 0 && emitCount % 10_000 == 0) {
                    System.out.println("[DataMonitor] "
                            + componentId + "-" + taskId
                            + " cumulative emits=" + emitCount);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // Stat-window sampling (Equations 1-6)
    // -------------------------------------------------------------------

    /**
     * Fires every STAT_WINDOW_SEC (5) seconds.
     *
     * Step 1 — Real resource metrics (Equations 3-6):
     *   Reads /proc/stat, /proc/meminfo, /proc/diskstats via ProcfsMetricsReader.
     *   Calls resModel.updateNodeUtilization(cpu, mem, io) with live values.
     *   On non-Linux systems falls back to 0.0 gracefully.
     *
     * Step 2 — Tuple rates (Equations 1-2):
     *   Computes delta emit counts per edge → commModel.recordSample().
     *
     * Step 3 — Eq.1 smoothing:
     *   commModel.updateAllEdges() drops outliers and sets each Edge's Tr.
     *
     * Step 4 — SchedulingController status log:
     *   If a controller is registered, logs how many reschedules have fired.
     *   The controller's own timer handles the actual check independently.
     */
    private void sampleWindow() {
        try {
            long winNum  = windowCount.incrementAndGet();
            long startMs = System.currentTimeMillis();

            // ----------------------------------------------------------
            // Step 1: Real resource metrics → ResourceModel (Eq. 3-6)
            // ----------------------------------------------------------
            double[] sys   = metricsReader.readSystemMetrics();
            double cpuUtil = sys[0];
            double memUtil = sys[1];
            double ioUtil  = sys[2];
            resModel.updateNodeUtilization(cpuUtil, memUtil, ioUtil);

            if (winNum % 3 == 0) {
                System.out.printf("[DataMonitor] win=%d  cpu=%.3f  mem=%.3f  io=%.3f%n",
                        winNum, cpuUtil, memUtil, ioUtil);
            }

            // ----------------------------------------------------------
            // Step 2: Tuple transmission rates → CommunicationModel (Eq. 1-2)
            // ----------------------------------------------------------
            long edgesProc = 0;
            for (Edge edge : app.getEdges()) {
                Task src = edge.getSource();
                Task tgt = edge.getTarget();

                long emitCount = metricsTracker.getEmitCount(
                        src.getFunctionName(),
                        src.getTaskId());

                double rate = rateCalc.calculateEdgeRate(
                        src.getId(),
                        tgt.getId(),
                        emitCount,
                        STAT_WINDOW_SEC);

                commModel.recordSample(src, tgt, rate);
                edgesProc++;

                if (winNum == 1 || winNum % 3 == 0) {
                    System.out.printf("[DataMonitor] win=%d  %s→%s  raw=%.2f t/s%n",
                            winNum, src.getId(), tgt.getId(), rate);
                }
            }

            // ----------------------------------------------------------
            // Step 3: Apply Eq.1 smoothing to all edges
            // ----------------------------------------------------------
            commModel.updateAllEdges(app.getEdges());

            if (winNum % 3 == 0) {
                System.out.println("[DataMonitor] === WINDOW " + winNum + " SUMMARY ===");
                for (Edge edge : app.getEdges()) {
                    System.out.println("  " + edge);
                }
                System.out.printf("  Total edge weight: %.2f%n",
                        app.getTotalEdgeWeight());
            }

            rateCalc.updatePreviousEmitCounts(metricsTracker);
            metricsTracker.reset();
            edgesUpdated.addAndGet(edgesProc);

            // ----------------------------------------------------------
            // Step 4: SchedulingController status
            // ----------------------------------------------------------
            SchedulingController ctrl = schedulingController;
            if (ctrl != null && winNum % 6 == 0) {
                System.out.printf("[DataMonitor] win=%d — controller active, "
                                + "reschedules=%d, ready=%b%n",
                        winNum, ctrl.getRescheduleCount(), ctrl.isRescheduleReady());
            }

            System.out.printf("[DataMonitor] Window %d done in %dms, %d edges%n",
                    winNum,
                    System.currentTimeMillis() - startMs,
                    edgesProc);

        } catch (Exception e) {
            System.err.println("[DataMonitor] sampleWindow error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // -------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------

    @Override
    public void cleanup() {
        System.out.printf("[DataMonitor] Shutdown. windows=%d, edgesUpdated=%d%n",
                windowCount.get(), edgesUpdated.get());
        scheduler.shutdown();
        SchedulingController ctrl = schedulingController;
        if (ctrl != null) ctrl.stop();
    }

    // -------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------

    public long getWindowCount()                    { return windowCount.get(); }
    public TaskMetricsTracker getMetricsTracker()   { return metricsTracker; }
    public StreamApplication getApp()               { return app; }
    public ProcfsMetricsReader getMetricsReader()   { return metricsReader; }
}