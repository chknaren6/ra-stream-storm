package com.rastream.monitor;

import com.rastream.dag.StreamApplication;
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

public class DataMonitor implements IMetricsConsumer {
    private final CommunicationModel commModel;
    private final ResourceModel resModel;
    private final StreamApplication app;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public DataMonitor(StreamApplication app, CommunicationModel comm, ResourceModel res) {
        this.app = app;
        this.commModel = comm;
        this.resModel = res;
    }

    @Override
    public void prepare(Map<String, Object> conf, Object reg, TopologyContext ctx, IErrorReporter err) {
        // Sample every 5 seconds (paper's statistical time frame st)
        scheduler.scheduleAtFixedRate(this::sample, 5, 5, TimeUnit.SECONDS);
        System.out.println("DataMonitor started");
    }

    private void sample() {
        // Update tuple rates for every edge (Eq.1 + Eq.2)
        commModel.updateAllEdges(app.getEdges());
        // Update node resources (you can extend with Linux /proc if needed)
        resModel.updateNodeUtilization(0.65, 0.60, 0.55); // placeholder – replace with real later
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        // Storm already sends __emit-count / __ack-count per task
        // You can enhance later to feed exact per-task counts
    }

    @Override
    public void cleanup() { scheduler.shutdown(); }
}