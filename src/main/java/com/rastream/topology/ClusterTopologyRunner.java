package com.rastream.topology;

import com.rastream.metrics.MetricsCSVWriter;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * ClusterTopologyRunner — submits the Taxi topology to the Docker cluster.
 *
 * Before running:
 *   1. docker-compose up -d       (bring up all 15 containers)
 *   2. mvn package -DskipTests    (build ra-stream.jar)
 *   3. Run this class from the nimbus container:
 *        docker exec ra-nimbus java -cp /opt/storm/lib/ra-stream.jar \
 *          com.rastream.topology.ClusterTopologyRunner
 *
 * Or submit via storm CLI:
 *   docker exec ra-nimbus storm jar /opt/storm/lib/ra-stream.jar \
 *     com.rastream.topology.ClusterTopologyRunner
 *
 * The Ra-Stream scheduler (IScheduler) takes over automatically
 * because storm.yaml sets storm.scheduler to RaStreamScheduler.
 */
public class ClusterTopologyRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Ra-Stream: Submitting Taxi Topology to Cluster ===");
        System.out.println("Dataset: NYC Taxi (set CSV_PATH env or /data/taxi/combined.csv)");

        TopologyBuilder builder = TaxiTopology.buildTopology();

        Config conf = new Config();
        conf.setDebug(false);

        // 14 supervisors × 4 slots = 56 slots available
        // Paper uses up to 14 workers depending on stream rate
        conf.setNumWorkers(14);

        // 1.5GB heap per worker (paper: 2GB node minus OS overhead)
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1536);

        // Storm UI / Nimbus coordinates (Docker service names)
        conf.put(Config.NIMBUS_SEEDS,
                java.util.Arrays.asList("nimbus"));
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS,
                java.util.Arrays.asList("nimbus"));
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        // Register DataMonitor as IMetricsConsumer so Storm calls
        // handleDataPoints() for every task every tick.
        // DataMonitor feeds CommunicationModel + ResourceModel.
        conf.registerMetricsConsumer(
                com.rastream.monitor.DataMonitor.class, 1);

        // Stat window = 5 seconds (paper Table 4)
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);

        // Max pending tuples before backpressure kicks in
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);

        // Ack timeout — must be > max processing time through pipeline
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);

        StormSubmitter.submitTopology(
                "taxi-rastream-cluster",
                conf,
                builder.createTopology());

        System.out.println("=== Topology submitted ===");
        System.out.println("Monitor: http://localhost:8081");
        System.out.println("Metrics: docker exec ra-nimbus cat /metrics/master.csv");
    }
}