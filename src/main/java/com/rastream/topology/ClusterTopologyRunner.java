package com.rastream.topology;

import com.rastream.metrics.MetricsCSVWriter;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class ClusterTopologyRunner {

    public static void main(String[] args) throws Exception {

        System.out.println("=== Submitting Taxi Topology to Docker Cluster ===");

        TopologyBuilder builder = TaxiTopology.buildTopology();

        Config conf = new Config();
        conf.setDebug(false);

        // Number of worker JVMs across the cluster
        // 5 supervisors × 4 slots = 20 slots available
        conf.setNumWorkers(5);
        // Worker memory
        conf.registerMetricsConsumer(com.rastream.monitor.DataMonitor.class, 1);
        //conf.registerMetricsConsumer(
        //        com.rastream.metrics.StormMetricsConsumer.class, 1);
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 1536);
        // Tell Storm where nimbus is
        conf.put(Config.NIMBUS_SEEDS,
                java.util.Arrays.asList("localhost"));
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);

        conf.put(Config.STORM_ZOOKEEPER_SERVERS,
                java.util.Arrays.asList("localhost"));
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        // Submit topology — Storm handles distribution
        StormSubmitter.submitTopology(
                "taxi-rastream-docker",
                conf,
                builder.createTopology());

        System.out.println("Topology submitted successfully");
        System.out.println("Monitor at: http://localhost:8081");
    }
}