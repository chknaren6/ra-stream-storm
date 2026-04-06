package com.rastream.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyRunner {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = TaxiTopology.buildTopology();

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        // 🔥 CRITICAL FOR FAULT TOLERANCE
        conf.setNumAckers(2);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 30);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("taxi-topology", conf, builder.createTopology());

        // Run for some time
        Thread.sleep(60000);

        cluster.killTopology("taxi-topology");
        cluster.shutdown();
    }
}