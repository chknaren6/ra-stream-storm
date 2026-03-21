package com.rastream.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyRunner {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = WordCountTopology.buildTopology();

        Config conf = new Config();
        conf.setDebug(false);

        // Tell Storm to use RaStreamScheduler instead of the default EvenScheduler
        conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY,
                "com.rastream.scheduler.RaStreamScheduler");

        // Number of worker processes
        conf.setNumWorkers(2);

        // StormSubmitter for real cluster deployment
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordcount-rastream",
                conf, builder.createTopology());

        System.out.println("Topology submitted — running for 60s");
        Thread.sleep(60_000);

        cluster.killTopology("wordcount-rastream");
        cluster.close();
        System.out.println("Done");
    }
}