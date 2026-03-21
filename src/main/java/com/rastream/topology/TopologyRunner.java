package com.rastream.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyRunner {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = WordCountTopology.buildTopology();
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordcount-rastream",
                conf, builder.createTopology());

        System.out.println("Topology running — will report every 1000 tuples");
        System.out.println("Press Ctrl+C to stop early\n");

        // Run for 60 seconds
        Thread.sleep(60_000);

        // Print final metrics
        WordCountTopology.LATENCY_TRACKER.printReport();
        WordCountTopology.THROUGHPUT_TRACKER.printReport();

        cluster.killTopology("wordcount-rastream");
        cluster.close();
    }
}