package com.rastream.topology;

import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputTracker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.Random;

public class WordCountTopology {
    // Shared metric trackers — static so all bolts access same instance
    // In a real cluster these would go through Storm's metrics API
    public static final LatencyTracker LATENCY_TRACKER
            = new com.rastream.metrics.LatencyTracker();
    public static final ThroughputTracker THROUGHPUT_TRACKER
            = new com.rastream.metrics.ThroughputTracker();

    // v1: reader — reads data tuples (Spout in Storm)
    // Parallelism = 2 tasks, matching paper Figure 2
    public static class ReaderSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private final Random random = new Random();

        // Sample sentences — in real deployment this
        // reads from Kafka using Alibaba Tianchi dataset
        private static final String[] SENTENCES = {
                "the quick brown fox jumps over the lazy dog",
                "stream computing systems demonstrate exemplary performance",
                "resource scaling is crucial for stream computing",
                "apache storm is a distributed real time computation system",
                "fine grained task scheduling reduces communication cost"
        };

        @Override
        public void open(Map<String, Object> conf,
                         TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            String sentence = SENTENCES[random.nextInt(SENTENCES.length)];
            String tupleId  = java.util.UUID.randomUUID().toString();

            // Record emit time for latency tracking
            WordCountTopology.LATENCY_TRACKER.recordEmit(tupleId);

            collector.emit(new Values(sentence, tupleId));
            try { Thread.sleep(1); } catch (InterruptedException e) {}
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // Add tupleId field so it flows through the topology
            declarer.declare(new Fields("sentence", "tupleId"));
        }
    }
    // v2: split — splits sentences into words
    // Parallelism = 3 tasks, matching paper Figure 2
    public static class SplitBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String tupleId  = tuple.getStringByField("tupleId");
            for (String word : sentence.split("\\s+")) {
                collector.emit(new Values(word, tupleId));
            }
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "tupleId"));
        }
    }
    // v3: count — counts word occurrences
    // Parallelism = 3 tasks, matching paper Figure 2
    public static class CountBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;

        // In-memory word count store
        private final Map<String, Integer> counts =
                new java.util.HashMap<>();

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple tuple) {
            String word    = tuple.getStringByField("word");
            String tupleId = tuple.getStringByField("tupleId");
            counts.merge(word, 1, Integer::sum);
            collector.emit(new Values(word, counts.get(word), tupleId));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count", "tupleId"));
        }
    }

    // v4: out — outputs results
    // Parallelism = 2 tasks, matching paper Figure 2
    public static class OutputBolt
            extends org.apache.storm.topology.base.BaseRichBolt {

        private org.apache.storm.task.OutputCollector collector;
        private long tupleCount = 0;

        @Override
        public void prepare(Map<String, Object> conf,
                            TopologyContext context,
                            org.apache.storm.task.OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(org.apache.storm.tuple.Tuple tuple) {
            String tupleId = tuple.getStringByField("tupleId");

            // Record latency and throughput
            WordCountTopology.LATENCY_TRACKER.recordReceive(tupleId);
            WordCountTopology.THROUGHPUT_TRACKER.recordTuple();

            tupleCount++;
            if (tupleCount % 1000 == 0) {
                System.out.println("[Output] tuples=" + tupleCount
                        + " | avg_latency="
                        + String.format("%.2f",
                        WordCountTopology.LATENCY_TRACKER
                                .getAverageLatencyMs()) + "ms"
                        + " | throughput="
                        + String.format("%.0f",
                        WordCountTopology.THROUGHPUT_TRACKER
                                .getThroughput()) + " t/s");
            }
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(
                OutputFieldsDeclarer declarer) {
            // Terminal bolt — no output fields needed
            declarer.declare(new Fields());
        }
    }

    // Build the full topology — matches Figure 1 of the paper
    // reader(2) → split(3) → count(3) → out(2)
    public static TopologyBuilder buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        // v1: reader spout — 2 parallel tasks
        builder.setSpout("reader", new ReaderSpout(), 2);

        // v2: split bolt — 3 parallel tasks
        // shuffleGrouping = distribute tuples across all split tasks
        builder.setBolt("split", new SplitBolt(), 3)
                .shuffleGrouping("reader");

        // v3: count bolt — 3 parallel tasks
        // fieldsGrouping on "word" = same word always goes
        // to same count task (ensures correct counting)
        builder.setBolt("count", new CountBolt(), 3)
                .fieldsGrouping("split", new Fields("word"));

        // v4: output bolt — 2 parallel tasks
        builder.setBolt("out", new OutputBolt(), 2)
                .shuffleGrouping("count");

        return builder;
    }
}