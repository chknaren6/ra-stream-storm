package com.rastream.topology;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class KafkaDataProducer {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Random random = new Random();
    private volatile boolean running = true;

    // Sample data matching Alibaba Tianchi text dataset style
    private static final String[] WORDS = {
            "stream", "computing", "resource", "scaling", "task",
            "scheduling", "latency", "throughput", "distributed",
            "apache", "storm", "flink", "kafka", "cluster", "node",
            "partition", "subgraph", "communication", "cost", "data"
    };

    public KafkaDataProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }
    // Generates a random sentence from word pool
    private String generateSentence() {
        int wordCount = 5 + random.nextInt(6);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) sb.append(" ");
            sb.append(WORDS[random.nextInt(WORDS.length)]);
        }
        return sb.toString();
    }

    // Send tuples at a given rate (tuples per second)
    private void sendAtRate(int tuplesPerSecond,
                            long durationMs) throws InterruptedException {
        long startTime    = System.currentTimeMillis();
        long intervalMs   = 1000L / tuplesPerSecond;

        System.out.println("Sending at " + tuplesPerSecond
                + " tuples/s for " + durationMs/1000 + "s");

        while (running &&
                System.currentTimeMillis() - startTime < durationMs) {
            String sentence = generateSentence();
            producer.send(new ProducerRecord<>(topic,
                    "key-" + random.nextInt(100), sentence));
            Thread.sleep(intervalMs);
        }
    }

    // Reproduce the fluctuating stream profile from paper Section 6.4
    // Rate: 1800 → 3000 at 60s, peak 4500 at 210s,
    //       down to 2700 at 270s, 2000 at 330s
    public void runFluctuatingStream() throws InterruptedException {
        System.out.println("Starting fluctuating stream profile...");

        // t=0 to t=60s: baseline 1800 tuples/s
        sendAtRate(1800, 60_000);

        // t=60s: jump to 3000
        sendAtRate(3000, 150_000);

        // t=210s: peak at 4500
        sendAtRate(4500, 60_000);

        // t=270s: drop to 2700
        sendAtRate(2700, 60_000);

        // t=330s: settle at 2000
        sendAtRate(2000, 270_000);

        producer.close();
        System.out.println("Stream profile complete");
    }

    public void stop() {
        running = false;
    }

    // Quick test — send at stable 3000 tuples/s for 60s
    public void runStableStream(int tuplesPerSecond,
                                long durationMs)
            throws InterruptedException {
        sendAtRate(tuplesPerSecond, durationMs);
        producer.close();
    }
}