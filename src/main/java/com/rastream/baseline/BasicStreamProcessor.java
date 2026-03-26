package com.rastream.baseline;

import com.rastream.metrics.LatencyTracker;
import com.rastream.metrics.ThroughputMonitor;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class BasicStreamProcessor {
    public static class Tuple {
        public final long id;
        public final long tsNs;
        public final byte[] payload;
        public Tuple(long id, long tsNs, byte[] payload) {
            this.id = id; this.tsNs = tsNs; this.payload = payload;
        }
    }

    public static class LatencySnapshot {
        public final double avgMs, p50Ms, p95Ms, p99Ms;
        public LatencySnapshot(double avgMs, double p50Ms, double p95Ms, double p99Ms) {
            this.avgMs = avgMs; this.p50Ms = p50Ms; this.p95Ms = p95Ms; this.p99Ms = p99Ms;
        }
    }

    private final BlockingQueue<Tuple> queue;
    private final ExecutorService workers;
    private final int workerThreads;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong produced = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();

    private final ThroughputMonitor throughput = new ThroughputMonitor();
    private final ConcurrentLinkedQueue<Long> latencyNs = new ConcurrentLinkedQueue<>();
    private final LatencyTracker latencyTracker = new LatencyTracker(); // keep for compatibility

    public BasicStreamProcessor(int queueCapacity, int workerThreads) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.workerThreads = workerThreads;
        this.workers = Executors.newFixedThreadPool(workerThreads);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        throughput.start();
        for (int i = 0; i < workerThreads; i++) workers.submit(this::consumeLoop);
    }

    public void stop() {
        running.set(false);
        workers.shutdownNow();
        throughput.stop();
    }

    public boolean submit(Tuple t, long timeoutMs) throws InterruptedException {
        boolean ok = queue.offer(t, timeoutMs, TimeUnit.MILLISECONDS);
        if (ok) produced.incrementAndGet();
        return ok;
    }

    private void consumeLoop() {
        while (running.get()) {
            try {
                Tuple t = queue.poll(100, TimeUnit.MILLISECONDS);
                if (t == null) continue;

                int sum = 0;
                for (byte b : t.payload) sum += b;
                if (sum == Integer.MIN_VALUE) System.out.print("");

                consumed.incrementAndGet();
                long l = System.nanoTime() - t.tsNs;
                latencyNs.add(l);
                throughput.markProcessed(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception ignored) {}
        }
    }

    public long produced() { return produced.get(); }
    public long consumed() { return consumed.get(); }
    public int queueSize() { return queue.size(); }

    public ThroughputMonitor.Snapshot throughputSnapshot() { return throughput.snapshot(); }

    public LatencySnapshot latencySnapshot() {
        List<Long> vals = new ArrayList<>(latencyNs);
        if (vals.isEmpty()) return new LatencySnapshot(0,0,0,0);
        Collections.sort(vals);
        double avg = vals.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
        double p50 = percentile(vals, 50) / 1_000_000.0;
        double p95 = percentile(vals, 95) / 1_000_000.0;
        double p99 = percentile(vals, 99) / 1_000_000.0;
        return new LatencySnapshot(avg, p50, p95, p99);
    }

    private long percentile(List<Long> sorted, int p) {
        int idx = (int)Math.ceil((p / 100.0) * sorted.size()) - 1;
        idx = Math.max(0, Math.min(idx, sorted.size() - 1));
        return sorted.get(idx);
    }
}
