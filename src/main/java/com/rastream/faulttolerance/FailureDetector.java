package com.rastream.faulttolerance;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class FailureDetector {
    private final Map<String, Long> heartbeats = new ConcurrentHashMap<>();
    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    private final long timeoutMs;
    private final Consumer<String> onNodeFailure;

    public FailureDetector(long timeoutMs, Consumer<String> onNodeFailure) {
        this.timeoutMs = timeoutMs;
        this.onNodeFailure = onNodeFailure;
    }

    public void start(long checkIntervalMs) {
        ses.scheduleAtFixedRate(this::check, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() { ses.shutdownNow(); }

    public void heartbeat(String workerId) { heartbeats.put(workerId, System.currentTimeMillis()); }

    private void check() {
        long now = System.currentTimeMillis();
        for (var e : heartbeats.entrySet()) {
            if (now - e.getValue() > timeoutMs) {
                onNodeFailure.accept(e.getKey());
                heartbeats.remove(e.getKey());
            }
        }
    }
}