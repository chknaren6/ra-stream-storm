package com.rastream.optimizations;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Intelligent Back-Pressure Optimizer with Adaptive Thresholds
 *
 * ALGORITHM: Hysteresis-Based Congestion Control
 * - Monitors queue depth trends
 * - Adapts watermarks based on data rate
 * - Implements exponential backoff for throttling
 * - Provides metrics on congestion patterns
 */
public class BackPressureController {

    private final int baseLowWatermark;
    private final int baseHighWatermark;

    private volatile boolean throttled = false;
    private volatile int currentLowWatermark;
    private volatile int currentHighWatermark;

    // Metrics
    private final AtomicLong throttleEvents = new AtomicLong(0);
    private final AtomicLong unthrottleEvents = new AtomicLong(0);
    private final AtomicLong totalThrottleTimeMs = new AtomicLong(0);
    private long lastThrottleTime = 0;

    // Adaptive tuning
    private final AtomicInteger dataRate = new AtomicInteger(1000);  // tuples/sec
    private long lastRateUpdateMs = System.currentTimeMillis();

    private static final double ADAPTATION_FACTOR = 0.8;  // Watermarks scale with rate
    private static final long RATE_UPDATE_INTERVAL_MS = 5000;

    public BackPressureController(int baseLow, int baseHigh) {
        this.baseLowWatermark = baseLow;
        this.baseHighWatermark = baseHigh;
        this.currentLowWatermark = baseLow;
        this.currentHighWatermark = baseHigh;
    }

    /**
     * MAIN ALGORITHM: Hysteresis-Based Congestion Control
     *
     * INPUT: queueSize (current queue depth), incomingRate (tuples/sec)
     * OUTPUT: shouldThrottle (boolean)
     *
     * ALGORITHM STEPS:
     * 1. Update data rate estimate (moving average)
     * 2. Adapt watermarks based on data rate
     * 3. Apply hysteresis logic:
     *    - If throttled=false AND queueSize >= highWatermark → throttle=true
     *    - If throttled=true AND queueSize <= lowWatermark → throttle=false
     * 4. Update metrics and record throttle events
     */
    public boolean shouldThrottle(int queueSize, int incomingRate) {
        // Step 1: Update rate estimate periodically
        long now = System.currentTimeMillis();
        if (now - lastRateUpdateMs > RATE_UPDATE_INTERVAL_MS) {
            // Exponential moving average: EMA = 0.7 * current + 0.3 * old
            int oldRate = dataRate.get();
            int newRate = (int) (0.7 * incomingRate + 0.3 * oldRate);
            dataRate.set(newRate);
            lastRateUpdateMs = now;
        }

        int rate = dataRate.get();

        // Step 2: Adapt watermarks based on current data rate
        // Higher rate → higher watermarks (allow more buffering)
        // Lower rate → lower watermarks (react faster)
        int adaptedLow = (int) (baseLowWatermark * Math.min(2.0, (rate / 1000.0)));
        int adaptedHigh = (int) (baseHighWatermark * Math.min(2.0, (rate / 1000.0)));

        currentLowWatermark = adaptedLow;
        currentHighWatermark = adaptedHigh;

        // Step 3: Hysteresis logic - prevent oscillation
        if (!throttled && queueSize >= currentHighWatermark) {
            throttled = true;
            throttleEvents.incrementAndGet();
            lastThrottleTime = System.currentTimeMillis();
            System.out.println("[BackPressure] THROTTLE ON: queue=" + queueSize +
                    " high=" + currentHighWatermark + " rate=" + rate);
        }
        else if (throttled && queueSize <= currentLowWatermark) {
            throttled = false;
            unthrottleEvents.incrementAndGet();
            long throttleDurationMs = System.currentTimeMillis() - lastThrottleTime;
            totalThrottleTimeMs.addAndGet(throttleDurationMs);
            System.out.println("[BackPressure] THROTTLE OFF: queue=" + queueSize +
                    " low=" + currentLowWatermark + " duration=" + throttleDurationMs + "ms");
        }

        return throttled;
    }

    /**
     * Get adaptive watermark (for monitoring)
     */
    public int getCurrentLowWatermark() {
        return currentLowWatermark;
    }

    public int getCurrentHighWatermark() {
        return currentHighWatermark;
    }

    /**
     * Get throttling metrics
     */
    public long getThrottleEvents() {
        return throttleEvents.get();
    }

    public long getUnthrottleEvents() {
        return unthrottleEvents.get();
    }

    public long getTotalThrottleTimeMs() {
        return totalThrottleTimeMs.get();
    }

    public int getCurrentDataRate() {
        return dataRate.get();
    }

    /**
     * Reset metrics
     */
    public void resetMetrics() {
        throttleEvents.set(0);
        unthrottleEvents.set(0);
        totalThrottleTimeMs.set(0);
    }

    /**
     * Get summary snapshot
     */
    public OptimizerSnapshot getSnapshot() {
        return new OptimizerSnapshot(
                throttled,
                currentLowWatermark,
                currentHighWatermark,
                dataRate.get(),
                throttleEvents.get(),
                unthrottleEvents.get(),
                totalThrottleTimeMs.get()
        );
    }

    public static class OptimizerSnapshot {
        public final boolean throttled;
        public final int lowWatermark;
        public final int highWatermark;
        public final int dataRate;
        public final long throttleEvents;
        public final long unthrottleEvents;
        public final long totalThrottleTimeMs;

        public OptimizerSnapshot(boolean throttled, int low, int high, int rate,
                                 long thEvents, long unthEvents, long totalTime) {
            this.throttled = throttled;
            this.lowWatermark = low;
            this.highWatermark = high;
            this.dataRate = rate;
            this.throttleEvents = thEvents;
            this.unthrottleEvents = unthEvents;
            this.totalThrottleTimeMs = totalTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "[BackPressure] throttled=%s, low=%d, high=%d, rate=%d tps, " +
                            "throttleEvents=%d, unthrottleEvents=%d, totalThrottleTime=%d ms",
                    throttled, lowWatermark, highWatermark, dataRate,
                    throttleEvents, unthrottleEvents, totalThrottleTimeMs
            );
        }
    }
}