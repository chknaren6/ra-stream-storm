package com.rastream.optimizations;

public class BackPressureController {
    private final int lowWatermark, highWatermark;
    private volatile boolean throttled = false;

    public BackPressureController(int lowWatermark, int highWatermark) {
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
    }

    public boolean shouldThrottle(int queueSize) {
        if (queueSize >= highWatermark) throttled = true;
        else if (queueSize <= lowWatermark) throttled = false;
        return throttled;
    }
}