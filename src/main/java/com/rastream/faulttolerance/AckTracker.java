package com.rastream.faulttolerance;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AckTracker {
    static class Window {
        final long base; final int size; final BitSet bitmap;
        Window(long base, int size) { this.base = base; this.size = size; this.bitmap = new BitSet(size); }
    }

    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public void createWindow(String streamId, long baseTupleId, int size) {
        windows.put(streamId, new Window(baseTupleId, size));
    }

    public boolean ack(String streamId, long tupleId) {
        Window w = windows.get(streamId);
        if (w == null) return false;
        long off = tupleId - w.base;
        if (off < 0 || off >= w.size) return false;
        w.bitmap.set((int) off);
        return true;
    }

    public boolean isComplete(String streamId) {
        Window w = windows.get(streamId);
        return w != null && w.bitmap.cardinality() == w.size;
    }
}