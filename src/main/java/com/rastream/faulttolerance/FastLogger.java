package com.rastream.faulttolerance;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;

public class FastLogger implements Closeable {
    public static class Record {
        public final long tupleId, timestamp;
        public final String source, destination;
        public Record(long tupleId, long timestamp, String source, String destination) {
            this.tupleId = tupleId; this.timestamp = timestamp; this.source = source; this.destination = destination;
        }
        public byte[] bytes() {
            return (tupleId + "," + timestamp + "," + source + "," + destination + "\n").getBytes();
        }
    }

    private final BlockingQueue<Record> queue;
    private final ExecutorService writer = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final Path file;
    private final int batchSize;
    private volatile boolean running = true;

    public FastLogger(Path file, int ringBufferSize, int batchSize, long flushMs) throws IOException {
        this.file = file;
        this.batchSize = batchSize;
        this.queue = new ArrayBlockingQueue<>(ringBufferSize);
        Files.createDirectories(file.getParent());
        timer.scheduleAtFixedRate(this::flushAsync, flushMs, flushMs, TimeUnit.MILLISECONDS);
    }

    public boolean log(long tupleId, long timestamp, String source, String destination) {
        return running && queue.offer(new Record(tupleId, timestamp, source, destination));
    }

    public void flushAsync() { writer.submit(this::flushBatch); }

    private void flushBatch() {
        try {
            int n = Math.min(batchSize, queue.size());
            if (n == 0) return;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < n; i++) {
                Record r = queue.poll();
                if (r == null) break;
                bos.write(r.bytes());
            }
            byte[] gz = gzip(bos.toByteArray());
            append(gz);
        } catch (Exception ignored) {}
    }

    private void append(byte[] payload) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
             FileChannel ch = raf.getChannel()) {
            long pos = ch.size();
            MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_WRITE, pos, payload.length + 4L);
            mb.putInt(payload.length);
            mb.put(payload);
            mb.force();
        }
    }

    private static byte[] gzip(byte[] in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) { gz.write(in); }
        return bos.toByteArray();
    }

    @Override public void close() {
        running = false;
        flushAsync();
        timer.shutdownNow();
        writer.shutdown();
    }
}