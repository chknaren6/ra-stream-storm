package com.rastream.optimizations;

import java.nio.ByteBuffer;

public class ZeroCopyTransfer {
    public ByteBuffer wrap(byte[] data) { return ByteBuffer.wrap(data); }
    public byte[] toArray(ByteBuffer buf) {
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }
}