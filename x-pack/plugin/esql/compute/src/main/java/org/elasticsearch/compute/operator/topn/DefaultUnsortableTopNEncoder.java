/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefCursor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * A {@link TopNEncoder} that doesn't encode values so they are sortable but is
 * capable of encoding any values.
 */
public class DefaultUnsortableTopNEncoder implements TopNEncoder {
    public static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    public static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    public static final VarHandle FLOAT = MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.BIG_ENDIAN);
    public static final VarHandle DOUBLE = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.BIG_ENDIAN);

    @Override
    public void encodeLong(long value, PagedBytesRefBuilder builder) {
        builder.append(value);
    }

    @Override
    public long decodeLong(BytesRef bytes) {
        if (bytes.length < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        long v = (long) LONG.get(bytes.bytes, bytes.offset);
        bytes.offset += Long.BYTES;
        bytes.length -= Long.BYTES;
        return v;
    }

    @Override
    public long decodeLong(PagedBytesRefCursor bytes) {
        return bytes.readLong();
    }

    /**
     * Writes an int in variable-length format. Writes between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers
     * will always use all 5 bytes.
     */
    public void encodeVInt(int value, PagedBytesRefBuilder builder) {
        builder.appendVInt(value);
    }

    /**
     * Reads an int stored in variable-length format.
     */
    public int decodeVInt(PagedBytesRefCursor cursor) {
        return cursor.readVInt();
    }

    /**
     * Reads an int stored in variable-length format. Reads between one and
     * five bytes. Smaller values take fewer bytes. Negative numbers
     * will always use all 5 bytes.
     */
    public int decodeVInt(BytesRef bytes) {
        /*
         * The loop for this is unrolled because we unrolled the loop in StreamInput.
         * I presume it's a decent choice here because it was a good choice there.
         */
        byte b = bytes.bytes[bytes.offset];
        if (b >= 0) {
            bytes.offset += 1;
            bytes.length -= 1;
            return b;
        }
        int i = b & 0x7F;
        b = bytes.bytes[bytes.offset + 1];
        i |= (b & 0x7F) << 7;
        if (b >= 0) {
            bytes.offset += 2;
            bytes.length -= 2;
            return i;
        }
        b = bytes.bytes[bytes.offset + 2];
        i |= (b & 0x7F) << 14;
        if (b >= 0) {
            bytes.offset += 3;
            bytes.length -= 3;
            return i;
        }
        b = bytes.bytes[bytes.offset + 3];
        i |= (b & 0x7F) << 21;
        if (b >= 0) {
            bytes.offset += 4;
            bytes.length -= 4;
            return i;
        }
        b = bytes.bytes[bytes.offset + 4];
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid last byte for a vint [" + Integer.toHexString(b) + "]");
        }
        bytes.offset += 5;
        bytes.length -= 5;
        return i;
    }

    @Override
    public void encodeInt(int value, PagedBytesRefBuilder builder) {
        builder.append(value);
    }

    @Override
    public int decodeInt(BytesRef bytes) {
        if (bytes.length < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        int v = (int) INT.get(bytes.bytes, bytes.offset);
        bytes.offset += Integer.BYTES;
        bytes.length -= Integer.BYTES;
        return v;
    }

    @Override
    public int decodeInt(PagedBytesRefCursor bytes) {
        return bytes.readInt();
    }

    @Override
    public void encodeFloat(float value, PagedBytesRefBuilder builder) {
        builder.append(Float.floatToRawIntBits(value));
    }

    @Override
    public float decodeFloat(BytesRef bytes) {
        if (bytes.length < Float.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        float v = (float) FLOAT.get(bytes.bytes, bytes.offset);
        bytes.offset += Float.BYTES;
        bytes.length -= Float.BYTES;
        return v;
    }

    @Override
    public float decodeFloat(PagedBytesRefCursor bytes) {
        return Float.intBitsToFloat(bytes.readInt());
    }

    @Override
    public void encodeDouble(double value, PagedBytesRefBuilder builder) {
        builder.append(Double.doubleToRawLongBits(value));
    }

    @Override
    public double decodeDouble(BytesRef bytes) {
        if (bytes.length < Double.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        double v = (double) DOUBLE.get(bytes.bytes, bytes.offset);
        bytes.offset += Double.BYTES;
        bytes.length -= Double.BYTES;
        return v;
    }

    @Override
    public double decodeDouble(PagedBytesRefCursor bytes) {
        return Double.longBitsToDouble(bytes.readLong());
    }

    @Override
    public void encodeBoolean(boolean value, PagedBytesRefBuilder builder) {
        builder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public boolean decodeBoolean(BytesRef bytes) {
        if (bytes.length < Byte.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        boolean v = bytes.bytes[bytes.offset] == 1;
        bytes.offset += Byte.BYTES;
        bytes.length -= Byte.BYTES;
        return v;
    }

    @Override
    public boolean decodeBoolean(PagedBytesRefCursor bytes) {
        return bytes.readByte() == 1;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        final int len = decodeVInt(bytes);
        scratch.bytes = bytes.bytes;
        scratch.offset = bytes.offset;
        scratch.length = len;
        bytes.offset += len;
        bytes.length -= len;
        return scratch;
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesRefBuilder builder) {
        builder.appendVInt(value.length);
        builder.append(value);
    }

    @Override
    public BytesRef decodeBytesRef(PagedBytesRefCursor cursor, BytesRef scratch) {
        return cursor.readBytesRef(cursor.readVInt(), scratch);
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return TopNEncoder.DEFAULT_SORTABLE.toSortable(asc);
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }

    @Override
    public String toString() {
        return "DefaultUnsortable";
    }
}
