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

/**
 * A {@link TopNEncoder} that doesn't encode values so they are sortable but is
 * capable of encoding any values.
 */
public class DefaultUnsortableTopNEncoder implements TopNEncoder {
    @Override
    public void encodeLong(long value, PagedBytesRefBuilder builder) {
        builder.appendNative(value);
    }

    @Override
    public long decodeLong(PagedBytesRefCursor bytes) {
        return bytes.readLongNative();
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

    @Override
    public void encodeInt(int value, PagedBytesRefBuilder builder) {
        builder.appendNative(value);
    }

    @Override
    public int decodeInt(PagedBytesRefCursor bytes) {
        return bytes.readIntNative();
    }

    @Override
    public void encodeFloat(float value, PagedBytesRefBuilder builder) {
        builder.appendNative(Float.floatToRawIntBits(value));
    }

    @Override
    public float decodeFloat(PagedBytesRefCursor bytes) {
        return Float.intBitsToFloat(bytes.readIntNative());
    }

    @Override
    public void encodeDouble(double value, PagedBytesRefBuilder builder) {
        builder.appendNative(Double.doubleToRawLongBits(value));
    }

    @Override
    public double decodeDouble(PagedBytesRefCursor bytes) {
        return Double.longBitsToDouble(bytes.readLongNative());
    }

    @Override
    public void encodeBoolean(boolean value, PagedBytesRefBuilder builder) {
        builder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public boolean decodeBoolean(PagedBytesRefCursor bytes) {
        return bytes.readByte() == 1;
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
