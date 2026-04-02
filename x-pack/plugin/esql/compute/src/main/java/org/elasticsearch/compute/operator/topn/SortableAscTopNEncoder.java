/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefCursor;

/**
 * A {@link TopNEncoder} that encodes values to byte arrays that may be sorted directly.
 */
public abstract class SortableAscTopNEncoder implements TopNEncoder {
    @Override
    public final void encodeLong(long value, PagedBytesRefBuilder builder) {
        builder.append(value ^ Long.MIN_VALUE);
    }

    @Override
    public final long decodeLong(PagedBytesRefCursor bytes) {
        return bytes.readLong() ^ Long.MIN_VALUE;
    }

    @Override
    public final void encodeInt(int value, PagedBytesRefBuilder builder) {
        builder.append(value ^ Integer.MIN_VALUE);
    }

    @Override
    public final int decodeInt(PagedBytesRefCursor bytes) {
        return bytes.readInt() ^ Integer.MIN_VALUE;
    }

    @Override
    public final void encodeFloat(float value, PagedBytesRefBuilder builder) {
        encodeInt(NumericUtils.floatToSortableInt(value), builder);
    }

    @Override
    public final float decodeFloat(PagedBytesRefCursor bytes) {
        return NumericUtils.sortableIntToFloat(decodeInt(bytes));
    }

    @Override
    public final void encodeDouble(double value, PagedBytesRefBuilder builder) {
        encodeLong(NumericUtils.doubleToSortableLong(value), builder);
    }

    @Override
    public final double decodeDouble(PagedBytesRefCursor bytes) {
        return NumericUtils.sortableLongToDouble(decodeLong(bytes));
    }

    @Override
    public final void encodeBoolean(boolean value, PagedBytesRefBuilder builder) {
        builder.append(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public final boolean decodeBoolean(PagedBytesRefCursor bytes) {
        return bytes.readByte() == 1;
    }
}
