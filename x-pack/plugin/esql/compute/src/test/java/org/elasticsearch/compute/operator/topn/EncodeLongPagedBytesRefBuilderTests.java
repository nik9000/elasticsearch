/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRef;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class EncodeLongPagedBytesRefBuilderTests extends ESTestCase {
    private static final VarHandle BIG_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    private final MockPageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);

    /**
     * The {@link PagedBytesRefBuilder} path produces the same bytes as the
     * {@link BreakingBytesRefBuilder} path for the asc sortable encoder.
     */
    public void testAscMatchesOldPath() {
        long value = randomLong();
        assertMatchesOldPath(TopNEncoder.DEFAULT_SORTABLE, value);
    }

    /**
     * The {@link PagedBytesRefBuilder} path produces the same bytes as the
     * {@link BreakingBytesRefBuilder} path for the desc sortable encoder.
     */
    public void testDescMatchesOldPath() {
        long value = randomLong();
        assertMatchesOldPath(TopNEncoder.DEFAULT_SORTABLE.toSortable(false), value);
    }

    /**
     * The unsortable encoder writes big-endian bytes into the {@link PagedBytesRefBuilder}
     * (unlike the {@link BreakingBytesRefBuilder} path which uses native byte order).
     */
    public void testUnsortableEncodesBigEndian() {
        long value = randomLong();
        try (PagedBytesRefBuilder builder = newBuilder()) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(value, builder);
            try (PagedBytesRef ref = builder.build()) {
                BytesRef flat = toFlat(ref);
                assertThat(flat.length, equalTo(Long.BYTES));
                long decoded = (long) BIG_ENDIAN_LONG.get(flat.bytes, flat.offset);
                assertThat(decoded, equalTo(value));
            }
        }
    }

    /** Asc encoder preserves sort order. */
    public void testAscSortOrder() {
        long a = randomLong();
        long b = randomValueOtherThan(a, ESTestCase::randomLong);
        try (
            PagedBytesRefBuilder aBytes = newBuilder();
            PagedBytesRefBuilder bBytes = newBuilder()
        ) {
            TopNEncoder.DEFAULT_SORTABLE.encodeLong(a, aBytes);
            TopNEncoder.DEFAULT_SORTABLE.encodeLong(b, bBytes);
            if (Long.compare(a, b) < 0) {
                assertThat(aBytes, lessThan(bBytes));
            } else {
                assertThat(aBytes, greaterThan(bBytes));
            }
        }
    }

    /** Desc encoder reverses sort order. */
    public void testDescSortOrder() {
        long a = randomLong();
        long b = randomValueOtherThan(a, ESTestCase::randomLong);
        TopNEncoder desc = TopNEncoder.DEFAULT_SORTABLE.toSortable(false);
        try (
            PagedBytesRefBuilder aBytes = newBuilder();
            PagedBytesRefBuilder bBytes = newBuilder()
        ) {
            desc.encodeLong(a, aBytes);
            desc.encodeLong(b, bBytes);
            if (Long.compare(a, b) < 0) {
                assertThat(aBytes, greaterThan(bBytes));
            } else {
                assertThat(aBytes, lessThan(bBytes));
            }
        }
    }

    private void assertMatchesOldPath(TopNEncoder encoder, long value) {
        BreakingBytesRefBuilder old = new BreakingBytesRefBuilder(new NoopCircuitBreaker("test"), "old");
        encoder.encodeLong(value, old);
        BytesRef expected = old.bytesRefView();

        try (PagedBytesRefBuilder builder = newBuilder()) {
            encoder.encodeLong(value, builder);
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(expected));
            }
        }
    }

    private PagedBytesRefBuilder newBuilder() {
        return new PagedBytesRefBuilder(new NoopCircuitBreaker("test"), "test", 0, recycler);
    }

    private static BytesRef toFlat(PagedBytesRef ref) {
        byte[] flat = new byte[ref.length()];
        int pos = 0;
        for (byte[] page : ref.pages()) {
            int toCopy = Math.min(page.length, ref.length() - pos);
            System.arraycopy(page, 0, flat, pos, toCopy);
            pos += toCopy;
        }
        return new BytesRef(flat);
    }
}
