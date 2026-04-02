/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesRef;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefCursor;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.versionfield.Version;

import java.nio.ByteOrder;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class TopNEncoderTests extends ESTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] { TopNEncoder.DEFAULT_SORTABLE },
            new Object[] { TopNEncoder.UTF8 },
            new Object[] { TopNEncoder.VERSION },
            new Object[] { TopNEncoder.IP },
            new Object[] { TopNEncoder.DEFAULT_UNSORTABLE }
        );
    }

    private final TopNEncoder encoder;

    public TopNEncoderTests(TopNEncoder encoder) {
        this.encoder = encoder;
    }

    public void testLong() {
        long v = randomLong();
        try (PagedBytesRefBuilder builder = newPagedBuilder()) {
            encoder.encodeLong(v, builder);
            try (PagedBytesRef ref = builder.build()) {
                PagedBytesRefCursor cursor = ref.cursor();
                assertThat(encoder.decodeLong(cursor), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testInt() {
        int v = randomInt();
        try (PagedBytesRefBuilder builder = newPagedBuilder()) {
            encoder.encodeInt(v, builder);
            try (PagedBytesRef ref = builder.build()) {
                PagedBytesRefCursor cursor = ref.cursor();
                assertThat(encoder.decodeInt(cursor), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testDouble() {
        double v = randomDouble();
        try (PagedBytesRefBuilder builder = newPagedBuilder()) {
            encoder.encodeDouble(v, builder);
            try (PagedBytesRef ref = builder.build()) {
                PagedBytesRefCursor cursor = ref.cursor();
                assertThat(encoder.decodeDouble(cursor), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testBoolean() {
        boolean v = randomBoolean();
        try (PagedBytesRefBuilder builder = newPagedBuilder()) {
            encoder.encodeBoolean(v, builder);
            try (PagedBytesRef ref = builder.build()) {
                PagedBytesRefCursor cursor = ref.cursor();
                assertThat(encoder.decodeBoolean(cursor), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    public void testAlpha() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef(randomAlphaOfLength(6)));
    }

    public void testUtf8() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef(randomRealisticUnicodeOfLength(6)));
    }

    /**
     * Round trip the highest unicode character to encode without a continuation.
     */
    public void testDel() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef("\u007F"));
    }

    /**
     * Round trip the lowest unicode character to encode using a continuation byte.
     */
    public void testPaddingCharacter() {
        assumeTrue("unsupported", encoder == TopNEncoder.UTF8);
        roundTripBytesRef(new BytesRef("\u0080"));
    }

    public void testVersion() {
        assumeTrue("unsupported", encoder == TopNEncoder.VERSION);
        roundTripBytesRef(randomVersion().toBytesRef());
    }

    public void testPointAsWKB() {
        assumeTrue("unsupported", encoder == TopNEncoder.DEFAULT_UNSORTABLE);
        roundTripBytesRef(randomPointAsWKB());
    }

    public void testIp() {
        assumeTrue("unsupported", encoder == TopNEncoder.IP);
        roundTripBytesRef(new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))));
    }

    private void roundTripBytesRef(BytesRef v) {
        try (PagedBytesRefBuilder builder = newPagedBuilder()) {
            encoder.encodeBytesRef(v, builder);
            try (PagedBytesRef ref = builder.build()) {
                PagedBytesRefCursor cursor = ref.cursor();
                assertThat(encoder.decodeBytesRef(cursor, new BytesRef()), equalTo(v));
                assertThat(cursor.remaining(), equalTo(0));
            }
        }
    }

    static PagedBytesRefBuilder newPagedBuilder() {
        return new PagedBytesRefBuilder(
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            "topn",
            0,
            new MockPageCacheRecycler(Settings.EMPTY)
        );
    }

    static Version randomVersion() {
        // TODO degenerate versions and stuff
        return switch (between(0, 3)) {
            case 0 -> new Version(Integer.toString(between(0, 100)));
            case 1 -> new Version(between(0, 100) + "." + between(0, 100));
            case 2 -> new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100));
            case 3 -> TopNOperatorTests.randomVersion();
            default -> throw new IllegalArgumentException();
        };
    }

    static BytesRef randomPointAsWKB() {
        Point point = randomBoolean() ? GeometryTestUtils.randomPoint() : ShapeTestUtils.randomPoint();
        byte[] wkb = WellKnownBinary.toWKB(point, ByteOrder.LITTLE_ENDIAN);
        return new BytesRef(wkb);
    }
}
