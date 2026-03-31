/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

@Repeat(iterations = 1000)
public class PagedBytesRefBuilderTests extends ESTestCase {
    public void testBreakOnBuild() {
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(0));
        expectThrows(
            CircuitBreakingException.class,
            () -> new PagedBytesRefBuilder(breaker, label, 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)
        );
    }

    public void testAppendByte() {
        testAppendByte(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testAppendByteCranky() {
        try {
            testAppendByte(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendByte(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte b = randomByte();
            builder.append(b);
            return new byte[] { b };
        });
    }

    public void testAppendBytes() {
        testAppendBytes(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testAppendBytesCranky() {
        try {
            testAppendBytes(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendBytes(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte[] b = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 2));
            builder.append(b, 0, b.length);
            return b;
        });
    }

    public void testAppendBytesRef() {
        testAppendBytesRef(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testAppendBytesRefCranky() {
        try {
            testAppendBytesRef(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendBytesRef(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            byte[] b = randomByteArrayOfLength(randomIntBetween(1, BYTE_PAGE_SIZE * 2));
            builder.append(new BytesRef(b));
            return b;
        });
    }

    public void testSpansMultiplePages() {
        long limit = BYTE_PAGE_SIZE * 4 + PagedBytesRefBuilder.SHALLOW_SIZE * 4;
        testSpansMultiplePages(newLimitedBreaker(ByteSizeValue.ofBytes(limit)));
    }

    public void testSpansMultiplePagesCranky() {
        try {
            testSpansMultiplePages(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testSpansMultiplePages(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            builder.append(chunk, 0, chunk.length);
            builder.append(chunk, 0, chunk.length);

            assertThat(builder.length(), equalTo(BYTE_PAGE_SIZE * 3));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(ref.length(), equalTo(BYTE_PAGE_SIZE * 3));
                assertThat(ref.pages().length, equalTo(3));
            }
        }
    }

    public void testBreakOnGrow() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(PagedBytesRefBuilder.PAGE_RAM_BYTES_USED * 2));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            // next append triggers a new page which should break
            expectThrows(CircuitBreakingException.class, () -> builder.append((byte) 0));
        }
    }

    public void testRamBytesUsed() {
        int limit = BYTE_PAGE_SIZE * 10;
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(limit));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
        }
    }

    public void testCompareTo() {
        long limit = BYTE_PAGE_SIZE * 10;
        for (int i = 0; i < 100; i++) {
            testCompareTo(newLimitedBreaker(ByteSizeValue.ofBytes(limit * 2)));
        }
    }

    public void testCompareToCranky() {
        try {
            testCompareTo(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testCompareTo(CircuitBreaker breaker) {
        byte[] lhsFlat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        byte[] rhsFlat = randomBoolean() ? lhsFlat.clone() : randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        try (
            PagedBytesRefBuilder lhs = new PagedBytesRefBuilder(breaker, "lhs", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE);
            PagedBytesRefBuilder rhs = new PagedBytesRefBuilder(breaker, "rhs", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)
        ) {
            lhs.append(lhsFlat, 0, lhsFlat.length);
            rhs.append(rhsFlat, 0, rhsFlat.length);
            int actual = Integer.signum(lhs.compareTo(rhs));
            try (PagedBytesRef lhsRef = lhs.build(); PagedBytesRef rhsRef = rhs.build()) {
                assertThat(actual, equalTo(Integer.signum(lhsRef.compareTo(rhsRef))));
            }
        }
    }

    public void testEqualsAndHashCode() {
        long limit = BYTE_PAGE_SIZE * 10;
        for (int i = 0; i < 100; i++) {
            testEqualsAndHashCode(newLimitedBreaker(ByteSizeValue.ofBytes(limit * 2)));
        }
    }

    public void testEqualsAndHashCodeCranky() {
        try {
            testEqualsAndHashCode(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testEqualsAndHashCode(CircuitBreaker breaker) {
        byte[] flat = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE * 3));
        try (
            PagedBytesRefBuilder a = new PagedBytesRefBuilder(breaker, "a", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE);
            PagedBytesRefBuilder b = new PagedBytesRefBuilder(breaker, "b", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)
        ) {
            a.append(flat, 0, flat.length);
            b.append(flat, 0, flat.length);
            assertEquals(a, b);
            assertThat(a.hashCode(), equalTo(b.hashCode()));
            int aHash = a.hashCode();
            try (PagedBytesRef aRef = a.build()) {
                assertThat(aHash, equalTo(aRef.hashCode()));
            }
        }
    }

    public void testNeverBuild() {
        testNeverBuild(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testNeverBuildCranky() {
        try {
            testNeverBuild(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testNeverBuild(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertTrue(breaker.getUsed() > 0);
        }
    }

    public void testBuildTransfersBreakerToRef() {
        testBuildTransfersBreakerToRef(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testBuildTransfersBreakerToRefCranky() {
        try {
            testBuildTransfersBreakerToRef(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testBuildTransfersBreakerToRef(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            long preCloseCharge = breaker.getUsed();
            try (PagedBytesRef ref = builder.build()) {
                // builder is now empty; breaker charge transferred to ref
                assertThat(breaker.getUsed(), equalTo(preCloseCharge));
            }
            // ref is closed; charge released
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        // closing the empty builder releases nothing — charge was already released by ref
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Appends chunks via the supplied function, then checks that {@link PagedBytesRefBuilder#build()}
     * returns a {@link PagedBytesRef} whose bytes match the concatenation of all appended chunks.
     */
    private void testAgainstOracle(CircuitBreaker breaker, Function<PagedBytesRefBuilder, byte[]> appender) {
        int capacity = BYTE_PAGE_SIZE * 10;
        PagedBytesRefBuilder builder = randomBoolean()
            ? new PagedBytesRefBuilder(breaker, "test", 0, PageCacheRecycler.NON_RECYCLING_INSTANCE)
            : new PagedBytesRefBuilder(breaker, "test", capacity, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        try (builder) {
            byte[] expected = new byte[0];
            int iterations = randomIntBetween(1, 5);
            for (int i = 0; i < iterations; i++) {
                byte[] chunk = appender.apply(builder);
                expected = concat(expected, chunk);
            }
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytesRef built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
                assertThat(built, equalTo(PagedBytesRefTests.newPagedBytesRef(expected)));
            }
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static byte[] toFlat(PagedBytesRef ref) {
        byte[] flat = new byte[ref.length()];
        int pos = 0;
        for (int p = 0; p < ref.pages().length; p++) {
            int toCopy = Math.min(ref.pages()[p].length, ref.length() - pos);
            System.arraycopy(ref.pages()[p], 0, flat, pos, toCopy);
            pos += toCopy;
        }
        return flat;
    }

    private static long bytesArrayRamBytesUsed(int capacity) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) capacity * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }
}
