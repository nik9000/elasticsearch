/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class PagedBytesRefBuilderTests extends ESTestCase {
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final PageCacheRecycler recycler = new MockPageCacheRecycler(Settings.EMPTY);
    public void testBreakOnBuild() {
        String label = randomAlphaOfLength(4);
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(0));
        expectThrows(
            CircuitBreakingException.class,
            () -> new PagedBytesRefBuilder(breaker, label, 0, recycler)
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

    public void testAppendInt() {
        testAppendInt(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testAppendIntCranky() {
        try {
            testAppendInt(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendInt(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            int v = randomInt();
            builder.append(v);
            byte[] expected = new byte[Integer.BYTES];
            INT.set(expected, 0, v);
            return expected;
        });
    }

    public void testAppendManyInts() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        // 3 pages worth of ints — enough to shift to PAGED and span multiple pages
        int count = BYTE_PAGE_SIZE / Integer.BYTES * 3;
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = randomInt();
        }
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            for (int v : values) {
                builder.append(v);
            }
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(count * Integer.BYTES));
            byte[] expected = new byte[count * Integer.BYTES];
            for (int i = 0; i < count; i++) {
                INT.set(expected, i * Integer.BYTES, values[i]);
            }
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(expected));
            }
        }
    }

    public void testAppendLong() {
        testAppendLong(newLimitedBreaker(ByteSizeValue.ofBytes(BYTE_PAGE_SIZE * 10)));
    }

    public void testAppendLongCranky() {
        try {
            testAppendLong(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendLong(CircuitBreaker breaker) {
        testAgainstOracle(breaker, builder -> {
            long v = randomLong();
            builder.append(v);
            byte[] expected = new byte[Long.BYTES];
            LONG.set(expected, 0, v);
            return expected;
        });
    }

    public void testAppendPagedBytesRefBuilder() {
        testAppendPagedBytesRefBuilder(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesRefBuilderCranky() {
        try {
            testAppendPagedBytesRefBuilder(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesRefBuilder(CircuitBreaker breaker) {
        int pages = randomIntBetween(0, 10);
        byte[] expected = randomByteArrayOfLength(pages * BYTE_PAGE_SIZE + randomIntBetween(0, BYTE_PAGE_SIZE - 1));
        try (
            PagedBytesRefBuilder src = new PagedBytesRefBuilder(breaker, "src", 0, recycler);
            PagedBytesRefBuilder dst = new PagedBytesRefBuilder(breaker, "dst", 0, recycler)
        ) {
            src.append(expected, 0, expected.length);
            dst.append(src);
            assertThat(dst.length(), equalTo(expected.length));
            try (PagedBytesRef built = dst.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendPagedBytesRefShort() {
        testAppendPagedBytesRefShort(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesRefShortCranky() {
        try {
            testAppendPagedBytesRefShort(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesRefShort(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            byte[] expected = randomByteArrayOfLength(randomIntBetween(0, BYTE_PAGE_SIZE - 1));
            PagedBytesRef ref = PagedBytesRefTests.newPagedBytesRef(expected);
            builder.append(ref);
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytesRef built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendPagedBytesRefMultiPage() {
        testAppendPagedBytesRefMultiPage(newLimitedBreaker(ByteSizeValue.ofMb(50)));
    }

    public void testAppendPagedBytesRefMultiPageCranky() {
        try {
            testAppendPagedBytesRefMultiPage(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testAppendPagedBytesRefMultiPage(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            int pages = randomIntBetween(1, 10);
            byte[] expected = randomByteArrayOfLength(pages * BYTE_PAGE_SIZE + randomIntBetween(0, BYTE_PAGE_SIZE - 1));
            PagedBytesRef ref = PagedBytesRefTests.newPagedBytesRef(expected);
            builder.append(ref);
            assertThat(builder.length(), equalTo(expected.length));
            try (PagedBytesRef built = builder.build()) {
                assertThat(toFlat(built), equalTo(expected));
            }
        }
    }

    public void testAppendManyLongs() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        // 3 pages worth of longs — enough to shift to PAGED and span multiple pages
        int count = BYTE_PAGE_SIZE / Long.BYTES * 3;
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
        }
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            for (long v : values) {
                builder.append(v);
            }
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(count * Long.BYTES));
            byte[] expected = new byte[count * Long.BYTES];
            for (int i = 0; i < count; i++) {
                LONG.set(expected, i * Long.BYTES, values[i]);
            }
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(expected));
            }
        }
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

    public void testBigButNotHuge() {
        testBigButNotHuge(newLimitedBreaker(ByteSizeValue.ofGb(1)));
    }

    public void testBigButNotHugeCranky() {
        try {
            testBigButNotHuge(new CrankyCircuitBreakerService.CrankyCircuitBreaker());
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    private void testBigButNotHuge(CircuitBreaker breaker) {
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            byte[] chunk = randomByteArrayOfLength(Math.toIntExact(ByteSizeValue.ofKb(6).getBytes()));
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            builder.append(chunk, 0, chunk.length);

            assertThat(builder.length(), equalTo(chunk.length * 3));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.BUILT));
                assertThat(ref.length(), equalTo(chunk.length * 3));
                assertThat(ref.pages().length, equalTo(2));
            }
        }
    }

    public void testSpansMultiplePages() {
        testSpansMultiplePages(newLimitedBreaker(ByteSizeValue.ofMb(50)));
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
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            builder.append(chunk, 0, chunk.length);
            builder.append(chunk, 0, chunk.length);

            assertThat(builder.length(), equalTo(BYTE_PAGE_SIZE * 3));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.BUILT));
                assertThat(ref.length(), equalTo(BYTE_PAGE_SIZE * 3));
                assertThat(ref.pages().length, equalTo(3));
            }
        }
    }

    public void testInitialCapacityThreeQuartersPageSize() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(
            breaker,
            "test",
            BYTE_PAGE_SIZE * 3 / 4,
            recycler
        )) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
        }
    }

    public void testBreakOnGrow() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(PagedBytesRefBuilder.PAGE_RAM_BYTES_USED * 2));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            byte[] chunk = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(chunk, 0, chunk.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            // next append triggers a new page which should break
            expectThrows(CircuitBreakingException.class, () -> builder.append((byte) 0));
        }
    }

    public void testRamBytesUsed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));
        }
    }

    public void testCompareTo() {
        for (int i = 0; i < 100; i++) {
            testCompareTo(newLimitedBreaker(ByteSizeValue.ofMb(50)));
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
            PagedBytesRefBuilder lhs = new PagedBytesRefBuilder(breaker, "lhs", 0, recycler);
            PagedBytesRefBuilder rhs = new PagedBytesRefBuilder(breaker, "rhs", 0, recycler)
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
        for (int i = 0; i < 100; i++) {
            testEqualsAndHashCode(newLimitedBreaker(ByteSizeValue.ofMb(50)));
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
            PagedBytesRefBuilder a = new PagedBytesRefBuilder(breaker, "a", 0, recycler);
            PagedBytesRefBuilder b = new PagedBytesRefBuilder(breaker, "b", 0, recycler)
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
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
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
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            builder.append(randomByteArrayOfLength(BYTE_PAGE_SIZE), 0, BYTE_PAGE_SIZE);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            long preCloseCharge = breaker.getUsed();
            try (PagedBytesRef ref = builder.build()) {
                assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.BUILT));
                // builder is now empty; breaker charge transferred to ref
                assertThat(breaker.getUsed(), equalTo(preCloseCharge));
            }
            // ref is closed; charge released
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        // closing the empty builder releases nothing — charge was already released by ref
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testClearSmallTail() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            byte[] first = randomByteArrayOfLength(randomIntBetween(1, PagedBytesRefBuilder.MAX_SMALL_TAIL_SIZE - 1));
            builder.append(first, 0, first.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            long chargeBeforeClear = breaker.getUsed();

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.SMALL_TAIL));
            assertThat(builder.length(), equalTo(0));
            assertThat(breaker.getUsed(), equalTo(chargeBeforeClear));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(randomIntBetween(1, PagedBytesRefBuilder.MAX_SMALL_TAIL_SIZE - 1));
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    public void testClearPagedOnePage() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            byte[] data = randomByteArrayOfLength(BYTE_PAGE_SIZE);
            builder.append(data, 0, data.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            long chargeBeforeClear = breaker.getUsed();

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(0));
            assertThat(breaker.getUsed(), equalTo(chargeBeforeClear));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(BYTE_PAGE_SIZE / 2);
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    public void testClearPagedMultiplePages() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(50));
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(breaker, "test", 0, recycler)) {
            int pageCount = randomIntBetween(2, 5);
            byte[] data = randomByteArrayOfLength(pageCount * BYTE_PAGE_SIZE);
            builder.append(data, 0, data.length);
            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));

            builder.clear();

            assertThat(builder.mode(), equalTo(PagedBytesRefBuilder.Mode.PAGED));
            assertThat(builder.length(), equalTo(0));
            // breaker should reflect only one page remaining (plus shallow + pages array)
            assertThat(breaker.getUsed(), equalTo(builder.ramBytesUsed()));

            // can still append after clear
            byte[] second = randomByteArrayOfLength(BYTE_PAGE_SIZE / 2);
            builder.append(second, 0, second.length);
            assertThat(builder.length(), equalTo(second.length));
            try (PagedBytesRef ref = builder.build()) {
                assertThat(toFlat(ref), equalTo(second));
            }
        }
    }

    /**
     * Appends chunks via the supplied function, then checks that {@link PagedBytesRefBuilder#build()}
     * returns a {@link PagedBytesRef} whose bytes match the concatenation of all appended chunks.
     */
    private void testAgainstOracle(CircuitBreaker breaker, Function<PagedBytesRefBuilder, byte[]> appender) {
        int capacity = BYTE_PAGE_SIZE * 10;
        PagedBytesRefBuilder builder = randomBoolean()
            ? new PagedBytesRefBuilder(breaker, "test", 0, recycler)
            : new PagedBytesRefBuilder(breaker, "test", capacity, recycler);
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
