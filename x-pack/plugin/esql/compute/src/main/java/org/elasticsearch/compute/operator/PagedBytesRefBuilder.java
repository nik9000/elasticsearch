/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Builder for {@link PagedBytesRef}. Runs in one of three modes:
 * <ol>
 *     <li>
 *         When the bytes sequence is {@code <= BYTE_PAGE_SIZE / 2}
 *         it allocates an array on the heap. We call this "small tail" mode because {@code tail}
 *         is the name of the variable that holds the heap allocated array. And because it's cute.
 *     </li>
 *     <li>
 *         Otherwise run in "paged" mode and allocate pages {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
 *     </li>
 *     <li>
 *         After {@link #build()} completes, the builder enters "built" mode. Ownership of all
 *         allocated memory is transferred to the returned {@link PagedBytesRef}. The builder is
 *         invalid after this point.
 *     </li>
 * </ol>
 * <p>
 *     "Small tail" mode grows exponentially starting at 64 bytes. Then 128, then 256, on and on
 *     until 8kb. When the next growth would exceed {@link #MAX_SMALL_TAIL_SIZE} (8kb) we shift
 *     to paged mode instead.
 * </p>
 */
public class PagedBytesRefBuilder implements Accountable, Releasable, Comparable<PagedBytesRefBuilder> {
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PagedBytesRefBuilder.class);
    static final int MIN_SIZE = 64;
    static final int MAX_SMALL_TAIL_SIZE = BYTE_PAGE_SIZE / 2;

    private final CircuitBreaker breaker;
    private final String label;
    private final PageCacheRecycler recycler;

    /**
     * Recycler pages. If this is {@code null} then we're in "small tail"
     * mode. Otherwise, entries {@code 0..usedPages-1} are in use.
     * Set to {@code null} by {@link #build()} — the builder is invalid after that.
     */
    private Recycler.V<byte[]>[] pages;

    /**
     * Number of recycler pages in use.
     */
    private int usedPages;

    /**
     * The page currently being filled. When {@code pages == null} then we're
     * in "small tail" mode and this is a heap allocated array. Otherwise, we're in
     * "paged" mode and this is {@code pages[usedPages - 1].v()}.
     */
    private byte[] tail;

    /**
     * Number of bytes written into {@link #tail}.
     */
    private int tailOffset;

    public PagedBytesRefBuilder(CircuitBreaker breaker, String label, int initialCapacity, PageCacheRecycler recycler) {
        this.recycler = recycler;
        this.breaker = breaker;
        this.label = label;
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, label);
        boolean success = false;
        initialCapacity = initialCapacity <= MIN_SIZE ? MIN_SIZE : nextPowerOfTwo(initialCapacity);
        try {
            if (initialCapacity < MAX_SMALL_TAIL_SIZE) {
                allocateSmallTail(initialCapacity);
            } else {
                allocatePages(initialCapacity);
            }
            success = true;
        } finally {
            if (success == false) {
                breaker.addEstimateBytesAndMaybeBreak(-SHALLOW_SIZE, label);
            }
        }
    }

    /**
     * Append a byte.
     */
    public void append(byte b) {
        if (smallTailPreflight(1)) {
            tail[tailOffset++] = b;
            return;
        }
        if (tailOffset == tail.length) {
            nextPage();
        }
        tail[tailOffset++] = b;
    }

    /**
     * Append bytes.
     */
    public void append(byte[] b, int off, int len) {
        if (smallTailPreflight(len)) {
            System.arraycopy(b, off, tail, tailOffset, len);
            tailOffset += len;
            return;
        }
        while (len > 0) {
            if (tailOffset == tail.length) {
                nextPage();
            }
            int toCopy = Math.min(tail.length - tailOffset, len);
            System.arraycopy(b, off, tail, tailOffset, toCopy);
            tailOffset += toCopy;
            off += toCopy;
            len -= toCopy;
        }
    }

    /**
     * Append the bitwise NOT of bytes.
     * NOCOMMIT optimize
     */
    public void appendNot(byte[] b, int off, int len) {
        if (smallTailPreflight(len)) {
            for (int i = 0; i < len; i++) {
                tail[tailOffset++] = (byte) ~b[off + i];
            }
            return;
        }
        while (len > 0) {
            if (tailOffset == tail.length) {
                nextPage();
            }
            int toCopy = Math.min(tail.length - tailOffset, len);
            for (int i = 0; i < toCopy; i++) {
                tail[tailOffset++] = (byte) ~b[off + i];
            }
            off += toCopy;
            len -= toCopy;
        }
    }

    /**
     * Append bytes.
     */
    public void append(BytesRef b) {
        append(b.bytes, b.offset, b.length);
    }

    /**
     * Append bytes.
     */
    public void append(PagedBytesRefBuilder b) {
        for (int i = 0; i < b.usedPages - 1; i++) {
            append(b.pages[i].v(), 0, BYTE_PAGE_SIZE);
        }
        if (b.tail != null) {
            append(b.tail, 0, b.tailOffset);
        }
    }

    /**
     * Append bytes.
     */
    public void append(PagedBytesRef b) {
        int remaining = b.length();
        for (byte[] page : b.pages()) {
            int toCopy = Math.min(page.length, remaining);
            append(page, 0, toCopy);
            remaining -= toCopy;
        }
    }

    /**
     * Append an int in big-endian order.
     * NOCOMMIT optimize
     */
    public void append(int v) {
        append((byte) (v >> 24));
        append((byte) (v >> 16));
        append((byte) (v >> 8));
        append((byte) v);
    }

    /**
     * Append a long in big-endian order.
     * NOCOMMIT optimize
     */
    public void append(long v) {
        append((byte) (v >> 56));
        append((byte) (v >> 48));
        append((byte) (v >> 40));
        append((byte) (v >> 32));
        append((byte) (v >> 24));
        append((byte) (v >> 16));
        append((byte) (v >> 8));
        append((byte) v);
    }

    /**
     * Total bytes written so far.
     */
    public int length() {
        return (usedPages == 0 ? 0 : (usedPages - 1) * BYTE_PAGE_SIZE) + tailOffset;
    }

    /**
     * Reset to zero length without changing mode. In small-tail mode the heap array
     * is kept at its current capacity. In paged mode all pages beyond the first are
     * released back to the recycler; the first page is kept so the next write does
     * not immediately allocate.
     */
    public void clear() {
        assert mode() != Mode.BUILT : "clear() called on a built PagedBytesRefBuilder";
        if (pages != null && usedPages > 1) {
            long released = (long) (usedPages - 1) * PAGE_RAM_BYTES_USED;
            for (int i = 1; i < usedPages; i++) {
                pages[i].close();
                pages[i] = null;
            }
            usedPages = 1;
            tail = pages[0].v();
            breaker.addWithoutBreaking(-released);
        }
        tailOffset = 0;
    }

    /**
     * Build a {@link PagedBytesRef} from the bytes written so far. Transfers ownership
     * of all allocated memory (either {@link #tail} in small-tail mode or {@link #pages}
     * in paged mode) to the result — this builder is invalid after this call.
     */
    public PagedBytesRef build() {
        if (length() == 0) {
            return PagedBytesRef.EMPTY;
        }
        if (usedPages == 0) {
            // Small case: only a small tail, no recycler pages.
            PagedBytesRef result = new PagedBytesRef(new byte[][] { tail }, tailOffset, new Releasable() {
                private final long charge = ramBytesUsed();

                @Override
                public void close() {
                    breaker.addWithoutBreaking(-charge);
                }
            });
            moveToBuilt();
            return result;
        }
        byte[][] bytePages = new byte[usedPages][];
        for (int i = 0; i < usedPages; i++) {
            bytePages[i] = pages[i].v();
        }
        PagedBytesRef result = new PagedBytesRef(bytePages, length(), new Releasable() {
            private final Recycler.V<byte[]>[] recycledPages = pages;
            private final long charge = ramBytesUsed();

            @Override
            public void close() {
                Releasables.close(Releasables.wrap(recycledPages), () -> breaker.addWithoutBreaking(-charge));
            }
        });
        moveToBuilt();
        return result;
    }

    private void moveToBuilt() {
        pages = null;
        usedPages = 0;
        tail = null;
        tailOffset = 0;
        assert mode() == Mode.BUILT;
    }

    private boolean smallTailPreflight(int needed) {
        if (pages != null) {
            // Already in paged mode
            return false;
        }
        int end = tailOffset + needed;
        if (end < tail.length) {
            // Fits in the small tail
            return true;
        }
        // Got to grow
        int length = nextPowerOfTwo(end);
        if (length > MAX_SMALL_TAIL_SIZE) {
            // Would grow too large
            promoteToPaged(length);
            return false;
        }
        growSmallTail(length);
        return true;
    }

    private void nextPage() {
        maybeGrowPagesArray();
        grabNextPageFromRecycler();
        tailOffset = 0;
    }

    private void maybeGrowPagesArray() {
        if (usedPages < pages.length) {
            return;
        }
        int newLength = ArrayUtil.oversize(pages.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        int oldLength = pages.length;
        breaker.addEstimateBytesAndMaybeBreak(pagesRamBytesUsed(newLength), label);
        pages = Arrays.copyOf(pages, newLength);
        breaker.addWithoutBreaking(-pagesRamBytesUsed(oldLength));
        // NOCOMMIT this should fail a cranky
    }

    private void grabNextPageFromRecycler() {
        breaker.addEstimateBytesAndMaybeBreak(PAGE_RAM_BYTES_USED, label);
        Recycler.V<byte[]> v = recycler.bytePage(false);
        pages[usedPages++] = v;
        tail = v.v();
    }

    /**
     * Promote from "small tail" mode to "paged" mode.
     */
    private void promoteToPaged(int length) {
        assert mode() == Mode.SMALL_TAIL;

        byte[] oldTail = tail;
        allocatePages(length);
        System.arraycopy(oldTail, 0, tail, 0, tailOffset);
        breaker.addWithoutBreaking(-smallTailRamBytesUsed(oldTail.length));
    }

    @SuppressWarnings("unchecked")
    private void allocatePages(int needed) {
        int size = (needed + BYTE_PAGE_SIZE - 1) / BYTE_PAGE_SIZE;
        assert size > 0;
        breaker.addEstimateBytesAndMaybeBreak(pagesRamBytesUsed(size), label);
        boolean success = false;
        try {
            pages = new Recycler.V[size];
            grabNextPageFromRecycler();
            success = true;
        } finally {
            if (success == false) {
                pages = null;
                breaker.addEstimateBytesAndMaybeBreak(-pagesRamBytesUsed(size), label);
            }
        }
    }

    private void growSmallTail(int length) {
        assert length <= MAX_SMALL_TAIL_SIZE;
        breaker.addEstimateBytesAndMaybeBreak(smallTailRamBytesUsed(length), label);
        int oldLength = tail.length;
        tail = Arrays.copyOf(tail, length);
        breaker.addEstimateBytesAndMaybeBreak(-smallTailRamBytesUsed(oldLength), label);
    }

    private void allocateSmallTail(int length) {
        assert length <= MAX_SMALL_TAIL_SIZE;
        breaker.addEstimateBytesAndMaybeBreak(smallTailRamBytesUsed(length), label);
        tail = new byte[length];
    }

    private static int nextPowerOfTwo(int n) {
        // Next power of two.
        return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
    }

    /**
     * Ram bytes used by each page.
     */
    static final long PAGE_RAM_BYTES_USED = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + BYTE_PAGE_SIZE);

    /**
     * Ram bytes used by {@link #tail} when it is heap allocated. When {@link #tail}
     * isn't heap allocated, we track it as another page in {@link #pages}.
     */
    private static long smallTailRamBytesUsed(int size) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + size);
    }

    /**
     * Ram bytes used by the {@link #pages}.
     */
    private static long pagesRamBytesUsed(int capacity) {
        return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) capacity * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj.getClass() != PagedBytesRefBuilder.class) {
            return false;
        }
        PagedBytesRefBuilder rhs = (PagedBytesRefBuilder) obj;
        if (this.length() != rhs.length()) {
            return false;
        }
        return compareTo(rhs) == 0;
    }

    @Override
    public int hashCode() {
        int len = length();
        int fullPages = len / BYTE_PAGE_SIZE;
        int tailLen = len % BYTE_PAGE_SIZE;
        int result = len;
        for (int page = 0; page < fullPages; page++) {
            result = 31 * result + Arrays.hashCode(pages[page].v());
        }
        for (int i = 0; i < tailLen; i++) {
            result = 31 * result + tail[i];
        }
        return result;
    }

    @Override
    public int compareTo(PagedBytesRefBuilder rhs) {
        int remaining = Math.min(this.length(), rhs.length());
        int fullPages = remaining / BYTE_PAGE_SIZE;
        int tailLen = remaining % BYTE_PAGE_SIZE;

        for (int page = 0; page < fullPages; page++) {
            int diff = Arrays.compareUnsigned(this.pages[page].v(), rhs.pages[page].v());
            if (diff != 0) {
                return diff;
            }
        }

        if (tailLen > 0) {
            byte[] lhsTail = this.usedPages > 0 ? this.pages[fullPages].v() : this.tail;
            byte[] rhsTail = rhs.usedPages > 0 ? rhs.pages[fullPages].v() : rhs.tail;
            int diff = Arrays.compareUnsigned(lhsTail, 0, tailLen, rhsTail, 0, tailLen);
            if (diff != 0) {
                return diff;
            }
        }

        // All shared bytes are the same.
        return this.length() - rhs.length();
    }

    enum Mode {
        /** Heap-allocated tail, no recycler pages. */
        SMALL_TAIL,
        /** One or more recycler pages in use. */
        PAGED,
        /**
         * {@link #build()} has been called; ownership of all memory was transferred
         * to the returned {@link PagedBytesRef}.
         */
        BUILT,
    }

    Mode mode() {
        if (pages != null) {
            return Mode.PAGED;
        }
        if (tail != null) {
            return Mode.SMALL_TAIL;
        }
        return Mode.BUILT;
    }

    @Override
    public long ramBytesUsed() {
        return switch (mode()) {
            case BUILT -> 0;
            case SMALL_TAIL -> SHALLOW_SIZE + smallTailRamBytesUsed(tail.length);
            case PAGED -> SHALLOW_SIZE + pagesRamBytesUsed(pages.length) + (long) usedPages * PAGE_RAM_BYTES_USED;
        };
    }

    @Override
    public void close() {
        if (mode() == Mode.BUILT) {
            return;
        }
        long charge = ramBytesUsed();
        if (pages != null) {
            Releasables.close(pages);
            pages = null;
        }
        tail = null;
        if (charge > 0) {
            breaker.addWithoutBreaking(-charge);
        }
    }
}
