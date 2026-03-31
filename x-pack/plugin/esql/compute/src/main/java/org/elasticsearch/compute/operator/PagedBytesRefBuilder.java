/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
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
 * Builder for {@link PagedBytesRef} that checks its size against a {@link CircuitBreaker}.
 */
public class PagedBytesRefBuilder implements Accountable, Releasable, Comparable<PagedBytesRefBuilder> {
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PagedBytesRefBuilder.class);

    /**
     * Build with a small initial pages array.
     */
    public static PagedBytesRefBuilder smallCapacity(CircuitBreaker breaker, String label, PageCacheRecycler recycler) {
        return new PagedBytesRefBuilder(breaker, label, 1, recycler);
    }

    /**
     * Build, pre-sizing the pages array for {@code initialCapacity} bytes without
     * allocating any pages.
     */
    public static PagedBytesRefBuilder withInitialCapacity(
        CircuitBreaker breaker,
        String label,
        int initialCapacity,
        PageCacheRecycler recycler
    ) {
        int pageCount = (initialCapacity + BYTE_PAGE_SIZE - 1) / BYTE_PAGE_SIZE;
        return new PagedBytesRefBuilder(breaker, label, pageCount, recycler);
    }

    /**
     * All pages, including the tail. Entries {@code 0..usedPages-1} are in use.
     * Set to {@code null} by {@link #build()} — the builder is invalid after that.
     */
    private Recycler.V<byte[]>[] pages;

    /**
     * Number of pages in use.
     */
    private int usedPages;

    /**
     * The page currently being filled. Always {@code pages[usedPages - 1]} once
     * at least one page has been allocated, {@code null} otherwise.
     */
    private byte[] tail;

    /**
     * Number of bytes written into {@link #tail}.
     */
    private int tailOffset;

    private final CircuitBreaker breaker;
    private final String label;
    private final PageCacheRecycler recycler;

    @SuppressWarnings("unchecked")
    private PagedBytesRefBuilder(CircuitBreaker breaker, String label, int initialSizeOfPages, PageCacheRecycler recycler) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE + bytesArrayRamBytesUsed(initialSizeOfPages), label);
        this.pages = new Recycler.V[initialSizeOfPages];
        this.recycler = recycler;
        this.breaker = breaker;
        this.label = label;
    }

    /**
     * Append a byte.
     */
    public void append(byte b) {
        if (tail == null || tailOffset == BYTE_PAGE_SIZE) {
            addPage();
        }
        tail[tailOffset++] = b;
    }

    /**
     * Append bytes.
     */
    public void append(byte[] b, int off, int len) {
        while (len > 0) {
            if (tail == null || tailOffset == BYTE_PAGE_SIZE) {
                addPage();
            }
            int toCopy = Math.min(BYTE_PAGE_SIZE - tailOffset, len);
            System.arraycopy(b, off, tail, tailOffset, toCopy);
            tailOffset += toCopy;
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
     * Total bytes written so far.
     */
    public int length() {
        return usedPages == 0 ? 0 : (usedPages - 1) * BYTE_PAGE_SIZE + tailOffset;
    }

    /**
     * Build a {@link PagedBytesRef} from the bytes written so far. Transfers ownership
     * of {@link #pages} to the result — this builder is invalid after this call.
     */
    public PagedBytesRef build() {
        if (usedPages == 0) {
            return PagedBytesRef.EMPTY;
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
        pages = null;
        usedPages = 0;
        tail = null;
        tailOffset = 0;
        return result;
    }

    private void addPage() {
        if (usedPages == pages.length) {
            int newLength = pages.length * 2;
            int oldLength = pages.length;
            breaker.addEstimateBytesAndMaybeBreak(bytesArrayRamBytesUsed(newLength), label);
            pages = Arrays.copyOf(pages, newLength);
            breaker.addWithoutBreaking(-bytesArrayRamBytesUsed(oldLength));
        }
        breaker.addEstimateBytesAndMaybeBreak(pageRamBytesUsed(), label);
        Recycler.V<byte[]> v = recycler.bytePage(false);
        pages[usedPages++] = v;
        tail = v.v();
        tailOffset = 0;
    }

    private static long pageRamBytesUsed() {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + BYTE_PAGE_SIZE);
    }

    private static long bytesArrayRamBytesUsed(int capacity) {
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
            int diff = Arrays.compareUnsigned(this.pages[fullPages].v(), 0, tailLen, rhs.pages[fullPages].v(), 0, tailLen);
            if (diff != 0) {
                return diff;
            }
        }

        // All shared bytes are the same.
        return this.length() - rhs.length();
    }

    @Override
    public long ramBytesUsed() {
        return pages == null ? 0 : SHALLOW_SIZE + bytesArrayRamBytesUsed(pages.length) + (long) usedPages * pageRamBytesUsed();
    }

    @Override
    public void close() {
        if (pages != null) {
            breaker.addWithoutBreaking(-ramBytesUsed());
            Releasables.close(pages);
        }
    }
}
