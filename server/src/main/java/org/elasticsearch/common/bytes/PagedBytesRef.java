/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Represents a slice of pages spread across multiple {@code byte[]}. All pages
 * except the last have exactly {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
 * {@link #length} tracks how many of those bytes are valid.
 */
public final class PagedBytesRef implements Comparable<PagedBytesRef>, Releasable {
    /**
     * The actual data. All entries have {@link PageCacheRecycler#BYTE_PAGE_SIZE} bytes;
     * only the first {@link #length} bytes across all pages are valid.
     */
    private final byte[][] pages;
    /**
     * Number of valid bytes across all {@link #pages}.
     */
    private final int length;
    /**
     * Called on {@link #close()}.
     */
    private final Releasable onClose;

    public static final PagedBytesRef EMPTY = new PagedBytesRef(new byte[0][], 0, () -> {});

    PagedBytesRef(byte[][] pages, int length, Releasable onClose) {
        if (Assertions.ENABLED) {
            for (int i = 0; i < pages.length - 1; i++) {
                if (pages[i].length != BYTE_PAGE_SIZE) {
                    throw new IllegalStateException("page " + i + " has length " + pages[i].length + " but expected " + BYTE_PAGE_SIZE);
                }
            }
            if (pages.length > 0 && pages[pages.length - 1].length > BYTE_PAGE_SIZE) {
                throw new IllegalStateException(
                    "last page has length " + pages[pages.length - 1].length + " but expected at most " + BYTE_PAGE_SIZE
                );
            }
        }
        this.pages = pages;
        this.length = length;
        this.onClose = onClose;
    }

    /**
     * The actual data. All entries have exactly {@link PageCacheRecycler#BYTE_PAGE_SIZE} bytes;
     * only the first {@link #length} bytes across all pages are valid. Generally this will be
     * in one of two shapes:
     * <ul>
     *     <li>A single page containing a short value (e.g. {@code length=3}): {@snippet lang=java :
     *     new byte[][] { page }  // page.length == BYTE_PAGE_SIZE, only first 3 bytes are valid
     *     }</li>
     *     <li>Many full pages (e.g. {@code length = 5 * BYTE_PAGE_SIZE}): {@snippet lang=java :
     *     new byte[][] { page1, page2, page3, page4, page5 }
     *     }</li>
     * </ul>
     */
    public byte[][] pages() {
        return pages;
    }

    /**
     * Number of valid bytes across all {@link #pages}.
     */
    public int length() {
        return length;
    }

    /**
     * Returns {@code true} if this paged byte sequence is equal to {@code other}.
     * Compatible with {@link BytesRef#bytesEquals(BytesRef)}.
     */
    public boolean bytesEquals(BytesRef other) {
        if (length != other.length) {
            return false;
        }
        int otherOffset = other.offset;
        int remaining = length;
        for (byte[] page : pages) {
            int len = Math.min(page.length, remaining);
            for (int i = 0; i < len; i++) {
                if (page[i] != other.bytes[otherOffset++]) {
                    return false;
                }
            }
            remaining -= len;
            if (remaining == 0) {
                break;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj.getClass() != PagedBytesRef.class) {
            return false;
        }
        PagedBytesRef rhs = (PagedBytesRef) obj;
        if (this.length != rhs.length) {
            return false;
        }
        return compareTo(rhs) == 0;
    }

    @Override
    public int hashCode() {
        // Must match BytesRef.hashCode() for the same byte sequence so that paged and
        // flat keys are interchangeable in BytesRefHashTable.
        // BytesRef.hashCode() delegates to StringHelper.murmurhash3_x86_32.
        // NOCOMMIT: this materializes all bytes into a temporary array. Replace with a
        // streaming murmur3 implementation that processes pages in-place.
        byte[] flat = new byte[length];
        int off = 0;
        int remaining = length;
        for (byte[] page : pages) {
            int len = Math.min(page.length, remaining);
            System.arraycopy(page, 0, flat, off, len);
            off += len;
            remaining -= len;
            if (remaining == 0) break;
        }
        return StringHelper.murmurhash3_x86_32(flat, 0, length, StringHelper.GOOD_FAST_HASH_SEED);
    }

    @Override
    public int compareTo(PagedBytesRef rhs) {
        int remaining = Math.min(this.length, rhs.length);
        int fullPages = remaining / BYTE_PAGE_SIZE;
        int tail = remaining % BYTE_PAGE_SIZE;

        for (int page = 0; page < fullPages; page++) {
            int diff = Arrays.compareUnsigned(this.pages[page], rhs.pages[page]);
            if (diff != 0) {
                return diff;
            }
        }

        if (tail > 0) {
            int diff = Arrays.compareUnsigned(this.pages[fullPages], 0, tail, rhs.pages[fullPages], 0, tail);
            if (diff != 0) {
                return diff;
            }
        }

        // All shared pages are the same.
        return this.length - rhs.length;
    }

    @Override
    public void close() {
        onClose.close();
    }
}
