/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * A sequential-read cursor over a {@link PagedBytesRef}. Tracks the current page
 * index and offset within that page. Callers advance the cursor by calling the
 * {@code read*} methods.
 */
public class PagedBytesRefCursor {
    // NOCOMMIT should we use native byte order instead of big-endian? It'd be faster on x86, but we'd need to audit all callers.
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final byte[][] pages;
    private int pageIndex;
    private int pageOffset;
    private int remaining;

    // NOCOMMIT rename to PagedBytesCursor
    PagedBytesRefCursor(PagedBytesRef ref) {
        this.pages = ref.pages();
        this.remaining = ref.length();
    }

    /**
     * Wrap a flat {@link BytesRef} as a single-page cursor. No copy is made.
     */
    public static PagedBytesRefCursor fromBytesRef(BytesRef ref) {
        return new PagedBytesRefCursor(ref);
    }

    private PagedBytesRefCursor(BytesRef ref) {
        this.pages = new byte[][] { ref.bytes };
        this.pageIndex = 0;
        this.pageOffset = ref.offset;
        this.remaining = ref.length;
    }

    /**
     * Number of bytes not yet read.
     */
    public int remaining() {
        return remaining;
    }

    /**
     * Read one byte and advance.
     */
    public byte readByte() {
        if (remaining <= 0) {
            throw new IllegalArgumentException("no bytes remaining");
        }
        byte b = pages[pageIndex][pageOffset++];
        remaining--;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return b;
    }

    /**
     * Read a big-endian {@code int} and advance.
     */
    public int readInt() {
        if (remaining < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Integer.BYTES) {
            int v = (int) INT.get(pages[pageIndex], pageOffset);
            pageOffset += Integer.BYTES;
            remaining -= Integer.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((readByte() & 0xFF) << 24)
            | ((readByte() & 0xFF) << 16)
            | ((readByte() & 0xFF) << 8)
            |  (readByte() & 0xFF);
    }

    /**
     * Read a big-endian {@code long} and advance.
     */
    public long readLong() {
        if (remaining < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Long.BYTES) {
            long v = (long) LONG.get(pages[pageIndex], pageOffset);
            pageOffset += Long.BYTES;
            remaining -= Long.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((long)(readByte() & 0xFF) << 56)
            | ((long)(readByte() & 0xFF) << 48)
            | ((long)(readByte() & 0xFF) << 40)
            | ((long)(readByte() & 0xFF) << 32)
            | ((long)(readByte() & 0xFF) << 24)
            | ((long)(readByte() & 0xFF) << 16)
            | ((long)(readByte() & 0xFF) << 8)
            |  (long)(readByte() & 0xFF);
    }

    /**
     * Read an int stored in variable-length format. Reads between one and five bytes.
     * Smaller values take fewer bytes. Negative numbers always use all 5 bytes.
     */
    public int readVInt() {
        byte b = readByte();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid last byte for a vint [" + Integer.toHexString(b) + "]");
        }
        return i;
    }

    /**
     * Read {@code len} bytes and advance. Returns a {@link BytesRef} pointing directly
     * into the current page when the bytes fit within it (zero-copy), or copies into
     * {@code scratch} when they span a page boundary.
     * <p>
     * TODO: callers that mutate the result or hold it past the lifetime of the
     * {@link PagedBytesRef} must copy — migrate them to do so explicitly.
     * </p>
     */
    public BytesRef readBytesRef(int len, BytesRef scratch) {
        if (remaining < len) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= len) {
            scratch.bytes = pages[pageIndex];
            scratch.offset = pageOffset;
            scratch.length = len;
            pageOffset += len;
            remaining -= len;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return scratch;
        }
        scratch.bytes = ArrayUtil.grow(scratch.bytes, len);
        scratch.offset = 0;
        scratch.length = len;
        for (int i = 0; i < len; i++) {
            scratch.bytes[i] = readByte();
        }
        return scratch;
    }

    /**
     * Read {@code len} bytes and advance, always copying into {@code scratch}.
     * Use when the caller will mutate the returned bytes in-place.
     * NOCOMMIT optimize: use arraycopy for the within-page case
     */
    public BytesRef readBytesRefMutable(int len, BytesRef scratch) {
        if (remaining < len) {
            throw new IllegalArgumentException("not enough bytes");
        }
        scratch.bytes = ArrayUtil.grow(scratch.bytes, len);
        scratch.offset = 0;
        scratch.length = len;
        for (int i = 0; i < len; i++) {
            scratch.bytes[i] = readByte();
        }
        return scratch;
    }

    /**
     * Read bytes up to (and consuming) {@code terminator}, copying them into {@code scratch}.
     * Always copies — callers are expected to mutate the result in place.
     * <p>
     * TODO: callers that do not mutate the result may be able to use
     * {@link #readBytesRef(int, BytesRef)} with a pre-scanned length to get the
     * zero-copy fast path.
     * </p>
     */
    public BytesRef readTerminatedBytesRef(byte terminator, BytesRef scratch) {
        int len = findTerminator(terminator);
        readBytesRefMutable(len, scratch);
        readByte(); // consume the terminator
        return scratch;
    }

    private int findTerminator(byte terminator) {
        int scanPageIndex = pageIndex;
        int scanOffset = pageOffset;
        int scanRemaining = remaining;
        int len = 0;
        while (scanRemaining > 0) {
            if (pages[scanPageIndex][scanOffset] == terminator) {
                return len;
            }
            len++;
            scanOffset++;
            scanRemaining--;
            if (scanOffset >= pages[scanPageIndex].length && scanRemaining > 0) {
                scanPageIndex++;
                scanOffset = 0;
            }
        }
        throw new IllegalArgumentException("terminator not found");
    }
}
