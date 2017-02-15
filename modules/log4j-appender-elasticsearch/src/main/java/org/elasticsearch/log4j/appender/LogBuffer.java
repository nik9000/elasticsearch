/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.log4j.appender;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.MutableLogEvent;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * Buffers logs before sending them to Elasticsearch.
 */
class LogBuffer {
    private final BiConsumer<String, Throwable> debugLogger;
    private final BiConsumer<String, Throwable> warnLogger;
    private final BiConsumer<String, Throwable> errorLogger;
    private final BooleanSupplier flushReady;
    private final BiConsumer<Iterable<? extends LogEvent>, Runnable> flush;
    /**
     * Number of logs that must be buffered before we start flushing the buffer to Elasticsearch. Defaults to {@code 100} which seems
     * like as good a number as any.
     */
    private final int targetBatchSize;
    /**
     * Maximum number of logs that can be buffered at any point before we start overwriting the last buffered log. Defaults to
     * {@code 1000} which means that the buffer takes up about a megabyte of heap.
     */
    private final int maxBatchSize;
    private final long flushAfterMillis;

    private volatile MutableLogEvent[] building;
    private volatile int next;
    private volatile boolean flushing;
    private volatile MutableLogEvent[] flip;

    LogBuffer(BiConsumer<String, Throwable> debugLogger, BiConsumer<String, Throwable> warnLogger,
            BiConsumer<String, Throwable> errorLogger, BooleanSupplier flushReady,
            BiConsumer<Iterable<? extends LogEvent>, Runnable> flush, int targetBatchSize, int maxBatchSize, long flushAfterMillis) {
        if (targetBatchSize > maxBatchSize) {
            throw new IllegalArgumentException("targetBatchSize [" + targetBatchSize + "] must be <= maxBatchSize [" + maxBatchSize + "]");
        }
        this.debugLogger = debugLogger;
        this.warnLogger = warnLogger;
        this.errorLogger = errorLogger;
        this.flushReady = flushReady;
        this.flush = flush;
        this.targetBatchSize = targetBatchSize;
        this.maxBatchSize = maxBatchSize;
        this.flushAfterMillis = flushAfterMillis;
        building = initBuffer();
        flip = initBuffer();
    }

    /**
     * Buffer a log event.
     * @return true if the buffer is currently over its target size
     */
    boolean buffer(LogEvent event) {
        // TODO be much fancier about stuff! Like:
        // * Flush on errors and fatals?
        boolean overTargetSize = false;
        List<? extends LogEvent> toFlush = null;
        synchronized (this) {
            if (building == null) {
                throw new IllegalStateException("can't log event while buffer is closed");
            }
            building[next++].initFrom(event);
            if (next >= targetBatchSize) {
                if (flushing || false == flushReady.getAsBoolean()) {
                    if (next >= maxBatchSize) {
                        // TODO throw away more intelligently
                        warnLogger.accept("throwing away log that that would overfill buffer", null);
                        next--;
                    } else {
                        overTargetSize = true;
                    }
                } else {
                    flushing = true;
                    toFlush = flip();
                }
            }
        }
        if (toFlush != null) {
            debugLogger.accept("flushing due to size", null);
            flush.accept(toFlush, this::flushed);
        }
        return overTargetSize;
    }

    /**
     * Is the buffer currently over its target size?
     */
    boolean overTargetSize() {
        return next > targetBatchSize;
    }

    void flush(long timeout, TimeUnit unit) throws InterruptedException {
        long end = System.nanoTime() + unit.toNanos(timeout);
        List<? extends LogEvent> toFlush = null;
        synchronized (this) {
            while (true) {
                if (next == 0 || building == null) {
                    // Nothing to flush so we return immediately claiming we've flushed everything.
                    return;
                }
                if (false == flushReady.getAsBoolean()) {
                    if (end - System.nanoTime() < 0) {
                        throw new RuntimeException("timed out while waiting for flush to become ready");
                    }
                    Thread.sleep(100);
                    continue;
                }
                if (flushing) {
                    if (end - System.nanoTime() < 0) {
                        throw new RuntimeException("timed out waiting ongoing flush to finish");
                    }
                    Thread.sleep(100);
                    continue;
                }
                toFlush = flip();
                flushing = true;
                break;
            }
        }
        debugLogger.accept("explicitly flushing", null);
        flush.accept(toFlush, this::flushed);
        while (flushing) {
            if (end - System.nanoTime() < 0) {
                throw new RuntimeException("timed out waiting flush to finish");
            }
            Thread.sleep(100);
        }
    }

    /**
     * Initiate a flush if the oldest log enough is more than {@link #flushAfterMillis} millis behind {@code now}. Note that this uses wall
     * clock time ({@link System#currentTimeMillis()}) instead of elapsed time ({@link System#nanoTime()}) because elapsed time is usually
     * not stored by log4j. In addition this uses the first element in the buffer as the "earliest" element when this isn't *technically*
     * true. The elements after it *might* have lower times but they are unlikely to be different enough to matter at a time scale of
     * seconds and anyone who flushes more than once a second is going to be in trouble anyway.
     *
     * @return returns the number of milliseconds to wait until the next poll with all negative numbers meaning "don't reschedule at all"
     */
    long pollForFlushMillis(long now) {
        if (building == null) {
            // Closed
            return Long.MIN_VALUE;
        }
        if (next == 0) {
            // Nothing in the buffer
            return flushAfterMillis;
        }
        if (false == flushReady.getAsBoolean()) {
            // Not ready, check back in a second
            return 1000;
        }
        long toWait = flushAfterMillis;
        List<? extends LogEvent> toFlush = null;
        synchronized (this) {
            long timeSince = now - building[0].getTimeMillis();
            if (timeSince >= flushAfterMillis) {
                toFlush = flip();
            } else {
                toWait -= timeSince;
            }
        }
        if (toFlush != null) {
            debugLogger.accept("Flushing due to log age", null);
            flushing = true;
            flush.accept(toFlush, this::flushed);
        }
        return toWait;
    }

    private void flushed() {
        debugLogger.accept("Flushed", null);
        flushing = false;
        // TODO immediately start a flush if we are at or over target batch size?
    }

    boolean close(long timeout, TimeUnit timeUnit) {
        long end = System.nanoTime() + timeUnit.toNanos(timeout);

        // Shutdown the buffer under lock
        List<? extends LogEvent> toFlush;
        synchronized (this) {
            if (building == null) {
                throw new IllegalStateException("Already closed");
            }
            flip = null;
            toFlush = flip();
        }

        // If there is a flush outstanding then wait for it to complete
        debugLogger.accept("Waiting for outstanding flush", null);
        while (flushing) {
            if (end - System.nanoTime() < 0) {
                errorLogger.accept("timed out while waiting for in progress flush to elasticsearch to complete", null);
                return false;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                errorLogger.accept("interrupted waiting for in progress flush to elasticsearch to complete", e);
                Thread.currentThread().interrupt();
                return false;
            }
        }

        // Flush any remaining logs
        if (false == toFlush.isEmpty()) {
            if (false == flushReady.getAsBoolean()) {
                debugLogger.accept("flush is not ready so skipping flush", null);
                while (false == flushReady.getAsBoolean()) {
                    if (end - System.nanoTime() < 0) {
                        errorLogger.accept("timed out waiting for flush to become ready for shutting down", null);
                        return false;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        errorLogger.accept("interrupted waiting for flush to become ready for shutting down", e);
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
            debugLogger.accept("flushing for shutdown", null);
            flushing = true;
            flush.accept(toFlush, this::flushed);

            while (flushing) {
                if (end - System.nanoTime() < 0) {
                    errorLogger.accept("timed out while flushing to Elasticsearch before shutting down", null);
                    return false;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    errorLogger.accept("interrupted while flushing to Elasticsearch before shutting down", e);
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Flip the backup buffer on top of the current one and return a list of {@link LogEvent}s to flush.
     */
    private List<? extends LogEvent> flip() {
        List<? extends LogEvent> toFlush = Arrays.asList(building);
        MutableLogEvent[] tmp = building;
        building = flip;
        flip = tmp;
        toFlush = toFlush.subList(0, next);
        next = 0;
        return toFlush;
    }

    private MutableLogEvent[] initBuffer() {
        MutableLogEvent[] buffer = new MutableLogEvent[maxBatchSize];
        for (int i = 0; i < maxBatchSize; i++) {
            buffer[i] = new MutableLogEvent();
        }
        return buffer;
    }
}
