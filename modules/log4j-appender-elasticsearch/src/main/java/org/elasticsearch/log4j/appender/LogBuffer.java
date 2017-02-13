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

/**
 * Buffers logs before sending them to Elasticsearch.
 */
class LogBuffer {
    private final BiConsumer<String, Throwable> debugLogger;
    private final BiConsumer<String, Throwable> warnLogger;
    private final BiConsumer<String, Throwable> errorLogger;
    private final BiConsumer<Iterable<? extends LogEvent>, Runnable> flush;
    private final int batchSize;

    private volatile MutableLogEvent[] building;
    private volatile int next;
    private volatile boolean flushing;
    private volatile MutableLogEvent[] flip;

    public LogBuffer(BiConsumer<String, Throwable> debugLogger, BiConsumer<String, Throwable> warnLogger,
            BiConsumer<String, Throwable> errorLogger, BiConsumer<Iterable<? extends LogEvent>, Runnable> flush, int batchSize) {
        this.debugLogger = debugLogger;
        this.warnLogger = warnLogger;
        this.errorLogger = errorLogger;
        this.flush = flush;
        this.batchSize = batchSize;
        building = initBuffer();
        flip = initBuffer();
    }

    public void buffer(LogEvent event) {
        // TODO be much fancier about stuff! Like:
        // * Flush on errors and fatals.
        // * Flush on a deadline so things don't sit in the buffer forever. Very important, I think.
        // * Support min and max batch size.
        List<? extends LogEvent> toFlush = null;
        synchronized (this) {
            if (building == null) {
                throw new IllegalStateException("Can't log event while buffer is closed");
            }
            building[next++].initFrom(event);
            if (next >= batchSize) {
                if (flushing) {
                    // TODO throw away more intelligently
                    warnLogger.accept("throwing away log that arrived too quickly", null);
                    next--;
                } else {
                    flushing = true;
                    toFlush = flip();
                }
            }
        }
        if (toFlush != null) {
            debugLogger.accept("Flushing", null);
            flush.accept(toFlush, this::flushed);
        }
    }

    private void flushed() {
        debugLogger.accept("Flushed", null);
        flushing = false;
    }

    public boolean close(long timeout, TimeUnit timeUnit) {
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
            if (end - System.nanoTime() > 0) {
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
            debugLogger.accept("Flushing for shutdown", null);
            flush.accept(toFlush, this::flushed);
            flushing = true;

            while (flushing) {
                if (end - System.nanoTime() > 0) {
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
        MutableLogEvent[] buffer = new MutableLogEvent[batchSize];
        for (int i = 0; i < batchSize; i++) {
            buffer[i] = new MutableLogEvent();
        }
        return buffer;
    }
}
