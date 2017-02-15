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
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static java.lang.Math.max;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LogBufferTests extends ESTestCase {
    public void testLessThanBuffer() {
        int batchSize = between(1000, 50000);
        AtomicReference<Iterable<? extends LogEvent>> flushData = new AtomicReference<>();
        LogBuffer buffer = newLogBuffer(batchSize, batchSize, Long.MAX_VALUE, () -> true, (events, callback) -> {
            Object old = flushData.getAndSet(events);
            assertNull("Only expected one flush", old);
            callback.run();
        });

        int count = between(1, batchSize);
        for (int i = 0; i < count; i++) {
            MutableLogEvent event = new MutableLogEvent();
            event.setTimeMillis(i);
            buffer.buffer(event);
        }

        assertNull("Shouldn't have flushed yet", flushData.get());
        assertTrue(buffer.close(30, TimeUnit.SECONDS));

        Iterator<? extends LogEvent> event = flushData.get().iterator();
        for (int i = 0; i < count; i++) {
            assertTrue(event.hasNext());
            assertEquals(i, event.next().getTimeMillis());
        }
        assertFalse(event.hasNext());
    }

    public void testInProcessFlush() {
        AtomicBoolean closing = new AtomicBoolean(false);
        int batchSize = between(50, 1000);
        int count = between(batchSize, 50000);
        List<Long> flushData = new ArrayList<>(count);
        LogBuffer buffer = newLogBuffer(batchSize, batchSize, Long.MAX_VALUE, () -> true, (events, callback) -> {
            int flushSize = 0;
            for (LogEvent event: events) {
                flushData.add(event.getTimeMillis());
                flushSize++;
            }
            if (false == closing.get()) {
                assertEquals(batchSize, flushSize);
            }
            callback.run();
        });

        for (int i = 0; i < count; i++) {
            MutableLogEvent event = new MutableLogEvent();
            event.setTimeMillis(i);
            buffer.buffer(event);
        }

        closing.set(true);
        assertTrue(buffer.close(30, TimeUnit.SECONDS));

        assertThat(flushData, hasSize(count));
        for (int i = 0; i < count; i++) {
            assertEquals((Long) (long) i, flushData.get(i));
        }
    }

    public void testConcurrentLogs() throws InterruptedException {
        AtomicBoolean closing = new AtomicBoolean(false);
        int batchSize = between(50, 100);
        int count = between(batchSize, 5000);
        Thread[] threads = new Thread[between(2, max(3, Runtime.getRuntime().availableProcessors()))];
        List<Long> flushData = new ArrayList<>(count);
        int maxBatchSize = batchSize * threads.length;
        LogBuffer buffer = newLogBuffer(batchSize, maxBatchSize, Long.MAX_VALUE, () -> true, (events, callback) -> {
            int flushSize = 0;
            for (LogEvent event: events) {
                flushData.add(event.getTimeMillis());
                flushSize++;
            }
            if (false == closing.get()) {
                assertThat(flushSize, both(greaterThanOrEqualTo(batchSize)).and(lessThanOrEqualTo(maxBatchSize)));
            }
            callback.run();
        });

        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    MutableLogEvent event = new MutableLogEvent();
                    event.setTimeMillis(i);
                    boolean overTargetSize = buffer.buffer(event);
                    while (overTargetSize) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        overTargetSize = buffer.overTargetSize();
                    }
                }
            });
            threads[t].start();
        }

        for (int t = 0; t < threads.length; t++) {
            threads[t].join();
        }

        closing.set(true);
        assertTrue(buffer.close(30, TimeUnit.SECONDS));

        assertThat(flushData, hasSize(count * threads.length));
    }

    // NOCOMMIT test flushready

    public void testPollForFlushMillis() {
        long flushMillis = between(10, 100000);
        List<Long> flushData = new ArrayList<>();
        LogBuffer buffer = newLogBuffer(5000, 5000, flushMillis, () -> true, (events, callback) -> {
            for (LogEvent event: events) {
                flushData.add(event.getTimeMillis());
            }
            callback.run();
        });

        // Totally empty
        assertEquals(flushMillis, buffer.pollForFlushMillis(0));
        assertThat(flushData, hasSize(0));

        // One event
        MutableLogEvent event = new MutableLogEvent();
        event.setTimeMillis(0);
        buffer.buffer(event);
        assertThat(flushData, hasSize(0));

        assertEquals(flushMillis, buffer.pollForFlushMillis(0));
        assertThat(flushData, hasSize(0));
        assertEquals(flushMillis - 1, buffer.pollForFlushMillis(1));
        assertThat(flushData, hasSize(0));
        assertEquals(flushMillis, buffer.pollForFlushMillis(flushMillis));
        assertThat(flushData, hasSize(1));

        // A couple of events
        event.setTimeMillis(flushMillis);
        buffer.buffer(event);
        event.setTimeMillis(flushMillis);
        buffer.buffer(event);
        assertThat(flushData, hasSize(1));

        assertEquals(flushMillis - 1, buffer.pollForFlushMillis(flushMillis + 1));
        assertThat(flushData, hasSize(1));
        assertEquals(flushMillis, buffer.pollForFlushMillis(2 * flushMillis));
        assertThat(flushData, hasSize(3));

        // An event new the timer wrap
        event.setTimeMillis(Long.MAX_VALUE);
        buffer.buffer(event);
        assertThat(flushData, hasSize(3));

        assertEquals(flushMillis, buffer.pollForFlushMillis(Long.MAX_VALUE));
        assertThat(flushData, hasSize(3));
        assertEquals(flushMillis - 1, buffer.pollForFlushMillis(Long.MAX_VALUE + 1));
        assertThat(flushData, hasSize(3));
        assertEquals(flushMillis, buffer.pollForFlushMillis(Long.MAX_VALUE + flushMillis));
        assertThat(flushData, hasSize(4));

        assertTrue(buffer.close(30, TimeUnit.SECONDS));
        assertThat(buffer.pollForFlushMillis(randomLong()), lessThan(0L));
    }

    private LogBuffer newLogBuffer(int targetBatchSize, int maxBatchSize, long flushAfterMillis, BooleanSupplier flushReady,
            BiConsumer<Iterable<? extends LogEvent>, Runnable> flush) {
        return new LogBuffer(
                (m, t) -> {},
                (m, t) -> {throw new AssertionError(m, t);},
                (m, t) -> {throw new AssertionError(m, t);},
                flushReady, flush, targetBatchSize, maxBatchSize, flushAfterMillis);
    }
}
