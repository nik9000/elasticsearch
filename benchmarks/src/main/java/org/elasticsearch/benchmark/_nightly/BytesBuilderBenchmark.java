/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytes;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the throughput of four byte-buffer builders for writing and reading
 * back integer data:
 * <ul>
 *   <li>{@code paged} – {@link PagedBytesBuilder}, page-cache-backed, circuit-broken</li>
 *   <li>{@code breaking} – {@link BreakingBytesRefBuilder}, single contiguous array, circuit-broken</li>
 *   <li>{@code plain} – Lucene's {@link BytesRefBuilder}, single contiguous array, no breaker</li>
 *   <li>{@code stream} – {@link BytesStreamOutput}, BigArrays-backed stream, no breaker</li>
 * </ul>
 * <p>
 * The {@code data} parameter controls what is written per invocation, e.g.
 * {@code 1000_ints} writes 1000 big-endian {@code int} values (4,000 bytes total).
 * <p>
 * The {@code operation} parameter selects:
 * <ul>
 *   <li>{@code write} – clear the builder then write all data items from scratch</li>
 *   <li>{@code read} – iterate over the pre-written bytes and sum them (no write cost)</li>
 * </ul>
 * <p>
 * The builders are intentionally not pre-sized per invocation: after the first
 * iteration each builder has already grown to accommodate the data, so subsequent
 * {@code write} iterations measure steady-state append throughput without
 * allocation cost.
 * <p>
 * The {@link #selfTest()} runs before any benchmark iteration and exercises all
 * four implementations, which primes the JIT with all the code paths it needs
 * to compile efficiently.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class BytesBuilderBenchmark {

    private static final VarHandle INT_BIG_ENDIAN = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    static {
        Utils.configureBenchmarkLogging();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod.
            // Also primes the JIT with all four implementations before JMH begins warmup.
            selfTest();
        }
    }

    @Param({ "paged", "breaking", "plain", "stream" })
    public String impl;

    @Param({ "write", "read" })
    public String operation;

    @Param({ "1000_ints", "4000_ints", "1000_vints" })
    public String data;

    // parsed from data param
    private int count;
    private String dataType;
    private int[] ints;

    private Paged paged;
    private Breaking breaking;
    private Plain plain;
    private Stream stream;

    @Setup
    public void setup() throws IOException {
        parseData();
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("benchmark");
        int byteSize = count * Integer.BYTES;
        paged = new Paged(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker, byteSize);
        breaking = new Breaking(breaker, byteSize);
        plain = new Plain();
        stream = new Stream(byteSize);

        if (operation.equals("read")) {
            // Pre-populate the active builder so that read benchmarks measure only
            // the read path, not write cost.
            write();
        }
    }

    private void parseData() {
        // Format: "{count}_{type}", e.g. "1000_ints" or "1000_vints"
        int underscore = data.indexOf('_');
        if (underscore < 0) {
            throw new IllegalArgumentException("data param must be '{count}_{type}', got: " + data);
        }
        count = Integer.parseInt(data.substring(0, underscore));
        dataType = data.substring(underscore + 1);
        ints = new int[count];
        Random rng = new Random(42);
        for (int i = 0; i < count; i++) {
            ints[i] = rng.nextInt();
        }
    }

    @TearDown
    public void teardown() {
        paged.close();
        breaking.close();
    }

    @Benchmark
    public long run() throws IOException {
        return switch (operation) {
            case "write" -> write();
            case "read" -> read();
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };
    }

    private long write() throws IOException {
        return switch (dataType) {
            case "ints" -> switch (impl) {
                case "paged" -> paged.writeInts(ints);
                case "breaking" -> breaking.writeInts(ints);
                case "plain" -> plain.writeInts(ints);
                case "stream" -> stream.writeInts(ints);
                default -> throw new IllegalArgumentException("unknown impl: " + impl);
            };
            case "vints" -> switch (impl) {
                case "paged" -> paged.writeVInts(ints);
                case "breaking" -> breaking.writeVInts(ints);
                case "plain" -> plain.writeVInts(ints);
                case "stream" -> stream.writeVInts(ints);
                default -> throw new IllegalArgumentException("unknown impl: " + impl);
            };
            default -> throw new IllegalArgumentException("unknown data type: " + dataType);
        };
    }

    private long read() throws IOException {
        return switch (dataType) {
            case "ints" -> switch (impl) {
                case "paged" -> paged.readInts();
                case "breaking" -> breaking.readInts();
                case "plain" -> plain.readInts();
                case "stream" -> stream.readInts();
                default -> throw new IllegalArgumentException("unknown impl: " + impl);
            };
            case "vints" -> switch (impl) {
                case "paged" -> paged.readVInts();
                case "breaking" -> breaking.readVInts();
                case "plain" -> plain.readVInts();
                case "stream" -> stream.readVInts();
                default -> throw new IllegalArgumentException("unknown impl: " + impl);
            };
            default -> throw new IllegalArgumentException("unknown data type: " + dataType);
        };
    }

    interface Destination {
        long writeInts(int[] ints) throws IOException;

        long readInts() throws IOException;

        long writeVInts(int[] ints) throws IOException;

        long readVInts() throws IOException;
    }

    static class Paged implements Destination {
        private final PagedBytesBuilder builder;

        Paged(PageCacheRecycler recycler, NoopCircuitBreaker breaker, int initialCapacity) {
            builder = new PagedBytesBuilder(recycler, breaker, "benchmark", initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.append(v);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor();
            long sum = 0;
            while (cursor.remaining() >= Integer.BYTES) {
                sum += cursor.readInt();
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.appendVInt(v);
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor();
            long sum = 0;
            while (cursor.remaining() > 0) {
                sum += cursor.readVInt();
            }
            return sum;
        }

        void close() {
            builder.close();
        }
    }

    static class Breaking implements Destination {
        private final BreakingBytesRefBuilder builder;

        Breaking(NoopCircuitBreaker breaker, int initialCapacity) {
            builder = new BreakingBytesRefBuilder(breaker, "benchmark", initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + Integer.BYTES);
                INT_BIG_ENDIAN.set(builder.bytes(), builder.length(), v);
                builder.setLength(builder.length() + Integer.BYTES);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            BytesRef ref = builder.bytesRefView();
            long sum = 0;
            for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
                sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + 5);
                builder.setLength(appendVInt(builder.bytes(), builder.length(), v));
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            BytesRef ref = builder.bytesRefView();
            return sumVInts(ref.bytes, ref.offset, ref.offset + ref.length);
        }

        void close() {
            builder.close();
        }
    }

    static class Plain implements Destination {
        private final BytesRefBuilder builder = new BytesRefBuilder();

        @Override
        public long writeInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + Integer.BYTES);
                INT_BIG_ENDIAN.set(builder.bytes(), builder.length(), v);
                builder.setLength(builder.length() + Integer.BYTES);
            }
            return builder.length();
        }

        @Override
        public long readInts() {
            BytesRef ref = builder.get();
            long sum = 0;
            for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
                sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) {
            builder.clear();
            for (int v : ints) {
                builder.grow(builder.length() + 5);
                builder.setLength(appendVInt(builder.bytes(), builder.length(), v));
            }
            return builder.length();
        }

        @Override
        public long readVInts() {
            BytesRef ref = builder.get();
            return sumVInts(ref.bytes, ref.offset, ref.offset + ref.length);
        }
    }

    static class Stream implements Destination {
        private final BytesStreamOutput output;

        Stream(int initialCapacity) {
            output = new BytesStreamOutput(initialCapacity);
        }

        @Override
        public long writeInts(int[] ints) throws IOException {
            output.reset();
            for (int v : ints) {
                output.writeInt(v);
            }
            return output.size();
        }

        /**
         * Note: {@link BytesStreamOutput#bytes()} returns a concrete {@link org.elasticsearch.common.bytes.BytesArray}
         * for small outputs, so {@link org.elasticsearch.common.bytes.BytesReference#streamInput()} and
         * {@link StreamInput#readInt()} here are likely monomorphic — giving this variant an inherent
         * advantage over the others that may not appear in real workloads where multiple implementations
         * are live.
         */
        @Override
        public long readInts() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() >= Integer.BYTES) {
                sum += in.readInt();
            }
            return sum;
        }

        @Override
        public long writeVInts(int[] ints) throws IOException {
            output.reset();
            for (int v : ints) {
                output.writeVInt(v);
            }
            return output.size();
        }

        @Override
        public long readVInts() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() > 0) {
                sum += in.readVInt();
            }
            return sum;
        }
    }

    private static int appendVInt(byte[] bytes, int offset, int v) {
        while ((v & ~0x7F) != 0) {
            bytes[offset++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        bytes[offset++] = (byte) v;
        return offset;
    }

    private static long sumVInts(byte[] bytes, int offset, int end) {
        long sum = 0;
        while (offset < end) {
            byte b = bytes[offset++];
            int v = b & 0x7F;
            if (b < 0) {
                b = bytes[offset++];
                v |= (b & 0x7F) << 7;
                if (b < 0) {
                    b = bytes[offset++];
                    v |= (b & 0x7F) << 14;
                    if (b < 0) {
                        b = bytes[offset++];
                        v |= (b & 0x7F) << 21;
                        if (b < 0) {
                            b = bytes[offset++];
                            v |= (b & 0x0F) << 28;
                        }
                    }
                }
            }
            sum += v;
        }
        return sum;
    }

    static void selfTest() {
        for (String dataParam : new String[] { "1000_ints", "4000_ints", "1000_vints" }) {
            for (String implParam : new String[] { "paged", "breaking", "plain", "stream" }) {
                for (String opParam : new String[] { "write", "read" }) {
                    BytesBuilderBenchmark b = new BytesBuilderBenchmark();
                    b.data = dataParam;
                    b.impl = implParam;
                    b.operation = opParam;
                    try {
                        b.setup();
                    } catch (Exception e) {
                        throw new AssertionError("error setting up [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                    }
                    long expectedLength = (long) b.count * Integer.BYTES;
                    long expectedSum = 0;
                    for (int v : b.ints) {
                        expectedSum += v;
                    }
                    try {
                        long result = b.run();
                        boolean ok = opParam.equals("write")
                            ? (b.dataType.equals("vints") ? result >= (long) b.count : result == expectedLength)
                            : result == expectedSum;
                        long expected = opParam.equals("write") ? expectedLength : expectedSum;
                        if (!ok) {
                            throw new AssertionError(
                                "[" + implParam + "/" + opParam + "/" + dataParam + "] expected " + expected + " but got " + result
                            );
                        }
                    } catch (AssertionError ae) {
                        throw ae;
                    } catch (Exception e) {
                        throw new AssertionError("error running [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                    } finally {
                        b.teardown();
                    }
                }
            }
        }
    }
}
