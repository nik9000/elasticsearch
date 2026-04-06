/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytes;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
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
 *   The {@code data} parameter controls what is written per invocation, e.g.
 *   {@code 1000_ints} writes 1000 big-endian {@code int} values (4,000 bytes total).
 * </p>
 * <p>
 *   The {@code operation} parameter selects:
 * </p>
 * <ul>
 *   <li>{@code write} – clear the builder then write all data items from scratch</li>
 *   <li>{@code read} – iterate over the pre-written bytes and sum them (no write cost)</li>
 * </ul>
 * <p>
 *   The builders are intentionally not pre-sized per invocation: after the first
 *   iteration each builder has already grown to accommodate the data, so subsequent
 *   {@code write} iterations measure steady-state append throughput without
 *   allocation cost.
 * </p>
 * <p>
 *   The {@link #selfTest()} runs before any benchmark iteration and exercises all
 *   four implementations. This "poisons" operations that are {@code invokevirtual}
 *   but that only use one invocation in each path, forcing the JVM to invoke them
 *   a more prod-like way.
 * </p>
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
            // Confusingly, this makes the test *faster* in some cases and *slower* in others.
            // If you want to know why, ask a wizard. I don't know.
            selfTest();
        }
    }

    @Param({ "paged", "breaking", "plain", "stream" })
    public String impl;

    @Param({ "write", "read" })
    public String operation;

    @Param({ "1000_ints", "4000_ints", "1000_vints", "100_15_bytes", "100_15_not_bytes" })
    public String data;

    // parsed from data param
    private int count;
    private String dataType;
    private int chunkSize;
    private int[] ints;
    private byte[][] chunks;
    private long expected;

    private Paged paged;
    private Breaking breaking;
    private Plain plain;
    private Stream stream;
    private Destination selected;

    @Setup
    public void setup() throws IOException {
        parseData();
        expected = switch (operation) {
            case "write" -> switch (dataType) {
                case "vints" -> {
                    long total = 0;
                    for (int v : ints) {
                        total += vIntSize(v);
                    }
                    yield total;
                }
                case "bytes", "not_bytes" -> {
                    long total = 0;
                    for (byte[] chunk : chunks) {
                        total += chunk.length;
                    }
                    yield total;
                }
                default -> (long) count * Integer.BYTES;
            };
            case "read" -> switch (dataType) {
                case "bytes" -> {
                    long sum = 0;
                    for (byte[] chunk : chunks) {
                        for (byte b : chunk) {
                            sum += b & 0xFF;
                        }
                    }
                    yield sum;
                }
                case "not_bytes" -> {
                    long sum = 0;
                    for (byte[] chunk : chunks) {
                        for (byte b : chunk) {
                            sum += (~b) & 0xFF;
                        }
                    }
                    yield sum;
                }
                default -> {
                    long sum = 0;
                    for (int v : ints) {
                        sum += v;
                    }
                    yield sum;
                }
            };
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("benchmark");
        int byteSize = switch (dataType) {
            case "bytes", "not_bytes" -> count * chunkSize;
            default -> count * Integer.BYTES;
        };
        paged = new Paged(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker, byteSize);
        breaking = new Breaking(breaker, byteSize);
        plain = new Plain();
        stream = new Stream(0);
        selected = switch (impl) {
            case "paged" -> paged;
            case "breaking" -> breaking;
            case "plain" -> plain;
            case "stream" -> stream;
            default -> throw new IllegalArgumentException("unknown impl: " + impl);
        };

        if (operation.equals("read")) {
            // Pre-populate the active builder so that read benchmarks measure only
            // the read path, not write cost.
            switch (dataType) {
                case "ints" -> writeInts();
                case "vints" -> writeVInts();
                case "bytes" -> writeBytes();
                case "not_bytes" -> writeNotBytes();
                default -> throw new IllegalArgumentException("operation read does not support data type: " + dataType);
            }
        }
    }

    private void parseData() {
        // Format: "{count}_{type}" or "{count}x{chunkSize}_{type}"
        int underscore = data.indexOf('_');
        if (underscore < 0) {
            throw new IllegalArgumentException("data param must be '{count}_{type}', got: " + data);
        }
        count = Integer.parseInt(data.substring(0, underscore));
        String typePart = data.substring(underscore + 1);
        if (typePart.endsWith("_not_bytes")) {
            dataType = "not_bytes";
        } else if (typePart.endsWith("_bytes")) {
            dataType = "bytes";
        } else {
            dataType = typePart;
        }
        Random rng = new Random(42);
        switch (dataType) {
            case "ints", "vints" -> {
                ints = new int[count];
                for (int i = 0; i < count; i++) {
                    // vints are typically small positive values in production (counts, lengths, ordinals)
                    ints[i] = dataType.equals("vints") ? rng.nextInt(1 << 14) : rng.nextInt();
                }
            }
            case "bytes", "not_bytes" -> {
                String chunkSizeStr = typePart.substring(0, typePart.length() - ("_" + dataType).length());
                chunkSize = parseChunkSize(chunkSizeStr);
                chunks = new byte[count][chunkSize];
                for (byte[] chunk : chunks) {
                    rng.nextBytes(chunk);
                }
            }
            default -> throw new IllegalArgumentException("unknown data type: " + typePart);
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
            case "write" -> switch (dataType) {
                case "ints" -> writeInts();
                case "vints" -> writeVInts();
                case "bytes" -> writeBytes();
                case "not_bytes" -> writeNotBytes();
                default -> throw new IllegalArgumentException("operation " + operation + " does not support data type: " + dataType);
            };
            case "read" -> switch (dataType) {
                case "ints" -> readInts();
                case "vints" -> readVInts();
                case "bytes" -> readBytes();
                case "not_bytes" -> readNotBytes();
                default -> throw new IllegalArgumentException("operation " + operation + " does not support data type: " + dataType);
            };
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };
    }

    private long writeInts() throws IOException {
        return selected.writeInts(ints);
    }

    private long writeVInts() throws IOException {
        return selected.writeVInts(ints);
    }

    private long readInts() throws IOException {
        return selected.readInts();
    }

    private long readVInts() throws IOException {
        return selected.readVInts();
    }

    private long readBytes() throws IOException {
        return selected.readBytes();
    }

    private long readNotBytes() throws IOException {
        return selected.readNotBytes();
    }

    private long writeBytes() throws IOException {
        return selected.writeBytes(chunks);
    }

    private long writeNotBytes() throws IOException {
        return selected.writeNot(chunks);
    }

    interface Destination {
        long writeInts(int[] ints) throws IOException;

        long readInts() throws IOException;

        long writeVInts(int[] ints) throws IOException;

        long readVInts() throws IOException;

        long writeBytes(byte[][] chunks) throws IOException;

        long readBytes() throws IOException;

        long writeNot(byte[][] chunks) throws IOException;

        long readNotBytes() throws IOException;
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

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.append(chunk, 0, chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            PagedBytes view = builder.view();
            PagedBytesCursor cursor = view.cursor();
            BytesRef scratch = new BytesRef();
            long sum = 0;
            while (cursor.remaining() > 0) {
                BytesRef ref = cursor.readBytesRef(Math.min(cursor.remaining(), PageCacheRecycler.BYTE_PAGE_SIZE), scratch);
                for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                    sum += ref.bytes[i] & 0xFF;
                }
            }
            return sum;
        }

        @Override
        public long writeNot(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.appendNot(chunk, 0, chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
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

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                System.arraycopy(chunk, 0, builder.bytes(), builder.length(), chunk.length);
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            BytesRef ref = builder.bytesRefView();
            long sum = 0;
            for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                sum += ref.bytes[i] & 0xFF;
            }
            return sum;
        }

        @Override
        public long writeNot(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                for (int i = 0; i < chunk.length; i++) {
                    builder.bytes()[builder.length() + i] = (byte) ~chunk[i];
                }
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
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

        @Override
        public long writeBytes(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                System.arraycopy(chunk, 0, builder.bytes(), builder.length(), chunk.length);
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readBytes() {
            BytesRef ref = builder.get();
            long sum = 0;
            for (int i = ref.offset; i < ref.offset + ref.length; i++) {
                sum += ref.bytes[i] & 0xFF;
            }
            return sum;
        }

        @Override
        public long writeNot(byte[][] chunks) {
            builder.clear();
            for (byte[] chunk : chunks) {
                builder.grow(builder.length() + chunk.length);
                for (int i = 0; i < chunk.length; i++) {
                    builder.bytes()[builder.length() + i] = (byte) ~chunk[i];
                }
                builder.setLength(builder.length() + chunk.length);
            }
            return builder.length();
        }

        @Override
        public long readNotBytes() {
            return readBytes();
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

        @Override
        public long writeNot(byte[][] chunks) throws IOException {
            output.reset();
            for (byte[] chunk : chunks) {
                for (byte b : chunk) {
                    output.writeByte((byte) ~b);
                }
            }
            return output.size();
        }

        @Override
        public long writeBytes(byte[][] chunks) throws IOException {
            output.reset();
            for (byte[] chunk : chunks) {
                output.write(chunk);
            }
            return output.size();
        }

        @Override
        public long readBytes() throws IOException {
            StreamInput in = output.bytes().streamInput();
            long sum = 0;
            while (in.available() > 0) {
                sum += in.readByte() & 0xFF;
            }
            return sum;
        }

        @Override
        public long readNotBytes() throws IOException {
            return readBytes();
        }
    }

    void assertResult(String label, long result) {
        if (result != expected) {
            throw new AssertionError("[" + label + "] expected " + expected + " but got " + result);
        }
    }

    private static int parseChunkSize(String s) {
        if (s.endsWith("kb")) {
            return Integer.parseInt(s.substring(0, s.length() - 2)) * 1024;
        }
        return Integer.parseInt(s);
    }

    private static int vIntSize(int v) {
        int size = 1;
        while ((v & ~0x7F) != 0) {
            size++;
            v >>>= 7;
        }
        return size;
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
        try {
            for (String dataParam : BytesBuilderBenchmark.class.getField("data").getAnnotation(Param.class).value()) {
                for (String implParam : BytesBuilderBenchmark.class.getField("impl").getAnnotation(Param.class).value()) {
                    for (String opParam : BytesBuilderBenchmark.class.getField("operation").getAnnotation(Param.class).value()) {
                        BytesBuilderBenchmark b = new BytesBuilderBenchmark();
                        b.data = dataParam;
                        b.impl = implParam;
                        b.operation = opParam;
                        try {
                            b.setup();
                        } catch (Exception e) {
                            throw new AssertionError("error setting up [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                        }
                        try {
                            b.assertResult(implParam + "/" + opParam + "/" + dataParam, b.run());
                        } catch (IllegalArgumentException e) {
                            // skip invalid (operation, dataType) combos
                        } catch (Exception e) {
                            throw new AssertionError("error running [" + implParam + "/" + opParam + "/" + dataParam + "]", e);
                        } finally {
                            b.teardown();
                        }
                    }
                }
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }
}
