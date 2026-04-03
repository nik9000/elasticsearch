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
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class BytesBuilderBenchmark {

    static {
        Utils.configureBenchmarkLogging();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod
            selfTest();
        }
    }

    private static final VarHandle INT_BIG_ENDIAN = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    @Param({ "paged", "breaking", "plain", "stream" })
    public String impl;

    @Param({ "write", "read" })
    public String operation;

    @Param({ "1000_ints" })
    public String data;

    // parsed from data param
    private int count;
    private int[] ints;

    private PagedBytesBuilder pagedBytesBuilder;
    private BreakingBytesRefBuilder breakingBytesRefBuilder;
    private BytesRefBuilder bytesRefBuilder;
    private BytesStreamOutput bytesStreamOutput;

    @Setup
    public void setup() throws IOException {
        parseData();
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("benchmark");
        int byteSize = count * Integer.BYTES;
        pagedBytesBuilder = new PagedBytesBuilder(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker, "benchmark", byteSize);
        breakingBytesRefBuilder = new BreakingBytesRefBuilder(breaker, "benchmark", byteSize);
        bytesRefBuilder = new BytesRefBuilder();
        bytesStreamOutput = new BytesStreamOutput(byteSize);

        if (operation.equals("read")) {
            // Pre-populate the active builder so that read benchmarks measure only
            // the read path, not write cost.
            write();
        }
    }

    private void parseData() {
        // Format: "{count}_{type}", e.g. "1000_ints"
        int underscore = data.indexOf('_');
        if (underscore < 0) {
            throw new IllegalArgumentException("data param must be '{count}_{type}', got: " + data);
        }
        count = Integer.parseInt(data.substring(0, underscore));
        String type = data.substring(underscore + 1);
        if (type.equals("ints") == false) {
            throw new IllegalArgumentException("unsupported data type: " + type);
        }
        ints = new int[count];
        Random rng = new Random(42);
        for (int i = 0; i < count; i++) {
            ints[i] = rng.nextInt();
        }
    }

    @TearDown
    public void teardown() {
        pagedBytesBuilder.close();
        breakingBytesRefBuilder.close();
    }

    @Benchmark
    public long run() throws IOException {
        return switch (operation) {
            case "write" -> write();
            case "read" -> read();
            default -> throw new IllegalArgumentException("unknown operation: " + operation);
        };
    }

    // ---- write helpers -------------------------------------------------------

    private long write() throws IOException {
        return switch (impl) {
            case "paged" -> writePaged();
            case "breaking" -> writeBreaking();
            case "plain" -> writePlain();
            case "stream" -> writeStream();
            default -> throw new IllegalArgumentException("unknown impl: " + impl);
        };
    }

    private long writePaged() {
        pagedBytesBuilder.clear();
        for (int i = 0; i < count; i++) {
            pagedBytesBuilder.append(ints[i]);
        }
        return pagedBytesBuilder.length();
    }

    private long writeBreaking() {
        breakingBytesRefBuilder.clear();
        for (int i = 0; i < count; i++) {
            breakingBytesRefBuilder.grow(breakingBytesRefBuilder.length() + Integer.BYTES);
            INT_BIG_ENDIAN.set(breakingBytesRefBuilder.bytes(), breakingBytesRefBuilder.length(), ints[i]);
            breakingBytesRefBuilder.setLength(breakingBytesRefBuilder.length() + Integer.BYTES);
        }
        return breakingBytesRefBuilder.length();
    }

    private long writePlain() {
        bytesRefBuilder.clear();
        for (int i = 0; i < count; i++) {
            bytesRefBuilder.grow(bytesRefBuilder.length() + Integer.BYTES);
            INT_BIG_ENDIAN.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), ints[i]);
            bytesRefBuilder.setLength(bytesRefBuilder.length() + Integer.BYTES);
        }
        return bytesRefBuilder.length();
    }

    private long writeStream() throws IOException {
        bytesStreamOutput.reset();
        for (int i = 0; i < count; i++) {
            bytesStreamOutput.writeInt(ints[i]);
        }
        return bytesStreamOutput.size();
    }

    // ---- read helpers --------------------------------------------------------

    /**
     * Reads back all integers from the pre-populated builder and returns their sum.
     * Each impl reads from data written by its own {@code write} method in {@link #setup},
     * so this measures the read-back cost of each builder's output format without write overhead.
     */
    private long read() {
        return switch (impl) {
            case "paged" -> readPaged();
            case "breaking" -> readBreaking();
            case "plain" -> readPlain();
            case "stream" -> readStream();
            default -> throw new IllegalArgumentException("unknown impl: " + impl);
        };
    }

    private long readPaged() {
        PagedBytes view = pagedBytesBuilder.view();
        PagedBytesCursor cursor = view.cursor();
        long sum = 0;
        while (cursor.remaining() >= Integer.BYTES) {
            sum += cursor.readInt();
        }
        return sum;
    }

    private long readBreaking() {
        BytesRef ref = breakingBytesRefBuilder.bytesRefView();
        long sum = 0;
        for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
            sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
        }
        return sum;
    }

    private long readPlain() {
        BytesRef ref = bytesRefBuilder.get();
        long sum = 0;
        for (int i = ref.offset; i + Integer.BYTES <= ref.offset + ref.length; i += Integer.BYTES) {
            sum += (int) INT_BIG_ENDIAN.get(ref.bytes, i);
        }
        return sum;
    }

    /**
     * Note: {@link BytesStreamOutput#bytes()} returns a concrete {@link org.elasticsearch.common.bytes.BytesArray}
     * for small outputs, so {@link org.elasticsearch.common.bytes.BytesReference#streamInput()} and
     * {@link StreamInput#readInt()} here are likely monomorphic — giving this variant an inherent
     * advantage over the others that may not appear in real workloads where multiple implementations
     * are live.
     */
    private long readStream() throws IOException {
        StreamInput in = bytesStreamOutput.bytes().streamInput();
        long sum = 0;
        while (in.available() >= Integer.BYTES) {
            sum += in.readInt();
        }
        return sum;
    }

    // ---- self-test -----------------------------------------------------------

    static void selfTest() {
        for (String dataParam : new String[] { "1000_ints" }) {
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
                        long expected = opParam.equals("write") ? expectedLength : expectedSum;
                        if (result != expected) {
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
