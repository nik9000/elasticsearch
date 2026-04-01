/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractVersionTopNEncoderWithPagedTests extends AbstractSortableTopNEncoderWithPagedTests {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCase<?>> tests = new ArrayList<>();
        tests.add(testCase("version", () -> TopNOperatorTests.randomVersion().toBytesRef()));
        tests.add(testCase("version inside garbage", () -> embedInRandomBytes(TopNOperatorTests.randomVersion().toBytesRef())));
        return tests.stream().map(t -> new Object[] { t }).toList();
    }

    protected AbstractVersionTopNEncoderWithPagedTests(TestCase<?> testCase) {
        super(testCase);
    }

    private static TestCase<BytesRef> testCase(String name, Supplier<BytesRef> randomValue) {
        return new TestCase<>(
            name,
            randomValue,
            BytesRef::compareTo,
            TopNEncoder::encodeBytesRef,
            (encoder, cursor) -> encoder.decodeBytesRef(cursor, new BytesRef())
        );
    }

    public void testContainingNul() {
        BytesRef v = (BytesRef) testCase.randomValue().get();
        insertNul(v);
        try (PagedBytesRefBuilder builder = new PagedBytesRefBuilder(new NoopCircuitBreaker("test"), "bytes", 0, recycler())) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> encoder().encodeBytesRef(v, builder));
            assertThat(e.getMessage(), equalTo("Can't sort versions containing nul"));
        }
    }

    private void insertNul(BytesRef v) {
        v.bytes[between(v.offset, v.offset + v.length - 1)] = 0;
    }
}
