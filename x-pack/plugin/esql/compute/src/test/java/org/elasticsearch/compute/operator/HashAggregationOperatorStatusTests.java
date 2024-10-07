/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.aggregation.blockhash.BasicStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.BooleanBlockHashStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.BytesRefLongBlockHashStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.LongLongBlockHashStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.NullBlockHashStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.PackedValuesBlockHashStatusTests;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHashStatusTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class HashAggregationOperatorStatusTests extends AbstractWireSerializingTestCase<HashAggregationOperator.Status> {
    public static HashAggregationOperator.Status simple() {
        return new HashAggregationOperator.Status(500012, new BasicStatusTests().simple(), 200012, 123);
    }

    public static String simpleToJson() {
        return """
            {
              "hash" : {
                "nanos" : 500012,
                "time" : "500micros",
                "status" : $STATUS$
              },
              "aggregation_nanos" : 200012,
              "aggregation_time" : "200micros",
              "pages_processed" : 123
            }""".replace("$STATUS$", new BasicStatusTests().simpleToJson().replace("\n", "\n    "));
    }

    public void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }

    @Override
    protected Writeable.Reader<HashAggregationOperator.Status> instanceReader() {
        return HashAggregationOperator.Status::new;
    }

    @Override
    public HashAggregationOperator.Status createTestInstance() {
        return new HashAggregationOperator.Status(randomNonNegativeLong(), randomStatus(), randomNonNegativeLong(), randomNonNegativeInt());
    }

    private static BlockHash.Status randomStatus() {
        return switch (between(0, 6)) {
            case 0 -> BasicStatusTests.randomStatus();
            case 1 -> BooleanBlockHashStatusTests.randomStatus();
            case 2 -> BytesRefLongBlockHashStatusTests.randomStatus();
            case 3 -> LongLongBlockHashStatusTests.randomStatus();
            case 4 -> NullBlockHashStatusTests.randomStatus();
            case 5 -> PackedValuesBlockHashStatusTests.randomStatus();
            case 6 -> TimeSeriesBlockHashStatusTests.randomStatus();
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    protected HashAggregationOperator.Status mutateInstance(HashAggregationOperator.Status instance) {
        long hashNanos = instance.hashNanos();
        BlockHash.Status status = instance.hashStatus();
        long aggregationNanos = instance.aggregationNanos();
        int pagesProcessed = instance.pagesProcessed();
        switch (between(0, 3)) {
            case 0 -> hashNanos = randomValueOtherThan(hashNanos, ESTestCase::randomNonNegativeLong);
            case 1 -> status = randomValueOtherThan(status, HashAggregationOperatorStatusTests::randomStatus);
            case 2 -> aggregationNanos = randomValueOtherThan(aggregationNanos, ESTestCase::randomNonNegativeLong);
            case 3 -> pagesProcessed = randomValueOtherThan(pagesProcessed, ESTestCase::randomNonNegativeInt);
            default -> throw new UnsupportedOperationException();
        }
        return new HashAggregationOperator.Status(hashNanos, status, aggregationNanos, pagesProcessed);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(BlockHash.getNamedWriteables());
    }
}
