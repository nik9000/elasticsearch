/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class TimeSeriesBlockHashStatusTests extends AbstractBlockHashStatusTests<TimeSeriesBlockHash.Status> {
    public static TimeSeriesBlockHash.Status randomStatus() {
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        return new TimeSeriesBlockHash.Status(processedVectors, processedVectorPositions);
    }

    @Override
    protected TimeSeriesBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected TimeSeriesBlockHash.Status mutateInstance(TimeSeriesBlockHash.Status instance) throws IOException {
        int processedVectors = instance.processedVectors();
        long processedVectorPositions = instance.processedVectorPositions();
        if (randomBoolean()) {
            processedVectors = randomValueOtherThan(processedVectors, ESTestCase::randomNonNegativeInt);
        } else {
            processedVectorPositions = randomValueOtherThan(processedVectorPositions, ESTestCase::randomNonNegativeLong);
        }
        return new TimeSeriesBlockHash.Status(processedVectors, processedVectorPositions);
    }

    @Override
    public TimeSeriesBlockHash.Status simple() {
        return new TimeSeriesBlockHash.Status(123, 123000);
    }

    @Override
    public String simpleToJson() {
        return """
            {
              "processed" : {
                "vectors" : 123,
                "vector_positions" : 123000
              }
            }""";
    }
}
