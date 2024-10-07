/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BytesRefBlockHashStatusTests extends AbstractBlockHashStatusTests<BytesRefBlockHashStatus> {
    public static BytesRefBlockHashStatus randomStatus() {
        boolean seenNull = randomBoolean();
        int entries = randomNonNegativeInt();
        ByteSizeValue size = randomByteSizeValue();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        int processedAllNulls = randomNonNegativeInt();
        long processedAllNullPositions = randomNonNegativeLong();
        int processedOrdinalBlocks = randomNonNegativeInt();
        long processedOrdinalBlockPositions = randomNonNegativeLong();
        int processedOrdinalVectors = randomNonNegativeInt();
        long processedOrdinalVectorPositions = randomNonNegativeLong();
        return new BytesRefBlockHashStatus(
            seenNull,
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions,
            processedOrdinalBlocks,
            processedOrdinalBlockPositions,
            processedOrdinalVectors,
            processedOrdinalVectorPositions
        );
    }

    @Override
    protected BytesRefBlockHashStatus createTestInstance() {
        return randomStatus();
    }

    @Override
    protected BytesRefBlockHashStatus mutateInstance(BytesRefBlockHashStatus instance) throws IOException {
        boolean seenNull = instance.seenNull();
        int entries = instance.entries();
        ByteSizeValue size = instance.size();
        int processedBlocks = instance.processedBlocks();
        long processedBlockPositions = instance.processedBlockPositions();
        int processedVectors = instance.processedVectors();
        long processedVectorPositions = instance.processedVectorPositions();
        int processedAllNulls = instance.processedAllNulls();
        long processedAllNullPositions = instance.processedAllNullPositions();
        int processedOrdinalBlocks = instance.processedBlocks();
        long processedOrdinalBlockPositions = instance.processedBlockPositions();
        int processedOrdinalVectors = instance.processedVectors();
        long processedOrdinalVectorPositions = instance.processedVectorPositions();
        switch (between(0, 12)) {
            case 0 -> seenNull = seenNull == false;
            case 1 -> entries = randomValueOtherThan(entries, ESTestCase::randomNonNegativeInt);
            case 2 -> size = randomValueOtherThan(size, ESTestCase::randomByteSizeValue);
            case 3 -> processedBlocks = randomValueOtherThan(processedBlocks, ESTestCase::randomNonNegativeInt);
            case 4 -> processedBlockPositions = randomValueOtherThan(processedBlockPositions, ESTestCase::randomNonNegativeLong);
            case 5 -> processedVectors = randomValueOtherThan(processedVectors, ESTestCase::randomNonNegativeInt);
            case 6 -> processedVectorPositions = randomValueOtherThan(processedVectorPositions, ESTestCase::randomNonNegativeLong);
            case 7 -> processedAllNulls = randomValueOtherThan(processedAllNulls, ESTestCase::randomNonNegativeInt);
            case 8 -> processedAllNullPositions = randomValueOtherThan(processedAllNullPositions, ESTestCase::randomNonNegativeLong);
            case 9 -> processedOrdinalBlocks = randomValueOtherThan(processedOrdinalBlocks, ESTestCase::randomNonNegativeInt);
            case 10 -> processedOrdinalBlockPositions = randomValueOtherThan(
                processedOrdinalBlockPositions,
                ESTestCase::randomNonNegativeLong
            );
            case 11 -> processedOrdinalVectors = randomValueOtherThan(processedOrdinalVectors, ESTestCase::randomNonNegativeInt);
            case 12 -> processedOrdinalVectorPositions = randomValueOtherThan(
                processedOrdinalVectorPositions,
                ESTestCase::randomNonNegativeLong
            );
            default -> throw new UnsupportedOperationException();
        }
        return new BytesRefBlockHashStatus(
            seenNull,
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions,
            processedOrdinalBlocks,
            processedOrdinalBlockPositions,
            processedOrdinalVectors,
            processedOrdinalVectorPositions
        );
    }

    @Override
    public BytesRefBlockHashStatus simple() {
        return new BytesRefBlockHashStatus(false, 100, ByteSizeValue.ofBytes(1324), 0, 0, 123, 123000, 0, 0, 1, 23, 0, 0);
    }

    @Override
    public String simpleToJson() {
        return """
            {
              "seen" : {
                "null" : false,
                "entries" : 100
              },
              "size" : "1.2kb",
              "processed" : {
                "blocks" : 0,
                "block_positions" : 0,
                "vectors" : 123,
                "vector_positions" : 123000,
                "all_nulls" : 0,
                "all_null_positions" : 0
              }
            }""";
    }
}
