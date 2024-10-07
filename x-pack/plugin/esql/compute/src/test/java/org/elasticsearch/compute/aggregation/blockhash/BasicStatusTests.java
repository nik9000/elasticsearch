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

public class BasicStatusTests extends AbstractBlockHashStatusTests<BlockHash.BasicStatus> {
    public static BlockHash.BasicStatus randomStatus() {
        boolean seenNull = randomBoolean();
        int entries = randomNonNegativeInt();
        ByteSizeValue size = randomByteSizeValue();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        int processedAllNulls = randomNonNegativeInt();
        long processedAllNullPositions = randomNonNegativeLong();
        return new BlockHash.BasicStatus(
            seenNull,
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions
        );
    }

    @Override
    protected BlockHash.BasicStatus createTestInstance() {
        return randomStatus();
    }

    @Override
    protected BlockHash.BasicStatus mutateInstance(BlockHash.BasicStatus instance) throws IOException {
        boolean seenNull = instance.seenNull();
        int entries = instance.entries();
        ByteSizeValue size = instance.size();
        int processedBlocks = instance.processedBlocks();
        long processedBlockPositions = instance.processedBlockPositions();
        int processedVectors = instance.processedVectors();
        long processedVectorPositions = instance.processedVectorPositions();
        int processedAllNulls = instance.processedAllNulls();
        long processedAllNullPositions = instance.processedAllNullPositions();
        switch (between(0, 8)) {
            case 0 -> seenNull = seenNull == false;
            case 1 -> entries = randomValueOtherThan(entries, ESTestCase::randomNonNegativeInt);
            case 2 -> size = randomValueOtherThan(size, ESTestCase::randomByteSizeValue);
            case 3 -> processedBlocks = randomValueOtherThan(processedBlocks, ESTestCase::randomNonNegativeInt);
            case 4 -> processedBlockPositions = randomValueOtherThan(processedBlockPositions, ESTestCase::randomNonNegativeLong);
            case 5 -> processedVectors = randomValueOtherThan(processedVectors, ESTestCase::randomNonNegativeInt);
            case 6 -> processedVectorPositions = randomValueOtherThan(processedVectorPositions, ESTestCase::randomNonNegativeLong);
            case 7 -> processedAllNulls = randomValueOtherThan(processedAllNulls, ESTestCase::randomNonNegativeInt);
            case 8 -> processedAllNullPositions = randomValueOtherThan(processedAllNullPositions, ESTestCase::randomNonNegativeLong);
            default -> throw new UnsupportedOperationException();
        }
        return new BlockHash.BasicStatus(
            seenNull,
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions
        );
    }

    @Override
    public BlockHash.BasicStatus simple() {
        return new BlockHash.BasicStatus(false, 100, ByteSizeValue.ofBytes(1324), 0, 0, 123, 123000, 0, 0);
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
