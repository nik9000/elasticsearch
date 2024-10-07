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

public class LongLongBlockHashStatusTests extends AbstractBlockHashStatusTests<LongLongBlockHash.Status> {
    public static LongLongBlockHash.Status randomStatus() {
        int entries = randomNonNegativeInt();
        ByteSizeValue size = randomByteSizeValue();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        return new LongLongBlockHash.Status(
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions
        );
    }

    @Override
    protected LongLongBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected LongLongBlockHash.Status mutateInstance(LongLongBlockHash.Status instance) throws IOException {
        int entries = instance.entries();
        ByteSizeValue size = instance.size();
        int processedBlocks = instance.processedBlocks();
        long processedBlockPositions = instance.processedBlockPositions();
        int processedVectors = instance.processedVectors();
        long processedVectorPositions = instance.processedVectorPositions();
        switch (between(0, 5)) {
            case 0 -> entries = randomValueOtherThan(entries, ESTestCase::randomNonNegativeInt);
            case 1 -> size = randomValueOtherThan(size, ESTestCase::randomByteSizeValue);
            case 2 -> processedBlocks = randomValueOtherThan(processedBlocks, ESTestCase::randomNonNegativeInt);
            case 3 -> processedBlockPositions = randomValueOtherThan(processedBlockPositions, ESTestCase::randomNonNegativeLong);
            case 4 -> processedVectors = randomValueOtherThan(processedVectors, ESTestCase::randomNonNegativeInt);
            case 5 -> processedVectorPositions = randomValueOtherThan(processedVectorPositions, ESTestCase::randomNonNegativeLong);
        }
        return new LongLongBlockHash.Status(
            entries,
            size,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions
        );
    }

    @Override
    public LongLongBlockHash.Status simple() {
        return new LongLongBlockHash.Status(190, ByteSizeValue.ofKb(45), 0, 0, 123, 123000);
    }

    @Override
    public String simpleToJson() {
        return """
            {
              "seen" : {
                "entries" : 190
              },
              "size" : "45kb",
              "processed" : {
                "blocks" : 0,
                "block_positions" : 0,
                "vectors" : 123,
                "vector_positions" : 123000
              }
            }""";
    }
}
