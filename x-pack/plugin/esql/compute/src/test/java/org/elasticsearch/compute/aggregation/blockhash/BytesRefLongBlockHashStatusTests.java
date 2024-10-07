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

public class BytesRefLongBlockHashStatusTests extends AbstractBlockHashStatusTests<BytesRefLongBlockHash.Status> {
    public static BytesRefLongBlockHash.Status randomStatus() {
        int finalEntries = randomNonNegativeInt();
        ByteSizeValue finalSize = randomByteSizeValue();
        BytesRefBlockHashStatus bytesStatus = BytesRefBlockHashStatusTests.randomStatus();
        int processedLongsBlocks = randomNonNegativeInt();
        long processedLongsBlockPositions = randomNonNegativeLong();
        int processedLongsVectors = randomNonNegativeInt();
        long processedLongsVectorPositions = randomNonNegativeLong();
        return new BytesRefLongBlockHash.Status(
            finalEntries,
            finalSize,
            processedLongsBlocks,
            processedLongsBlockPositions,
            processedLongsVectors,
            processedLongsVectorPositions,
            bytesStatus
        );
    }

    @Override
    protected BytesRefLongBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected BytesRefLongBlockHash.Status mutateInstance(BytesRefLongBlockHash.Status instance) throws IOException {
        int finalEntries = instance.finalEntries();
        ByteSizeValue finalSize = instance.finalSize();
        int finalProcessedBlocks = instance.finalProcessedBlocks();
        long finalProcessedBlockPositions = instance.finalProcessedBlockPositions();
        int finalProcessedVectors = instance.finalProcessedVectors();
        long finalProcessedVectorPositions = instance.finalProcessedVectorPositions();
        BytesRefBlockHashStatus bytesStatus = instance.bytesStatus();
        switch (between(0, 6)) {
            case 0 -> finalEntries = randomValueOtherThan(finalEntries, ESTestCase::randomNonNegativeInt);
            case 1 -> finalSize = randomValueOtherThan(finalSize, ESTestCase::randomByteSizeValue);
            case 2 -> finalProcessedBlocks = randomValueOtherThan(finalProcessedBlocks, ESTestCase::randomNonNegativeInt);
            case 3 -> finalProcessedBlockPositions = randomValueOtherThan(finalProcessedBlockPositions, ESTestCase::randomNonNegativeLong);
            case 4 -> finalProcessedVectors = randomValueOtherThan(finalProcessedVectors, ESTestCase::randomNonNegativeInt);
            case 5 -> finalProcessedVectorPositions = randomValueOtherThan(
                finalProcessedVectorPositions,
                ESTestCase::randomNonNegativeLong
            );
            case 6 -> bytesStatus = randomValueOtherThan(bytesStatus, BytesRefBlockHashStatusTests::randomStatus);
        }
        return new BytesRefLongBlockHash.Status(
            finalEntries,
            finalSize,
            finalProcessedBlocks,
            finalProcessedBlockPositions,
            finalProcessedVectors,
            finalProcessedVectorPositions,
            bytesStatus
        );
    }

    @Override
    public BytesRefLongBlockHash.Status simple() {
        return new BytesRefLongBlockHash.Status(
            190,
            ByteSizeValue.ofKb(45),
            0,
            0,
            123,
            123000,
            new BytesRefBlockHashStatusTests().simple()
        );
    }

    @Override
    public String simpleToJson() {
        return """
            {
              "final" : {
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
              },
              "bytes" : {
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
              }
            }""";
    }
}
