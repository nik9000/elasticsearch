/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BooleanBlockHashStatusTests extends AbstractBlockHashStatusTests<BooleanBlockHash.Status> {
    public static BooleanBlockHash.Status randomStatus() {
        boolean seenNull = randomBoolean();
        boolean seenFalse = randomBoolean();
        boolean seenTrue = randomBoolean();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        int processedAllNulls = randomNonNegativeInt();
        long processedAllNullPositions = randomNonNegativeLong();
        return new BooleanBlockHash.Status(
            seenNull,
            seenFalse,
            seenTrue,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions
        );
    }

    @Override
    protected BooleanBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected BooleanBlockHash.Status mutateInstance(BooleanBlockHash.Status instance) throws IOException {
        boolean seenNull = randomBoolean();
        boolean seenFalse = randomBoolean();
        boolean seenTrue = randomBoolean();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        int processedVectors = randomNonNegativeInt();
        long processedVectorPositions = randomNonNegativeLong();
        int processedAllNulls = randomNonNegativeInt();
        long processedAllNullPositions = randomNonNegativeLong();
        switch (between(0, 8)) {
            case 0 -> seenNull = seenNull == false;
            case 1 -> seenFalse = seenFalse == false;
            case 2 -> seenTrue = seenTrue == false;
            case 3 -> processedBlocks = randomValueOtherThan(processedBlocks, ESTestCase::randomNonNegativeInt);
            case 4 -> processedBlockPositions = randomValueOtherThan(processedBlockPositions, ESTestCase::randomNonNegativeLong);
            case 5 -> processedVectors = randomValueOtherThan(processedVectors, ESTestCase::randomNonNegativeInt);
            case 6 -> processedVectorPositions = randomValueOtherThan(processedVectorPositions, ESTestCase::randomNonNegativeLong);
            case 7 -> processedAllNulls = randomValueOtherThan(processedAllNulls, ESTestCase::randomNonNegativeInt);
            case 8 -> processedAllNullPositions = randomValueOtherThan(processedAllNullPositions, ESTestCase::randomNonNegativeLong);
        }
        return new BooleanBlockHash.Status(
            seenNull,
            seenFalse,
            seenTrue,
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions
        );
    }

    @Override
    public BooleanBlockHash.Status simple() {
        return new BooleanBlockHash.Status(false, true, true, 0, 0, 3, 2031, 0, 0);
    }

    @Override
    public String simpleToJson() {
        return """
            {
              "seen" : {
                "null" : false,
                "false" : true,
                "true" : true
              },
              "processed" : {
                "blocks" : 0,
                "block_positions" : 0,
                "vectors" : 3,
                "vector_positions" : 2031,
                "all_nulls" : 0,
                "all_null_positions" : 0
              }
            }""";
    }
}
