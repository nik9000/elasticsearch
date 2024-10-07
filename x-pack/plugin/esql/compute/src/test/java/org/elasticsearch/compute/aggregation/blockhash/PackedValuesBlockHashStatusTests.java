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

public class PackedValuesBlockHashStatusTests extends AbstractBlockHashStatusTests<PackedValuesBlockHash.Status> {
    public static PackedValuesBlockHash.Status randomStatus() {
        int entries = randomNonNegativeInt();
        ByteSizeValue size = randomByteSizeValue();
        int processedBlocks = randomNonNegativeInt();
        long processedBlockPositions = randomNonNegativeLong();
        return new PackedValuesBlockHash.Status(entries, size, processedBlocks, processedBlockPositions);
    }

    @Override
    protected PackedValuesBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected PackedValuesBlockHash.Status mutateInstance(PackedValuesBlockHash.Status instance) throws IOException {
        int entries = instance.entries();
        ByteSizeValue size = instance.size();
        int processedBlocks = instance.processedBlocks();
        long processedBlockPositions = instance.processedBlockPositions();
        switch (between(0, 3)) {
            case 0 -> entries = randomValueOtherThan(entries, ESTestCase::randomNonNegativeInt);
            case 1 -> size = randomValueOtherThan(size, ESTestCase::randomByteSizeValue);
            case 2 -> processedBlocks = randomValueOtherThan(processedBlocks, ESTestCase::randomNonNegativeInt);
            case 3 -> processedBlockPositions = randomValueOtherThan(processedBlockPositions, ESTestCase::randomNonNegativeLong);
        }
        return new PackedValuesBlockHash.Status(entries, size, processedBlocks, processedBlockPositions);
    }

    @Override
    public PackedValuesBlockHash.Status simple() {
        return new PackedValuesBlockHash.Status(190, ByteSizeValue.ofKb(45), 123, 123000);
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
                "blocks" : 123,
                "block_positions" : 123000
              }
            }""";
    }
}
