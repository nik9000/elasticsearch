/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.FALSE_ORD;
import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.NULL_ORD;
import static org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeBoolean.TRUE_ORD;

/**
 * Maps a {@link BooleanBlock} column to group ids. Assigns
 * {@code 0} to {@code null}, {@code 1} to {@code false}, and
 * {@code 2} to {@code true}.
 */
final class BooleanBlockHash extends BlockHash {
    private final int channel;
    private final boolean[] everSeen = new boolean[TRUE_ORD + 1];

    private int processedVectors;
    private long processedVectorPositions;
    private int processedBlocks;
    private long processedBlockPositions;
    private int processedAllNulls;
    private long processedAllNullPositions;

    BooleanBlockHash(int channel, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            processedAllNulls++;
            processedAllNullPositions += block.getPositionCount();
            everSeen[NULL_ORD] = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
            return;
        }
        BooleanBlock booleanBlock = page.getBlock(channel);
        BooleanVector booleanVector = booleanBlock.asVector();
        if (booleanVector == null) {
            processedBlocks++;
            processedBlockPositions += booleanBlock.getPositionCount();
            try (IntBlock groupIds = add(booleanBlock)) {
                addInput.add(0, groupIds);
            }
        } else {
            processedVectors++;
            processedVectorPositions += booleanBlock.getPositionCount();
            try (IntVector groupIds = add(booleanVector)) {
                addInput.add(0, groupIds);
            }
        }
    }

    private IntVector add(BooleanVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i, MultivalueDedupeBoolean.hashOrd(everSeen, vector.getBoolean(i)));
            }
            return builder.build();
        }
    }

    private IntBlock add(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(blockFactory, everSeen);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }
        BooleanBlock castBlock = page.getBlock(channel);
        BooleanVector vector = castBlock.asVector();
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(BooleanVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                boolean v = vector.getBoolean(i);
                int ord = v ? TRUE_ORD : FALSE_ORD;
                if (everSeen[ord]) {
                    builder.appendInt(ord);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(BooleanBlock block) {
        return new MultivalueDedupeBoolean(block).hash(blockFactory, new boolean[TRUE_ORD + 1]);
    }

    @Override
    public BooleanBlock[] getKeys() {
        try (BooleanBlock.Builder builder = blockFactory.newBooleanBlockBuilder(everSeen.length)) {
            if (everSeen[NULL_ORD]) {
                builder.appendNull();
            }
            if (everSeen[FALSE_ORD]) {
                builder.appendBoolean(false);
            }
            if (everSeen[TRUE_ORD]) {
                builder.appendBoolean(true);
            }
            return new BooleanBlock[] { builder.build() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        try (IntVector.Builder builder = blockFactory.newIntVectorBuilder(everSeen.length)) {
            for (int i = 0; i < everSeen.length; i++) {
                if (everSeen[i]) {
                    builder.appendInt(i);
                }
            }
            return builder.build();
        }
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        BitArray seen = new BitArray(everSeen.length, bigArrays);
        for (int i = 0; i < everSeen.length; i++) {
            if (everSeen[i]) {
                seen.set(i);
            }
        }
        return seen;
    }

    @Override
    public void close() {
        // Nothing to close
    }

    @Override
    public String toString() {
        return "BooleanBlockHash[" + channel + "]";
    }

    @Override
    public Status status() {
        return new Status(
            everSeen[NULL_ORD],
            everSeen[FALSE_ORD],
            everSeen[TRUE_ORD],
            processedBlocks,
            processedBlockPositions,
            processedVectors,
            processedVectorPositions,
            processedAllNulls,
            processedAllNullPositions
        );
    }

    public record Status(
        boolean seenNull,
        boolean seenFalse,
        boolean seenTrue,
        int processedBlocks,
        long processedBlockPositions,
        int processedVectors,
        long processedVectorPositions,
        int processedAllNulls,
        long processedAllNullPositions
    ) implements BlockHash.Status {

        static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(BlockHash.Status.class, "Boolean", Status::new);

        private Status(StreamInput in) throws IOException {
            this(
                in.readBoolean(),
                in.readBoolean(),
                in.readBoolean(),
                in.readVInt(),
                in.readVLong(),
                in.readVInt(),
                in.readVLong(),
                in.readVInt(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(seenNull);
            out.writeBoolean(seenFalse);
            out.writeBoolean(seenTrue);
            out.writeVInt(processedBlocks);
            out.writeVLong(processedBlockPositions);
            out.writeVInt(processedVectors);
            out.writeVLong(processedVectorPositions);
            out.writeVInt(processedAllNulls);
            out.writeVLong(processedAllNullPositions);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("seen").field("null", seenNull).field("false", seenFalse).field("true", seenTrue).endObject();
            builder.startObject("processed")
                .field("blocks", processedBlocks)
                .field("block_positions", processedBlockPositions)
                .field("vectors", processedVectors)
                .field("vector_positions", processedVectorPositions)
                .field("all_nulls", processedAllNulls)
                .field("all_null_positions", processedAllNullPositions)
                .endObject();
            return builder.endObject();
        }
    }
}
