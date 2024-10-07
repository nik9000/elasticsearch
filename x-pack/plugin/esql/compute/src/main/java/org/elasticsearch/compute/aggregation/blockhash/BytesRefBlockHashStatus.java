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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public record BytesRefBlockHashStatus(
    boolean seenNull,
    int entries,
    ByteSizeValue size,
    int processedBlocks,
    long processedBlockPositions,
    int processedVectors,
    long processedVectorPositions,
    int processedAllNulls,
    long processedAllNullPositions,
    int processedOrdinalBlocks,
    long processedOrdinalBlockPositions,
    int processedOrdinalVectors,
    long processedOrdinalVectorPositions
) implements BlockHash.Status {
    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        BlockHash.Status.class,
        "BytesRef",
        BytesRefBlockHashStatus::new
    );

    BytesRefBlockHashStatus(StreamInput in) throws IOException {
        this(
            in.readBoolean(),
            in.readVInt(),
            ByteSizeValue.readFrom(in),
            in.readVInt(),
            in.readVLong(),
            in.readVInt(),
            in.readVLong(),
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
        out.writeVInt(entries);
        size.writeTo(out);
        out.writeVInt(processedBlocks);
        out.writeVLong(processedBlockPositions);
        out.writeVInt(processedVectors);
        out.writeVLong(processedVectorPositions);
        out.writeVInt(processedAllNulls);
        out.writeVLong(processedAllNullPositions);
        out.writeVInt(processedOrdinalBlocks);
        out.writeVLong(processedOrdinalBlockPositions);
        out.writeVInt(processedOrdinalVectors);
        out.writeVLong(processedOrdinalVectorPositions);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("seen").field("null", seenNull).field("entries", entries).endObject();
        builder.field("size", size);
        builder.startObject("processed")
            .field("blocks", processedBlocks)
            .field("block_positions", processedBlockPositions)
            .field("vectors", processedVectors)
            .field("vector_positions", processedVectorPositions)
            .field("all_nulls", processedAllNulls)
            .field("all_null_positions", processedAllNullPositions)
            .field("ordinal_blocks", processedBlocks)
            .field("ordinal_block_positions", processedBlockPositions)
            .field("ordinal_vectors", processedVectors)
            .field("ordinal_vector_positions", processedVectorPositions)
            .endObject();
        return builder.endObject();
    }
}
