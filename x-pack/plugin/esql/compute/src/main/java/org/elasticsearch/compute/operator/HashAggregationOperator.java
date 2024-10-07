/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HashAggregationOperator implements Operator {

    public record HashAggregationOperatorFactory(
        List<BlockHash.GroupSpec> groups,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new HashAggregationOperator(
                aggregators,
                () -> BlockHash.build(groups, driverContext.blockFactory(), maxPageSize, false),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "HashAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private boolean finished;
    private Page output;

    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    private final DriverContext driverContext;

    /**
     * Nanoseconds this operator has spent hashing grouping keys.
     */
    private long hashNanos;
    /**
     * Nanoseconds this operator has spent running the aggregations.
     */
    private long aggregationNanos;
    /**
     * Count of pages this operator has processed.
     */
    private int pagesProcessed;

    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        this.aggregators = new ArrayList<>(aggregators.size());
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.blockHash = blockHash.get();
            for (GroupingAggregator.Factory a : aggregators) {
                this.aggregators.add(a.apply(driverContext));
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            class AddInput implements GroupingAggregatorFunction.AddInput {
                long hashStart = System.nanoTime();
                long aggStart;

                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    IntVector groupIdsVector = groupIds.asVector();
                    if (groupIdsVector != null) {
                        add(positionOffset, groupIdsVector);
                    } else {
                        startAggEndHash();
                        for (GroupingAggregatorFunction.AddInput p : prepared) {
                            p.add(positionOffset, groupIds);
                        }
                        end();
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    startAggEndHash();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                private void startAggEndHash() {
                    aggStart = System.nanoTime();
                    hashNanos += aggStart - hashStart;
                }

                private void end() {
                    hashStart = System.nanoTime();
                    aggregationNanos += hashStart - aggStart;
                }

                @Override
                public void close() {
                    Releasables.closeExpectNoException(prepared);
                }
            }
            try (AddInput add = new AddInput()) {
                if (needsInput() == false) {
                    throw new IllegalStateException("Operator is already finishing");
                }
                requireNonNull(page, "page is null");

                for (int i = 0; i < prepared.length; i++) {
                    prepared[i] = aggregators.get(i).prepareProcessPage(blockHash, page);
                }

                blockHash.add(wrapPage(page), add);
                hashNanos += System.nanoTime() - add.hashStart;
            }
        } finally {
            page.releaseBlocks();
            pagesProcessed++;
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        Block[] blocks = null;
        IntVector selected = null;
        boolean success = false;
        try {
            selected = blockHash.nonEmpty();
            Block[] keys = blockHash.getKeys();
            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            int offset = keys.length;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluate(blocks, offset, selected, driverContext);
                offset += aggBlockCounts[i];
            }
            output = new Page(blocks);
            success = true;
        } finally {
            // selected should always be closed
            if (selected != null) {
                selected.close();
            }
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public void close() {
        if (output != null) {
            output.releaseBlocks();
        }
        Releasables.close(blockHash, () -> Releasables.close(aggregators));
    }

    @Override
    public Operator.Status status() {
        return new Status(hashNanos, blockHash.status(), aggregationNanos, pagesProcessed);
    }

    protected Page wrapPage(Page page) {
        return page;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("blockHash=").append(blockHash).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "hashagg",
            Status::new
        );

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        private final long hashNanos;
        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        private final long aggregationNanos;
        /**
         * Count of pages this operator has processed.
         */
        private final int pagesProcessed;
        /**
         * Status of the {@link BlockHash}.
         */
        private final BlockHash.Status hashStatus;

        /**
         * Build.
         * @param hashNanos Nanoseconds this operator has spent hashing grouping keys.
         * @param hashStatus Status of the {@link BlockHash}
         * @param aggregationNanos Nanoseconds this operator has spent running the aggregations.
         * @param pagesProcessed Count of pages this operator has processed.
         */
        public Status(long hashNanos, BlockHash.Status hashStatus, long aggregationNanos, int pagesProcessed) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
            this.hashStatus = hashStatus;
        }

        protected Status(StreamInput in) throws IOException {
            hashNanos = in.readVLong();
            hashStatus = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_BLOCK_HASH_STATUS)
                ? in.readNamedWriteable(BlockHash.Status.class)
                : new BlockHash.BasicStatus(false, 0, ByteSizeValue.ZERO, 0, 0, 0, 0, 0, 0);
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(hashNanos);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_BLOCK_HASH_STATUS)) {
                out.writeNamedWriteable(hashStatus);
            }
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        /**
         * Nanoseconds this operator has spent hashing grouping keys.
         */
        public long hashNanos() {
            return hashNanos;
        }

        /**
         * Status of the {@link BlockHash}.
         */
        public BlockHash.Status hashStatus() {
            return hashStatus;
        }

        /**
         * Nanoseconds this operator has spent running the aggregations.
         */
        public long aggregationNanos() {
            return aggregationNanos;
        }

        /**
         * Count of pages this operator has processed.
         */
        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("hash");
            {
                builder.field("nanos", hashNanos);
                if (builder.humanReadable()) {
                    builder.field("time", TimeValue.timeValueNanos(hashNanos));
                }
                builder.field("status", hashStatus);
            }
            builder.endObject();
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return hashNanos == status.hashNanos
                && hashStatus.equals(status.hashStatus)
                && aggregationNanos == status.aggregationNanos
                && pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, hashStatus, aggregationNanos, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_14_0;
        }
    }
}
