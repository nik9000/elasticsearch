/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Aggregates field values for float.
 * This class is generated. Edit @{code X-ValuesAggregator.java.st} instead
 * of this file.
 */
@Aggregator({ @IntermediateState(name = "values", type = "FLOAT_BLOCK") })
@GroupingAggregator
class ValuesFloatAggregator {
    public static SingleState initSingle(BigArrays bigArrays) {
        return new SingleState(bigArrays);
    }

    public static void combine(SingleState state, float v) {
        state.values.add(Float.floatToIntBits(v));
    }

    public static void combineIntermediate(SingleState state, FloatBlock values) {
        int start = values.getFirstValueIndex(0);
        int end = start + values.getValueCount(0);
        for (int i = start; i < end; i++) {
            combine(state, values.getFloat(i));
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory());
    }

    public static GroupingState initGrouping(BigArrays bigArrays) {
        return new GroupingState(bigArrays);
    }

    public static void combine(GroupingState state, int groupId, float v) {
        /*
         * Encode the groupId and value into a single long -
         * the top 32 bits for the group, the bottom 32 for the value.
         */
        state.values.add((((long) groupId) << Float.SIZE) | (Float.floatToIntBits(v) & 0xFFFFFFFFL));
    }

    public static void combineIntermediate(GroupingState state, int groupId, FloatBlock values, int valuesPosition) {
        int start = values.getFirstValueIndex(valuesPosition);
        int end = start + values.getValueCount(valuesPosition);
        for (int i = start; i < end; i++) {
            combine(state, groupId, values.getFloat(i));
        }
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        for (int id = 0; id < state.values.size(); id++) {
            long both = state.values.get(id);
            int group = (int) (both >>> Float.SIZE);
            if (group == statePosition) {
                float value = Float.intBitsToFloat((int) both);
                combine(current, currentGroupId, value);
            }
        }
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.toBlock(driverContext.blockFactory(), selected);
    }

    public static class SingleState implements AggregatorState {
        private final LongHash values;

        private SingleState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory());
        }

        Block toBlock(BlockFactory blockFactory) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(1);
            }
            if (values.size() == 1) {
                return blockFactory.newConstantFloatBlockWith(Float.intBitsToFloat((int) values.get(0)), 1);
            }
            try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder((int) values.size())) {
                builder.beginPositionEntry();
                for (int id = 0; id < values.size(); id++) {
                    builder.appendFloat(Float.intBitsToFloat((int) values.get(id)));
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        @Override
        public void close() {
            values.close();
        }
    }

    /**
     * State for a grouped {@code VALUES} aggregation. This implementation
     * emphasizes collect-time performance over the performance of rendering
     * results. That's good, but it's a pretty intensive emphasis, requiring
     * an {@code O(n^2)} operation for collection to support a {@code O(1)}
     * collector operation. But at least it's fairly simple.
     */
    public static class GroupingState implements GroupingAggregatorState {
        private final LongHash values;

        private GroupingState(BigArrays bigArrays) {
            values = new LongHash(1, bigArrays);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext.blockFactory(), selected);
        }

        /**
         * Builds a {@link Block} with the unique values collected for the {@code #selected}
         * groups. This is the implementation of the final and intermediate results of the agg.
         */
        Block toBlock(BlockFactory blockFactory, IntVector selected) {
            if (values.size() == 0) {
                return blockFactory.newConstantNullBlock(selected.getPositionCount());
            }

            long selectedCountsSize = 0;
            long idsSize = 0;
            try {
                /*
                 * Get a count of all groups less than the maximum selected group. Count
                 * *downwards* so that we can flip the sign on all of the actually selected
                 * groups. Negative values in this array are always unselected groups.
                 */
                int selectedCountsLen = selected.max() + 1;
                long adjust = RamUsageEstimator.alignObjectSize(
                    RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + selectedCountsLen * Integer.BYTES
                );
                blockFactory.adjustBreaker(adjust);
                selectedCountsSize = adjust;
                int[] selectedCounts = new int[selectedCountsLen];
                for (int id = 0; id < values.size(); id++) {
                    long both = values.get(id);
                    int group = (int) (both >>> Float.SIZE);
                    if (group < selectedCounts.length) {
                        selectedCounts[group]--;
                    }
                }

                /*
                 * Total the selected groups and turn them into a running count.
                 * Unselected groups will still have negative counts.
                 */
                int total = 0;
                for (int s = 0; s < selected.getPositionCount(); s++) {
                    int group = selected.getInt(s);
                    int count = -selectedCounts[group];
                    selectedCounts[group] = total;
                    total += count;
                }

                /*
                 * Build a list of ids to insert in order *and* convert the running
                 * count in selectedCounts[group] into the end index in ids for each
                 * group.
                 * Here we use the negative counts to signal that a group hasn't been
                 * selected and the id containing values for that group is ignored.
                 */
                adjust = RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + total * Integer.BYTES);
                blockFactory.adjustBreaker(adjust);
                idsSize = adjust;
                int[] ids = new int[total];
                for (int id = 0; id < values.size(); id++) {
                    long both = values.get(id);
                    int group = (int) (both >>> Float.SIZE);
                    if (group < selectedCounts.length && selectedCounts[group] >= 0) {
                        ids[selectedCounts[group]++] = id;
                    }
                }

                /*
                 * Insert the ids in order.
                 */
                try (FloatBlock.Builder builder = blockFactory.newFloatBlockBuilder(selected.getPositionCount())) {
                    int start = 0;
                    for (int s = 0; s < selected.getPositionCount(); s++) {
                        int group = selected.getInt(s);
                        int end = selectedCounts[group];
                        int count = end - start;
                        switch (count) {
                            case 0 -> builder.appendNull();
                            case 1 -> append(builder, ids[start]);
                            default -> {
                                builder.beginPositionEntry();
                                for (int i = start; i < end; i++) {
                                    append(builder, ids[i]);
                                }
                                builder.endPositionEntry();
                            }
                        }
                        start = end;
                    }
                    return builder.build();
                }
            } finally {
                blockFactory.adjustBreaker(-selectedCountsSize - idsSize);
            }
        }

        private void append(FloatBlock.Builder builder, int id) {
            long both = values.get(id);
            float value = Float.intBitsToFloat((int) both);
            builder.appendFloat(value);
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seen) {
            // we figure out seen values from nulls on the values block
        }

        @Override
        public void close() {
            values.close();
        }
    }
}
