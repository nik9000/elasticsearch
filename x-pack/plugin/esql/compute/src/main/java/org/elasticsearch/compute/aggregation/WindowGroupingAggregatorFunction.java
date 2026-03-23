/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A {@link GroupingAggregatorFunction} that wraps another, and apply a window function on the final aggregation.
 */
public record WindowGroupingAggregatorFunction(GroupingAggregatorFunction next, AggregatorFunctionSupplier supplier, Duration window)
    implements
        GroupingAggregatorFunction {

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        return next.prepareProcessRawInputPage(seenGroupIds, page);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        next.selectedMayContainUnseenGroups(seenGroupIds);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groupIdVector, Page page) {
        next.addIntermediateInput(positionOffset, groupIdVector, page);
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateIntermediate(IntVector selected) {
        return next.prepareEvaluateIntermediate(selected);
    }

    @Override
    public GroupingAggregatorFunction.PreparedForEvaluation prepareEvaluateFinal(IntVector selected) {
        // TODO rework passing `evaluationContext` down.
        return (blocks, offset, selectedInPage, evaluationContext) -> {
            if (evaluationContext instanceof TimeSeriesGroupingAggregatorEvaluationContext timeSeriesContext) {
                evaluateFinalWithWindow(blocks, offset, selectedInPage, timeSeriesContext);
            } else {
                next.prepareEvaluateFinal(selected).evaluate(blocks, offset, selectedInPage, evaluationContext);
            }
        };
    }

    private void evaluateFinalWithWindow(
        Block[] blocks,
        int offset,
        IntVector selected,
        TimeSeriesGroupingAggregatorEvaluationContext evaluationContext
    ) {
        if (selected.getPositionCount() > 0) {
            // TODO: rewrite to NO_WINDOW in the planner if the bucket and the window are the same
            int groupId = selected.getInt(0);
            long startTime = evaluationContext.rangeStartInMillis(groupId);
            long endTime = evaluationContext.rangeEndInMillis(groupId);
            if (endTime - startTime == window.toMillis()) {
                next.prepareEvaluateFinal(selected).evaluate(blocks, offset, selected, evaluationContext);
                return;
            }
        }
        int blockCount = next.intermediateBlockCount();
        List<Integer> channels = IntStream.range(0, blockCount).boxed().toList();
        GroupingAggregator.Factory aggregatorFactory = supplier.groupingAggregatorFactory(AggregatorMode.FINAL, channels);
        try (GroupingAggregator finalAgg = aggregatorFactory.apply(evaluationContext.driverContext())) {
            Block[] intermediateBlocks = new Block[blockCount];
            int[] backwards = new int[selected.getPositionCount()];
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int groupId = selected.getInt(i);
                backwards = ArrayUtil.grow(backwards, groupId + 1);
                backwards[groupId] = i;
            }
            try {
                next.prepareEvaluateIntermediate(selected).evaluate(intermediateBlocks, 0, selected, evaluationContext);
                Page page = new Page(intermediateBlocks);
                finalAgg.aggregatorFunction().addIntermediateInput(0, selected, page);
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int groupId = selected.getInt(i);
                    mergeBucketsFromWindow(groupId, backwards, page, finalAgg.aggregatorFunction(), evaluationContext);
                }
            } finally {
                Releasables.close(intermediateBlocks);
            }
            try (PreparedForEvaluation delegate = finalAgg.prepareForEvaluate(selected)) {
                delegate.evaluate(
                    blocks,
                    offset,
                    selected,
                    // expand the window to cover the new range
                    new TimeSeriesGroupingAggregatorEvaluationContext(evaluationContext.driverContext()) {
                        @Override
                        public long rangeStartInMillis(int groupId) {
                            return evaluationContext.rangeStartInMillis(groupId);
                        }

                        @Override
                        public long rangeEndInMillis(int groupId) {
                            return rangeStartInMillis(groupId) + window.toMillis();
                        }

                        @Override
                        public List<Integer> groupIdsFromWindow(int startingGroupId, Duration window) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public int previousGroupId(int currentGroupId) {
                            return -1;
                        }

                        @Override
                        public int nextGroupId(int currentGroupId) {
                            return -1;
                        }

                        @Override
                        public void computeAdjacentGroupIds() {
                            // not used by #nextGroupId and #previousGroupId
                        }
                    }
                );
            }
        }
    }

    private void mergeBucketsFromWindow(
        int startingGroupId,
        int[] groupIdToPositions,
        Page page,
        GroupingAggregatorFunction fn,
        TimeSeriesGroupingAggregatorEvaluationContext context
    ) {
        var groupIds = context.groupIdsFromWindow(startingGroupId, window);
        if (groupIds.size() > 1) {
            try (IntVector oneGroup = context.driverContext().blockFactory().newConstantIntVector(startingGroupId, 1)) {
                for (int g : groupIds) {
                    if (g != startingGroupId) {
                        int position = groupIdToPositions[g];
                        fn.addIntermediateInput(position, oneGroup, page);
                    }
                }
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return next.intermediateBlockCount();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(next);
    }

    @Override
    public String toString() {
        return "Window[agg=" + next + ", window=" + window + "]";
    }
}
