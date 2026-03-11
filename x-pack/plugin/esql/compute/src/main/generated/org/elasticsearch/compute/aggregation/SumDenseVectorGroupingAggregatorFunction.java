// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.ArithmeticException;
import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SumDenseVectorAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SumDenseVectorGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("sum", ElementType.FLOAT),
      new IntermediateStateDesc("failed", ElementType.BOOLEAN)  );

  private final SumDenseVectorGroupingState state;

  private final Warnings warnings;

  private final List<ExpressionEvaluator> inputs;

  private final DriverContext driverContext;

  SumDenseVectorGroupingAggregatorFunction(Warnings warnings, List<ExpressionEvaluator> inputs,
      DriverContext driverContext) {
    this.warnings = warnings;
    this.inputs = inputs;
    this.state = SumDenseVectorAggregator.initGrouping(driverContext.bigArrays());
    this.driverContext = driverContext;
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public GroupingAggregatorFunction.AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds,
      Page page) {
    FloatBlock vectorBlock = (FloatBlock) inputs.get(0).eval(page);
    maybeEnableGroupIdTracking(seenGroupIds, vectorBlock);
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vectorBlock);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vectorBlock);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, vectorBlock);
      }

      @Override
      public void close() {
        vectorBlock.close();
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, FloatBlock vectorBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        try {
          SumDenseVectorAggregator.combine(state, groupId, valuesPosition, vectorBlock);
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block sumUncast = inputs.get(0).eval(page); Block failedUncast = inputs.get(1).eval(page)) {
      if (sumUncast.areAllValuesNull()) {
        return;
      }
      FloatBlock sum = (FloatBlock) sumUncast;
      if (failedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
      assert sum.getPositionCount() == failed.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          SumDenseVectorAggregator.combineIntermediate(state, groupId, sum, failed.getBoolean(valuesPosition), valuesPosition);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, FloatBlock vectorBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        if (state.hasFailed(groupId)) {
          continue;
        }
        try {
          SumDenseVectorAggregator.combine(state, groupId, valuesPosition, vectorBlock);
        } catch (ArithmeticException e) {
          warnings.registerException(e);
          state.setFailed(groupId);
        }
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block sumUncast = inputs.get(0).eval(page); Block failedUncast = inputs.get(1).eval(page)) {
      if (sumUncast.areAllValuesNull()) {
        return;
      }
      FloatBlock sum = (FloatBlock) sumUncast;
      if (failedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
      assert sum.getPositionCount() == failed.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          SumDenseVectorAggregator.combineIntermediate(state, groupId, sum, failed.getBoolean(valuesPosition), valuesPosition);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, FloatBlock vectorBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      if (state.hasFailed(groupId)) {
        continue;
      }
      try {
        SumDenseVectorAggregator.combine(state, groupId, valuesPosition, vectorBlock);
      } catch (ArithmeticException e) {
        warnings.registerException(e);
        state.setFailed(groupId);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block sumUncast = inputs.get(0).eval(page); Block failedUncast = inputs.get(1).eval(page)) {
      if (sumUncast.areAllValuesNull()) {
        return;
      }
      FloatBlock sum = (FloatBlock) sumUncast;
      if (failedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector failed = ((BooleanBlock) failedUncast).asVector();
      assert sum.getPositionCount() == failed.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        int groupId = groups.getInt(groupPosition);
        int valuesPosition = groupPosition + positionOffset;
        SumDenseVectorAggregator.combineIntermediate(state, groupId, sum, failed.getBoolean(valuesPosition), valuesPosition);
      }
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, FloatBlock vectorBlock) {
    if (vectorBlock.mayHaveNulls()) {
      state.enableGroupIdTracking(seenGroupIds);
    }
  }

  @Override
  public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
    state.enableGroupIdTracking(seenGroupIds);
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
    state.toIntermediate(blocks, offset, selected, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, IntVector selected,
      GroupingAggregatorEvaluationContext ctx) {
    blocks[offset] = SumDenseVectorAggregator.evaluateFinal(state, selected, ctx);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("inputs=").append(inputs);
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void close() {
    state.close();
  }
}
