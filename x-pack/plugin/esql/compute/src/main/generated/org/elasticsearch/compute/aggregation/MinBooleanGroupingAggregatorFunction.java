// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link MinBooleanAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class MinBooleanGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("min", ElementType.BOOLEAN),
      new IntermediateStateDesc("seen", ElementType.BOOLEAN)  );

  private final BooleanArrayState state;

  private final List<ExpressionEvaluator> inputs;

  private final DriverContext driverContext;

  MinBooleanGroupingAggregatorFunction(List<ExpressionEvaluator> inputs,
      DriverContext driverContext) {
    this.inputs = inputs;
    this.state = new BooleanArrayState(driverContext.bigArrays(), MinBooleanAggregator.init());
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
    BooleanBlock vBlock = (BooleanBlock) inputs.get(0).eval(page);
    BooleanVector vVector = vBlock.asVector();
    if (vVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, vBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, vBlock);
        }

        @Override
        public void close() {
          vBlock.close();
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, vVector);
      }

      @Override
      public void close() {
        vBlock.close();
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BooleanBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          boolean vValue = vBlock.getBoolean(vOffset);
          state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BooleanVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        boolean vValue = vVector.getBoolean(valuesPosition);
        state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block minUncast = inputs.get(0).eval(page); Block seenUncast = inputs.get(1).eval(page)) {
      if (minUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector min = ((BooleanBlock) minUncast).asVector();
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert min.getPositionCount() == seen.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          if (seen.getBoolean(valuesPosition)) {
            state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), min.getBoolean(valuesPosition)));
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BooleanBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int vStart = vBlock.getFirstValueIndex(valuesPosition);
        int vEnd = vStart + vBlock.getValueCount(valuesPosition);
        for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
          boolean vValue = vBlock.getBoolean(vOffset);
          state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BooleanVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        boolean vValue = vVector.getBoolean(valuesPosition);
        state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block minUncast = inputs.get(0).eval(page); Block seenUncast = inputs.get(1).eval(page)) {
      if (minUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector min = ((BooleanBlock) minUncast).asVector();
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert min.getPositionCount() == seen.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          if (seen.getBoolean(valuesPosition)) {
            state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), min.getBoolean(valuesPosition)));
          }
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BooleanBlock vBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (vBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int vStart = vBlock.getFirstValueIndex(valuesPosition);
      int vEnd = vStart + vBlock.getValueCount(valuesPosition);
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        boolean vValue = vBlock.getBoolean(vOffset);
        state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BooleanVector vVector) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      boolean vValue = vVector.getBoolean(valuesPosition);
      state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), vValue));
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block minUncast = inputs.get(0).eval(page); Block seenUncast = inputs.get(1).eval(page)) {
      if (minUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector min = ((BooleanBlock) minUncast).asVector();
      if (seenUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
      assert min.getPositionCount() == seen.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        int groupId = groups.getInt(groupPosition);
        int valuesPosition = groupPosition + positionOffset;
        if (seen.getBoolean(valuesPosition)) {
          state.set(groupId, MinBooleanAggregator.combine(state.getOrDefault(groupId), min.getBoolean(valuesPosition)));
        }
      }
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BooleanBlock vBlock) {
    if (vBlock.mayHaveNulls()) {
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
    blocks[offset] = state.toValuesBlock(selected, ctx.driverContext());
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
