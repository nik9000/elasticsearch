// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link AnyBytesRefAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class AnyBytesRefGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("observed", ElementType.BOOLEAN),
      new IntermediateStateDesc("values", ElementType.BYTES_REF)  );

  private final AnyBytesRefAggregator.GroupingState state;

  private final List<ExpressionEvaluator> inputs;

  private final DriverContext driverContext;

  AnyBytesRefGroupingAggregatorFunction(List<ExpressionEvaluator> inputs,
      DriverContext driverContext) {
    this.inputs = inputs;
    this.state = AnyBytesRefAggregator.initGrouping(driverContext);
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
    BytesRefBlock valuesBlock = (BytesRefBlock) inputs.get(0).eval(page);
    maybeEnableGroupIdTracking(seenGroupIds, valuesBlock);
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, valuesBlock);
      }

      @Override
      public void close() {
        valuesBlock.close();
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefBlock valuesBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        AnyBytesRefAggregator.combine(state, groupId, valuesPosition, valuesBlock);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block observedUncast = inputs.get(0).eval(page); Block valuesUncast = inputs.get(1).eval(page)) {
      if (observedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector observed = ((BooleanBlock) observedUncast).asVector();
      if (valuesUncast.areAllValuesNull()) {
        return;
      }
      BytesRefBlock values = (BytesRefBlock) valuesUncast;
      assert observed.getPositionCount() == values.getPositionCount();
      BytesRef valuesScratch = new BytesRef();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          AnyBytesRefAggregator.combineIntermediate(state, groupId, observed.getBoolean(valuesPosition), values, valuesPosition);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BytesRefBlock valuesBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        AnyBytesRefAggregator.combine(state, groupId, valuesPosition, valuesBlock);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block observedUncast = inputs.get(0).eval(page); Block valuesUncast = inputs.get(1).eval(page)) {
      if (observedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector observed = ((BooleanBlock) observedUncast).asVector();
      if (valuesUncast.areAllValuesNull()) {
        return;
      }
      BytesRefBlock values = (BytesRefBlock) valuesUncast;
      assert observed.getPositionCount() == values.getPositionCount();
      BytesRef valuesScratch = new BytesRef();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          AnyBytesRefAggregator.combineIntermediate(state, groupId, observed.getBoolean(valuesPosition), values, valuesPosition);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefBlock valuesBlock) {
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      AnyBytesRefAggregator.combine(state, groupId, valuesPosition, valuesBlock);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block observedUncast = inputs.get(0).eval(page); Block valuesUncast = inputs.get(1).eval(page)) {
      if (observedUncast.areAllValuesNull()) {
        return;
      }
      BooleanVector observed = ((BooleanBlock) observedUncast).asVector();
      if (valuesUncast.areAllValuesNull()) {
        return;
      }
      BytesRefBlock values = (BytesRefBlock) valuesUncast;
      assert observed.getPositionCount() == values.getPositionCount();
      BytesRef valuesScratch = new BytesRef();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        int groupId = groups.getInt(groupPosition);
        int valuesPosition = groupPosition + positionOffset;
        AnyBytesRefAggregator.combineIntermediate(state, groupId, observed.getBoolean(valuesPosition), values, valuesPosition);
      }
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BytesRefBlock valuesBlock) {
    if (valuesBlock.mayHaveNulls()) {
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
    blocks[offset] = AnyBytesRefAggregator.evaluateFinal(state, selected, ctx);
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
