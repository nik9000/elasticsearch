// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation.spatial;

import java.lang.Override;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link GroupingAggregatorFunction} implementation for {@link SpatialExtentGeoShapeSourceValuesAggregator}.
 * This class is generated. Edit {@code GroupingAggregatorImplementer} instead.
 */
public final class SpatialExtentGeoShapeSourceValuesGroupingAggregatorFunction implements GroupingAggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.INT),
      new IntermediateStateDesc("bottom", ElementType.INT),
      new IntermediateStateDesc("negLeft", ElementType.INT),
      new IntermediateStateDesc("negRight", ElementType.INT),
      new IntermediateStateDesc("posLeft", ElementType.INT),
      new IntermediateStateDesc("posRight", ElementType.INT)  );

  private final SpatialExtentGroupingStateWrappedLongitudeState state;

  private final List<ExpressionEvaluator> inputs;

  private final DriverContext driverContext;

  SpatialExtentGeoShapeSourceValuesGroupingAggregatorFunction(List<ExpressionEvaluator> inputs,
      DriverContext driverContext) {
    this.inputs = inputs;
    this.state = SpatialExtentGeoShapeSourceValuesAggregator.initGrouping();
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
    BytesRefBlock bytesBlock = (BytesRefBlock) inputs.get(0).eval(page);
    BytesRefVector bytesVector = bytesBlock.asVector();
    if (bytesVector == null) {
      maybeEnableGroupIdTracking(seenGroupIds, bytesBlock);
      return new GroupingAggregatorFunction.AddInput() {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
          addRawInput(positionOffset, groupIds, bytesBlock);
        }

        @Override
        public void close() {
          bytesBlock.close();
        }
      };
    }
    return new GroupingAggregatorFunction.AddInput() {
      @Override
      public void add(int positionOffset, IntArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void add(int positionOffset, IntBigArrayBlock groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void add(int positionOffset, IntVector groupIds) {
        addRawInput(positionOffset, groupIds, bytesVector);
      }

      @Override
      public void close() {
        bytesBlock.close();
      }
    };
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
        int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
        for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
          BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
          SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntArrayBlock groups, BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
        SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block topUncast = inputs.get(0).eval(page); Block bottomUncast = inputs.get(1).eval(page); Block negLeftUncast = inputs.get(2).eval(page); Block negRightUncast = inputs.get(3).eval(page); Block posLeftUncast = inputs.get(4).eval(page); Block posRightUncast = inputs.get(5).eval(page)) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      IntVector top = ((IntBlock) topUncast).asVector();
      if (bottomUncast.areAllValuesNull()) {
        return;
      }
      IntVector bottom = ((IntBlock) bottomUncast).asVector();
      if (negLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector negLeft = ((IntBlock) negLeftUncast).asVector();
      if (negRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector negRight = ((IntBlock) negRightUncast).asVector();
      if (posLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector posLeft = ((IntBlock) posLeftUncast).asVector();
      if (posRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector posRight = ((IntBlock) posRightUncast).asVector();
      assert top.getPositionCount() == bottom.getPositionCount() && top.getPositionCount() == negLeft.getPositionCount() && top.getPositionCount() == negRight.getPositionCount() && top.getPositionCount() == posLeft.getPositionCount() && top.getPositionCount() == posRight.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          SpatialExtentGeoShapeSourceValuesAggregator.combineIntermediate(state, groupId, top.getInt(valuesPosition), bottom.getInt(valuesPosition), negLeft.getInt(valuesPosition), negRight.getInt(valuesPosition), posLeft.getInt(valuesPosition), posRight.getInt(valuesPosition));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
        int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
        for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
          BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
          SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntBigArrayBlock groups,
      BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      if (groups.isNull(groupPosition)) {
        continue;
      }
      int valuesPosition = groupPosition + positionOffset;
      int groupStart = groups.getFirstValueIndex(groupPosition);
      int groupEnd = groupStart + groups.getValueCount(groupPosition);
      for (int g = groupStart; g < groupEnd; g++) {
        int groupId = groups.getInt(g);
        BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
        SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block topUncast = inputs.get(0).eval(page); Block bottomUncast = inputs.get(1).eval(page); Block negLeftUncast = inputs.get(2).eval(page); Block negRightUncast = inputs.get(3).eval(page); Block posLeftUncast = inputs.get(4).eval(page); Block posRightUncast = inputs.get(5).eval(page)) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      IntVector top = ((IntBlock) topUncast).asVector();
      if (bottomUncast.areAllValuesNull()) {
        return;
      }
      IntVector bottom = ((IntBlock) bottomUncast).asVector();
      if (negLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector negLeft = ((IntBlock) negLeftUncast).asVector();
      if (negRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector negRight = ((IntBlock) negRightUncast).asVector();
      if (posLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector posLeft = ((IntBlock) posLeftUncast).asVector();
      if (posRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector posRight = ((IntBlock) posRightUncast).asVector();
      assert top.getPositionCount() == bottom.getPositionCount() && top.getPositionCount() == negLeft.getPositionCount() && top.getPositionCount() == negRight.getPositionCount() && top.getPositionCount() == posLeft.getPositionCount() && top.getPositionCount() == posRight.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        if (groups.isNull(groupPosition)) {
          continue;
        }
        int groupStart = groups.getFirstValueIndex(groupPosition);
        int groupEnd = groupStart + groups.getValueCount(groupPosition);
        for (int g = groupStart; g < groupEnd; g++) {
          int groupId = groups.getInt(g);
          int valuesPosition = groupPosition + positionOffset;
          SpatialExtentGeoShapeSourceValuesAggregator.combineIntermediate(state, groupId, top.getInt(valuesPosition), bottom.getInt(valuesPosition), negLeft.getInt(valuesPosition), negRight.getInt(valuesPosition), posLeft.getInt(valuesPosition), posRight.getInt(valuesPosition));
        }
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefBlock bytesBlock) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      if (bytesBlock.isNull(valuesPosition)) {
        continue;
      }
      int groupId = groups.getInt(groupPosition);
      int bytesStart = bytesBlock.getFirstValueIndex(valuesPosition);
      int bytesEnd = bytesStart + bytesBlock.getValueCount(valuesPosition);
      for (int bytesOffset = bytesStart; bytesOffset < bytesEnd; bytesOffset++) {
        BytesRef bytesValue = bytesBlock.getBytesRef(bytesOffset, bytesScratch);
        SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
      }
    }
  }

  private void addRawInput(int positionOffset, IntVector groups, BytesRefVector bytesVector) {
    BytesRef bytesScratch = new BytesRef();
    for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
      int valuesPosition = groupPosition + positionOffset;
      int groupId = groups.getInt(groupPosition);
      BytesRef bytesValue = bytesVector.getBytesRef(valuesPosition, bytesScratch);
      SpatialExtentGeoShapeSourceValuesAggregator.combine(state, groupId, bytesValue);
    }
  }

  @Override
  public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
    state.enableGroupIdTracking(new SeenGroupIds.Empty());
    assert inputs.size() == intermediateBlockCount();
    try (Block topUncast = inputs.get(0).eval(page); Block bottomUncast = inputs.get(1).eval(page); Block negLeftUncast = inputs.get(2).eval(page); Block negRightUncast = inputs.get(3).eval(page); Block posLeftUncast = inputs.get(4).eval(page); Block posRightUncast = inputs.get(5).eval(page)) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      IntVector top = ((IntBlock) topUncast).asVector();
      if (bottomUncast.areAllValuesNull()) {
        return;
      }
      IntVector bottom = ((IntBlock) bottomUncast).asVector();
      if (negLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector negLeft = ((IntBlock) negLeftUncast).asVector();
      if (negRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector negRight = ((IntBlock) negRightUncast).asVector();
      if (posLeftUncast.areAllValuesNull()) {
        return;
      }
      IntVector posLeft = ((IntBlock) posLeftUncast).asVector();
      if (posRightUncast.areAllValuesNull()) {
        return;
      }
      IntVector posRight = ((IntBlock) posRightUncast).asVector();
      assert top.getPositionCount() == bottom.getPositionCount() && top.getPositionCount() == negLeft.getPositionCount() && top.getPositionCount() == negRight.getPositionCount() && top.getPositionCount() == posLeft.getPositionCount() && top.getPositionCount() == posRight.getPositionCount();
      for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
        int groupId = groups.getInt(groupPosition);
        int valuesPosition = groupPosition + positionOffset;
        SpatialExtentGeoShapeSourceValuesAggregator.combineIntermediate(state, groupId, top.getInt(valuesPosition), bottom.getInt(valuesPosition), negLeft.getInt(valuesPosition), negRight.getInt(valuesPosition), posLeft.getInt(valuesPosition), posRight.getInt(valuesPosition));
      }
    }
  }

  private void maybeEnableGroupIdTracking(SeenGroupIds seenGroupIds, BytesRefBlock bytesBlock) {
    if (bytesBlock.mayHaveNulls()) {
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
    blocks[offset] = SpatialExtentGeoShapeSourceValuesAggregator.evaluateFinal(state, selected, ctx);
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
