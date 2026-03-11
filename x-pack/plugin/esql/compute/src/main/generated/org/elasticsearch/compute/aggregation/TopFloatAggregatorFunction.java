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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link TopFloatAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class TopFloatAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("top", ElementType.FLOAT)  );

  private final DriverContext driverContext;

  private final TopFloatAggregator.SingleState state;

  private final List<ExpressionEvaluator> inputs;

  private final int limit;

  private final boolean ascending;

  TopFloatAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs,
      int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = TopFloatAggregator.initSingle(driverContext.bigArrays(), limit, ascending);
  }

  public static List<IntermediateStateDesc> intermediateStateDesc() {
    return INTERMEDIATE_STATE_DESC;
  }

  @Override
  public int intermediateBlockCount() {
    return INTERMEDIATE_STATE_DESC.size();
  }

  @Override
  public void addRawInput(Page page, BooleanVector mask) {
    if (mask.allFalse()) {
      // Entire page masked away
    } else if (mask.allTrue()) {
      addRawInputNotMasked(page);
    } else {
      addRawInputMasked(page, mask);
    }
  }

  private void addRawInputMasked(Page page, BooleanVector mask) {
    try (FloatBlock vBlock = (FloatBlock) inputs.get(0).eval(page)) {
      FloatVector vVector = vBlock.asVector();
      if (vVector == null) {
        addRawBlock(vBlock, mask);
        return;
      }
      addRawVector(vVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (FloatBlock vBlock = (FloatBlock) inputs.get(0).eval(page)) {
      FloatVector vVector = vBlock.asVector();
      if (vVector == null) {
        addRawBlock(vBlock);
        return;
      }
      addRawVector(vVector);
    }
  }

  private void addRawVector(FloatVector vVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      float vValue = vVector.getFloat(valuesPosition);
      TopFloatAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(FloatVector vVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      float vValue = vVector.getFloat(valuesPosition);
      TopFloatAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(FloatBlock vBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        TopFloatAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(FloatBlock vBlock, BooleanVector mask) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        float vValue = vBlock.getFloat(vOffset);
        TopFloatAggregator.combine(state, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (Block topUncast = inputs.get(0).eval(page)) {
      if (topUncast.areAllValuesNull()) {
        return;
      }
      FloatBlock top = (FloatBlock) topUncast;
      assert top.getPositionCount() == 1;
      TopFloatAggregator.combineIntermediate(state, top);
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = TopFloatAggregator.evaluateFinal(state, driverContext);
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
