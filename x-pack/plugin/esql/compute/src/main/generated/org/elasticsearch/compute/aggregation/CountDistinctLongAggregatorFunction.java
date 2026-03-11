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
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link CountDistinctLongAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class CountDistinctLongAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("hll", ElementType.BYTES_REF)  );

  private final DriverContext driverContext;

  private final HllStates.SingleState state;

  private final List<ExpressionEvaluator> inputs;

  private final int precision;

  CountDistinctLongAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs,
      int precision) {
    this.precision = precision;
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = CountDistinctLongAggregator.initSingle(driverContext, precision);
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
    try (LongBlock vBlock = (LongBlock) inputs.get(0).eval(page)) {
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        addRawBlock(vBlock, mask);
        return;
      }
      addRawVector(vVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (LongBlock vBlock = (LongBlock) inputs.get(0).eval(page)) {
      LongVector vVector = vBlock.asVector();
      if (vVector == null) {
        addRawBlock(vBlock);
        return;
      }
      addRawVector(vVector);
    }
  }

  private void addRawVector(LongVector vVector) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      long vValue = vVector.getLong(valuesPosition);
      CountDistinctLongAggregator.combine(state, vValue);
    }
  }

  private void addRawVector(LongVector vVector, BooleanVector mask) {
    for (int valuesPosition = 0; valuesPosition < vVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      long vValue = vVector.getLong(valuesPosition);
      CountDistinctLongAggregator.combine(state, vValue);
    }
  }

  private void addRawBlock(LongBlock vBlock) {
    for (int p = 0; p < vBlock.getPositionCount(); p++) {
      int vValueCount = vBlock.getValueCount(p);
      if (vValueCount == 0) {
        continue;
      }
      int vStart = vBlock.getFirstValueIndex(p);
      int vEnd = vStart + vValueCount;
      for (int vOffset = vStart; vOffset < vEnd; vOffset++) {
        long vValue = vBlock.getLong(vOffset);
        CountDistinctLongAggregator.combine(state, vValue);
      }
    }
  }

  private void addRawBlock(LongBlock vBlock, BooleanVector mask) {
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
        long vValue = vBlock.getLong(vOffset);
        CountDistinctLongAggregator.combine(state, vValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (Block hllUncast = inputs.get(0).eval(page)) {
      if (hllUncast.areAllValuesNull()) {
        return;
      }
      BytesRefVector hll = ((BytesRefBlock) hllUncast).asVector();
      assert hll.getPositionCount() == 1;
      BytesRef hllScratch = new BytesRef();
      CountDistinctLongAggregator.combineIntermediate(state, hll.getBytesRef(0, hllScratch));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = CountDistinctLongAggregator.evaluateFinal(state, driverContext);
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
