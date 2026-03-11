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
import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunction} implementation for {@link SpatialCentroidPointSourceValuesAggregator}.
 * This class is generated. Edit {@code AggregatorImplementer} instead.
 */
public final class SpatialCentroidPointSourceValuesAggregatorFunction implements AggregatorFunction {
  private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
      new IntermediateStateDesc("xVal", ElementType.DOUBLE),
      new IntermediateStateDesc("xDel", ElementType.DOUBLE),
      new IntermediateStateDesc("yVal", ElementType.DOUBLE),
      new IntermediateStateDesc("yDel", ElementType.DOUBLE),
      new IntermediateStateDesc("count", ElementType.LONG)  );

  private final DriverContext driverContext;

  private final CentroidPointAggregator.CentroidState state;

  private final List<ExpressionEvaluator> inputs;

  SpatialCentroidPointSourceValuesAggregatorFunction(DriverContext driverContext,
      List<ExpressionEvaluator> inputs) {
    this.driverContext = driverContext;
    this.inputs = inputs;
    this.state = SpatialCentroidPointSourceValuesAggregator.initSingle();
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
    try (BytesRefBlock wkbBlock = (BytesRefBlock) inputs.get(0).eval(page)) {
      BytesRefVector wkbVector = wkbBlock.asVector();
      if (wkbVector == null) {
        addRawBlock(wkbBlock, mask);
        return;
      }
      addRawVector(wkbVector, mask);
    }
  }

  private void addRawInputNotMasked(Page page) {
    try (BytesRefBlock wkbBlock = (BytesRefBlock) inputs.get(0).eval(page)) {
      BytesRefVector wkbVector = wkbBlock.asVector();
      if (wkbVector == null) {
        addRawBlock(wkbBlock);
        return;
      }
      addRawVector(wkbVector);
    }
  }

  private void addRawVector(BytesRefVector wkbVector) {
    BytesRef wkbScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < wkbVector.getPositionCount(); valuesPosition++) {
      BytesRef wkbValue = wkbVector.getBytesRef(valuesPosition, wkbScratch);
      SpatialCentroidPointSourceValuesAggregator.combine(state, wkbValue);
    }
  }

  private void addRawVector(BytesRefVector wkbVector, BooleanVector mask) {
    BytesRef wkbScratch = new BytesRef();
    for (int valuesPosition = 0; valuesPosition < wkbVector.getPositionCount(); valuesPosition++) {
      if (mask.getBoolean(valuesPosition) == false) {
        continue;
      }
      BytesRef wkbValue = wkbVector.getBytesRef(valuesPosition, wkbScratch);
      SpatialCentroidPointSourceValuesAggregator.combine(state, wkbValue);
    }
  }

  private void addRawBlock(BytesRefBlock wkbBlock) {
    BytesRef wkbScratch = new BytesRef();
    for (int p = 0; p < wkbBlock.getPositionCount(); p++) {
      int wkbValueCount = wkbBlock.getValueCount(p);
      if (wkbValueCount == 0) {
        continue;
      }
      int wkbStart = wkbBlock.getFirstValueIndex(p);
      int wkbEnd = wkbStart + wkbValueCount;
      for (int wkbOffset = wkbStart; wkbOffset < wkbEnd; wkbOffset++) {
        BytesRef wkbValue = wkbBlock.getBytesRef(wkbOffset, wkbScratch);
        SpatialCentroidPointSourceValuesAggregator.combine(state, wkbValue);
      }
    }
  }

  private void addRawBlock(BytesRefBlock wkbBlock, BooleanVector mask) {
    BytesRef wkbScratch = new BytesRef();
    for (int p = 0; p < wkbBlock.getPositionCount(); p++) {
      if (mask.getBoolean(p) == false) {
        continue;
      }
      int wkbValueCount = wkbBlock.getValueCount(p);
      if (wkbValueCount == 0) {
        continue;
      }
      int wkbStart = wkbBlock.getFirstValueIndex(p);
      int wkbEnd = wkbStart + wkbValueCount;
      for (int wkbOffset = wkbStart; wkbOffset < wkbEnd; wkbOffset++) {
        BytesRef wkbValue = wkbBlock.getBytesRef(wkbOffset, wkbScratch);
        SpatialCentroidPointSourceValuesAggregator.combine(state, wkbValue);
      }
    }
  }

  @Override
  public void addIntermediateInput(Page page) {
    assert inputs.size() == intermediateBlockCount();
    try (Block xValUncast = inputs.get(0).eval(page); Block xDelUncast = inputs.get(1).eval(page); Block yValUncast = inputs.get(2).eval(page); Block yDelUncast = inputs.get(3).eval(page); Block countUncast = inputs.get(4).eval(page)) {
      if (xValUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector xVal = ((DoubleBlock) xValUncast).asVector();
      assert xVal.getPositionCount() == 1;
      if (xDelUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector xDel = ((DoubleBlock) xDelUncast).asVector();
      assert xDel.getPositionCount() == 1;
      if (yValUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector yVal = ((DoubleBlock) yValUncast).asVector();
      assert yVal.getPositionCount() == 1;
      if (yDelUncast.areAllValuesNull()) {
        return;
      }
      DoubleVector yDel = ((DoubleBlock) yDelUncast).asVector();
      assert yDel.getPositionCount() == 1;
      if (countUncast.areAllValuesNull()) {
        return;
      }
      LongVector count = ((LongBlock) countUncast).asVector();
      assert count.getPositionCount() == 1;
      SpatialCentroidPointSourceValuesAggregator.combineIntermediate(state, xVal.getDouble(0), xDel.getDouble(0), yVal.getDouble(0), yDel.getDouble(0), count.getLong(0));
    }
  }

  @Override
  public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
    state.toIntermediate(blocks, offset, driverContext);
  }

  @Override
  public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
    blocks[offset] = SpatialCentroidPointSourceValuesAggregator.evaluateFinal(state, driverContext);
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
