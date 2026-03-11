// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.
package org.elasticsearch.compute.aggregation;

import java.lang.Integer;
import java.lang.Override;
import java.lang.String;
import java.util.List;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPage;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link SampleBooleanAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class SampleBooleanAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  public SampleBooleanAggregatorFunctionSupplier(int limit) {
    this.limit = limit;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return SampleBooleanAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return SampleBooleanGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public SampleBooleanAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    List<ExpressionEvaluator> inputs = channels.stream().<ExpressionEvaluator>map(LoadFromPage::new).toList();
    return new SampleBooleanAggregatorFunction(driverContext, inputs, limit);
  }

  @Override
  public SampleBooleanGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    List<ExpressionEvaluator> inputs = channels.stream().<ExpressionEvaluator>map(LoadFromPage::new).toList();
    return new SampleBooleanGroupingAggregatorFunction(inputs, driverContext, limit);
  }

  @Override
  public String describe() {
    return "sample of booleans";
  }
}
