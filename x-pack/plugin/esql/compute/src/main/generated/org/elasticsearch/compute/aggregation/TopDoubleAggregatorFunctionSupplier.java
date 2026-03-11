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
 * {@link AggregatorFunctionSupplier} implementation for {@link TopDoubleAggregator}.
 * This class is generated. Edit {@code AggregatorFunctionSupplierImplementer} instead.
 */
public final class TopDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
  private final int limit;

  private final boolean ascending;

  public TopDoubleAggregatorFunctionSupplier(int limit, boolean ascending) {
    this.limit = limit;
    this.ascending = ascending;
  }

  @Override
  public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
    return TopDoubleAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
    return TopDoubleGroupingAggregatorFunction.intermediateStateDesc();
  }

  @Override
  public TopDoubleAggregatorFunction aggregator(DriverContext driverContext,
      List<Integer> channels) {
    List<ExpressionEvaluator> inputs = channels.stream().<ExpressionEvaluator>map(LoadFromPage::new).toList();
    return new TopDoubleAggregatorFunction(driverContext, inputs, limit, ascending);
  }

  @Override
  public TopDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext,
      List<Integer> channels) {
    List<ExpressionEvaluator> inputs = channels.stream().<ExpressionEvaluator>map(LoadFromPage::new).toList();
    return new TopDoubleGroupingAggregatorFunction(inputs, driverContext, limit, ascending);
  }

  @Override
  public String describe() {
    return "top of doubles";
  }
}
