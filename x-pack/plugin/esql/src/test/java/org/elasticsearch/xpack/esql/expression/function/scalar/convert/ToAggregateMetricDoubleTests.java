/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

@FunctionName("to_aggregate_metric_double")
public class ToAggregateMetricDoubleTests extends AbstractScalarFunctionTestCase {
    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.get(0).dataType() == DataType.AGGREGATE_METRIC_DOUBLE) {
            assumeTrue("Test sometimes wraps literals as fields", args.get(0).foldable());
        }
        return new ToAggregateMetricDouble(source, args.get(0));
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        helper("Int").ints()
            .expectedFromInt(i -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral((double) i, (double) i, (double) i, 1))
            .build(suppliers);
        helper("Long").longs()
            .expectedFromLong(
                l -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral((double) l, (double) l, (double) l, 1)
            )
            .build(suppliers);
        helper("UnsignedLong").unsignedLongs().expectedFromBigInteger(ul -> {
            var newVal = ul.doubleValue();
            return new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(newVal, newVal, newVal, 1);
        }).build(suppliers);
        helper("Double").doubles()
            .expectedFromDouble(d -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(d, d, d, 1))
            .build(suppliers);
        helper("AggregateMetricDouble").testCases(TestCaseSupplier.aggregateMetricDoubleCases()).expected(agg -> agg).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper helper(String type) {
        return unary().expectedOutputType(DataType.AGGREGATE_METRIC_DOUBLE)
            .evaluatorToString("ToAggregateMetricDoubleFrom" + type + "Evaluator[field=%0]");
    }

    public ToAggregateMetricDoubleTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }
}
