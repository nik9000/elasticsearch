/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class AbsTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        doubleCase().doubles(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY).expectedFromDouble(Math::abs).build(suppliers);
        intCase().ints(Integer.MIN_VALUE + 1, Integer.MAX_VALUE).expectedFromInt(Math::absExact).build(suppliers);
        intCase().ints(Integer.MIN_VALUE, Integer.MIN_VALUE).expectNullAndWarnings(overflowWarning("Integer")).build(suppliers);
        longCase().longs(Long.MIN_VALUE + 1, Long.MAX_VALUE).expectedFromLong(Math::absExact).build(suppliers);
        longCase().longs(Long.MIN_VALUE, Long.MIN_VALUE).expectNullAndWarnings(overflowWarning("Long")).build(suppliers);
        unsignedLongCase().unsignedLongs().expected(n -> n).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double").expectedOutputType(DataType.DOUBLE);
    }

    private static UnaryTestCaseHelper intCase() {
        return helper("Int").expectedOutputType(DataType.INTEGER);
    }

    private static UnaryTestCaseHelper longCase() {
        return helper("Long").expectedOutputType(DataType.LONG);
    }

    private static Function<Object, List<String>> overflowWarning(String type) {
        return o -> List.of("Line 1:1: java.lang.ArithmeticException: Overflow to represent absolute value of " + type + ".MIN_VALUE");
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return TestCaseSupplier.unary().expectedOutputType(DataType.UNSIGNED_LONG).evaluatorToString("%0");
    }

    private static UnaryTestCaseHelper helper(String type) {
        return TestCaseSupplier.unary().evaluatorToString("Abs" + type + "Evaluator[fieldVal=%0]");
    }

    public AbsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Abs(source, args.get(0));
    }
}
