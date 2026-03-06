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
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;

public class SqrtTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        intCase().ints(0, Integer.MAX_VALUE).expectedFromInt(Math::sqrt).build(suppliers);
        longCase().longs(0, Long.MAX_VALUE).expectedFromLong(Math::sqrt).build(suppliers);
        unsignedLongCase().unsignedLongs()
            .expectedFromBigInteger(ul -> Math.sqrt(unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))))
            .build(suppliers);
        doubleCase().doubles(-0d, Double.MAX_VALUE).expectedFromDouble(Math::sqrt).build(suppliers);
        suppliers = anyNullIsNull(true, suppliers);

        intCase().ints(Integer.MIN_VALUE, -1).expectNullAndWarnings(negativeWarning()).build(suppliers);
        longCase().longs(Long.MIN_VALUE, -1).expectNullAndWarnings(negativeWarning()).build(suppliers);
        doubleCase().doubles(Double.NEGATIVE_INFINITY, -Double.MIN_VALUE).expectNullAndWarnings(negativeWarning()).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper intCase() {
        return helper("Int");
    }

    private static UnaryTestCaseHelper longCase() {
        return helper("Long");
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return helper("UnsignedLong");
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double");
    }

    private static Function<Object, List<String>> negativeWarning() {
        return o -> List.of("Line 1:1: java.lang.ArithmeticException: Square root of negative");
    }

    private static UnaryTestCaseHelper helper(String type) {
        return TestCaseSupplier.unary().expectedOutputType(DataType.DOUBLE).evaluatorToString("Sqrt" + type + "Evaluator[val=%0]");
    }

    public SqrtTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Sqrt(source, args.get(0));
    }
}
