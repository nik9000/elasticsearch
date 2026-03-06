/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ToDegreesTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // int, long, unsigned long (cast to double), and doubles within safe range
        unary().evaluatorToString("ToDegreesEvaluator[deg=%0]")
            .expectedFromDouble(Math::toDegrees)
            .convertingToDouble(-3e306, 3e306, true, suppliers);
        // extreme doubles where toDegrees overflows to infinity
        unary().expectedOutputType(DataType.DOUBLE)
            .evaluatorToString("ToDegreesEvaluator[deg=%0]")
            .doubles(Double.MAX_VALUE, Double.POSITIVE_INFINITY)
            .doubles(Double.NEGATIVE_INFINITY, -Double.MAX_VALUE)
            .expectNullAndWarningsFromDouble(
                d -> List.of("Line 1:1: java.lang.ArithmeticException: not a finite double number: " + Math.toDegrees(d))
            )
            .build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public ToDegreesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDegrees(source, args.get(0));
    }
}
