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
import java.util.function.Supplier;

public class CeilTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        helper().expectedOutputType(DataType.DOUBLE)
            .evaluatorToString("CeilDoubleEvaluator[val=%0]")
            .doubles()
            .expectedFromDouble(Math::ceil)
            .build(suppliers);
        helper().expectedOutputType(DataType.INTEGER).evaluatorToString("%0").ints().expectedFromInt(i -> i).build(suppliers);
        helper().expectedOutputType(DataType.LONG).evaluatorToString("%0").longs().expectedFromLong(l -> l).build(suppliers);
        helper().expectedOutputType(DataType.UNSIGNED_LONG).evaluatorToString("%0").unsignedLongs().expected(n -> n).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    private static UnaryTestCaseHelper helper() {
        return TestCaseSupplier.unary();
    }

    public CeilTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Ceil(source, args.get(0));
    }
}
