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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ExpTests extends AbstractScalarFunctionTestCase {
    // e^710 is Double.POSITIVE_INFINITY
    private static final int MAX_EXP_VALUE = 709;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        doubleCase().doubles(-Double.MAX_VALUE, MAX_EXP_VALUE).expectedFromDouble(Math::exp).build(suppliers);
        intCase().ints(Integer.MIN_VALUE, MAX_EXP_VALUE).expectedFromInt(i -> Math.exp(i)).build(suppliers);
        longCase().longs(Long.MIN_VALUE, MAX_EXP_VALUE).expectedFromLong(l -> Math.exp(l)).build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(MAX_EXP_VALUE))
            .expectedFromBigInteger(ul -> Math.exp(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))))
            .build(suppliers);

        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double");
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

    private static UnaryTestCaseHelper helper(String type) {
        return TestCaseSupplier.unary().expectedOutputType(DataType.DOUBLE).evaluatorToString("Exp" + type + "Evaluator[val=%0]");
    }

    public ExpTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Exp(source, args.get(0));
    }
}
