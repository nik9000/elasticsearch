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
import java.util.function.Supplier;

public class SignumTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        doubleCase().doubles(-Double.MAX_VALUE, Double.MAX_VALUE).expectedFromDouble(Math::signum).build(suppliers);
        intCase().ints().expectedFromInt(i -> (double) Math.signum(i)).build(suppliers);
        longCase().longs().expectedFromLong(l -> (double) Math.signum(l)).build(suppliers);
        unsignedLongCase().unsignedLongs()
            .expectedFromBigInteger(ul -> Math.signum(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))))
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
        return TestCaseSupplier.unary().expectedOutputType(DataType.DOUBLE).evaluatorToString("Signum" + type + "Evaluator[val=%0]");
    }

    public SignumTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Signum(source, args.get(0));
    }
}
