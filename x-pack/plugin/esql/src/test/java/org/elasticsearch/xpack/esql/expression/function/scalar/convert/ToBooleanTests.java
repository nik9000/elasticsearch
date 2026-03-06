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
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ToBooleanTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        unary().expectedOutputType(DataType.BOOLEAN).booleans().expected(b -> b).evaluatorToString("%0").build(suppliers);

        helper("Double", "d").doubles().expectedFromDouble(d -> d != 0d).build(suppliers);
        helper("Int", "i").ints().expectedFromInt(i -> i != 0).build(suppliers);
        helper("Long", "l").longs().expectedFromLong(l -> l != 0).build(suppliers);
        helper("String", "keyword").strings().expectedFromString(s -> s.toLowerCase(Locale.ROOT).equals("true")).build(suppliers);
        helper("UnsignedLong", "ul").unsignedLongs().expectedFromBigInteger(ul -> ul.compareTo(BigInteger.ZERO) != 0).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper helper(String type, String param) {
        return unary().expectedOutputType(DataType.BOOLEAN).evaluatorToString("ToBooleanFrom" + type + "Evaluator[" + param + "=%0]");
    }

    public ToBooleanTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToBoolean(source, args.get(0));
    }
}
