/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ToDoubleTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        helper("Boolean", "bool").booleans().expectedFromBoolean(b -> b ? 1d : 0d).build(suppliers);
        unary().expectedOutputType(DataType.DOUBLE)
            .evaluatorToString("%0")
            .doubles()
            .counterDoubles()
            .expectedFromDouble(d -> d)
            .build(suppliers);
        helper("Int", "i").ints().counterInts().expectedFromInt(i -> (double) i).build(suppliers);
        longCase().longs().counterLongs().expectedFromLong(l -> (double) l).build(suppliers);
        longCase().dates().expectedFromInstant(i -> (double) i.toEpochMilli()).build(suppliers);
        helper("UnsignedLong", "l").unsignedLongs().expectedFromBigInteger(BigInteger::doubleValue).build(suppliers);

        stringCase().strings().expectNullAndWarningsFromString(s -> {
            var exception = expectThrows(InvalidArgumentException.class, () -> EsqlDataTypeConverter.stringToDouble(s));
            return List.of("Line 1:1: " + exception);
        }).build(suppliers);
        stringCase().ints()
            .longs()
            .unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE))
            .doubles()
            .mapCases(DataType.KEYWORD, o -> new BytesRef(o.toString()))
            .expectedFromString(Double::valueOf)
            .build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper longCase() {
        return helper("Long", "l");
    }

    private static UnaryTestCaseHelper stringCase() {
        return helper("String", "in");
    }

    private static UnaryTestCaseHelper helper(String type, String paramName) {
        return unary().expectedOutputType(DataType.DOUBLE).evaluatorToString("ToDoubleFrom" + type + "Evaluator[" + paramName + "=%0]");
    }

    public ToDoubleTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDouble(source, args.get(0));
    }
}
