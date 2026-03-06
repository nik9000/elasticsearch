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
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

// ToLongSurrogateTests has a @FunctionName("to_long") annotation.
// That test has the complete set of types supported by to_long(value) and to_long(string,base).
// This test only covers to_long(value).
// So we use an unregistered function name here to prevent DocsV3 from overwriting
// the good .md generated from ToLongSurrogateTests with an incomplete .md generated from this test.
//
@FunctionName("_unregestered_to_long_tests")
public class ToLongTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        unary(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public static void unary(List<TestCaseSupplier> suppliers) {
        booleanCase().booleans().expectedFromBoolean(b -> b ? 1L : 0L).build(suppliers);
        doubleCase().doubles(Long.MIN_VALUE, Long.MAX_VALUE).expectedFromDouble(Math::round).build(suppliers);
        doubleCase() //
            .doubles(Double.NEGATIVE_INFINITY, Long.MIN_VALUE - 1d)
            .doubles(Long.MAX_VALUE + 1d, Double.POSITIVE_INFINITY)
            .expectNullAndWarningsFromDouble(
                d -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range")
            )
            .build(suppliers);
        intCase().ints().counterInts().expectedFromInt(i -> (long) i).build(suppliers);
        longCase().longs().counterLongs().expectedFromLong(l -> l).build(suppliers);
        longCase().dates().expectedFromInstant(DateUtils::toLongMillis).build(suppliers);
        longCase().dateNanos().expectedFromInstant(DateUtils::toLong).build(suppliers);
        longCase().geoGrids().expected(v -> v).build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE))
            .expectedFromBigInteger(BigInteger::longValue)
            .build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), UNSIGNED_LONG_MAX)
            .expectNullAndWarningsFromBigInteger(
                ul -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [long] range")
            )
            .build(suppliers);

        stringCase().strings()
            .expectNullAndWarningsFromString(
                s -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [" + s + "]")
            )
            .build(suppliers);
        for (DataType t : DataType.stringTypes()) {
            stringCase() //
                .longs()
                .ints()
                .mapCases(t, o -> new BytesRef(o.toString()))
                .expectedFromString(Long::valueOf)
                .build(suppliers);

            stringCase() //
                .doubles(Long.MIN_VALUE, Long.MAX_VALUE, false)
                .doubles(0, 0, true)
                .mapCases(t, o -> new BytesRef(o.toString()))
                .expectedFromString(s -> Math.round(Double.parseDouble(s)))
                .build(suppliers);

            stringCase() //
                .doubles(Double.NEGATIVE_INFINITY, Long.MIN_VALUE - 1d, false)
                .doubles(Long.MAX_VALUE + 1d, Double.POSITIVE_INFINITY, false)
                .mapCases(t, o -> new BytesRef(o.toString()))
                .expectNullAndWarningsFromString(
                    s -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [" + s + "]")
                )
                .build(suppliers);
        }
    }

    private static UnaryTestCaseHelper longCase() {
        return TestCaseSupplier.unary().expectedOutputType(DataType.LONG).evaluatorToString("%0");
    }

    private static UnaryTestCaseHelper stringCase() {
        return helper("String", "in");
    }

    private static UnaryTestCaseHelper booleanCase() {
        return helper("Boolean", "bool");
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double", "dbl");
    }

    private static UnaryTestCaseHelper intCase() {
        return helper("Int", "i");
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return helper("UnsignedLong", "ul");
    }

    private static UnaryTestCaseHelper helper(String type, String paramName) {
        return TestCaseSupplier.unary()
            .expectedOutputType(DataType.LONG)
            .evaluatorToString("ToLongFrom" + type + "Evaluator[" + paramName + "=%0]");
    }

    public ToLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLong(source, args.get(0));
    }
}
