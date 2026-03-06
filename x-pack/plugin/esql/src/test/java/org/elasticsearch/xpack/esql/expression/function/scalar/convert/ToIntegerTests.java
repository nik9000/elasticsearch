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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;

// ToIntegerSurrogateTests has a @FunctionName("to_integer") annotation.
// That test has the complete set of types supported by to_integer(value) and to_integer(string,base).
// This test only covers unary to_integer(value).
// So we use an unregistered function name here to prevent DocsV3 from overwriting
// the good .md generated from ToIntegerSurrogateTests with an incomplete .md generated from this test.
//
@FunctionName("_unregestered_to_integer_tests")
public class ToIntegerTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        unary(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public static void unary(List<TestCaseSupplier> suppliers) {
        booleanCase().booleans().expectedFromBoolean(b -> b ? 1 : 0).build(suppliers);
        doubleCase().doubles(Integer.MIN_VALUE, Integer.MAX_VALUE).expectedFromDouble(d -> safeToInt(Math.round(d))).build(suppliers);
        doubleCase().doubles(Double.NEGATIVE_INFINITY, Integer.MIN_VALUE - 1d)
            .doubles(Integer.MAX_VALUE + 1d, Double.POSITIVE_INFINITY)
            .expectNullAndWarningsFromDouble(
                d -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [integer] range")
            )
            .build(suppliers);
        intCase().ints().counterInts().expectedFromInt(i -> i).build(suppliers);
        longCase().longs(Integer.MIN_VALUE, Integer.MAX_VALUE).expectedFromLong(l -> (int) l).build(suppliers);
        longCase().longs(Long.MIN_VALUE, Integer.MIN_VALUE - 1L)
            .longs(Integer.MAX_VALUE + 1L, Long.MAX_VALUE)
            .expectNullAndWarningsFromLong(
                l -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [integer] range")
            )
            .build(suppliers);
        longCase().testCases(dateCases(0, Integer.MAX_VALUE)).expectedFromInstant(i -> (int) i.toEpochMilli()).build(suppliers);
        longCase().testCases(dateCases(Integer.MAX_VALUE + 1L, Long.MAX_VALUE)).expectNullAndWarnings(o -> {
            long millis = ((Instant) o).toEpochMilli();
            return List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + millis + "] out of [integer] range");
        }).build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(Integer.MAX_VALUE))
            .expectedFromBigInteger(BigInteger::intValue)
            .build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE), UNSIGNED_LONG_MAX)
            .expectNullAndWarningsFromBigInteger(
                ul -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [integer] range")
            )
            .build(suppliers);

        stringCase().strings()
            .expectNullAndWarningsFromString(
                s -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [" + s + "]")
            )
            .build(suppliers);
        for (DataType t : DataType.stringTypes()) {
            stringCase().ints().mapCases(t, o -> new BytesRef(o.toString())).expectedFromString(Integer::valueOf).build(suppliers);

            stringCase().doubles(Integer.MIN_VALUE, Integer.MAX_VALUE, false)
                .doubles(0, 0, true)
                .mapCases(t, o -> new BytesRef(o.toString()))
                .expectedFromString(s -> safeToInt(Math.round(Double.parseDouble(s))))
                .build(suppliers);

            stringCase().doubles(Double.NEGATIVE_INFINITY, Integer.MIN_VALUE - 1d, false)
                .doubles(Integer.MAX_VALUE + 1d, Double.POSITIVE_INFINITY, false)
                .mapCases(t, o -> new BytesRef(o.toString()))
                .expectNullAndWarningsFromString(
                    s -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number [" + s + "]")
                )
                .build(suppliers);
        }
    }

    private static UnaryTestCaseHelper intCase() {
        return TestCaseSupplier.unary().expectedOutputType(DataType.INTEGER).evaluatorToString("%0");
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

    private static UnaryTestCaseHelper longCase() {
        return helper("Long", "lng");
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return helper("UnsignedLong", "ul");
    }

    private static UnaryTestCaseHelper helper(String type, String paramName) {
        return TestCaseSupplier.unary()
            .expectedOutputType(DataType.INTEGER)
            .evaluatorToString("ToIntegerFrom" + type + "Evaluator[" + paramName + "=%0]");
    }

    private static List<TestCaseSupplier.TypedDataSupplier> dateCases(long min, long max) {
        List<TestCaseSupplier.TypedDataSupplier> dataSuppliers = new ArrayList<>(2);
        if (min == 0L) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-01T00:00:00Z>", () -> 0L, DataType.DATETIME));
        }
        if (max <= Integer.MAX_VALUE) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-25T20:31:23.647Z>", () -> 2147483647L, DataType.DATETIME));
        }
        dataSuppliers.add(
            new TestCaseSupplier.TypedDataSupplier("<date>", () -> ESTestCase.randomLongBetween(min, max), DataType.DATETIME)
        );
        return dataSuppliers;
    }

    public ToIntegerTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToInteger(source, args.get(0));
    }
}
