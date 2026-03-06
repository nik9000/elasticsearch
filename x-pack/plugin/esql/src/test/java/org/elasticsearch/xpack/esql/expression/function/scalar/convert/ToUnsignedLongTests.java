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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.UNSIGNED_LONG_MAX_AS_DOUBLE;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ToUnsignedLongTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        helper().unsignedLongs().expectedFromBigInteger(n -> n).evaluatorToString("%0").build(suppliers);

        helper().booleans()
            .expected(b -> (Boolean) b ? BigInteger.ONE : BigInteger.ZERO)
            .evaluatorToString("ToUnsignedLongFromBooleanEvaluator[bool=%0]")
            .build(suppliers);

        // datetimes
        longCase().dates().expectedFromInstant(instant -> BigInteger.valueOf(instant.toEpochMilli())).build(suppliers);

        // random strings that don't look like an unsigned_long
        stringCase().strings().expectNullAndWarningsFromString(s -> {
            // BigDecimal, used to parse unsigned_longs will throw NFEs with different messages depending on empty string, first
            // non-number character after a number-looking like prefix, or string starting with "e", maybe others -- safer to take
            // this shortcut here.
            Exception e = expectThrows(NumberFormatException.class, () -> new BigDecimal(s));
            return List.of("Line 1:1: java.lang.NumberFormatException: " + e.getMessage());
        }).build(suppliers);

        // from doubles within unsigned_long's range
        doubleCase().doubles(0d, UNSIGNED_LONG_MAX_AS_DOUBLE)
            .expectedFromDouble(d -> BigDecimal.valueOf(d).toBigInteger()) // note: not: new BigDecimal(d).toBigInteger
            .build(suppliers);
        // from doubles outside unsigned_long's range, negative
        doubleWarningCase().doubles(Double.NEGATIVE_INFINITY, -1d).build(suppliers);
        // from doubles outside Long's range, positive
        doubleWarningCase().doubles(UNSIGNED_LONG_MAX_AS_DOUBLE + 10e5, Double.POSITIVE_INFINITY).build(suppliers);

        // from long within unsigned_long's range
        longCase().longs(0L, Long.MAX_VALUE).expectedFromLong(BigInteger::valueOf).build(suppliers);
        // from long outside unsigned_long's range
        longWarningCase().longs(Long.MIN_VALUE, -1L).build(suppliers);

        // from int within unsigned_long's range
        intCase().ints(0, Integer.MAX_VALUE).expectedFromInt(BigInteger::valueOf).build(suppliers);
        // from int outside unsigned_long's range
        intWarningCase().ints(Integer.MIN_VALUE, -1).build(suppliers);

        // strings of random unsigned_longs
        stringCase().testCases(
            TestCaseSupplier.ulongCases(BigInteger.ZERO, UNSIGNED_LONG_MAX, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList()
        ).expectedFromBytesRef(bytesRef -> safeToUnsignedLong(bytesRef.utf8ToString())).build(suppliers);
        // strings of random doubles within unsigned_long's range
        stringCase().testCases(
            TestCaseSupplier.doubleCases(0, UNSIGNED_LONG_MAX_AS_DOUBLE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList()
        ).expectedFromBytesRef(bytesRef -> safeToUnsignedLong(bytesRef.utf8ToString())).build(suppliers);
        // strings of random doubles outside unsigned_long's range, negative
        stringDoubleWarningCase().testCases(
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, -1d, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList()
        ).build(suppliers);
        // strings of random doubles outside Integer's range, positive
        stringDoubleWarningCase().testCases(
            TestCaseSupplier.doubleCases(UNSIGNED_LONG_MAX_AS_DOUBLE + 10e5, Double.POSITIVE_INFINITY, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList()
        ).build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper helper() {
        return unary().expectedOutputType(DataType.UNSIGNED_LONG);
    }

    private static UnaryTestCaseHelper longCase() {
        return helper().evaluatorToString("ToUnsignedLongFromLongEvaluator[lng=%0]");
    }

    private static UnaryTestCaseHelper longWarningCase() {
        return longCase().expectNullAndWarnings(
            o -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + o + "] out of [unsigned_long] range")
        );
    }

    private static UnaryTestCaseHelper intCase() {
        return helper().evaluatorToString("ToUnsignedLongFromIntEvaluator[i=%0]");
    }

    private static UnaryTestCaseHelper intWarningCase() {
        return intCase().expectNullAndWarnings(
            o -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + o + "] out of [unsigned_long] range")
        );
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper().evaluatorToString("ToUnsignedLongFromDoubleEvaluator[dbl=%0]");
    }

    private static UnaryTestCaseHelper doubleWarningCase() {
        return doubleCase().expectNullAndWarningsFromDouble(
            d -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [unsigned_long] range")
        );
    }

    private static UnaryTestCaseHelper stringCase() {
        return helper().evaluatorToString("ToUnsignedLongFromStringEvaluator[in=%0]");
    }

    private static UnaryTestCaseHelper stringDoubleWarningCase() {
        return stringCase().expectNullAndWarningsFromString(
            s -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + s + "] out of [unsigned_long] range")
        );
    }

    public ToUnsignedLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToUnsignedLong(source, args.get(0));
    }
}
