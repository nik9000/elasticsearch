/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesDateNanos;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetimeTests.stringWarnings;

public class ToDateNanosTests extends AbstractConfigurationFunctionTestCase {
    public ToDateNanosTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        if (EsqlCapabilities.Cap.TO_DATE_NANOS.isEnabled() == false) {
            return List.of();
        }
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        unary().expectedOutputType(DataType.DATE_NANOS)
            .dateNanos()
            .evaluatorToString("%0")
            .expectedFromInstant(DateUtils::toLong)
            .build(suppliers);

        helper("Datetime").dates(0, DateUtils.MAX_NANOSECOND_INSTANT.toEpochMilli())
            .expectedFromInstant(i -> DateUtils.toNanoSeconds(i.toEpochMilli()))
            .build(suppliers);

        doubleCase().doubles(Double.NEGATIVE_INFINITY, -Double.MIN_VALUE)
            .expectNullAndWarningsFromDouble(
                d -> List.of(
                    "Line 1:1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
                )
            )
            .build(suppliers);
        doubleCase().doubles(9.223372036854777E18, Double.POSITIVE_INFINITY)
            .expectNullAndWarningsFromDouble(
                d -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range")
            )
            .build(suppliers);

        longCase().longs(0, Long.MAX_VALUE).expectedFromLong(l -> l).build(suppliers);
        longCase().longs(Long.MIN_VALUE, -1L)
            .expectNullAndWarnings(
                l -> List.of(
                    "Line 1:1: java.lang.IllegalArgumentException: Nanosecond dates before 1970-01-01T00:00:00.000Z are not supported."
                )
            )
            .build(suppliers);

        stringCase().strings().expectNullAndWarningsFromString(stringWarnings("strict_date_optional_time_nanos")).build(suppliers);

        unsignedLongCase().unsignedLongs(BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE))
            .expectedFromBigInteger(BigInteger::longValueExact)
            .build(suppliers);
        unsignedLongCase().unsignedLongs(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TWO), UNSIGNED_LONG_MAX)
            .expectNullAndWarningsFromBigInteger(
                bi -> List.of("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + bi + "] out of [long] range")
            )
            .build(suppliers);
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        fixedCases("2020-05-07T02:03:04.123456789Z", "America/New_York", "2020-05-07T02:03:04.123456789Z").build(suppliers);
        fixedCases("2020-05-07T02:03:04.123456789", "America/New_York", "2020-05-07T02:03:04.123456789-04:00").build(suppliers);
        fixedCases("2010-12-31", "Z", "2010-12-31T00:00:00.000000000Z").build(suppliers);
        fixedCases("2010-12-31", "America/New_York", "2010-12-31T00:00:00.000000000-05:00").build(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double");
    }

    private static UnaryTestCaseHelper longCase() {
        return helper("Long");
    }

    private static UnaryTestCaseHelper stringCase() {
        return unary().expectedOutputType(DataType.DATE_NANOS)
            .evaluatorToString("ToDateNanosFromStringEvaluator[in=%0, formatter=format[strict_date_optional_time_nanos] locale[]]");
    }

    private static UnaryTestCaseHelper unsignedLongCase() {
        return unary().expectedOutputType(DataType.DATE_NANOS).evaluatorToString("ToLongFromUnsignedLongEvaluator[ul=%0]");
    }

    private static UnaryTestCaseHelper helper(String type) {
        return unary().expectedOutputType(DataType.DATE_NANOS).evaluatorToString("ToDateNanosFrom" + type + "Evaluator[in=%0]");
    }

    private static UnaryTestCaseHelper fixedCases(String dateString, String zoneIdString, String expectedDate) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        return stringCase().name(s -> dateString + "::" + s.getFirst().type().typeName() + ", " + zoneIdString + ", " + expectedDate)
            .strings("date", () -> dateString)
            .expected(o -> matchesDateNanos(expectedDate))
            .configuration(() -> configurationForTimezone(zoneId));
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToDateNanos(source, args.get(0), configuration);
    }
}
