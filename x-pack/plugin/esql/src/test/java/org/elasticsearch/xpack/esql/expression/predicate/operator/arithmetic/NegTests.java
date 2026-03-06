/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.mockito.ArgumentMatchers.startsWith;

public class NegTests extends AbstractScalarFunctionTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        intCase().ints(Integer.MIN_VALUE + 1, Integer.MAX_VALUE).expectedFromInt(Math::negateExact).build(suppliers);
        // out of bounds integer
        intCase().ints(Integer.MIN_VALUE, Integer.MIN_VALUE)
            .expectNullAndWarnings(o -> List.of("Line 1:1: java.lang.ArithmeticException: integer overflow"))
            .build(suppliers);

        longCase().longs(Long.MIN_VALUE + 1, Long.MAX_VALUE).expectedFromLong(Math::negateExact).build(suppliers);
        // out of bounds long
        longCase().longs(Long.MIN_VALUE, Long.MIN_VALUE)
            .expectNullAndWarnings(o -> List.of("Line 1:1: java.lang.ArithmeticException: long overflow"))
            .build(suppliers);

        // TODO: Probably we don't want to allow negative zeros
        unary().expectedOutputType(DataType.DOUBLE)
            .evaluatorToString("NegDoublesEvaluator[v=%0]")
            .doubles()
            .expectedFromDouble(d -> -d)
            .build(suppliers);

        foldCase().expectedOutputType(DataType.TIME_DURATION).timeDuration().expectedFromDuration(Duration::negated).build(suppliers);
        foldCase().expectedOutputType(DataType.DATE_PERIOD).datePeriod().expectedFromPeriod(Period::negated).build(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    private static UnaryTestCaseHelper intCase() {
        return unary().expectedOutputType(DataType.INTEGER).evaluatorToString("NegIntsEvaluator[v=%0]");
    }

    private static UnaryTestCaseHelper longCase() {
        return unary().expectedOutputType(DataType.LONG).evaluatorToString("NegLongsEvaluator[v=%0]");
    }

    private static UnaryTestCaseHelper foldCase() {
        return unary().evaluatorToString(startsWith("LiteralsEvaluator[lit=")).withoutEvaluator();
    }

    public NegTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Neg(source, args.get(0));
    }

    public void testEdgeCases() {
        // Run the assertions for the current test cases type only to avoid running the same assertions multiple times.
        // TODO: These remaining cases should get rolled into generation functions for periods and durations
        DataType testCaseType = testCase.getData().get(0).type();
        if (testCaseType == DataType.DATE_PERIOD) {
            Period maxPeriod = Period.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
            Period negatedMaxPeriod = Period.of(-Integer.MAX_VALUE, -Integer.MAX_VALUE, -Integer.MAX_VALUE);
            assertEquals(negatedMaxPeriod, foldTemporalAmount(maxPeriod));

            Period minPeriod = Period.of(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
            VerificationException e = expectThrows(
                VerificationException.class,
                "Expected exception when negating minimal date period.",
                () -> foldTemporalAmount(minPeriod)
            );
            assertEquals(e.getMessage(), "arithmetic exception in expression []: [integer overflow]");
        } else if (testCaseType == DataType.TIME_DURATION) {
            Duration maxDuration = Duration.ofSeconds(Long.MAX_VALUE, 0);
            Duration negatedMaxDuration = Duration.ofSeconds(-Long.MAX_VALUE, 0);
            assertEquals(negatedMaxDuration, foldTemporalAmount(maxDuration));

            Duration minDuration = Duration.ofSeconds(Long.MIN_VALUE, 0);
            VerificationException e = expectThrows(
                VerificationException.class,
                "Expected exception when negating minimal time duration.",
                () -> foldTemporalAmount(minDuration)
            );
            assertEquals(
                e.getMessage(),
                "arithmetic exception in expression []: [Exceeds capacity of Duration: 9223372036854775808000000000]"
            );
        }
    }

    private Object foldTemporalAmount(Object val) {
        Neg neg = new Neg(Source.EMPTY, new Literal(Source.EMPTY, val, typeOf(val)));
        return neg.fold(FoldContext.small());
    }

    private static DataType typeOf(Object val) {
        if (val instanceof Integer) {
            return DataType.INTEGER;
        }
        if (val instanceof Long) {
            return DataType.LONG;
        }
        if (val instanceof Double) {
            return DataType.DOUBLE;
        }
        if (val instanceof Duration) {
            return DataType.TIME_DURATION;
        }
        if (val instanceof Period) {
            return DataType.DATE_PERIOD;
        }
        throw new UnsupportedOperationException("unsupported type [" + val.getClass() + "]");
    }
}
