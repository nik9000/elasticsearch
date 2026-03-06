/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class SpaceTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        helper().ints(0, 10).expectedFromInt(i -> new BytesRef(" ".repeat(i))).build(cases);
        helper().ints(-10, -1)
            .expectNullAndWarnings(
                o -> List.of("Line 1:1: java.lang.IllegalArgumentException: Number parameter cannot be negative, found [" + o + "]")
            )
            .foldingException(IllegalArgumentException.class, o -> "Number parameter cannot be negative, found [" + o + "]")
            .build(cases);
        int max = (int) MB.toBytes(1);
        helper().ints(max + 1, max + 10)
            .expectNullAndWarnings(
                o -> List.of(
                    "Line 1:1: java.lang.IllegalArgumentException: Creating strings longer than [" + max + "] bytes is not supported"
                )
            )
            .foldingException(IllegalArgumentException.class, o -> "Creating strings longer than [" + max + "] bytes is not supported")
            .build(cases);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    private static UnaryTestCaseHelper helper() {
        return unary().expectedOutputType(DataType.KEYWORD).evaluatorToString("SpaceEvaluator[number=%0]");
    }

    public SpaceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Space(source, args.get(0));
    }
}
