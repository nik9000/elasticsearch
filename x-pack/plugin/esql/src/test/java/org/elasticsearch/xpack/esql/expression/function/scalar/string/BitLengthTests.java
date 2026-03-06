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

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class BitLengthTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        UnaryTestCaseHelper base = unary().expectedOutputType(DataType.INTEGER).evaluatorToString("BitLengthEvaluator[val=%0]");
        base.strings().expected(v -> ((BytesRef) v).length * Byte.SIZE).build(suppliers);
        base.strings("empty string", () -> "").expectedFromString(s -> 0).build(suppliers);
        base.strings("single ascii character", () -> "a").expectedFromString(s -> 8).build(suppliers);
        base.strings("ascii string", () -> "clump").expectedFromString(s -> 40).build(suppliers);
        base.strings("3 bytes, 1 code point", () -> "☕").expectedFromString(s -> 24).build(suppliers);
        base.strings("6 bytes, 2 code points", () -> "❗️").expectedFromString(s -> 48).build(suppliers);
        base.strings("100 random alpha", () -> randomAlphaOfLength(100)).expectedFromString(s -> 800).build(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    public BitLengthTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new BitLength(source, args.get(0));
    }
}
