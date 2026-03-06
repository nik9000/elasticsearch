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

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;

public class ReverseTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        UnaryTestCaseHelper base = unary().expectedOutputType(DataType.KEYWORD).evaluatorToString("ReverseEvaluator[val=%0]");
        base.strings().expectedFromString(v -> new BytesRef(Reverse.reverseStringWithUnicodeCharacters(v))).build(suppliers);
        base.strings("empty string", () -> "").expectedFromString(s -> matchesBytesRef("")).build(suppliers);
        base.strings("single ascii character", () -> "a").expectedFromString(s -> matchesBytesRef("a")).build(suppliers);
        base.strings("ascii string", () -> "clump").expectedFromString(s -> matchesBytesRef("pmulc")).build(suppliers);
        base.strings("3 bytes, 1 code point", () -> "☕").expectedFromString(s -> matchesBytesRef("☕")).build(suppliers);
        base.strings("6 bytes, 2 code points", () -> "❗️").expectedFromString(s -> matchesBytesRef("❗\uFE0F")).build(suppliers);
        base.strings("Japanese", () -> "日本語").expectedFromString(s -> matchesBytesRef("語本日")).build(suppliers);
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    public ReverseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Reverse(source, args.get(0));
    }
}
