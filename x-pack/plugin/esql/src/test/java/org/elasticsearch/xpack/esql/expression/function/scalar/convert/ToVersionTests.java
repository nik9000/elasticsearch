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
import org.elasticsearch.xpack.versionfield.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToVersionTests extends AbstractScalarFunctionTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Converting a version to a version doesn't change anything. Everything should succeed.
        helper().versions().evaluatorToString("%0").expected(v -> v).build(suppliers);

        // None of the random strings ever look like versions so they should all become "invalid" versions:
        // https://github.com/elastic/elasticsearch/issues/98989
        // TODO should this return null with warnings? they aren't version shaped at all.
        stringCase().strings().expectedFromString(s -> new Version(s).toBytesRef()).build(suppliers);

        // But strings that are shaped like versions do parse to valid versions
        for (DataType inputType : DataType.stringTypes()) {
            stringCase().strings("1.2").expected(v -> new Version("1.2").toBytesRef()).build(suppliers);
            stringCase().strings("1.2.3").expected(v -> new Version("1.2.3").toBytesRef()).build(suppliers);
            stringCase().strings("1.2.3-SNAPSHOT").expected(v -> new Version("1.2.3-SNAPSHOT").toBytesRef()).build(suppliers);
            stringCase().versions().mapCases(inputType, o -> {
                BytesRef ref = (BytesRef) o;
                String strWithTrailing = new Version(ref) + " ";
                return new BytesRef(strWithTrailing);
            }).expected(bytesRef -> new Version((BytesRef) bytesRef).toBytesRef()).build(suppliers);
        }

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper helper() {
        return TestCaseSupplier.unary().expectedOutputType(DataType.VERSION);
    }

    private static UnaryTestCaseHelper stringCase() {
        return helper().evaluatorToString("ToVersionFromStringEvaluator[asString=%0]");
    }

    public ToVersionTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToVersion(source, args.get(0));
    }
}
