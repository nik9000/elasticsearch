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
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;

public class ToGeoPointTests extends AbstractFunctionTestCase {
    public ToGeoPointTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        final String attribute = "Attribute[channel=0]";
        final Function<String, String> evaluatorName = s -> "ToGeoPoint" + s + "Evaluator[field=" + attribute + "]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryGeoPoint(suppliers, attribute, EsqlDataTypes.GEO_POINT, v -> v, List.of());
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("FromLong"),
            EsqlDataTypes.GEO_POINT,
            GEO::longAsWKB,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        // random strings that don't look like a geo point
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName.apply("FromString"),
            EsqlDataTypes.GEO_POINT,
            bytesRef -> null,
            bytesRef -> {
                var exception = expectThrows(Exception.class, () -> GEO.stringAsWKB(bytesRef.utf8ToString()));
                return List.of(
                    "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                    "Line -1:-1: " + exception
                );
            }
        );
        // strings that are geo point representations
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("FromString"),
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "<geo point as string>",
                    () -> new BytesRef(GEO.pointAsString(GeometryTestUtils.randomPoint())),
                    DataTypes.KEYWORD
                )
            ),
            EsqlDataTypes.GEO_POINT,
            bytesRef -> GEO.stringAsWKB(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToGeoPoint(source, args.get(0));
    }
}
