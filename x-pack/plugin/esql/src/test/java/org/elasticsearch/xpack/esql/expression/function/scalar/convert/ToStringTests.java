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
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ReadableMatchers;
import org.elasticsearch.xpack.esql.WriteableExponentialHistogram;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.UnaryTestCaseHelper;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractConfigurationFunctionTestCase;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ReadableMatchers.matchesBytesRef;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.TEST_SOURCE;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.appliesTo;
import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.unary;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ToStringTests extends AbstractConfigurationFunctionTestCase {
    public ToStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        unary(
            suppliers,
            "ToStringFromDatetimeEvaluator[datetime=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            TestCaseSupplier.dateCases(),
            DataType.KEYWORD,
            i -> matchesBytesRef(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(DateUtils.toLongMillis((Instant) i))),
            List.of()
        );
        unary(
            suppliers,
            "ToStringFromDateNanosEvaluator[datetime=" + read + ", formatter=format[strict_date_optional_time_nanos] locale[]]",
            TestCaseSupplier.dateNanosCases(),
            DataType.KEYWORD,
            i -> matchesBytesRef(DateFieldMapper.DEFAULT_DATE_TIME_NANOS_FORMATTER.formatNanos(DateUtils.toLong((Instant) i))),
            List.of()
        );
        // Set the config to UTC for date cases
        suppliers = TestCaseSupplier.mapTestCases(
            suppliers,
            tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
        );

        intCase().ints().expectedFromInt(i -> matchesBytesRef(Integer.toString(i))).build(suppliers);
        longCase().longs().expectedFromLong(l -> matchesBytesRef(Long.toString(l))).build(suppliers);
        helper("UnsignedLong", "lng").unsignedLongs().expectedFromBigInteger(ul -> matchesBytesRef(ul.toString())).build(suppliers);
        doubleCase().doubles(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
            .expectedFromDouble(d -> matchesBytesRef(Double.toString(d)))
            .build(suppliers);
        helper("Float", "flt").denseVectors()
            .expectedFromFloatList(l -> l.stream().map(f -> new BytesRef(f.toString())).toList())
            .build(suppliers);
        booleanCase().booleans().expectedFromBoolean(b -> matchesBytesRef(b.toString())).build(suppliers);
        TestCaseSupplier.forUnaryGeoPoint(
            suppliers,
            "ToStringFromGeoPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianPoint(
            suppliers,
            "ToStringFromCartesianPointEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryGeoShape(
            suppliers,
            "ToStringFromGeoShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(GEO.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryCartesianShape(
            suppliers,
            "ToStringFromCartesianShapeEvaluator[wkb=" + read + "]",
            DataType.KEYWORD,
            wkb -> matchesBytesRef(CARTESIAN.wkbToWkt(wkb)),
            List.of()
        );
        TestCaseSupplier.forUnaryIp(
            suppliers,
            "ToStringFromIPEvaluator[ip=" + read + "]",
            DataType.KEYWORD,
            ip -> matchesBytesRef(DocValueFormat.IP.format(ip)),
            List.of()
        );

        helper("%0").strings().expectedFromString(ReadableMatchers::matchesBytesRef).build(suppliers);
        TestCaseSupplier.forUnaryVersion(
            suppliers,
            "ToStringFromVersionEvaluator[version=" + read + "]",
            DataType.KEYWORD,
            v -> matchesBytesRef(v.toString()),
            List.of()
        );
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            helper("ToStringFromGeoGridEvaluator[gridId=%0, dataType=" + gridType + "]").geoGrids(gridType)
                .expected(v -> matchesBytesRef(EsqlDataTypeConverter.geoGridToString((long) v, gridType)))
                .build(suppliers);
        }
        TestCaseSupplier.forUnaryAggregateMetricDouble(
            suppliers,
            "ToStringFromAggregateMetricDoubleEvaluator[field=" + read + "]",
            DataType.KEYWORD,
            agg -> matchesBytesRef(EsqlDataTypeConverter.aggregateMetricDoubleLiteralToString(agg)),
            List.of()
        );
        TestCaseSupplier.forUnaryExponentialHistogram(
            suppliers,
            "ToStringFromExponentialHistogramEvaluator[histogram=" + read + "]",
            DataType.KEYWORD,
            eh -> matchesBytesRef(EsqlDataTypeConverter.exponentialHistogramToString(eh)),
            List.of()
        );
        ExponentialHistogram largeExponentialHistogram = buildDummyHistogram(100_001);
        suppliers.add(
            new TestCaseSupplier(
                "<too many exponential histogram buckets>",
                List.of(DataType.EXPONENTIAL_HISTOGRAM),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            new WriteableExponentialHistogram(largeExponentialHistogram),
                            DataType.EXPONENTIAL_HISTOGRAM,
                            "large exponential histogram"
                        )
                    ),
                    "ToStringFromExponentialHistogramEvaluator[histogram=" + read + "]",
                    DataType.KEYWORD,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning(
                        "Line 1:1: java.lang.IllegalArgumentException: Exponential histogram is too big to be converted to a string"
                    )
            )
        );

        TestCaseSupplier.forUnaryHistogram(
            suppliers,
            "ToStringFromHistogramEvaluator[histogram=" + read + "]",
            DataType.KEYWORD,
            h -> matchesBytesRef(EsqlDataTypeConverter.histogramToString(h)),
            List.of()
        );
        // doesn't matter if it's not an actual encoded histogram, as we should never get to the decoding step
        BytesRef largeTDigest = new BytesRef(new byte[3 * 1024 * 1024]);
        suppliers.add(
            new TestCaseSupplier(
                "<too large histograms>",
                List.of(DataType.HISTOGRAM),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(largeTDigest, DataType.HISTOGRAM, "large histogram")),
                    "ToStringFromHistogramEvaluator[histogram=" + read + "]",
                    DataType.KEYWORD,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: Histogram length is greater than 2MB")
            )
        );

        List<TestCaseSupplier> fixedTimezoneSuppliers = new ArrayList<>();
        dateCase("Datetime", "strict_date_optional_time").dates()
            .expectedFromInstant(date -> matchesBytesRef(EsqlDataTypeConverter.dateTimeToString(DateUtils.toLongMillis(date))))
            .build(fixedTimezoneSuppliers);
        dateCase("DateNanos", "strict_date_optional_time_nanos").dateNanos()
            .expectedFromInstant(date -> matchesBytesRef(EsqlDataTypeConverter.nanoTimeToString(DateUtils.toLong(date))))
            .build(fixedTimezoneSuppliers);
        TestCaseSupplier.forUnaryDateRange(
            fixedTimezoneSuppliers,
            "ToStringFromDateRangeEvaluator[field=" + read + ", formatter=format[strict_date_optional_time] locale[]]",
            DataType.KEYWORD,
            dr -> matchesBytesRef(EsqlDataTypeConverter.dateRangeToString(dr)),
            List.of()
        );
        suppliers.addAll(
            TestCaseSupplier.mapTestCases(
                fixedTimezoneSuppliers,
                tc -> tc.withConfiguration(TEST_SOURCE, configurationForTimezone(ZoneOffset.UTC))
            )
        );

        suppliers.addAll(casesForDate("2020-02-03T10:12:14Z", "Z", "2020-02-03T10:12:14.000Z"));
        suppliers.addAll(casesForDate("2020-02-03T10:12:14+01:00", "Europe/Madrid", "2020-02-03T10:12:14.000+01:00"));
        suppliers.addAll(casesForDate("2020-06-30T10:12:14+02:00", "Europe/Madrid", "2020-06-30T10:12:14.000+02:00"));

        FunctionAppliesTo histogramAppliesTo = appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", true);
        suppliers = TestCaseSupplier.mapTestCases(suppliers, tc -> tc.withData(tc.getData().stream().map(typedData -> {
            DataType type = typedData.type();
            if (type == DataType.HISTOGRAM || type == DataType.EXPONENTIAL_HISTOGRAM) {
                return typedData.withAppliesTo(histogramAppliesTo);
            }
            return typedData;
        }).toList()));
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static UnaryTestCaseHelper booleanCase() {
        return helper("Boolean", "bool");
    }

    private static UnaryTestCaseHelper intCase() {
        return helper("Int", "integer");
    }

    private static UnaryTestCaseHelper doubleCase() {
        return helper("Double", "dbl");
    }

    private static UnaryTestCaseHelper longCase() {
        return helper("Long", "lng");
    }

    private static UnaryTestCaseHelper dateCase(String type, String format) {
        return helper("ToStringFrom" + type + "Evaluator[datetime=%0, formatter=format[" + format + "] locale[]]");
    }

    private static UnaryTestCaseHelper helper(String type, String param) {
        return helper("ToStringFrom" + type + "Evaluator[" + param + "=%0]");
    }

    private static UnaryTestCaseHelper helper(String toString) {
        return TestCaseSupplier.unary().expectedOutputType(DataType.KEYWORD).evaluatorToString(toString);
    }

    private static List<TestCaseSupplier> casesForDate(String date, String zoneIdString, String expectedString) {
        ZoneId zoneId = ZoneId.of(zoneIdString);
        long dateAsLong = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(date);
        long dateAsNanos = DateUtils.toNanoSeconds(dateAsLong);

        var suppliers = new ArrayList<TestCaseSupplier>();
        suppliers.add(
            new TestCaseSupplier(
                "millis: " + date + ", " + zoneIdString + ", " + expectedString,
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateAsLong, DataType.DATETIME, "date")),
                    "ToStringFromDatetimeEvaluator[datetime=Attribute[channel=0], "
                        + "formatter=format[strict_date_optional_time] locale[]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "nanos: " + date + ", " + zoneIdString + ", " + expectedString,
                List.of(DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(dateAsNanos, DataType.DATE_NANOS, "date")),
                    "ToStringFromDateNanosEvaluator[datetime=Attribute[channel=0], "
                        + "formatter=format[strict_date_optional_time_nanos] locale[]]",
                    DataType.KEYWORD,
                    matchesBytesRef(expectedString)
                ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
            )
        );

        if (DataType.DATE_RANGE.supportedVersion().supportedLocally()) {
            suppliers.add(
                new TestCaseSupplier(
                    "date_range: " + date + ", " + zoneIdString + ", " + expectedString,
                    List.of(DataType.DATE_RANGE),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(
                                new LongRangeBlockBuilder.LongRange(dateAsLong, dateAsLong),
                                DataType.DATE_RANGE,
                                "date"
                            )
                        ),
                        "ToStringFromDateRangeEvaluator[field=Attribute[channel=0], "
                            + "formatter=format[strict_date_optional_time] locale[]]",
                        DataType.KEYWORD,
                        matchesBytesRef(expectedString + ".." + expectedString)
                    ).withConfiguration(TEST_SOURCE, configurationForTimezone(zoneId))
                )
            );
        }

        return suppliers;
    }

    private static ExponentialHistogram buildDummyHistogram(int bucketCount) {
        final ExponentialHistogram tooLarge;
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder((byte) 0, ExponentialHistogramCircuitBreaker.noop())) {
            for (int i = 0; i < bucketCount; i++) {
                // indices must be unique to count as distinct buckets
                if (i % 2 == 0) {
                    builder.setPositiveBucket(i, 1L);
                } else {
                    builder.setNegativeBucket(i, 1L);
                }
            }
            tooLarge = builder.build();
        }
        return tooLarge;
    }

    @Override
    protected Expression buildWithConfiguration(Source source, List<Expression> args, Configuration configuration) {
        return new ToString(source, args.get(0), configuration);
    }
}
