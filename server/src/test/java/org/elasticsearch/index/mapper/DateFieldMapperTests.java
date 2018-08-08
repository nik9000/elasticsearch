/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DateFieldMapperTests extends AbstractFieldMapperTestCase {
    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());
        assertEquals(null, mapper.relocatedFilter());
        assertEquals(emptyMap(), mapper.sourceRelocationHandlers());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("index", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("doc_values", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
    }

    public void testStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("store", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-11")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1457654400000L, storedField.numericValue().longValue());
    }

    public void testIgnoreMalformed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "2016-03-99")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("Cannot parse \"2016-03-99\""));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("ignore_malformed", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", ":1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, doc.rootDoc().getValues("_ignored"));
    }

    public void testChangeFormat() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("format", "epoch_second").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 1457654400)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1457654400000L, pointField.numericValue().longValue());
    }

    public void testFloatEpochFormat() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date")
                .field("format", "epoch_millis").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        double epochFloatMillisFromEpoch = (randomDouble() * 2 - 1) * 1000000;
        String epochFloatValue = String.format(Locale.US, "%f", epochFloatMillisFromEpoch);

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", epochFloatValue)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals((long)epochFloatMillisFromEpoch, pointField.numericValue().longValue());
    }

    public void testChangeLocale() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "date").field("locale", "fr").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 1457654400)
                        .endObject()),
                XContentType.JSON));
    }

    public void testNullValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("null_value", "2016-03-11")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .nullField("field")
                        .endObject()),
                XContentType.JSON));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(8, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1457654400000L, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1457654400000L, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNullConfigValuesFail() throws MapperParsingException, IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("format", (String) null)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        Exception e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("[format] must not have a [null] value", e.getMessage());
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "date")
            .field("format", "epoch_second").endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    /**
     * Test that time zones are correctly parsed by the {@link DateFieldMapper}.
     * There is a known bug with Joda 2.9.4 reported in https://github.com/JodaOrg/joda-time/issues/373.
     */
    public void testTimeZoneParsing() throws Exception {
        final String timeZonePattern = "yyyy-MM-dd" + randomFrom("ZZZ", "[ZZZ]", "'['ZZZ']'");

        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "date")
                            .field("format", timeZonePattern)
                        .endObject()
                    .endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping, mapper.mappingSource().toString());

        final DateTimeZone randomTimeZone = randomBoolean() ? DateTimeZone.forID(randomFrom("UTC", "CET")) : randomDateTimeZone();
        final DateTime randomDate = new DateTime(2016, 03, 11, 0, 0, 0, randomTimeZone);

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("field", DateTimeFormat.forPattern(timeZonePattern).print(randomDate))
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals(randomDate.withZone(DateTimeZone.UTC).getMillis(), fields[0].numericValue().longValue());
    }

    public void testMergeDate() throws IOException {
        String initMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("movie")
                    .startObject("properties")
                        .startObject("release_date")
                            .field("type", "date")
                            .field("relocate_to", "none")
                        .endObject()
                    .endObject()
                .endObject().endObject());
        indexService.mapperService().merge("movie", new CompressedXContent(initMapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(indexService.mapperService().fullName("release_date"), notNullValue());
        assertFalse(indexService.mapperService().fullName("release_date").stored());

        String updateFormatMapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("movie")
                    .startObject("properties")
                        .startObject("release_date")
                            .field("type", "date")
                            .field("format", "yyyy/MM/dd")
                        .endObject()
                    .endObject()
                .endObject().endObject());

        Exception formatException = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("movie", new CompressedXContent(updateFormatMapping),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(formatException.getMessage(), containsString("[mapper [release_date] has different [format] values]"));

        String startRelocating = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("movie")
                    .startObject("properties")
                        .startObject("release_date")
                            .field("type", "date")
                            .field("relocate_to", "doc_values")
                        .endObject()
                    .endObject()
                .endObject().endObject());
        Exception relocateToException = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("movie", new CompressedXContent(startRelocating),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals(
                "mapper [release_date] attempted to change [relocate_to] from [none] to "
                + "[doc_values] but [relocate_to] cannot be changed",
                relocateToException.getMessage());

        String okUpdate = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("movie")
                    .startObject("properties")
                        .startObject("release_date")
                            .field("type", "date")
                            .field("ignore_malformed", true)
                        .endObject()
                    .endObject()
                .endObject().endObject());
        indexService.mapperService().merge("movie", new CompressedXContent(okUpdate),
            MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testMergeText() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("date")
                            .field("type", "date")
                        .endObject()
                    .endObject()
                .endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().parse("_doc", new CompressedXContent(mapping), false);

        String mappingUpdate = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("date")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject().endObject());
        DocumentMapper update = indexService.mapperService().parse("_doc", new CompressedXContent(mappingUpdate), false);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapper.merge(update.mapping()));
        assertEquals("mapper [date] of different type, current_type [date], merged_type [text]", e.getMessage());
    }

    public void testInvalidRelocateTo() throws IOException {
        String invalidRelocateTo = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("movie")
                .startObject("properties")
                    .startObject("release_date")
                        .field("type", "date")
                        .field("relocate_to", "cats!")
                    .endObject()
                .endObject().endObject().endObject());
        Exception e = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("movie", new CompressedXContent(invalidRelocateTo),
                    MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals(
                "[relocate_to] must be one of [doc_values, none]",
                e.getCause().getMessage());
    }

    public void testRelocateToInsideMultifield() throws IOException {
        String nestedRelocateTo = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("movie")
                .startObject("properties")
                    .startObject("release_date")
                        .field("type", "text")
                        .startObject("fields")
                            .startObject("date")
                                .field("type", "date")
                                .field("format", "yyyy/MM/dd")
                                .field("relocate_to", "doc_values")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject());
        Exception e = expectThrows(MapperParsingException.class,
                () -> indexService.mapperService().merge("movie", new CompressedXContent(nestedRelocateTo),
                    MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals(
                "[release_date.date] sets [relocate_to] but that is not supported inside multifields",
                e.getCause().getMessage());
    }

    public void testRelocateToDocValuesWithDateFormats() throws IOException {
        relocateToDocValuesDateFormatGood(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format());
        relocateToDocValuesDateFormatGood("strict_date_optional_time");
        relocateToDocValuesDateFormatGood("epoch_millis");
        relocateToDocValuesDateFormatGood("yyyy.MM.dd'T'HH:mm:ss:SSSZ");
        relocateToDocValuesDateFormatGood("yyyy.MM.dd'T'hh:mm:ss:SSSZaa"); // hour of half day with AM/PM
        relocateToDocValuesDateFormatGood("YYYYG.MM.dd'T'HH:mm:ss:SSSZ");  // year of era with era
        relocateToDocValuesDateFormatGood("yyyy.DD'T'HH:mm:ss:SSSZ");      // day of year without month

        // Formats that omit some fields all fail
        relocateToDocValuesDateFormatBad("MM.dd'T'HH:mm:ss:SSSZ",   "year",   "2000-04-05T13:08:09.010Z", "1970-04-05T13:08:09.010Z");
        relocateToDocValuesDateFormatBad("yyyy.dd'T'HH:mm:ss:SSSZ", "month",  "1970-02-03T04:05:06.007Z", "1970-01-03T04:05:06.007Z");
        relocateToDocValuesDateFormatBad("yyyy.MM'T'HH:mm:ss:SSSZ", "day",    "1970-02-03T04:05:06.007Z", "1970-02-01T04:05:06.007Z");
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'mm:ss:SSSZ", "hour",   "1970-02-03T04:05:06.007Z", "1970-02-03T00:05:06.007Z");
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'HH:ss:SSSZ", "minute", "1970-02-03T04:05:06.007Z", "1970-02-03T04:00:06.007Z");
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'HH:mm:SSSZ", "second", "1970-02-03T04:05:06.007Z", "1970-02-03T04:05:00.007Z");
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'HH:mm:ssZ",  "milli",  "1970-02-03T04:05:06.007Z", "1970-02-03T04:05:06.000Z");

        // Some more "fun" formatting mistakes
        // year of era instead of regular year
        relocateToDocValuesDateFormatBad("YYYY.MM.dd'T'HH:mm:ss:SSSZ", "year", "-1000-08-09T10:11:12.865Z", "1001-08-09T10:11:12.865Z");
        // minutes instead of months
        relocateToDocValuesDateFormatBad("yyyy.mm.dd'T'HH:mm:ss:SSSZ", "month", "1970-02-03T04:05:06.007Z", "1970-01-03T04:05:06.007Z");
        // hour of half day without AM/PM
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'KK:mm:ss:SSSZ", "hour", "2000-04-05T13:08:09.010Z", "2000-04-05T01:08:09.010Z");
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'hh:mm:ss:SSSZ", "hour", "2000-04-05T13:08:09.010Z", "2000-04-05T01:08:09.010Z");
        // month instead of minute
        relocateToDocValuesDateFormatBad("yyyy.MM.dd'T'KK:MM:ss:SSSZ", "minute", "1970-02-03T04:05:06.007Z", "1970-02-03T04:00:06.007Z");
    }

    private void relocateToDocValuesDateFormatGood(String dateFormat) throws IOException {
        indexService.mapperService().parse("_doc", relocateToDocValueMapping(b -> b.field("format", dateFormat)), false);
    }

    private void relocateToDocValuesDateFormatBad(String dateFormat, String badField, String expected, String actual) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> relocateToDocValuesDateFormatGood(dateFormat));
        assertEquals("[date] sets [relocate_to] to [doc_values] but [format] to ["
                + dateFormat + "] which is invalid because it doesn't preserve the ["
                + badField + "] of the date. Expected [" + expected + "] but was ["
                + actual + "]", e.getMessage());
    }

    public void testRelocateToDocValuesWithoutDocValues() throws IOException {
        Exception e = expectThrows(MapperParsingException.class, () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(b -> b.field("doc_values", false)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [date] sets [relocate_to] to "
                + "[doc_values] which requires doc_values to be enabled", e.getMessage());
    }

    public void testRelocateToDocValuesWithIgnoreMalformed() throws IOException {
        Exception e = expectThrows(MapperParsingException.class, () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(b -> b.field("ignore_malformed", true)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [date] sets [relocate_to] to [doc_values] "
                + "and [ignore_malformed] to [true] which is not allowed because it'd cause "
                + "malformed dates to vanish", e.getMessage());
    }

    public void testAsThoughRelocated() throws IOException {
        DateTimeFormatter format = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping(b -> {}));
        Instant instant = new Instant(randomInt());
        asThoughRelocatedTestCase(docMapper, "{\"date\":\"2018-01-02T00:11:22.000Z\"}", "{\"date\":\"2018-01-02T00:11:22.000Z\"}");
        asThoughRelocatedTestCase(docMapper,
                "{\"date\":\"" + format.print(instant) + "\"}",
                "{\"date\":\"" + instant.getMillis() + "\"}");
        asThoughRelocatedTestCase(docMapper,
                "{\"date\":\"" + format.print(instant) + "\"}",
                "{\"date\":\"" + format.print(instant) + "\"}");
        asThoughRelocatedTestCase(docMapper, "{}", "{\"date\":null}");
    }

    public void testAsThoughRelocatedNullValue() throws IOException {
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping(b -> b.field("null_value", 0)));
        Instant instant = new Instant(randomInt());
        asThoughRelocatedTestCase(docMapper, "{\"date\":\"2018-01-02T00:11:22.000Z\"}", "{\"date\":\"2018-01-02T00:11:22.000Z\"}");
        asThoughRelocatedTestCase(docMapper, "{\"date\":\"1970-01-01T00:00:00.000Z\"}", "{\"date\":null}");
    }

    public void testAsThoughRelocatedWithFormat() throws IOException {
        String pattern = "yyyy.MM.dd'T'HH:mm:ss.SSSZ";
        DateTimeFormatter format = DateTimeFormat.forPattern(pattern);
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping(b -> b.field("format", pattern)));
        Instant instant = new Instant(randomInt());
        asThoughRelocatedTestCase(docMapper,
                "{\"date\":\"2018.01.02T00:11:22.000+0000\"}",
                "{\"date\":\"2018.01.02T00:11:22.000+0000\"}");
        asThoughRelocatedTestCase(docMapper,
                "{\"date\":\"" + format.print(instant) + "\"}",
                "{\"date\":\"" + format.print(instant) + "\"}");
        asThoughRelocatedTestCase(docMapper, "{}", "{\"date\":null}");
    }

    private CompressedXContent relocateToDocValueMapping(CheckedConsumer<XContentBuilder, IOException> extraFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("date");
                    {
                        builder.field("type", "date");
                        builder.field("relocate_to", "doc_values");
                        extraFields.accept(builder);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return new CompressedXContent(Strings.toString(builder));
    }

    public void testRelocateFromDocValuesNoValues() throws IOException {
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(false);
        DateFieldMapper.relocateFromDocValues("date", null, dv, docId, null);
        verify(dv).advanceExact(docId);
        verifyNoMoreInteractions(dv); // We never called docValueCount or nextValue or anything
    }

    public void testRelocateFromDocValuesSingleValue() throws IOException {
        DateTimeFormatter format = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
        Instant instant = new Instant(randomLong());
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(1);
        when(dv.nextValue()).thenReturn(instant.getMillis());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            DateFieldMapper.relocateFromDocValues("date", format, dv, docId, builder);
            builder.endObject();
            try (XContentParser parser = createParser(builder)) {
                assertEquals(singletonMap("date", format.print(instant)), parser.map());
            }
        }
    }

    public void testRelocateFromDocValuesMultipleValues() throws IOException {
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(between(2, 1000));
        expectThrows(IllegalStateException.class, () ->
                DateFieldMapper.relocateFromDocValues("date", null, dv, docId, null));
    }
}
