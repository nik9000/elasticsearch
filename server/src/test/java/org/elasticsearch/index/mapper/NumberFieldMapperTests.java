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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class NumberFieldMapperTests extends AbstractNumericFieldMapperTestCase {

    @Override
    protected void setTypeList() {
        TYPES = new HashSet<>(Arrays.asList("byte", "short", "integer", "long", "float", "double", "half_float"));
        WHOLE_TYPES = new HashSet<>(Arrays.asList("byte", "short", "integer", "long"));
    }

    @Override
    public void doTestDefaults(String type) throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    @Override
    public void doTestNotIndexed(String type) throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("index", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    @Override
    public void doTestNoDocValues(String type) throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("doc_values", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestStore(String type) throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("store", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", 123)
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(123, storedField.numericValue().doubleValue(), 0d);
    }

    @Override
    public void doTestCoerce(String type) throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "123")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("coerce", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper2.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper2.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "123")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    @Override
    protected void doTestDecimalCoerce(String type) throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "7.89")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField pointField = fields[0];
        assertEquals(7, pointField.numericValue().doubleValue(), 0d);
    }

    public void testIgnoreMalformed() throws Exception {
        for (String type : TYPES) {
            doTestIgnoreMalformed(type);
        }
    }

    private void doTestIgnoreMalformed(String type) throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "a")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);

        assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", type).field("ignore_malformed", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper2 = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper2.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "a")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        assertArrayEquals(new String[] { "field" }, doc.rootDoc().getValues("_ignored"));
    }

    public void testRejectNorms() throws IOException {
        // not supported as of 5.0
        for (String type : TYPES) {
            DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                        .field("norms", random().nextBoolean())
                    .endObject()
                .endObject().endObject().endObject());
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("Mapping definition for [foo] has unsupported parameters:  [norms"));
        }
    }

    /**
     * `index_options` was deprecated and is rejected as of 7.0
     */
    public void testRejectIndexOptions() throws IOException {
        for (String type : TYPES) {
            DocumentMapperParser parser = createIndex("index-" + type).mapperService().documentMapperParser();
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", type)
                    .field("index_options", randomFrom(new String[] { "docs", "freqs", "positions", "offsets" }))
                    .endObject()
                .endObject().endObject().endObject());
            MapperParsingException e = expectThrows(MapperParsingException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping)));
            assertThat(e.getMessage(), containsString("index_options not allowed in field [foo] of type [" + type +"]"));
        }
    }

    @Override
    protected void doTestNullValue(String type) throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
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

        Object missing;
        if (Arrays.asList("float", "double", "half_float").contains(type)) {
            missing = 123d;
        } else {
            missing = 123L;
        }
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", type)
                            .field("null_value", missing)
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
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    @Override
    public void testEmptyName() throws IOException {
        // after version 5
        for (String type : TYPES) {
            String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("").field("type", type).endObject().endObject()
                .endObject().endObject());

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping))
            );
            assertThat(e.getMessage(), containsString("name cannot be empty string"));
        }
    }

    public void testOutOfRangeValues() throws IOException {
        final List<OutOfRangeSpec<Object>> inputs = Arrays.asList(
            OutOfRangeSpec.of(NumberType.BYTE, "128", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "32768", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "2147483648", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "9223372036854775808", "out of range for a long"),

            OutOfRangeSpec.of(NumberType.BYTE, "-129", "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, "-32769", "is out of range for a short"),
            OutOfRangeSpec.of(NumberType.INTEGER, "-2147483649", "is out of range for an integer"),
            OutOfRangeSpec.of(NumberType.LONG, "-9223372036854775809", "out of range for a long"),

            OutOfRangeSpec.of(NumberType.BYTE, 128, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, 32768, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, 2147483648L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("9223372036854775808"), "out of range of long"),

            OutOfRangeSpec.of(NumberType.BYTE, -129, "is out of range for a byte"),
            OutOfRangeSpec.of(NumberType.SHORT, -32769, "out of range of Java short"),
            OutOfRangeSpec.of(NumberType.INTEGER, -2147483649L, " out of range of int"),
            OutOfRangeSpec.of(NumberType.LONG, new BigInteger("-9223372036854775809"), "out of range of long"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, "-65520", "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, "-3.4028235E39", "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, "-1.7976931348623157E309", "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NaN, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NaN, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NaN, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.POSITIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.POSITIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.POSITIVE_INFINITY, "[double] supports only finite values"),

            OutOfRangeSpec.of(NumberType.HALF_FLOAT, Float.NEGATIVE_INFINITY, "[half_float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.FLOAT, Float.NEGATIVE_INFINITY, "[float] supports only finite values"),
            OutOfRangeSpec.of(NumberType.DOUBLE, Double.NEGATIVE_INFINITY, "[double] supports only finite values")
        );

        for(OutOfRangeSpec<Object> item: inputs) {
            try {
                parseRequest(item.type, createIndexRequest(item.value));
                fail("Mapper parsing exception expected for [" + item.type + "] with value [" + item.value + "]");
            } catch (MapperParsingException e) {
                assertThat("Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                    e.getCause().getMessage(), containsString(item.message));
            }
        }
    }

    private void parseRequest(NumberType type, BytesReference content) throws IOException {
        createDocumentMapper(type).parse(SourceToParse.source("test", "type", "1", content, XContentType.JSON));
    }

    private DocumentMapper createDocumentMapper(NumberType type) throws IOException {
        String mapping = Strings
            .toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("field")
                                .field("type", type.typeName())
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject());

        return parser.parse("type", new CompressedXContent(mapping));
    }

    private BytesReference createIndexRequest(Object value) throws IOException {
        if (value instanceof BigInteger) {
            return BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .rawField("field", new ByteArrayInputStream(value.toString().getBytes("UTF-8")), XContentType.JSON)
                .endObject());
        } else {
            return BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", value).endObject());
        }
    }


    public void testInvalidRelocateTo() throws IOException {
        String invalidRelocateTo = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("movie")
            .startObject("properties")
                .startObject("some_number")
                    .field("type", randomFrom("integer", "long", "float", "double"))
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
                .startObject("something")
                    .field("type", "text")
                    .startObject("fields")
                        .startObject("number")
                            .field("type", randomFrom("integer", "long", "float", "double"))
                            .field("relocate_to", "doc_values")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().endObject().endObject());
        Exception e = expectThrows(MapperParsingException.class,
            () -> indexService.mapperService().merge("movie", new CompressedXContent(nestedRelocateTo),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals(
            "[something.number] sets [relocate_to] but that is not supported inside multifields",
            e.getCause().getMessage());
    }

    public void testRelocateToDocValuesWithoutDocValues() throws IOException {
        Exception e = expectThrows(MapperParsingException.class,
            () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(randomFrom("integer", "long", "float", "double"), b -> b.field("doc_values", false)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [number] sets [relocate_to] to "
                + "[doc_values] which requires doc_values to be enabled", e.getMessage());
    }

    public void testRelocateToDocValuesWithIgnoreMalformed() throws IOException {
        Exception e = expectThrows(MapperParsingException.class,
            () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(randomFrom("integer", "long", "float", "double"), b -> b.field("ignore_malformed", true)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [number] sets [relocate_to] to [doc_values] "
                + "and [ignore_malformed] to [true] which is not allowed because it'd cause "
                + "malformed numbers to vanish", e.getMessage());
    }

    // NOCOMMIT move these too
    public void testAsThoughRelocatedLong() throws IOException {
        long expected = randomLong();
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping("long", b -> {}));
        assertEquals(singletonMap("number", expected),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", expected)));
        assertEquals(emptyMap(),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", null)));
    }

    public void testAsThoughRelocatedLongRounding() throws IOException {
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping("long", b -> {}));
        assertEquals(singletonMap("number", 0L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", .1)));
        assertEquals(singletonMap("number", 0L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", -.1)));
        assertEquals(singletonMap("number", 12L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", 12.1)));
        assertEquals(singletonMap("number", 12L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", 12.9)));
        assertEquals(singletonMap("number", -12L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", -12.1)));
        assertEquals(singletonMap("number", -12L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", -12.9)));
    }

    public void testAsThoughRelocatedDouble() throws IOException {
        double expected = randomDouble();
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping("double", b -> {}));
        assertEquals(singletonMap("number", expected),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", expected)));
        assertEquals(emptyMap(),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", null)));
    }

    public void testAsThoughRelocatedNullValue() throws IOException {
        long expected = randomLong();
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping("long", b ->
                b.field("null_value", 123)));
        assertEquals(singletonMap("number", expected),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", expected)));
        assertEquals(singletonMap("number", 123L),
                docMapper.translogSourceNormalizingFilter().apply(singletonMap("number", null)));
    }

    private CompressedXContent relocateToDocValueMapping(String type,
            CheckedConsumer<XContentBuilder, IOException> extraFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("number");
                    {
                        builder.field("type", type);
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

    public void testRelocateFromDocValuesNoDoubleValues() throws IOException {
        NumberType type = randomFrom(NumberType.HALF_FLOAT, NumberType.FLOAT, NumberType.DOUBLE);
        int docId = randomInt();
        SortedNumericDoubleValues dv = mock(SortedNumericDoubleValues.class);
        when(dv.advanceExact(docId)).thenReturn(false);
        type.relocateFromDocValues(type.typeName(), afd(dv), docId, null);
        verify(dv).advanceExact(docId);
        verifyNoMoreInteractions(dv); // We never called docValueCount or nextValue or anything
    }

    public void testRelocateFromDocValuesSingleHalfFloatValue() throws IOException {
        double input = randomDouble();
        String expected = Float.toString(NumberFieldMapper.roundToHalfFloat(input));
        relocateFromDocValuesSingleDoubleValueTestCase(NumberType.HALF_FLOAT, input, expected);
    }

    public void testRelocateFromDocValuesSingleFloatValue() throws IOException {
        double input = randomDouble();
        String expected = Float.toString(NumericUtils.sortableIntToFloat(NumericUtils.floatToSortableInt((float) input)));
        relocateFromDocValuesSingleDoubleValueTestCase(NumberType.FLOAT, input, expected);
    }

    public void testRelocateFromDocValuesSingleDoubleValue() throws IOException {
        double input = randomDouble();
        String expected = Double.toString(input);
        relocateFromDocValuesSingleDoubleValueTestCase(NumberType.DOUBLE, input, expected);
    }

    private void relocateFromDocValuesSingleDoubleValueTestCase(NumberType type, double input, String expected) throws IOException {
        int docId = randomInt();
        SortedNumericDoubleValues dv = mock(SortedNumericDoubleValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(1);
        when(dv.nextValue()).thenReturn(input);
        relocateFromDocValuesSingleValueTestCase(type, afd(dv), docId, expected);
    }

    public void testRelocateFromDocValuesMultipleDoubleValues() throws IOException {
        NumberType type = randomFrom(NumberType.HALF_FLOAT, NumberType.FLOAT, NumberType.DOUBLE);
        int docId = randomInt();
        SortedNumericDoubleValues dv = mock(SortedNumericDoubleValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(between(2, 1000));
        expectThrows(IllegalStateException.class, () ->
                type.relocateFromDocValues(type.typeName(), afd(dv), docId, null));
    }

    private AtomicNumericFieldData afd(SortedNumericDoubleValues dv) {
        AtomicNumericFieldData afd = mock(AtomicNumericFieldData.class);
        when(afd.getDoubleValues()).thenReturn(dv);
        return afd;
    }

    public void testRelocateFromDocValuesNoLongValues() throws IOException {
        NumberType type = randomFrom(NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER, NumberType.LONG);
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(false);
        type.relocateFromDocValues(type.typeName(), afd(dv), docId, null);
        verify(dv).advanceExact(docId);
        verifyNoMoreInteractions(dv); // We never called docValueCount or nextValue or anything
    }

    public void testRelocateFromDocValuesSingleByteValue() throws IOException {
        relocateFromDocValuesSingleLongValueTestCase(NumberType.BYTE, randomByte());
    }

    public void testRelocateFromDocValuesSingleShortValue() throws IOException {
        relocateFromDocValuesSingleLongValueTestCase(NumberType.SHORT, randomShort());
    }

    public void testRelocateFromDocValuesSingleIntegerValue() throws IOException {
        relocateFromDocValuesSingleLongValueTestCase(NumberType.INTEGER, randomInt());
    }

    public void testRelocateFromDocValuesSingleLongValue() throws IOException {
        relocateFromDocValuesSingleLongValueTestCase(NumberType.LONG, randomLong());
    }

    private void relocateFromDocValuesSingleLongValueTestCase(NumberType type, long input) throws IOException {
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(1);
        when(dv.nextValue()).thenReturn(input);
        relocateFromDocValuesSingleValueTestCase(type, afd(dv), docId, Long.toString(input));
    }

    private void relocateFromDocValuesSingleValueTestCase(NumberType type, AtomicNumericFieldData afd,
            int docId, String expected) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            type.relocateFromDocValues(type.typeName(), afd, docId, builder);
            builder.endObject();
            try (XContentParser parser = createParser(builder)) {
                assertEquals(Token.START_OBJECT, parser.nextToken());
                assertEquals(Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                assertEquals(expected, parser.text());
                assertEquals(Token.END_OBJECT, parser.nextToken());
            }
        }
    }

    public void testRelocateFromDocValuesMultipleLongValues() throws IOException {
        NumberType type = randomFrom(NumberType.BYTE, NumberType.SHORT, NumberType.INTEGER, NumberType.LONG);
        int docId = randomInt();
        SortedNumericDocValues dv = mock(SortedNumericDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(between(2, 1000));
        expectThrows(IllegalStateException.class, () ->
                type.relocateFromDocValues(type.typeName(), afd(dv), docId, null));
    }

    private AtomicNumericFieldData afd(SortedNumericDocValues dv) {
        AtomicNumericFieldData afd = mock(AtomicNumericFieldData.class);
        when(afd.getLongValues()).thenReturn(dv);
        return afd;
    }
}
