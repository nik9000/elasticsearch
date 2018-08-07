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

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.net.InetAddress;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IpFieldMapperTests extends AbstractFieldMapperTestCase {
    public void testDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "::1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").field("index", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "::1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").field("doc_values", false).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "::1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
    }

    public void testStore() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").field("store", true).endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "::1")
                        .endObject()),
                XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddress.getByName("::1"))),
                storedField.binaryValue());
    }

    public void testIgnoreMalformed() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").endObject().endObject()
                .endObject().endObject());

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        assertEquals(mapping, mapper.mappingSource().toString());

        ThrowingRunnable runnable = () -> mapper.parse(SourceToParse.source("test", "type", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", ":1")
                        .endObject()),
                XContentType.JSON));
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("':1' is not an IP string literal"));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "ip").field("ignore_malformed", true).endObject().endObject()
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

    public void testNullValue() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "ip")
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
                            .field("type", "ip")
                            .field("null_value", "::1")
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
        assertEquals(16, pointField.fieldType().pointNumBytes());
        assertFalse(pointField.fieldType().stored());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), pointField.binaryValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_SET, dvField.fieldType().docValuesType());
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))), dvField.binaryValue());
        assertFalse(dvField.fieldType().stored());
    }

    public void testSerializeDefaults() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("field").field("type", "ip").endObject().endObject()
            .endObject().endObject());

        DocumentMapper docMapper = parser.parse("type", new CompressedXContent(mapping));
        IpFieldMapper mapper = (IpFieldMapper)docMapper.root().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.doXContentBody(builder, true, ToXContent.EMPTY_PARAMS);
        String got = Strings.toString(builder.endObject());

        // it would be nice to check the entire serialized default mapper, but there are
        // a whole lot of bogus settings right now it picks up from calling super.doXContentBody...
        assertTrue(got, got.contains("\"null_value\":null"));
        assertTrue(got, got.contains("\"ignore_malformed\":false"));
    }

    public void testEmptyName() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties").startObject("").field("type", "ip").endObject().endObject()
            .endObject().endObject());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> parser.parse("type", new CompressedXContent(mapping))
        );
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
    }

    public void testRelocateToDocValuesWithoutDocValues() throws IOException {
        Exception e = expectThrows(MapperParsingException.class, () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(b -> b.field("doc_values", false)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [ip] sets [relocate_to] to [doc_values] "
                + "which requires doc_values to be enabled", e.getMessage());
    }

    public void testRelocateToDocValuesWithIgnoreMalformed() throws IOException {
        Exception e = expectThrows(MapperParsingException.class, () -> indexService.mapperService().merge(
                "_doc",
                relocateToDocValueMapping(b -> b.field("ignore_malformed", true)),
                MapperService.MergeReason.MAPPING_UPDATE));
        assertEquals("Failed to parse mapping [_doc]: [ip] sets [relocate_to] to [doc_values] "
                + "and [ignore_malformed] to [true] which is not allowed because it'd cause "
                + "malformed ips to vanish", e.getMessage());
    }

    public void testAsThoughRelocated() throws IOException {
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping(b -> {}));
        asThoughRelocatedTestCase(docMapper, "{\"ip\":\"192.168.0.1\"}");
        asThoughRelocatedTestCase(docMapper, "{\"ip\":\"::1\"}");
        asThoughRelocatedTestCase(docMapper, "{}", "{\"ip\":null}");
        asThoughRelocatedTestCase(docMapper, "{\"ip\":\"::1\"}", "{\"ip\":\"0000:0000:0000:0000:0000:0000:0000:0001\"}");
    }

    public void testAsThoughRelocatedNullValue() throws IOException {
        DocumentMapper docMapper = parser.parse("_doc", relocateToDocValueMapping(b ->
                b.field("null_value", "0000:0000:0000:0000:0000:0000:0000:0001")));
        asThoughRelocatedTestCase(docMapper, "{\"ip\":\"192.168.0.1\"}");
        asThoughRelocatedTestCase(docMapper, "{\"ip\":\"::1\"}", "{\"ip\":null}");
    }

    private CompressedXContent relocateToDocValueMapping(CheckedConsumer<XContentBuilder, IOException> extraFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("ip");
                    {
                        builder.field("type", "ip");
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
        SortedBinaryDocValues dv = mock(SortedBinaryDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(false);
        IpFieldMapper.relocateFromDocValues("ip", dv, docId, null);
        verify(dv).advanceExact(docId);
        verifyNoMoreInteractions(dv); // We never called docValueCount or nextValue or anything
    }

    public void testRelocateFromDocValuesSingleValue() throws IOException {
        int docId = randomInt();
        InetAddress expectedValue = InetAddresses.forString("::1");
        SortedBinaryDocValues dv = mock(SortedBinaryDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(1);
        when(dv.nextValue()).thenReturn(new BytesRef(InetAddressPoint.encode(expectedValue)));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            IpFieldMapper.relocateFromDocValues("ip", dv, docId, builder);
            builder.endObject();
            try (XContentParser parser = createParser(builder)) {
                assertEquals(singletonMap("ip", InetAddresses.toAddrString(expectedValue)),
                        parser.map());
            }
        }
    }

    public void testRelocateFromDocValuesMultipleValues() throws IOException {
        int docId = randomInt();
        SortedBinaryDocValues dv = mock(SortedBinaryDocValues.class);
        when(dv.advanceExact(docId)).thenReturn(true);
        when(dv.docValueCount()).thenReturn(between(2, 1000));
        expectThrows(IllegalStateException.class, () ->
                IpFieldMapper.relocateFromDocValues("ip", dv, docId, null));
    }
}
