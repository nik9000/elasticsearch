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

package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.FieldMapper.SourceRelocationHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;


public class SourceLoaderTests extends ESTestCase {
    private BytesReference original;
    private XContent expectedXContent;

    public void testNullSource() throws IOException {
        original = null;
        // Null original and no fields to add means the result should be null
        assertNull(fromIndex(emptyMap()));
        assertNull(fromTranslog(emptyMap()));

        // If we have field to relocate when restoring from the index we'll write them
        expectedXContent = JsonXContent.jsonXContent;
        assertEquals(singletonMap("foo", "bar"), fromIndex(singletonMap("foo", b -> b.field("foo", "bar"))));

        // But we won't write anything if we're restoring from the translog because we didn't see the field's name
        assertNull(fromTranslog(singletonMap("foo", (p, b) -> b.field("foo", p.text()))));
    }

    public void testEmptySource() throws IOException {
        initTestData(b -> b.startObject().endObject());
        assertEquals(emptyMap(), fromIndex(emptyMap()));
        assertEquals(emptyMap(), fromTranslog(emptyMap()));

        assertEquals(emptyMap(), fromIndex(singletonMap("foo", b -> {})));
        assertEquals(emptyMap(), fromTranslog(singletonMap("foo", (p, b) -> {})));

        assertEquals(singletonMap("foo", "bar"), fromIndex(singletonMap("foo", b -> b.field("foo", "bar"))));
        assertEquals(emptyMap(), fromTranslog(singletonMap("foo", (p, b) -> b.field("foo", p.text()))));
    }

    public void testWithField() throws IOException {
        initTestData(b -> {
            b.startObject();
            {
                b.field("foo", 1);
            }
            b.endObject();
        });

        assertEquals(singletonMap("foo", 1), fromIndex(emptyMap()));
        assertEquals(singletonMap("foo", 1), fromTranslog(emptyMap()));

        assertEquals(singletonMap("foo", 1), fromIndex(singletonMap("foo", b -> b.field("foo", "bar"))));
        assertEquals(singletonMap("foo", "1"), fromTranslog(singletonMap("foo", (p, b) -> b.field("foo", p.text()))));

        {
            Map<String, Object> expected = new HashMap<>();
            expected.put("foo", 1);
            expected.put("bar", 2);
            assertEquals(expected, fromIndex(singletonMap("bar", b -> b.field("bar", 2))));
        }
        assertEquals(singletonMap("foo", 1), fromTranslog(singletonMap("bar", (p, b) -> b.field("bar", p.text()))));

        {
            Map<String, CheckedConsumer<XContentBuilder, IOException>> valueWriters = new TreeMap<>();
            valueWriters.put("foo", b -> b.field("foo", 1));
            valueWriters.put("bar", b -> b.field("bar", 2));
            Map<String, Object> expected = new HashMap<>();
            expected.put("foo", 1);
            expected.put("bar", 2);
            assertEquals(expected, fromIndex(valueWriters));
        }
        {
            Map<String, CheckedBiConsumer<XContentParser, XContentBuilder, IOException>> normalizers = new TreeMap<>();
            normalizers.put("foo", (p, b) -> b.field("foo", p.text()));
            normalizers.put("bar", (p, b) -> b.field("bar", p.text()));
            assertEquals(singletonMap("foo", "1"), fromTranslog(normalizers));
        }

        assertEquals(singletonMap("foo", 1), fromIndex(singletonMap("foo", b -> {})));
        assertEquals(emptyMap(), fromTranslog(singletonMap("foo", (p, b) -> {})));
    }

    public void testWithNullValuedField() throws IOException {
        initTestData(b -> {
            b.startObject();
            {
                b.field("foo").nullValue();
            }
            b.endObject();
        });

        assertEquals(singletonMap("foo", null), fromIndex(emptyMap()));
        assertEquals(singletonMap("foo", null), fromTranslog(emptyMap()));

        assertEquals(singletonMap("foo", null), fromIndex(singletonMap("foo", b -> b.field("foo", "bar"))));
        assertEquals(singletonMap("foo", "replaced"), fromTranslog(singletonMap("foo", (p, b) -> b.field("foo", "replaced"))));
    }

    public void testWithFieldObject() throws IOException {
        initTestData(b -> {
            b.startObject();
            {
                b.startObject("foo");
                {
                    b.field("inner", 1);
                }
                b.endObject();
            }
            b.endObject();
        });
        Map<String, Object> unchanged = singletonMap("foo", singletonMap("inner", 1));

        assertEquals(unchanged, fromIndex(emptyMap()));
        assertEquals(unchanged, fromTranslog(emptyMap()));

        assertEquals(unchanged, fromIndex(singletonMap("foo", b -> b.field("foo", "bar"))));
        assertEquals(unchanged, fromTranslog(singletonMap("foo", (p, b) -> b.field("foo", p.text()))));
    }

    private void initTestData(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        expectedXContent = randomFrom(XContentType.values()).xContent();
        try (XContentBuilder b = XContentBuilder.builder(expectedXContent)) {
            build.accept(b);
            original = BytesReference.bytes(b);
        }
    }

    private Map<String, Object> fromIndex(
                Map<String, CheckedConsumer<XContentBuilder, IOException>> simpleRelocationHandlers) throws IOException {
        final int mockDocId = randomInt();
        final Function<MappedFieldType, IndexFieldData<?>> mockFieldDataLookup = m -> null;
        Map<String, SourceRelocationHandler> relocationHandlers = new HashMap<>(simpleRelocationHandlers.size());
        for (Map.Entry<String, CheckedConsumer<XContentBuilder, IOException>> e : simpleRelocationHandlers.entrySet()) {
            relocationHandlers.put(e.getKey(), new SourceRelocationHandler() {
                @Override
                public void resynthesize(LeafReaderContext context, int docId,
                        Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup,
                        XContentBuilder builder) throws IOException {
                    assertNull(context);
                    assertEquals(mockDocId, docId);
                    assertSame(mockFieldDataLookup, fieldDataLookup);
                    e.getValue().accept(builder);
                }

                @Override
                public void asThoughRelocated(XContentParser translogSourceParser, XContentBuilder normalizedBuilder) {
                    throw new UnsupportedOperationException();
                }
            });
        }
        SourceLoader loader = SourceLoader.forReadingFromIndex(unmodifiableMap(relocationHandlers), mockFieldDataLookup);
        loader.setLoadedSource(original);
        loader.load(null, mockDocId);
        return loaderToMap(loader);
    }

    private Map<String, Object> fromTranslog(
                Map<String, CheckedBiConsumer<XContentParser, XContentBuilder, IOException>> simpleRelocationHandlers
                ) throws IOException {
        Map<String, SourceRelocationHandler> relocationHandlers = new HashMap<>(simpleRelocationHandlers.size());
        int i = 0;
        for (Map.Entry<String, CheckedBiConsumer<XContentParser, XContentBuilder, IOException>> e : simpleRelocationHandlers.entrySet()) {
            relocationHandlers.put(e.getKey(), new SourceRelocationHandler() {
                @Override
                public void resynthesize(LeafReaderContext context, int docId,
                        Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup,
                        XContentBuilder builder) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void asThoughRelocated(XContentParser translogSourceParser, XContentBuilder normalizedBuilder) throws IOException {
                    e.getValue().accept(translogSourceParser, normalizedBuilder);
                }
            });
        }
        SourceLoader loader = SourceLoader.forReadingFromTranslog(unmodifiableMap(relocationHandlers));
        loader.setLoadedSource(original);
        loader.load(null, randomInt());
        return loaderToMap(loader);
    }

    private Map<String, Object> loaderToMap(SourceLoader loader) throws IOException {
        if (loader.source() == null) {
            return null;
        }
        try (XContentParser parser = createParser(expectedXContent, loader.source())) {
            return parser.map();
        }
    }
}
