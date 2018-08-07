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

    // NOCOMMIT this just test from index, need from translog
    public void testNullSource() throws IOException {
        original = null;
        // Null original and no fields to add means the result should be null
        assertNull(synthesizeSource(emptyMap()));

        // But if there are fields to write then we will write them
        expectedXContent = JsonXContent.jsonXContent;
        try (XContentParser parser = synthesizeSource(singletonMap("foo", b -> b.field("foo", "bar")))) {
            assertEquals(singletonMap("foo", "bar"), parser.map());
        }
    }

    public void testEmptyObject() throws IOException {
        initTestData(b -> b.startObject().endObject());
        try (XContentParser parser = synthesizeSource(emptyMap())) {
            assertEquals(emptyMap(), parser.map());
        }

        try (XContentParser parser = synthesizeSource(singletonMap("foo", b -> {}))) {
            assertEquals(emptyMap(), parser.map());
        }

        try (XContentParser parser = synthesizeSource(singletonMap("foo", b -> b.field("foo", "bar")))) {
            assertEquals(singletonMap("foo", "bar"), parser.map());
        }
    }

    public void testWithField() throws IOException {
        initTestData(b -> {
            b.startObject();
            {
                b.field("foo", 1);
            }
            b.endObject();
        });

        try (XContentParser parser = synthesizeSource(singletonMap("foo", b -> b.field("foo", "bar")))) {
            assertEquals(singletonMap("foo", 1), parser.map());
        }

        try (XContentParser parser = synthesizeSource(singletonMap("bar", b -> b.field("bar", 2)))) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("foo", 1);
            expected.put("bar", 2);
            assertEquals(expected, parser.map());
        }

        Map<String, CheckedConsumer<XContentBuilder, IOException>> valueWriters = new TreeMap<>();
        valueWriters.put("foo", b -> b.field("foo", 1));
        valueWriters.put("bar", b -> b.field("bar", 2));
        try (XContentParser parser = synthesizeSource(valueWriters)) {
            Map<String, Object> expected = new HashMap<>();
            expected.put("foo", 1);
            expected.put("bar", 2);
            assertEquals(expected, parser.map());
        }
    }

    private void initTestData(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        expectedXContent = randomFrom(XContentType.values()).xContent();
        try (XContentBuilder b = XContentBuilder.builder(expectedXContent)) {
            build.accept(b);
            original = BytesReference.bytes(b);
        }
    }

    private XContentParser synthesizeSource(
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
                public void asThoughRelocated(XContentParser translogSourceParser,
                        XContentBuilder normalizedBuilder) {
                    // NOCOMMIT tests for this too
                    throw new UnsupportedOperationException();
                }
            });
        }
        SourceLoader loader = SourceLoader.forReadingFromIndex(unmodifiableMap(relocationHandlers), mockFieldDataLookup);
        loader.setLoadedSource(original);
        loader.load(null, mockDocId);
        if (loader.source() == null) {
            return null;
        }
        return expectedXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, loader.source().streamInput());
    }
}
