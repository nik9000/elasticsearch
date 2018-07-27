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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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

public class SourceSynthesizerTests extends ESTestCase {
    private BytesReference original;
    private XContent expectedXContent;

    public void testNullSource() throws IOException {
        original = null;
        expectedXContent = JsonXContent.jsonXContent;
        try (XContentParser parser = synthesizeSource(emptyMap())) {
            assertEquals(emptyMap(), parser.map());
        }

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
                Map<String, CheckedConsumer<XContentBuilder, IOException>> testValueWriters) throws IOException {
        Map<String, CheckedBiConsumer<Void, XContentBuilder, IOException>> contextValueWriters = new HashMap<>(testValueWriters.size());
        for (Map.Entry<String, CheckedConsumer<XContentBuilder, IOException>> w : testValueWriters.entrySet()) {
            contextValueWriters.put(w.getKey(), (v, b) -> w.getValue().accept(b));
        }
        BytesReference synthesized = SourceSynthesizer.synthesizeSource(original, null, unmodifiableMap(contextValueWriters));
        return expectedXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, synthesized.streamInput());
    }
}
