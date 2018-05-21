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

import java.io.IOException;

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
    public void testNullSource() throws IOException {
        try (XContentParser parser = synthesizeSource(null, b -> {})) {
            assertEquals(emptyMap(), parser.map());
        }

        try (XContentParser parser = synthesizeSource(null, b -> b.field("foo", "bar"))) {
            assertEquals(singletonMap("foo", "bar"), parser.map());
        }
    }


    public void testEmptyObject() throws IOException {
        try (XContentParser parser = synthesizeSource(
                b -> b.startObject().endObject(), b -> {})) {
            assertEquals(emptyMap(), parser.map());
        }

        try (XContentParser parser = synthesizeSource(
                b -> b.startObject().endObject(), b -> b.field("foo", "bar"))) {
            assertEquals(singletonMap("foo", "bar"), parser.map());
        }
    }

    // TODO non-empty starting, different stuff adding

    private XContentParser synthesizeSource(
                CheckedConsumer<XContentBuilder, IOException> build,
                CheckedConsumer<XContentBuilder, IOException> synthesizer) throws IOException {
        XContent xContent;
        BytesReference bytes;
        if (build == null) {
            xContent = JsonXContent.jsonXContent;
            bytes = null;
        } else {
            xContent = randomFrom(XContentType.values()).xContent();
            try (XContentBuilder b = XContentBuilder.builder(xContent)) {
                build.accept(b);
                bytes = BytesReference.bytes(b);
            }
        }

        BytesReference synthesized = SourceSynthesizer.synthesizeSource(bytes, synthesizer);
        return xContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, synthesized.streamInput());
    }
}
