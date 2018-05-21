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

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;

class SourceSynthesizer {
    private SourceSynthesizer() {
        // Static utils
    }

    static BytesReference synthesizeSource(BytesReference original,
            CheckedConsumer<XContentBuilder, IOException> resynthesize) throws IOException {
        XContentBuilder recreationBuilder;
        // TODO use bigarrays?
        if (original == null) {
            // TODO is json right here? probably not too wrong at least.
            recreationBuilder = new XContentBuilder(JsonXContent.jsonXContent, new BytesStreamOutput());
            recreationBuilder.startObject();
        } else {
            try (XContentParser originalParser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, original)) {
                recreationBuilder = new XContentBuilder(originalParser.contentType().xContent(), new BytesStreamOutput());
                copyAllButLastEndObject(originalParser, recreationBuilder);
            }
        }
        resynthesize.accept(recreationBuilder);
        recreationBuilder.endObject();
        return BytesReference.bytes(recreationBuilder);
    }

    private static void copyAllButLastEndObject(XContentParser originalParser, XContentBuilder recreationBuilder) throws IOException {
        if (originalParser.nextToken() != Token.START_OBJECT) {
            throw new IllegalStateException("unexpected xcontent layout [" + originalParser.currentToken() + "]");
        }
        recreationBuilder.startObject();
        while (originalParser.nextToken() != Token.END_OBJECT) {
            recreationBuilder.copyCurrentStructure(originalParser);
        }
    }
}
