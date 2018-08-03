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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.FieldMapper.SourceRelocationHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

/**
 * Loads {@code _source} from a Lucene index.
 */
public class SourceLoader {
    private static final Logger logger = LogManager.getLogger(SourceLoader.class);

    public static SourceLoader forReadingFromTranslog(
            Function<Map<String, ?>, Map<String, Object>> filter) {
        if (filter == null) {
            return new SourceLoader(emptyMap(), null);
        }
        return new TranslogNormalizingSourceLoader(filter);
    }

    private final Map<String, SourceRelocationHandler> relocationHandlers;
    private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;
    private BytesReference loadedSource;
    private BytesReference source;

    public SourceLoader(Map<String, SourceRelocationHandler> relocationHandlers,
            Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        this.relocationHandlers = relocationHandlers;
        this.fieldDataLookup = fieldDataLookup;
        // NOCOMMIT make class abstract with private and factory methods
    }

    /**
     * Called by {@link FieldsVisitor} when it moves to the next document.
     */
    final void reset() {
        loadedSource = null;
        source = null;
    }

    /**
     * Called by {@link FieldsVisitor} when it loads the stored field portion
     * of the {@code _source}.
     */
    final void setLoadedSource(BytesReference loadedSource) {
        this.loadedSource = loadedSource;
    }

    /**
     * Loads the remainder of the source from doc values.
     */
    public final void load(LeafReaderContext context, int docId) throws IOException {
        source = innerLoad(loadedSource, context, docId);
    }

    protected BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException {
        if (relocationHandlers.isEmpty()) {
            // Loading source from doc values is disabled
            return loadedSource;
        }
        return synthesizeSource(loadedSource, context, docId);
    }

    /**
     * The synthesized source.
     */
    public BytesReference source() {
        return source;
    }

    private BytesReference synthesizeSource(BytesReference original, LeafReaderContext context, int docId) throws IOException {
        XContentBuilder recreationBuilder;
        Map<String, SourceRelocationHandler> relocationHandlersToInvoke;
        if (original == null) {
            // TODO is json right here? probably not too wrong at least.
            recreationBuilder = new XContentBuilder(JsonXContent.jsonXContent, new BytesStreamOutput());
            recreationBuilder.startObject();
            relocationHandlersToInvoke = relocationHandlers;
        } else {
            relocationHandlersToInvoke = new HashMap<>(relocationHandlers);
            try (XContentParser originalParser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, original)) {
                if (logger.isWarnEnabled()) {
                    // TODO switch to trace
                    if (originalParser.contentType() == XContentType.JSON) {
                        logger.warn("enhancing loaded source [{}]", original.utf8ToString());
                    }
                }
                recreationBuilder = new XContentBuilder(originalParser.contentType().xContent(), new BytesStreamOutput());
                if (originalParser.nextToken() != Token.START_OBJECT) {
                    throw new IllegalStateException("unexpected xcontent layout [" + originalParser.currentToken() + "]");
                }
                recreationBuilder.startObject();
                Token token;
                while ((token = originalParser.nextToken()) != Token.END_OBJECT) {
                    assert token == Token.FIELD_NAME;
                    relocationHandlersToInvoke.remove(originalParser.currentName());
                    recreationBuilder.copyCurrentStructure(originalParser);
                }
            }
        }

        for (SourceRelocationHandler handler : relocationHandlersToInvoke.values()) {
            handler.resynthesize(context, docId, fieldDataLookup, recreationBuilder);
        }
        recreationBuilder.endObject();
        return BytesReference.bytes(recreationBuilder);
    }

    /**
     * {@linkplain SourceLoader} for loading the source from the translog and
     * rebuilding relocated fields as though they were loaded from doc values.
     */
    private static class TranslogNormalizingSourceLoader extends SourceLoader {
        private final Function<Map<String, ?>, Map<String, Object>> filter;

        private TranslogNormalizingSourceLoader(Function<Map<String, ?>, Map<String, Object>> filter) {
            super(emptyMap(), null);
            this.filter = filter;
        }

        @Override
        protected BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException {
            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(loadedSource, false);
            Map<String, Object> filteredSource = filter.apply(mapTuple.v2());

            BytesStreamOutput bStream = new BytesStreamOutput();
            XContentType contentType = mapTuple.v1();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, bStream).map(filteredSource);
            builder.close();
            return bStream.bytes();
        }
    }
}
