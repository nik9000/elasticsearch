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
public abstract class SourceLoader {
    private static final Logger logger = LogManager.getLogger(SourceLoader.class);

    public static SourceLoader forReadingFromTranslog(
            Function<Map<String, ?>, Map<String, Object>> filter) {
        if (filter == null) {
            return new PlainSourceLoader();
        }
        return new TranslogNormalizingSourceLoader(filter);
    }

    public static SourceLoader forReadingFromIndex(
            Map<String, SourceRelocationHandler> relocationHandlers,
            Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        if (relocationHandlers.isEmpty()) {
            return new PlainSourceLoader();
        }
        return new IndexSourceLoader(relocationHandlers, fieldDataLookup);
    }

    private BytesReference loadedSource;
    private BytesReference source;

    private SourceLoader() {
        // The only allowed subclasses are declared in this file
        // TODO it'd be simpler if these only loaded a single document but, for now, it works like FieldVisitor
    }

    /**
     * Called by {@link FieldsVisitor} when it loads the stored field portion
     * of the {@code _source}.
     */
    public final void setLoadedSource(BytesReference loadedSource) { // NOMCOMMIT remove public
        this.loadedSource = loadedSource;
    }

    /**
     * Loads the remainder of the source from doc values.
     */
    public final void load(LeafReaderContext context, int docId) throws IOException {
        source = innerLoad(loadedSource, context, docId);
    }

    protected abstract BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException;

    /**
     * The synthesized source.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * {@linkplain SourceLoader} for loading the without changing it at all.
     */
    private static class PlainSourceLoader extends SourceLoader {
        @Override
        protected BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException {
            return loadedSource;
        }
    }

    /**
     * {@linkplain SourceLoader} for loading the source from the translog and
     * rebuilding relocated fields as though they were loaded from doc values.
     */
    private static class TranslogNormalizingSourceLoader extends SourceLoader {
        private final Function<Map<String, ?>, Map<String, Object>> filter;

        private TranslogNormalizingSourceLoader(Function<Map<String, ?>, Map<String, Object>> filter) {
            this.filter = filter;
        }

        @Override
        protected BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException {
            if (loadedSource == null) {
                return null;
            }
            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(loadedSource, false);
            Map<String, Object> filteredSource = filter.apply(mapTuple.v2());

            BytesStreamOutput bStream = new BytesStreamOutput();
            XContentType contentType = mapTuple.v1();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, bStream).map(filteredSource);
            builder.close();
            return bStream.bytes();
        }
    }

    /**
     * {@linkplain SourceLoader} for loading the source from the Lucene index
     * and rebuilding relocated fields.
     */
    private static class IndexSourceLoader extends SourceLoader {
        private final Map<String, SourceRelocationHandler> relocationHandlers;
        private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;

        private IndexSourceLoader(
                Map<String, SourceRelocationHandler> relocationHandlers,
                Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
            this.relocationHandlers = relocationHandlers;
            this.fieldDataLookup = fieldDataLookup;
        }

        @Override
        protected BytesReference innerLoad(BytesReference loadedSource, LeafReaderContext context, int docId) throws IOException {
            XContentBuilder recreationBuilder;
            Map<String, SourceRelocationHandler> relocationHandlersToInvoke;
            if (loadedSource == null) {
                // NOCOMMIT is json right here? probably not too wrong at least.
                recreationBuilder = new XContentBuilder(JsonXContent.jsonXContent, new BytesStreamOutput());
                recreationBuilder.startObject();
                relocationHandlersToInvoke = relocationHandlers;
            } else {
                relocationHandlersToInvoke = new HashMap<>(relocationHandlers);
                try (XContentParser loadedSourceParser = XContentHelper.createParser(
                            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, loadedSource)) {
                    if (logger.isWarnEnabled()) {
                        // NOCOMMIT remove me when we make source relocation required if configured
                        if (loadedSourceParser.contentType() == XContentType.JSON) {
                            logger.warn("enhancing loaded source [{}]", loadedSource.utf8ToString());
                        }
                    }
                    recreationBuilder = new XContentBuilder(loadedSourceParser.contentType().xContent(), new BytesStreamOutput());
                    if (loadedSourceParser.nextToken() != Token.START_OBJECT) {
                        throw new IllegalStateException("unexpected xcontent layout [" + loadedSourceParser.currentToken() + "]");
                    }
                    recreationBuilder.startObject();
                    Token token;
                    while ((token = loadedSourceParser.nextToken()) != Token.END_OBJECT) {
                        assert token == Token.FIELD_NAME;
                        relocationHandlersToInvoke.remove(loadedSourceParser.currentName());
                        recreationBuilder.copyCurrentStructure(loadedSourceParser);
                    }
                }
            }

            for (SourceRelocationHandler handler : relocationHandlersToInvoke.values()) {
                handler.resynthesize(context, docId, fieldDataLookup, recreationBuilder);
            }
            recreationBuilder.endObject();
            return BytesReference.bytes(recreationBuilder);
        }
    }
}
