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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Mapper}s.
 */
public abstract class MapperTestCase extends MapperServiceTestCase {
    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    public final void testEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
        assertParseMinimalWarnings();
    }

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMinimalWarnings();
    }

    // TODO make this final once we remove FieldMapperTestCase2
    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMinimalWarnings();
    }

    protected void assertParseMinimalWarnings() {
        // Most mappers don't emit any warnings
    }

    /**
     * Override to disable testing {@code meta} in fields that don't support it.
     */
    protected boolean supportsMeta() {
        return true;
    }

    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    public final void testMeta() throws IOException {
        assumeTrue("Field doesn't support meta", supportsMeta());
        XContentBuilder mapping = fieldMapping(
            b -> {
                metaMapping(b);
                b.field("meta", Collections.singletonMap("foo", "bar"));
            }
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(this::metaMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public static List<?> fetchSourceValue(FieldMapper mapper, Object sourceValue) throws IOException {
        return fetchSourceValue(mapper, sourceValue, null);
    }

    public static List<?> fetchSourceValue(FieldMapper mapper, Object sourceValue, String format) throws IOException {
        String field = mapper.name();

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = mapper.valueFetcher(mapperService, null, format);
        SourceLookup lookup = new SourceLookup();
        lookup.setSource(Collections.singletonMap(field, sourceValue));
        return fetcher.fetchValues(lookup);
    }

    /**
     * Use a {@linkplain FieldMapper} to extract values from doc values.
     */
    protected final List<?> fetchFromDocValues(FieldMapper mapper, DocValueFormat format, Object sourceValue) throws IOException {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(any())).thenReturn(mapper.fieldType());
        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup = (mft, lookupSource) -> mft
            .fielddataBuilder("test", () -> { throw new UnsupportedOperationException(); })
            .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService(), mapperService);
        SetOnce<List<?>> result = new SetOnce<>();
        withLuceneIndex(mapperService, iw -> {
            ParseContext.Document doc = new ParseContext.Document();
            ParseContext context = mock(ParseContext.class);
            when(context.doc()).thenReturn(doc);
            when(context.sourceToParse()).thenReturn(source(b -> b.field(mapper.name(), sourceValue)));
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                context.sourceToParse().source(),
                context.sourceToParse().getXContentType()
            );
            when(context.parser()).thenReturn(parser);
            parser.nextToken();
            XContentParserUtils.ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
            XContentParserUtils.ensureFieldName(parser, parser.nextToken(), mapper.name());
            parser.nextToken();
            mapper.parse(context);
            parser.nextToken();
            XContentParserUtils.ensureExpectedToken(Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
            iw.addDocument(doc);
        }, iw -> {
            IndexSearcher indexSearcher = newSearcher(iw);
            SearchLookup lookup = new SearchLookup(mapperService, fieldDataLookup);
            ValueFetcher valueFetcher = new DocValueFetcher(format, lookup.doc().getForField(mapper.fieldType()));
            indexSearcher.search(new MatchAllDocsQuery(), new Collector() {
                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }

                @Override
                public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                    valueFetcher.setNextReader(context);
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) throws IOException {}

                        @Override
                        public void collect(int doc) throws IOException {
                            lookup.source().setSegmentAndDocument(context, doc);
                            result.set(valueFetcher.fetchValues(lookup.source()));
                        }
                    };
                }
            });
        });
        return result.get();
    }
}
