/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class TimeSeriesIdFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_tsid";
    public static final String CONTENT_TYPE = "_tsid";
    public static final TimeSeriesIdFieldType FIELD_TYPE = new TimeSeriesIdFieldType();

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Builder extends MetadataFieldMapper.Builder {
        protected Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of();
        }

        @Override
        public TimeSeriesIdFieldMapper build() {
            return new TimeSeriesIdFieldMapper();
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new TimeSeriesIdFieldMapper(),
        c -> new Builder()
    );

    public static final class TimeSeriesIdFieldType extends MappedFieldType {
        private TimeSeriesIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.NONE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this));
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }
            return DocValueFormat.TIME_SERIES_ID;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("term query not supported"); // NOCOMMIT erorr message or implement
        }
    }

    private TimeSeriesIdFieldMapper() {
        super(FIELD_TYPE, Lucene.KEYWORD_ANALYZER);
    }

    @Override
    public void preParse(DocumentParserContext context) throws IOException {
        if (false == context.indexSettings().inTimeSeriesMode()) {
            return;
        }
        assert fieldType().isSearchable() == false;

        String routing = context.sourceToParse().routing();
        if (routing == null) {
            // NOCOMMIT maybe we shouldn't be storing the time series id in routing?
            throw new IllegalArgumentException("In time series mode the tsid need to be in the routing");
        }
        // NOCOMMIT switch to native BytesRef over the wire
        BytesRef value = new BytesRef(Base64.getDecoder().decode(routing));
        context.doc().add(new SortedSetDocValuesField(fieldType().name(), value));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
