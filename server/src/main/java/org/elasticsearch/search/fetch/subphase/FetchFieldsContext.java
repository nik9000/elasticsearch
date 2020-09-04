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
package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.function.BiFunction;

/**
 * The context needed to retrieve fields.
 */
public class FetchFieldsContext {
    public static FetchFieldsContext create(
        String indexName,
        MapperService mapperService,
        SearchLookup searchLookup,
        List<FieldAndFormat> fields
    ) {
        DocumentMapper documentMapper = mapperService.documentMapper();
        if (documentMapper.sourceMapper().enabled() == false) {
            throw new IllegalArgumentException(
                "Unable to retrieve the requested [fields] since _source is " + "disabled in the mappings for index [" + indexName + "]"
            );
        }

        return new FetchFieldsContext((m, s) -> FieldValueRetriever.create(m, s, fields));
    }

    private final BiFunction<MapperService, SearchLookup, FieldValueRetriever> fieldValueRetriever;

    private FetchFieldsContext(BiFunction<MapperService, SearchLookup, FieldValueRetriever> fieldValueRetriever) {
        this.fieldValueRetriever = fieldValueRetriever;
    }

    public FieldValueRetriever fieldValueRetriever(MapperService mapperService, SearchLookup searchLookup) {
        return fieldValueRetriever.apply(mapperService, searchLookup);
    }
}
