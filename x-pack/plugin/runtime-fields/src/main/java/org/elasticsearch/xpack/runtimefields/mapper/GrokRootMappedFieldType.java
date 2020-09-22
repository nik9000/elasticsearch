/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;

import java.util.Map;

public class GrokRootMappedFieldType extends MappedFieldType {
    public GrokRootMappedFieldType(String name, Map<String, String> meta) {
        super(name, false, false, false, TextSearchInfo.NONE, meta);
    }

    @Override
    public String typeName() {
        return RuntimeFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        // NOCOMMIT this seems pretty lame
        throw new IllegalArgumentException("can't search a grok field directly - try a child field");
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        throw new IllegalArgumentException("can't search a grok field directly - try a child field");
    }
}
