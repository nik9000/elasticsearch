/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.FilterByFilter.CountCollectorSource;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Specialized {@link QueryToFilterAdapter} for {@link DocValuesFieldExistsQuery} that reads counts from metadata.
 */
class DocValuesFieldExistsAdapter extends QueryToFilterAdapter<DocValuesFieldExistsQuery> {
    private int resultsFromMetadata;

    DocValuesFieldExistsAdapter(IndexSearcher searcher, String key, DocValuesFieldExistsQuery query) {
        super(searcher, key, query);
    }

    @Override
    void countOrRegisterUnion(LeafReaderContext ctx, CountCollectorSource collectorSource, Bits live) throws IOException {
        if (collectorSource.canUseMetadata() && canCountFromMetadata(ctx)) {
            resultsFromMetadata++;
            PointValues points = ctx.reader().getPointValues(query().getField());
            collectorSource.count(points == null ? 0 : points.getDocCount());
        } else {
            super.countOrRegisterUnion(ctx, collectorSource, live);
        }
    }

    @Override
    long estimateCountCost(LeafReaderContext ctx, CheckedSupplier<Boolean, IOException> canUseMetadata) throws IOException {
        if (canUseMetadata.get() && canCountFromMetadata(ctx)) {
            return 0;
        }
        return super.estimateCountCost(ctx, canUseMetadata);
    }

    private boolean canCountFromMetadata(LeafReaderContext ctx) throws IOException {
        FieldInfo info = ctx.reader().getFieldInfos().fieldInfo(query().getField());
        if (info == null) {
            // If we don't have any info then there aren't any values anyway.
            return true;
        }
        return info.getPointDimensionCount() > 0;
    }

    @Override
    void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("specialized_for", "docvalues_field_exists");
        add.accept("results_from_metadata", resultsFromMetadata);
    }
}