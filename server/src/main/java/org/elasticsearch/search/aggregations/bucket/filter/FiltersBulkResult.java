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

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BulkReduce;
import org.elasticsearch.search.aggregations.Aggregator.CommonBulkResult;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator.BucketsBulkResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class FiltersBulkResult implements Aggregator.BulkResult {
    private final Aggregator.CommonBulkResult common;
    private final BucketsAggregator.BucketsBulkResult bucketsResult;
    private final List<String> keys;
    private final boolean keyed;

    public FiltersBulkResult(CommonBulkResult common, BucketsBulkResult bucketsResult, List<String> keys, boolean keyed) {
        this.common = common;
        this.bucketsResult = bucketsResult;
        this.keys = keys;
        this.keyed = keyed;
    }

    public FiltersBulkResult(StreamInput in) throws IOException {
        common = new Aggregator.CommonBulkResult(in);
        bucketsResult = new BucketsAggregator.BucketsBulkResult(in);
        keys = in.readStringList();
        keyed = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common.writeTo(out);
        bucketsResult.writeTo(out);
        out.writeCollection(keys, StreamOutput::writeString);
        out.writeBoolean(keyed);
    }

    @Override
    public String getWriteableName() {
        return FiltersAggregationBuilder.NAME;
    }

    @Override
    public void close() {
        bucketsResult.close();
    }

    @Override
    public InternalFilters buildAggregation(long owningBucketOrdinal) {
        // TODO consumeAndMaybeBreak?
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(keys.size());
        long bucketOrd = owningBucketOrdinal * keys.size();
        for (int i = 0; i < keys.size(); i++) {
            LogManager.getLogger().warn("ADFADF {} {} {}", owningBucketOrdinal, common.getName(), keys.get(i));
            buckets.add(new InternalFilters.InternalBucket(keys.get(i), bucketsResult.bucketDocCount(bucketOrd),
                    bucketsResult.bucketAggregations(bucketOrd), keyed));
            bucketOrd++;
        }
        return new InternalFilters(common.getName(), buckets, keyed, common.getPipelineAggregators(), common.getMetaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        List<InternalFilters.InternalBucket> buckets = new ArrayList<>(keys.size());
        InternalAggregations subAggs = bucketsResult.buildEmptySubAggregations();
        for (int i = 0; i < keys.size(); i++) {
            buckets.add(new InternalFilters.InternalBucket(keys.get(i), 0, subAggs, keyed));
        }
        return new InternalFilters(common.getName(), buckets, keyed, common.getPipelineAggregators(), common.getMetaData());
    }

    @Override
    public BulkReduce beginReducing(List<Aggregator.BulkResult> others, ReduceContext ctx) {
        List<BucketsAggregator.BucketsBulkResult> otherBuckets = others.stream()
                .map(r -> ((FiltersBulkResult) r).bucketsResult)
                .collect(toList());
        Aggregator.BulkReduce sub = bucketsResult.beginReducing(otherBuckets, ctx);
        long[] mutOtherOrds = new long[others.size()];
        return (myOrd, otherOrds) -> {
            System.arraycopy(otherOrds, 0, mutOtherOrds, 0, otherOrds.length);
            for (int k = 0; k < keys.size(); k++) {
                sub.reduce(myOrd + k, mutOtherOrds);
                for (int i = 0; i < mutOtherOrds.length; i++) {
                    mutOtherOrds[i]++;
                }
            }
        };
    }
}

