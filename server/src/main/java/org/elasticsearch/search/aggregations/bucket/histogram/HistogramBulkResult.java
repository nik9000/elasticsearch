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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BulkReduce;
import org.elasticsearch.search.aggregations.Aggregator.BulkResult;
import org.elasticsearch.search.aggregations.Aggregator.CommonBulkResult;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator.BucketsBulkResult;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram.EmptyBucketInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HistogramBulkResult implements Aggregator.BulkResult {
    private final Aggregator.CommonBulkResult common;
    private final BucketsAggregator.BucketsBulkResult bucketsResult;
    private final DocValueFormat format;
    private final double interval, offset;
    private final DoubleArray keys;
    private final LongArray ords;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;

    public HistogramBulkResult(CommonBulkResult common, BucketsBulkResult bucketsResult, DocValueFormat format, double interval,
            double offset, DoubleArray keys, LongArray ords, BucketOrder order, boolean keyed, long minDocCount, double minBound,
            double maxBound) {
        this.common = common;
        this.bucketsResult = bucketsResult;
        this.format = forFmat;
        this.interval = interval;
        this.offset = offset;
        this.keys = keys;
        this.ords = ords;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
    }

    public HistogramBulkResult(StreamInput in) throws IOException {
        common = new Aggregator.CommonBulkResult(in);
        bucketsResult = new BucketsAggregator.BucketsBulkResult(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        interval = in.readDouble();
        offset = in.readDouble();
        // TODO this is mega-lame
        long size = in.readVLong();
        keys = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(size);
        for (long i = 0; i < size; i++) {
            keys.set(i, in.readDouble());
        }
        size = in.readVLong();
        ords = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(size);
        for (long i = 0; i < size; i++) {
            ords.set(i, in.readVLong());
        }
        order = InternalOrder.Streams.readHistogramOrder(in);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        minBound = in.readDouble();
        maxBound = in.readDouble();
    }

    @Override
    public String getWriteableName() {
        return HistogramAggregationBuilder.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public void close() {
        Releasables.close(bucketsResult, keys, ords);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        /*
         * The aggregator is either the top level or wrapped in the
         * MultiBucketAggregatorWrapper which doen't support bulk results so
         * we'll always be called with the owningBucketOrdinal = 1. This is
         * important too because the aggregator builds ords in order, starting
         * at 0.
         */
        assert owningBucketOrdinal == 0;

        List<InternalHistogram.Bucket> buckets = new ArrayList<>((int) keys.size());
        for (long i = 0; i < keys.size(); i++) {
            double roundKey = keys.get(i);
            double key = roundKey * interval + offset;
            buckets.add(new InternalHistogram.Bucket(key, bucketsResult.bucketDocCount(i), keyed, format,
                    bucketsResult.bucketAggregations(i)));
            assert i == 0 || buckets.get((int) i - 1).key < buckets.get((int) i).key : "must be in order";
        }

        EmptyBucketInfo emptyBucketInfo = null;
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(interval, offset, minBound, maxBound, bucketsResult.buildEmptySubAggregations());
        }
        return new InternalHistogram(common.getName(), buckets, order, minDocCount, emptyBucketInfo, format, keyed,
                common.getPipelineAggregators(), common.getMetaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        EmptyBucketInfo emptyBucketInfo = null;
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(interval, offset, minBound, maxBound, bucketsResult.buildEmptySubAggregations());
        }
        return new InternalHistogram(common.getName(), Collections.emptyList(), order, minDocCount, emptyBucketInfo, format, keyed,
                common.getPipelineAggregators(), common.getMetaData());
    }

    @Override
    public BulkReduce beginReducing(List<BulkResult> others, ReduceContext ctx) {
        HistogramBulkResult[] o = others.stream()
                .map(r -> (HistogramBulkResult) r).toArray(HistogramBulkResult[]::new);
        long[] mutOrds = new long[others.size()];
        return (myOrd, otherOrds) -> {
            System.arraycopy(otherOrds, 0, mutOrds, 0, otherOrds.length);
            
        };
    }

}
