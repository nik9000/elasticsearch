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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 */
class DateHistogramAggregator extends BucketsAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    private final Rounding shardRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final ExtendedBounds extendedBounds;

    private final LongHash bucketOrds;

    DateHistogramAggregator(String name, AggregatorFactories factories, Rounding rounding, Rounding shardRounding,
            BucketOrder order, boolean keyed,
            long minDocCount, @Nullable ExtendedBounds extendedBounds, @Nullable ValuesSource.Numeric valuesSource,
            DocValueFormat formatter, SearchContext aggregationContext,
            Aggregator parent, Map<String, Object> metadata) throws IOException {

        super(name, factories, aggregationContext, parent, metadata);
        this.rounding = rounding;
        this.shardRounding = shardRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.valuesSource = valuesSource;
        this.formatter = formatter;

        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        // We can use shardRounding here, which is sometimes more efficient
                        // if daylight saving times are involved.
                        long rounded = shardRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        long bucketOrd = bucketOrds.add(rounded);
                        if (bucketOrd < 0) { // already seen
                            bucketOrd = -1 - bucketOrd;
                            collectExistingBucket(sub, doc, bucketOrd);
                        } else {
                            collectBucket(sub, doc, bucketOrd);
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds, 
            (bucketValue, docCount, subAggregationResults) -> {
                return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
            }, buckets -> {
                // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                // value source will be null for unmapped fields
                // Important: use `rounding` here, not `shardRounding`
                InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                        ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                        : null;
                return new InternalDateHistogram(name, buckets, order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                        keyed, metadata());
            });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
                : null;
        return new InternalDateHistogram(name, Collections.emptyList(), order, minDocCount, rounding.offset(), emptyBucketInfo, formatter,
                keyed, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
