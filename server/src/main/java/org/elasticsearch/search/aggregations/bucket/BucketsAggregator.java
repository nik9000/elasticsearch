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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;

import static java.util.stream.Collectors.toList;

public abstract class BucketsAggregator extends AggregatorBase {

    private final BigArrays bigArrays;
    private final IntConsumer multiBucketConsumer;
    private IntArray docCounts;

    public BucketsAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        bigArrays = context.bigArrays();
        docCounts = bigArrays.newIntArray(1, true);
        if (context.aggregations() != null) {
            multiBucketConsumer = context.aggregations().multiBucketConsumer();
        } else {
            multiBucketConsumer = (count) -> {};
        }
    }

    /**
     * Return an upper bound of the maximum bucket ordinal seen so far.
     */
    public final long maxBucketOrd() {
        return docCounts.size();
    }

    /**
     * Ensure there are at least <code>maxBucketOrd</code> buckets available.
     */
    public final void grow(long maxBucketOrd) {
        docCounts = bigArrays.grow(docCounts, maxBucketOrd);
    }

    /**
     * Utility method to collect the given doc in the given bucket (identified by the bucket ordinal)
     */
    public final void collectBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        grow(bucketOrd + 1);
        collectExistingBucket(subCollector, doc, bucketOrd);
    }

    /**
     * Same as {@link #collectBucket(LeafBucketCollector, int, long)}, but doesn't check if the docCounts needs to be re-sized.
     */
    public final void collectExistingBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        docCounts.increment(bucketOrd, 1);
        subCollector.collect(doc, bucketOrd);
    }

    public final void mergeBuckets(long[] mergeMap, long newNumBuckets) {
        try (IntArray oldDocCounts = docCounts) {
            docCounts = bigArrays.newIntArray(newNumBuckets, true);
            docCounts.fill(0, newNumBuckets, 0);
            for (int i = 0; i < oldDocCounts.size(); i++) {
                int docCount = oldDocCounts.get(i);

                // Skip any in the map which have been "removed", signified with -1
                if (docCount != 0 && mergeMap[i] != -1) {
                    docCounts.increment(mergeMap[i], docCount);
                }
            }
        }
    }

    public IntArray getDocCounts() {
        return docCounts;
    }

    /**
     * Utility method to increment the doc counts of the given bucket (identified by the bucket ordinal)
     */
    public final void incrementBucketDocCount(long bucketOrd, int inc) {
        docCounts = bigArrays.grow(docCounts, bucketOrd + 1);
        docCounts.increment(bucketOrd, inc);
    }

    /**
     * Utility method to return the number of documents that fell in the given bucket (identified by the bucket ordinal)
     */
    public final int bucketDocCount(long bucketOrd) {
        if (bucketOrd >= docCounts.size()) {
            // This may happen eg. if no document in the highest buckets is accepted by a sub aggregator.
            // For example, if there is a long terms agg on 3 terms 1,2,3 with a sub filter aggregator and if no document with 3 as a value
            // matches the filter, then the filter will never collect bucket ord 3. However, the long terms agg will call
            // bucketAggregations(3) on the filter aggregator anyway to build sub-aggregations.
            return 0;
        } else {
            return docCounts.get(bucketOrd);
        }
    }

    /**
     * Adds {@code count} buckets to the global count for the request and fails if this number is greater than
     * the maximum number of buckets allowed in a response
     */
    protected final void consumeBucketsAndMaybeBreak(int count) {
        multiBucketConsumer.accept(count);
    }

    /**
     * Required method to build the child aggregations of the given bucket (identified by the bucket ordinal).
     */
    protected final InternalAggregations bucketAggregations(long bucket) throws IOException {
        final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = subAggregators[i].buildAggregation(bucket);
        }
        return new InternalAggregations(Arrays.asList(aggregations));
    }

    /**
     * Utility method to build empty aggregations of the sub aggregators.
     */
    protected final InternalAggregations bucketEmptyAggregations() {
        final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = subAggregators[i].buildEmptyAggregation();
        }
        return new InternalAggregations(Arrays.asList(aggregations));
    }

    @Override
    public final void close() {
        try (Releasable releasable = docCounts) {
            super.close();
        }
    }

    protected boolean subsSupportBulkResult() {
        return Arrays.stream(subAggregators).allMatch(Aggregator::supportsBulkResult);
    }

    protected final BucketsBulkResult buildBucketsBulkResult() {
        List<BulkResult> subResults = new ArrayList<>(subAggregators.length);
        for (Aggregator sub : subAggregators) {
            assert sub.supportsBulkResult() : "["  + sub + "] doesn't support bulk result but we tried to use them";
            subResults.add(sub.buildBulkResult());
        }
        return new BucketsBulkResult(subResults, docCounts);
    }

    public static class BucketsBulkResult implements Writeable, Releasable {
        private final List<BulkResult> subResults;
        private final IntArray docCounts; // NOCOMMIT would we have to grow the ordinals?

        public BucketsBulkResult(List<BulkResult> subResults, IntArray docCounts) {
            this.subResults = subResults;
            this.docCounts = docCounts;
        }

        public BucketsBulkResult(StreamInput in) throws IOException {
            subResults = in.readNamedWriteableList(BulkResult.class);
            // TODO this is ulra mega lame
            long size = in.readVLong();
            docCounts = BigArrays.NON_RECYCLING_INSTANCE.newIntArray(size);
            for (long i = 0; i < size; i++) {
                docCounts.set(i, in.readVInt());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(subResults);
            out.writeVLong(docCounts.size());
            for (long i = 0; i < docCounts.size(); i++) {
                out.writeVInt(docCounts.get(i));
            }
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(subResults), docCounts);
        }

        public long bucketDocCount(long bucketOrd) {
            if (bucketOrd >= docCounts.size()) {
                // See BucketsAggregator.bucketDocCount for explanation of this check
                return 0;
            }
            return docCounts.get(bucketOrd);
        }

        public InternalAggregations bucketAggregations(long bucketOrd) {
            final InternalAggregation[] aggregations = new InternalAggregation[subResults.size()];
            for (int i = 0; i < subResults.size(); i++) {
                aggregations[i] = subResults.get(i).buildAggregation(bucketOrd);
            }
            return new InternalAggregations(Arrays.asList(aggregations));
        }

        public InternalAggregations buildEmptySubAggregations() {
            final InternalAggregation[] aggregations = new InternalAggregation[subResults.size()];
            for (int i = 0; i < subResults.size(); i++) {
                aggregations[i] = subResults.get(i).buildEmptyAggregation();
            }
            return new InternalAggregations(Arrays.asList(aggregations));        }

        /**
         * Begin reducing many BulkResults into this one.
         */
        public Aggregator.BulkReduce beginReducing(List<BucketsBulkResult> others, ReduceContext ctx) {
            Aggregator.BulkReduce[] subs = new Aggregator.BulkReduce[subResults.size()];
            for (int i = 0; i < subs.length; i++) {
                final int index = i;
                subs[i] = subResults.get(i).beginReducing(
                        others.stream().map(o -> o.subResults.get(index)).collect(toList()), ctx);
            }
            return (myOrd, otherOrds) -> {
                for (Aggregator.BulkReduce sub : subs) {
                    sub.reduce(myOrd, otherOrds);
                }
            };
        }
    }
}
