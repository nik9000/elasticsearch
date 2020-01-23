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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BulkReduce;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;

import java.io.IOException;
import java.util.List;

public class MaxBulkResult implements Aggregator.BulkResult {
    private final Aggregator.CommonBulkResult common;
    private final DocValueFormat format;
    private DoubleArray maxes;

    public MaxBulkResult(Aggregator.CommonBulkResult common, DocValueFormat format, DoubleArray maxes) {
        this.common = common;
        this.format = format;
        this.maxes = maxes; // NOCOMMIT check that this isn't closed before writing!
    }

    public MaxBulkResult(StreamInput in) throws IOException {
        common = new Aggregator.CommonBulkResult(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        // TODO this is ulra mega lame
        long size = in.readVLong();
        this.maxes = BigArrays.NON_RECYCLING_INSTANCE.newDoubleArray(size);
        for (int i = 0; i < size; i++) {
            this.maxes.set(i, in.readDouble());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common.writeTo(out);
        out.writeNamedWriteable(format);
        out.writeVLong(maxes.size());
        for (long i = 0; i < maxes.size(); i++) {
            out.writeDouble(maxes.get(i));
        }
    }

    @Override
    public void close() {
        maxes.close();
    }

    @Override
    public String getWriteableName() {
        return MaxAggregationBuilder.NAME;
    }

    @Override
    public InternalMax buildAggregation(long owningBucketOrdinal) {
        LogManager.getLogger().warn("ADFADF {} {} {}", owningBucketOrdinal, common.getName(), maxes.get(owningBucketOrdinal));
        return new InternalMax(common.getName(), maxes.get(owningBucketOrdinal), format, common.getPipelineAggregators(),
                common.getMetaData());
    }

    @Override
    public BulkReduce beginReducing(List<Aggregator.BulkResult> others, ReduceContext ctx) {
        DoubleArray[] o = others.stream().map(r -> ((MaxBulkResult) r).maxes).toArray(DoubleArray[]::new);
        return (myOrd, otherOrds) -> {
            // NOCOMMIT I bet you can get an ord that doesn't exist on our side.
            double max = maxes.get(myOrd);
            for (int i = 0; i < otherOrds.length; i++) {
                max = Double.max(max, o[i].get(otherOrds[i]));
            }
            maxes.set(myOrd, max);
        };
    }
}
