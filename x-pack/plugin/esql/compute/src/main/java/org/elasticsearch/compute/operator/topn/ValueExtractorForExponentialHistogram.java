/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;

public class ValueExtractorForExponentialHistogram implements ValueExtractor {
    private final ExponentialHistogramBlock block;

    private final BytesRef scratch = new BytesRef();
    private final ReusableTopNEncoderOutput reusableOutput = new ReusableTopNEncoderOutput();
    private final PagedReusableTopNEncoderOutput pagedReusableOutput = new PagedReusableTopNEncoderOutput();

    ValueExtractorForExponentialHistogram(TopNEncoder encoder, ExponentialHistogramBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    // NOCOMMIT remove old BreakingBytesRefBuilder override
    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        // number of multi-values first for compatibility with ValueExtractorForNull
        if (block.isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(0, values);
        } else {
            assert block.getValueCount(position) == 1 : "Multi-valued ExponentialHistogram blocks are not supported in TopN";
            TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(1, values);
            int valueIndex = block.getFirstValueIndex(position);
            reusableOutput.target = values;
            block.serializeExponentialHistogram(valueIndex, reusableOutput, scratch);
        }
    }

    @Override
    public void writeValue(PagedBytesRefBuilder values, int position) {
        // number of multi-values first for compatibility with ValueExtractorForNull
        if (block.isNull(position)) {
            values.appendVInt(0);
        } else {
            assert block.getValueCount(position) == 1 : "Multi-valued ExponentialHistogram blocks are not supported in TopN";
            values.appendVInt(1);
            int valueIndex = block.getFirstValueIndex(position);
            pagedReusableOutput.target = values;
            block.serializeExponentialHistogram(valueIndex, pagedReusableOutput, scratch);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForExponentialHistogram";
    }

    private static final class ReusableTopNEncoderOutput implements ExponentialHistogramBlock.SerializedOutput {
        BreakingBytesRefBuilder target;

        @Override
        public void appendDouble(double value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, target);
        }

        @Override
        public void appendLong(long value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(value, target);
        }

        @Override
        public void appendBytesRef(BytesRef value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBytesRef(value, target);
        }
    }

    private static final class PagedReusableTopNEncoderOutput implements ExponentialHistogramBlock.SerializedOutput {
        PagedBytesRefBuilder target;

        @Override
        public void appendDouble(double value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, target);
        }

        @Override
        public void appendLong(long value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(value, target);
        }

        @Override
        public void appendBytesRef(BytesRef value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBytesRef(value, target);
        }
    }
}
