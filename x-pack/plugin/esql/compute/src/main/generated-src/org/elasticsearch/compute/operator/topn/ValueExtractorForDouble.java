/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.PagedBytesRefBuilder;

/**
 * Extracts non-sort-key values for top-n from their {@link DoubleBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class ValueExtractorForDouble implements ValueExtractor {
    static ValueExtractorForDouble extractorFor(TopNEncoder encoder, boolean inKey, DoubleBlock block) {
        DoubleVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForDouble.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForDouble.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForDouble(TopNEncoder encoder, boolean inKey) {
        assert encoder.toUnsortable() == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    // NOCOMMIT remove old BreakingBytesRefBuilder overrides
    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    // NOCOMMIT remove old BreakingBytesRefBuilder overrides
    protected final void actualWriteValue(BreakingBytesRefBuilder values, double value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, values);
    }

    protected final void writeCount(PagedBytesRefBuilder values, int count) {
        values.appendVInt(count);
    }

    protected final void actualWriteValue(PagedBytesRefBuilder values, double value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, values);
    }

    static class ForVector extends ValueExtractorForDouble {
        private final DoubleVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, DoubleVector vector) {
            super(encoder, inKey);
            this.vector = vector;
        }

        // NOCOMMIT remove old BreakingBytesRefBuilder override
        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            writeCount(values, 1);
            if (inKey) {
                // will read results from the key
                return;
            }
            actualWriteValue(values, vector.getDouble(position));
        }

        @Override
        public void writeValue(PagedBytesRefBuilder values, int position) {
            writeCount(values, 1);
            if (inKey) {
                // will read results from the key
                return;
            }
            actualWriteValue(values, vector.getDouble(position));
        }
    }

    static class ForBlock extends ValueExtractorForDouble {
        private final DoubleBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, DoubleBlock block) {
            super(encoder, inKey);
            this.block = block;
        }

        // NOCOMMIT remove old BreakingBytesRefBuilder override
        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            int size = block.getValueCount(position);
            writeCount(values, size);
            if (size == 1 && inKey) {
                // Will read results from the key
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                actualWriteValue(values, block.getDouble(i));
            }
        }

        @Override
        public void writeValue(PagedBytesRefBuilder values, int position) {
            int size = block.getValueCount(position);
            writeCount(values, size);
            if (size == 1 && inKey) {
                // Will read results from the key
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                actualWriteValue(values, block.getDouble(i));
            }
        }
    }
}
