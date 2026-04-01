/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.PagedBytesRefBuilder;

class ValueExtractorForNull implements ValueExtractor {
    // NOCOMMIT remove old BreakingBytesRefBuilder override
    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        /*
         * Write 0 values which can be read by *any* result builder and will always
         * make a null value.
         */
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(0, values);
    }

    @Override
    public void writeValue(PagedBytesRefBuilder values, int position) {
        /*
         * Write 0 values which can be read by *any* result builder and will always
         * make a null value.
         */
        values.appendVInt(0);
    }

    @Override
    public String toString() {
        return "ValueExtractorForNull";
    }
}
