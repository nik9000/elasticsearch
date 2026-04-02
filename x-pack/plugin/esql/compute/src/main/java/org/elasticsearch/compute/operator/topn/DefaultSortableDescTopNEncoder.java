/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefCursor;

class DefaultSortableDescTopNEncoder extends SortableDescTopNEncoder {
    private final DefaultSortableAscTopNEncoder ascEncoder;

    DefaultSortableDescTopNEncoder(DefaultSortableAscTopNEncoder ascEncoder) {
        this.ascEncoder = ascEncoder;
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        throw new IllegalStateException("Cannot find encoder for BytesRef value");
    }

    @Override
    public void encodeBytesRef(BytesRef value, PagedBytesRefBuilder builder) {
        throw new IllegalStateException("Cannot find encoder for BytesRef value");
    }

    @Override
    public BytesRef decodeBytesRef(PagedBytesRefCursor cursor, BytesRef scratch) {
        throw new IllegalStateException("Cannot find encoder for BytesRef value");
    }

    @Override
    public String toString() {
        return "DefaultDesc";
    }

    @Override
    public TopNEncoder toSortable(boolean asc) {
        return asc ? ascEncoder : this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return TopNEncoder.DEFAULT_UNSORTABLE;
    }
}
