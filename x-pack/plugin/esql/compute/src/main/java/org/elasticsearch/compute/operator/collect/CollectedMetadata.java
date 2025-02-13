/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.collect;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;
import java.util.Objects;

public class CollectedMetadata implements Writeable {
    private final String prefix;
    private final int pageIds;

    public CollectedMetadata(String prefix, int pageIds) {
        this.prefix = prefix;
        this.pageIds = pageIds;
    }

    CollectedMetadata(StreamInput in) throws IOException {
        prefix = in.readString();
        pageIds = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(prefix);
        out.writeInt(pageIds);
    }

    String pageId(int page) {
        assert page <= pageIds;
        return CollectResultsService.pageId(prefix, page);
    }

    public int pageIds() {
        return pageIds;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectedMetadata that = (CollectedMetadata) o;
        return pageIds == that.pageIds && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefix, pageIds);
    }
}
