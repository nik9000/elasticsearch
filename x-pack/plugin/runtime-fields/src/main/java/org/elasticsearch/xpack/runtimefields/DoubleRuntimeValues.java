/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;

public interface DoubleRuntimeValues {
    public interface Factory {
        LeafFactory newFactory(SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleRuntimeValues newInstance(LeafReaderContext ctx) throws IOException;
    }

    void execute(int docId);

    void sort();

    double value(int idx);

    int count();
}
