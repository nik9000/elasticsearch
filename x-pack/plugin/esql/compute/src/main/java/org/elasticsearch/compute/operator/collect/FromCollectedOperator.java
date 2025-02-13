/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.collect;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FromCollectedOperator extends SourceOperator {
    public record Factory(CollectResultsService resultsService, IndexShard shard, CollectedMetadata metadata)
        implements
            SourceOperator.SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new FromCollectedOperator(resultsService, driverContext.blockFactory(), shard, metadata);
        }

        @Override
        public String describe() {
            return "";
        }
    }

    private final Map<LeafReader, PerLeafReader> perLeafReader = new HashMap<>();
    private final CollectResultsService resultsService;
    private final BlockFactory blockFactory;
    private final IndexShard shard;
    private final StoredFieldLoader storedFieldLoader;
    private final CollectedMetadata metadata;
    private Engine.GetResult[] pageGets;
    private int current;

    public FromCollectedOperator(
        CollectResultsService resultsService,
        BlockFactory blockFactory,
        IndexShard shard,
        CollectedMetadata metadata
    ) {
        this.resultsService = resultsService;
        this.blockFactory = blockFactory;
        this.shard = shard;
        this.metadata = metadata;
        this.storedFieldLoader = StoredFieldLoader.create(true, Set.of());
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public Page getOutput() {
        try {
            if (pageGets == null) {
                // NOCOMMIT security testing
                findDocIds();
                return null;
            }
            Engine.GetResult get = pageGets[current++];
            PerLeafReader perLeaf = perLeaf(get);
            BytesReference source = perLeaf.load(get.docIdAndVersion().docId);
            return resultsService.decodePage(blockFactory, source);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        Releasables.close(Releasables.wrap(pageGets));
    }

    private void findDocIds() throws IOException {
        this.pageGets = new Engine.GetResult[metadata.pageIds()];
        boolean success = false;
        try {
            for (int page = 0; page < pageGets.length; page++) {
                this.pageGets[page] = shard.get(new Engine.Get(false, false, metadata.pageId(page)));
            }
            success = true;
        } finally {
            if (success == false) {
                this.close();
            }
        }
    }

    private static class PerLeafReader {
        private final LeafStoredFieldLoader storedFieldLoader;

        private PerLeafReader(LeafStoredFieldLoader storedFieldLoader) {
            this.storedFieldLoader = storedFieldLoader;
        }

        public BytesReference load(int docId) throws IOException {
            storedFieldLoader.advanceTo(docId); // NOCOMMIT is it ok to go out of order here?
            return storedFieldLoader.source();
        }
    }

    private PerLeafReader perLeaf(Engine.GetResult pageGet) throws IOException {
        PerLeafReader perLeaf = perLeafReader.get(pageGet.docIdAndVersion().reader);
        if (perLeaf != null) {
            return perLeaf;
        }

        int docsCount = 0;
        for (Engine.GetResult r : pageGets) {
            if (r.docIdAndVersion().reader == pageGet.docIdAndVersion().reader) {
                docsCount++;
            }
        }
        int[] docs = new int[docsCount];
        {
            int d = 0;
            for (Engine.GetResult r : pageGets) {
                if (r.docIdAndVersion().reader == pageGet.docIdAndVersion().reader) {
                    docs[d] = pageGet.docIdAndVersion().docId;
                }
            }
        }
        Arrays.sort(docs);

        LeafStoredFieldLoader storedFieldLoader = this.storedFieldLoader.getLoader(pageGet.docIdAndVersion().reader.getContext(), docs);
        perLeaf = new PerLeafReader(storedFieldLoader);
        perLeafReader.put(pageGet.docIdAndVersion().reader, perLeaf);
        return perLeaf;
    }
}
