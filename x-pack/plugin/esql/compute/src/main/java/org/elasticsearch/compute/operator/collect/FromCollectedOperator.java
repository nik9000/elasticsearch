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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;

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
    public record Factory(CollectResultsService resultsService, IndexShard shard, AsyncExecutionId mainId, CollectedMetadata metadata)
        implements
            SourceOperator.SourceOperatorFactory {
        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new FromCollectedOperator(resultsService, driverContext.blockFactory(), shard, mainId, metadata);
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
    private final AsyncExecutionId mainId;
    private final CollectedMetadata metadata;
    private Engine.GetResult[] pageGets;
    private int current;

    private long findNanos;
    private long loadNanos;
    private int pagesEmitted;
    private long rowsEmitted;

    public FromCollectedOperator(
        CollectResultsService resultsService,
        BlockFactory blockFactory,
        IndexShard shard,
        AsyncExecutionId mainId,
        CollectedMetadata metadata
    ) {
        this.resultsService = resultsService;
        this.blockFactory = blockFactory;
        this.shard = shard;
        this.mainId = mainId;
        this.metadata = metadata;
        this.storedFieldLoader = StoredFieldLoader.create(true, Set.of());
    }

    @Override
    public void finish() {}

    @Override
    public boolean isFinished() {
        assert current <= metadata.pageCount();
        return current >= metadata.pageCount();
    }

    @Override
    public Page getOutput() {
        try {
            if (pageGets == null) {
                // NOCOMMIT security testing
                findDocIds();
                return null;
            }
            long start = System.nanoTime();
            Engine.GetResult get = pageGets[current++];
            PerLeafReader perLeaf = perLeaf(get);
            BytesReference source = perLeaf.load(get.docIdAndVersion().docId);
            Page page = resultsService.decodePage(blockFactory, source);
            loadNanos += System.nanoTime() - start;
            pagesEmitted++;
            rowsEmitted += page.getPositionCount();
            return page;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (pageGets != null) {
            Releasables.close(pageGets);
        }
    }

    private void findDocIds() throws IOException {
        long start = System.nanoTime();
        pageGets = new Engine.GetResult[metadata.pageCount()];
        for (int page = 0; page < pageGets.length; page++) {
            String pageId = CollectResultsService.pageId(mainId, page);
            pageGets[page] = shard.get(new Engine.Get(true, true, pageId));
            if (pageGets[page].exists() == false) {
                throw new IllegalArgumentException("couldn't find a page [" + pageId + "]");
            }
        }
        findNanos += System.nanoTime() - start;
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

    @Override
    public String toString() {
        return "FromCollectedOperator[mainId=" + mainId + ']';
    }

    @Override
    public Operator.Status status() {
        return new Status(findNanos, loadNanos, current, pagesEmitted, rowsEmitted);
    }

    public record Status(long findNanos, long loadNanos, int current, int pagesEmitted, long rowsEmitted) implements Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "from_collected",
            Status::new
        );

        public Status(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVInt(), in.readVInt(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(findNanos);
            out.writeVLong(loadNanos);
            out.writeVInt(current);
            out.writeVInt(pagesEmitted);
            out.writeVLong(rowsEmitted);
        }

        @Override
        public String getWriteableName() {
            return "from_collected";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ESQL_COLLECT;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("find_nanos", findNanos);
            if (builder.humanReadable()) {
                builder.field("find_time", TimeValue.timeValueNanos(findNanos));
            }
            builder.field("load_nanos", loadNanos);
            if (builder.humanReadable()) {
                builder.field("load_time", TimeValue.timeValueNanos(loadNanos));
            }
            builder.field("current", current);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }
    }
}
