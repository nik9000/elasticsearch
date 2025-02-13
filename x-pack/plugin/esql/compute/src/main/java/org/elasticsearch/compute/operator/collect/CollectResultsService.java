/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.collect;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.StoredAsyncResponse;

import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

public class CollectResultsService {
    private final AsyncTaskIndexService<StoredAsyncResponse<CollectedMetadata>> metadataStore;
    private final AsyncTaskIndexService<StoredAsyncResponse<Page>> pageStore;
    private final CircuitBreaker breaker;

    public CollectResultsService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedWriteableRegistry registry,
        BigArrays bigArrays
    ) {
        // NOCOMMIT make sure we use all of this.
        metadataStore = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            in -> new StoredAsyncResponse<>(CollectedMetadata::new, in),
            registry,
            bigArrays
        );
        pageStore = new AsyncTaskIndexService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            clusterService,
            threadPool.getThreadContext(),
            client,
            ASYNC_SEARCH_ORIGIN,
            in -> new StoredAsyncResponse<>(Page::new, in),
            registry,
            bigArrays
        );
        this.breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
    }

    void save(String prefix, int sequence, Page page, Instant expirationTime, ActionListener<DocWriteResponse> listener) {
        pageStore.createResponse(
            pageId(prefix, sequence),
            Map.of(),
            new StoredAsyncResponse<>(page, expirationTime.toEpochMilli()),
            listener
        );
    }

    void save(String name, CollectedMetadata metadata, Instant expirationTime, ActionListener<DocWriteResponse> listener) {
        metadataStore.createResponse(name, Map.of(), new StoredAsyncResponse<>(metadata, expirationTime.toEpochMilli()), listener);
    }

    void loadMetadata(String name, ActionListener<CollectedMetadata> listener) {
        metadataStore.getResponse(new AsyncExecutionId(name, new TaskId("dummy")), false, new ActionListener<>() {
            @Override
            public void onResponse(StoredAsyncResponse<CollectedMetadata> r) {
                if (r.getException() == null) {
                    listener.onResponse(r.getResponse());
                } else {
                    listener.onFailure(r.getException());
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    Page decodePage(BlockFactory blockFactory, BytesReference source) {
        /*
         * Reserve twice memory of the source length and then let go.
         * We're going to be parsing from xcontent and then decoding
         * base64. And the Page itself is going to reserve space too
         * while it's alive. That's all cool. Probably an overestimate
         * but we're going to be accurate once we've let go of the
         * temporary base64 and json.
         */
        final long reservedBytes = source.length() * 2L;
        // NOCOMMIT double check how reserves work. Should leave some underlying reserved.
        breaker.addEstimateBytesAndMaybeBreak(reservedBytes, "decode collected");
        try {
            StoredAsyncResponse<Page> r = pageStore.parseResponseFromIndex(
                source,
                false,
                true,
                in -> new BlockStreamInput(in, blockFactory),
                () -> new ResourceNotFoundException("can't find Page")
            );
            if (r.getException() == null) {
                throw new RuntimeException("page contained an error", r.getException());
            }
            return r.getResponse();
        } finally {
            breaker.addWithoutBreaking(-reservedBytes);
        }
    }

    static String pageId(String prefix, int sequence) {
        return prefix + ":" + sequence;
    }
}
