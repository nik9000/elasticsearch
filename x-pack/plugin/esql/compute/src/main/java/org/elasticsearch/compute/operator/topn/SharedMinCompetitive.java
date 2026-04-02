/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesRef;
import org.elasticsearch.common.bytes.PagedBytesRefBuilder;
import org.elasticsearch.common.bytes.PagedBytesRefCursor;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SideChannel;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.util.List;

import static org.elasticsearch.compute.operator.topn.TopNOperator.BIG_NULL;
import static org.elasticsearch.compute.operator.topn.TopNOperator.SMALL_NULL;

/**
 * A thread safe, shared holder for the min competitive value from a
 * set of {@link TopNOperator}s.
 */
public class SharedMinCompetitive extends SideChannel {
    public record KeyConfig(ElementType elementType, TopNEncoder encoder, boolean asc, boolean nullsFirst) {}

    public static class Supplier extends SideChannel.Supplier<SharedMinCompetitive> {
        private final PageCacheRecycler recycler;
        private final CircuitBreaker breaker;
        private final List<KeyConfig> keyConfigs;

        public Supplier(PageCacheRecycler recycler, CircuitBreaker breaker, List<KeyConfig> keyConfig) {
            this.recycler = recycler;
            this.breaker = breaker;
            this.keyConfigs = keyConfig;
        }

        @Override
        protected SharedMinCompetitive build() {
            return new SharedMinCompetitive(recycler, breaker, keyConfigs, this);
        }

        public List<KeyConfig> keyConfigs() {
            return keyConfigs;
        }
    }

    private final PagedBytesRefBuilder value;
    private final List<KeyConfig> keyConfig;

    private SharedMinCompetitive(PageCacheRecycler recycler, CircuitBreaker breaker, List<KeyConfig> keyConfig, Supplier supplier) {
        super(supplier);
        this.value = new PagedBytesRefBuilder(breaker, "min_competitive", 0, recycler);
        this.keyConfig = keyConfig;
    }

    public List<KeyConfig> configs() {
        return keyConfig;
    }

    /**
     * Offer an update to the min competitive value.
     * @param minCompetitive if it is accepted then the bytes are copied
     * @return whether the update was accepted. {@code false} here means the minimum
     *         value in the local top n is greater than or equal to the minimum
     *         competitive value already recorded
     */
    public boolean offer(PagedBytesRefBuilder minCompetitive) {
        synchronized (value) {
            if (value.length() > 0 && value.compareTo(minCompetitive) <= 0) {
                return false;
            }
            value.clear();
            value.append(minCompetitive);
            return true;
        }
    }

    /**
     * Read the min competitive value. This will return {@code null} if there
     * isn't yet a min competitive value. Otherwise, this will return a
     * {@link Page} that contains single-position, single-valued {@link Block}s.
     */
    @Nullable
    public Page get(BlockFactory blockFactory) {
        int length = value.length();
        if (length == 0) {
            return null;
        }
        try (
            PagedBytesRefBuilder copy = new PagedBytesRefBuilder(
                blockFactory.breaker(),
                "min_competitive_copy",
                length,
                blockFactory.bigArrays().recycler()
            )
        ) {
            synchronized (value) {
                if (value.length() == 0) {
                    // Not assigned anything yet
                    return null;
                }
                copy.append(value);
            }
            ResultBuilder[] builders = new ResultBuilder[keyConfig.size()];
            try (PagedBytesRef ref = copy.build()) {
                PagedBytesRefCursor cursor = new PagedBytesRefCursor(ref);
                for (int i = 0; i < builders.length; i++) {
                    KeyConfig config = keyConfig.get(i);
                    ResultBuilder builder = ResultBuilder.resultBuilderFor(blockFactory, config.elementType, config.encoder, true, 1);
                    builders[i] = builder;
                    if (cursor.readByte() == (config.nullsFirst ? SMALL_NULL : BIG_NULL)) {
                        builder.decodeValue(new BytesRef(new byte[] { 0 }));
                        continue;
                    }
                    builder.decodeKey(cursor, config.asc());
                    builder.decodeValue(new BytesRef(new byte[] { 1 }));
                }
                return new Page(ResultBuilder.buildAll(builders));
            } finally {
                Releasables.close(builders);
            }
        }
    }

    @Override
    protected void closeSideChannel() {
        value.close();
    }
}
