/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.PostRegistrationSingleton;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;

public class BigArraysModule extends AbstractModule implements PostRegistrationSingleton.Holder{
    private final PostRegistrationSingleton<PageCacheRecycler> pageCacheRecycler;
    private final PostRegistrationSingleton<BigArrays> bigArrays;

    // pkg private so tests can mock
    BiFunction<Settings, ThreadPool, PageCacheRecycler> pageCacheRecyclerImpl = PageCacheRecycler::new;
    BiFunction<PageCacheRecycler, CircuitBreakerService, BigArrays> bigArraysImpl = BigArrays::new;

    /**
     * Build with dependencies. Note that circuitBreakerModule can be null and
     * if it is then the BigArrays are built without circuit breakers.
     */
    public BigArraysModule(ThreadPool threadPool, SettingsModule settingsModule,
            @Nullable CircuitBreakerModule circuitBreakerModule) {
        pageCacheRecycler = new PostRegistrationSingleton<>(() -> pageCacheRecyclerImpl.apply(settingsModule.settings(), threadPool));
        bigArrays = new PostRegistrationSingleton<>(() -> bigArraysImpl.apply(pageCacheRecycler(),
                circuitBreakerModule == null ? null : circuitBreakerModule.circuitBreakerService()));
    }

    public PageCacheRecycler pageCacheRecycler() {
        return pageCacheRecycler.get();
    }

    public BigArrays bigArrays() {
        return bigArrays.get();
    }

    @Override
    protected void configure() {
        bind(PageCacheRecycler.class).toProvider(pageCacheRecycler);
        bind(BigArrays.class).toProvider(bigArrays);
    }

    @Override
    public Collection<PostRegistrationSingleton<?>> postRegistrationSingletons() {
        return Arrays.asList(pageCacheRecycler, bigArrays);
    }
}
