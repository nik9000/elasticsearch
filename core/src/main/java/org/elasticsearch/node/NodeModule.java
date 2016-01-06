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

package org.elasticsearch.node;

import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.BiFunction;

/**
 *
 */
public class NodeModule extends AbstractModule {

    public static final String CIRCUIT_BREAKER_TYPE = "indices.breaker.type";

    private final Node node;
    private final MonitorService monitorService;
    private final ThreadPool threadPool;
    private final SettingsModule settingsModule;
    private final CircuitBreakerModule circuitBreakerModule;

    private PageCacheRecycler pageCacheRecycler;
    private BigArrays bigArrays;

    // pkg private so tests can mock
    BiFunction<Settings, ThreadPool, PageCacheRecycler> pageCacheRecyclerImpl = PageCacheRecycler::new;
    BiFunction<PageCacheRecycler, CircuitBreakerService, BigArrays> bigArraysImpl = BigArrays::new;

    public NodeModule(Node node, MonitorService monitorService, ThreadPool threadPool, SettingsModule settingsModule,
            CircuitBreakerModule circuitBreakerModule) {
        this.node = node;
        this.monitorService = monitorService;
        this.threadPool = threadPool;
        this.settingsModule = settingsModule;
        this.circuitBreakerModule = circuitBreakerModule;
    }

    public PageCacheRecycler pageCacheRecycler() {
        if (pageCacheRecycler == null) {
            pageCacheRecycler = pageCacheRecyclerImpl.apply(settingsModule.settings(), threadPool);
        }
        return pageCacheRecycler;
    }

    public BigArrays bigArrays() {
        if (bigArrays == null) {
            bigArrays = bigArraysImpl.apply(pageCacheRecycler(), circuitBreakerModule.circuitBreakerService());
        }
        return bigArrays;
    }

    @Override
    protected void configure() {
        bind(PageCacheRecycler.class).toInstance(pageCacheRecycler());
        bind(BigArrays.class).toInstance(bigArrays());

        bind(Node.class).toInstance(node);
        bind(MonitorService.class).toInstance(monitorService);
        bind(NodeService.class).asEagerSingleton();
    }
}
