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

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.PostRegistrationSingleton;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.SettingsModule;

import java.util.Collection;

import static java.util.Collections.singleton;

public class CircuitBreakerModule extends AbstractModule implements PostRegistrationSingleton.Holder {

    public static final String TYPE_KEY = "indices.breaker.type";

    private final PostRegistrationSingleton<CircuitBreakerService> circuitBreakerService;

    public CircuitBreakerModule(SettingsModule settingsModule) {
        circuitBreakerService = new PostRegistrationSingleton<>(() -> {
            String type = settingsModule.settings().get(TYPE_KEY, "hierarchy");
            switch (type) {
            case "hierarchy":
                return new HierarchyCircuitBreakerService(settingsModule.settings(), settingsModule.clusterSettings());
            case "none":
                return new NoneCircuitBreakerService();
            default:
                throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
            }
        });
    }

    public CircuitBreakerService circuitBreakerService() {
        return circuitBreakerService.get();
    }

    @Override
    protected void configure() {
        bind(CircuitBreakerService.class).toProvider(circuitBreakerService);
    }

    @Override
    public Collection<PostRegistrationSingleton<?>> postRegistrationSingletons() {
        return singleton(circuitBreakerService);
    }
}
