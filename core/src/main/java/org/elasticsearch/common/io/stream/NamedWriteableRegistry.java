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

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for {@link NamedWriteable} objects. Allows to register and retrieve prototype instances of writeable objects
 * given their name.
 */
public class NamedWriteableRegistry {

    private final Map<Class<?>, InnerRegistry<?>> registry = new HashMap<>();

    /**
     * Registers a {@link NamedWriteable} reader given its category.
     */
    public synchronized <T> void register(Class<T> categoryClass, String name, Writeable.Reader<? extends T> reader) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>)registry.get(categoryClass);
        if (innerRegistry == null) {
            innerRegistry = new InnerRegistry<>(categoryClass);
            registry.put(categoryClass, innerRegistry);
        }
        innerRegistry.registerReader(name, reader);
    }

    /**
     * Registers a {@link NamedWriteable} prototype given its category.
     * @deprecated Prefer {@link #register(Class, String, org.elasticsearch.common.io.stream.Writeable.Reader)}.
     */
    @Deprecated
    // We're intentionally rough with types here because we'd like to remove the type parameter from Writeable
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends NamedWriteable> void registerPrototype(Class<T> categoryClass, T prototype) {
        // Work around https://bugs.openjdk.java.net/browse/JDK-8058112
        Writeable.Reader<? extends T> reader = new Writeable.Reader<T>() {
            @Override
            public T readFrom(StreamInput in) throws IOException {
                return (T) prototype.readFrom(in);
            }
        };
        register(categoryClass, prototype.getWriteableName(), reader);
    }

    /**
     * Returns a prototype of the {@link NamedWriteable} object identified by the name provided as argument and its category
     */
    public synchronized <T> Writeable.Reader<? extends T> getReader(Class<T> categoryClass, String name) {
        @SuppressWarnings("unchecked")
        InnerRegistry<T> innerRegistry = (InnerRegistry<T>)registry.get(categoryClass);
        if (innerRegistry == null) {
            throw new IllegalArgumentException("unknown named writeable category [" + categoryClass.getName() + "]");
        }
        return innerRegistry.getReader(name);
    }

    private static class InnerRegistry<T> {

        private final Map<String, Writeable.Reader<? extends T>> registry = new HashMap<>();
        private final Class<T> categoryClass;

        private InnerRegistry(Class<T> categoryClass) {
            this.categoryClass = categoryClass;
        }

        private void registerReader(String name, Writeable.Reader<? extends T> reader) {
            Writeable.Reader<? extends T> existingNamedWriteable = registry.get(name);
            if (existingNamedWriteable != null) {
                throw new IllegalArgumentException(
                        "named writeable [" + categoryClass.getName() + "][" + name + "] is already registered with reader ["
                                + existingNamedWriteable + "] so cannot be registered by with reader [" + reader + "]");
            }
            registry.put(name, reader);
        }

        private Writeable.Reader<? extends T> getReader(String name) {
            Writeable.Reader<? extends T> namedWriteable = registry.get(name);
            if (namedWriteable == null) {
                throw new IllegalArgumentException("unknown named writeable with name [" + name + "] within category [" + categoryClass.getName() + "]");
            }
            return namedWriteable;
        }
    }
}
