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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class NamedXContentRegistry {
    /**
     * The empty {@link NamedXContentRegistry} for use when you are sure that you aren't going to call
     * {@link XContentParser#namedXContent(Class, String, Object)}.
     */
    public static final NamedXContentRegistry EMPTY = new NamedXContentRegistry(emptyList());

    public interface FromXContent<T, C> {
        /**
         * Parses an object with the type T from parser.
         */
        T fromXContent(XContentParser parser, C context) throws IOException;
    }
    public static class Entry {
        /** The class that this entry can read. */
        public final Class<?> categoryClass;

        /** A name for the entry which is unique within the {@link #categoryClass}. */
        public final String name;

        /** A reader capability of reading the entry's class. */
        public final FromXContent<?, ?> reader;

        /** Creates a new entry which can be stored by the registry. */
        public <T> Entry(Class<T> categoryClass, String name, FromXContent<? extends T, ?> reader) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.reader = Objects.requireNonNull(reader);
        }
    }

    
    private final Map<Class<?>, Map<String, FromXContent<?, ?>>> registry;

    public NamedXContentRegistry(List<Entry> entries) {
        registry = unmodifiableMap(entries.stream().collect(groupingBy(e -> e.categoryClass,
                collectingAndThen(toMap(e -> e.name, e -> e.reader, (name, reader) -> {
                    throw new IllegalArgumentException("[" + name + "] already registered");
                }), Collections::unmodifiableMap))));
    }

    /**
     * Lookup a reader, throwing an exception if the reader isn't found.
     */
    public <T, C> FromXContent<? extends T, C> getFromXContent(Class<T> categoryClass, String name, XContentLocation location) {
        Map<String, FromXContent<?, ?>> parsers = registry.get(categoryClass);
        if (parsers == null) {
            throw new ElasticsearchException("Unknown NamedXContent category [" + categoryClass.getName() + "]");
        }
        @SuppressWarnings("unchecked")
        FromXContent<? extends T, C> reader = (FromXContent<? extends T, C>) parsers.get(name);
        if (reader == null) {
            throw new ParsingException(location, "Unknown NamedXContent [" + categoryClass.getName() + "][" + name + "]");
        }
        return reader;
    }
}
