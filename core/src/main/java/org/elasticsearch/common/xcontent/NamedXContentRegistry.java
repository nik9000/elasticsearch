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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class NamedXContentRegistry {
    /**
     * The empty {@link NamedXContentRegistry} for use when you are sure that you aren't going to call
     * {@link XContentParser#namedObject(Class, String, Object)}.
     */
    public static final NamedXContentRegistry EMPTY = new NamedXContentRegistry(emptyList());

    public interface FromXContent<T> {
        /**
         * Parses an object with the type T from parser.
         */
        T fromXContent(XContentParser parser, Object context) throws IOException;
    }
    public static class Entry {
        /** The class that this entry can read. */
        public final Class<?> categoryClass;

        /** A name for the entry which is unique within the {@link #categoryClass}. */
        public final ParseField name;

        /** A reader capability of reading the entry's class. */
        private final FromXContent<?> parser;

        /** Creates a new entry which can be stored by the registry. */
        public <T> Entry(Class<T> categoryClass, ParseField name, FromXContent<? extends T> reader) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.parser = Objects.requireNonNull(reader);
        }
    }

    
    private final Map<Class<?>, Map<String, Entry>> registry;

    public NamedXContentRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = emptyMap();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort((e1, e2) -> e1.categoryClass.getName().compareTo(e2.categoryClass.getName()));

        Map<Class<?>, Map<String, Entry>> registry = new HashMap<>();
        Map<String, Entry> parsers = null;
        Class<?> currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, unmodifiableMap(parsers));
                }
                parsers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            for (String name : entry.name.getAllNamesIncludedDeprecated()) {
                Object old = parsers.put(name, entry);
                if (old != null) {
                    throw new IllegalArgumentException("NamedXContent [" + currentCategory.getName() + "][" + entry.name + "]" +
                        " is already registered for [" + old.getClass().getName() + "]," +
                        " cannot register [" + entry.parser.getClass().getName() + "]");
                }
            }
        }
        // handle the last category
        registry.put(currentCategory, unmodifiableMap(parsers));

        this.registry = unmodifiableMap(registry);
    }

    /**
     * Parse a named object, throwing an exception if the parser isn't found.
     */
    public <T, C> T parseNamedObject(Class<T> categoryClass, String name, XContentParser parser, C context) throws IOException {
        Map<String, Entry> parsers = registry.get(categoryClass);
        if (parsers == null) {
            throw new ElasticsearchException("Unknown NamedXContent category [" + categoryClass.getName() + "]");
        }
        Entry entry = parsers.get(name);
        if (entry == null) {
            throw new ParsingException(parser.getTokenLocation(), "Unknown NamedXContent [" + categoryClass.getName() + "][" + name + "]");
        }
        if (false == entry.name.match(name, false)) {
            throw new ParsingException(parser.getTokenLocation(),
                    "Unknown NamedXContent [" + categoryClass.getName() + "][" + name + "]: Parser didn't match");
        }
        return categoryClass.cast(entry.parser.fromXContent(parser, context));
    }
}
