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

package org.elasticsearch.index.mapper;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.common.Explicit;

import static java.util.Collections.unmodifiableMap;

/**
 * Should this field be relocated from {@code _source} for more efficient
 * storage and, if so, how?
 */
public enum RelocateTo {
    /**
     * Don't attempt to relocate this field from the source.
     */
    NONE,
    /**
     * Attempt to relocate this field into doc values.
     */
    DOC_VALUES;

    /**
     * By default fields hould not be relocated.
     */
    public static final Explicit<RelocateTo> DEFAULT = new Explicit<>(NONE, false);

    private static final Map<String, RelocateTo> STRING_TO_VALUE;
    static {
        Map<String, RelocateTo> stringToValue = new TreeMap<>();
        for (RelocateTo option : values()) {
            stringToValue.put(option.toString(), option);
        }
        STRING_TO_VALUE = unmodifiableMap(stringToValue);
    }

    static RelocateTo fromString(String s) {
        RelocateTo value = STRING_TO_VALUE.get(s);
        if (s == null) {
            throw new MapperParsingException("[relocate_to] must be one of " + STRING_TO_VALUE.keySet());
        }
        return value;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
