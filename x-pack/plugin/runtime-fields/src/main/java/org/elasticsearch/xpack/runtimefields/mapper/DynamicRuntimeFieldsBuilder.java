/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.RuntimeField;

public final class DynamicRuntimeFieldsBuilder implements org.elasticsearch.index.mapper.DynamicRuntimeFieldsBuilder {

    public static final DynamicRuntimeFieldsBuilder INSTANCE = new DynamicRuntimeFieldsBuilder();

    private DynamicRuntimeFieldsBuilder() {}

    @Override
    public RuntimeField newDynamicStringField(String name) {
        return new KeywordScriptFieldType(name);
    }

    @Override
    public RuntimeField newDynamicLongField(String name) {
        return new LongScriptFieldType(name);
    }

    @Override
    public RuntimeField newDynamicDoubleField(String name) {
        return new DoubleScriptFieldType(name);
    }

    @Override
    public RuntimeField newDynamicBooleanField(String name) {
        return new BooleanScriptFieldType(name);
    }

    @Override
    public RuntimeField newDynamicDateField(String name, DateFormatter dateFormatter) {
        return new DateScriptFieldType(name, dateFormatter);
    }
}
