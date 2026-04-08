/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;

public record PromqlParamInfo(String name, PromqlDataType type, String description, boolean optional, boolean child) {
    public static PromqlParamInfo child(String name, PromqlDataType type, String description) {
        return new PromqlParamInfo(name, type, description, false, true);
    }

    public static PromqlParamInfo of(String name, PromqlDataType type, String description) {
        return new PromqlParamInfo(name, type, description, false, false);
    }

    public static PromqlParamInfo optional(String name, PromqlDataType type, String description) {
        return new PromqlParamInfo(name, type, description, true, false);
    }
}
