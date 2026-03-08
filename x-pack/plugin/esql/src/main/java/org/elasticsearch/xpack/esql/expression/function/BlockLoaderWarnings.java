/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;

/**
 * Shim between the {@link org.elasticsearch.index.mapper.blockloader.Warnings} in server and
 * our {@link Warnings}. Also adds laziness because our {@link Warnings} are a little expensive
 * on creation and {@link org.elasticsearch.index.mapper.blockloader.Warnings} wants to be
 * cheap to create.
 */
public class BlockLoaderWarnings implements org.elasticsearch.index.mapper.blockloader.Warnings {
    private final @Nullable List<String> warnings;
    private final Source source;
    private Warnings delegate;

    public BlockLoaderWarnings(@Nullable List<String> warnings, Source source) {
        this.warnings = warnings;
        this.source = source;
    }

    @Override
    public void registerException(Class<? extends Exception> exceptionClass, String message) {
        if (delegate == null) {
            delegate = Warnings.createOnlyWarnings(warnings, source);
        }
        delegate.registerException(exceptionClass, message);
    }

    @Override
    public String toString() {
        return "warnings for " + source;
    }
}
