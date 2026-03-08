/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Nullable;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Utilities to collect warnings for running an executor.
 */
public class Warnings {
    static final int MAX_ADDED_WARNINGS = 20;

    public static final Warnings NOOP_WARNINGS = new Warnings(null, -1, -2, null, "", "") {
        @Override
        public void registerException(Exception exception) {
            // this space intentionally left blank
        }

        @Override
        public void registerException(Class<? extends Exception> exceptionClass, String message) {
            // this space intentionally left blank
        }
    };

    /**
     * Create a new warnings object that appends to the given sink.
     * @param sink The sink to append warnings to, or {@code null} to ignore warnings
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarnings(@Nullable WarningsSink sink, WarningSourceLocation source) {
        return createWarnings(sink, source, "evaluation of [{}] failed, treating result as null");
    }

    /**
     * Create a new warnings object which warns that it treats the result as {@code false}.
     * @param sink The sink to append warnings to, or {@code null} to ignore warnings
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createWarningsTreatedAsFalse(@Nullable WarningsSink sink, WarningSourceLocation source) {
        return createWarnings(sink, source, "evaluation of [{}] failed, treating result as false");
    }

    /**
     * Create a new warnings object which warns that evaluation resulted in warnings.
     * @param sink The sink to append warnings to, or {@code null} to ignore warnings
     * @param source The source location information for warnings
     * @return A warnings collector object
     */
    public static Warnings createOnlyWarnings(@Nullable WarningsSink sink, WarningSourceLocation source) {
        return createWarnings(sink, source, "warnings during evaluation of [{}]");
    }

    private static Warnings createWarnings(@Nullable WarningsSink sink, WarningSourceLocation source, String first) {
        if (sink == null) {
            return NOOP_WARNINGS;
        }
        return new Warnings(sink, source.lineNumber(), source.columnNumber(), source.viewName(), source.text(), first);
    }

    @Nullable
    private final WarningsSink sink;
    private final String location;
    private final String first;

    private int addedWarnings;

    private Warnings(@Nullable WarningsSink sink, int lineNumber, int columnNumber, String viewName, String sourceText, String first) {
        this.sink = sink;
        if (viewName == null) {
            this.location = format("Line {}:{}: ", lineNumber, columnNumber);
        } else {
            this.location = format("Line {}:{} (in view [{}]): ", lineNumber, columnNumber, viewName);
        }
        this.first = format(null, "{}" + first + ". Only first {} failures recorded.", location, sourceText, MAX_ADDED_WARNINGS);
    }

    public void registerException(Exception exception) {
        registerException(exception.getClass(), exception.getMessage());
    }

    /**
     * Register an exception to be included in the warnings.
     * <p>
     *     This overload avoids the need to instantiate the exception, which can be expensive.
     *     Instead, it asks only the required pieces to build the warning.
     * </p>
     */
    public void registerException(Class<? extends Exception> exceptionClass, String message) {
        if (addedWarnings < MAX_ADDED_WARNINGS) {
            if (addedWarnings == 0) {
                sink.add(first);
            }
            sink.add(location + exceptionClass.getName() + ": " + message);
            addedWarnings++;
        }
    }
}
