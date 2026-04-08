/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

/**
 * Represents the parameter count constraints for a PromQL function.
 */
public record PromqlFunctionArity(int min, int max) {

    // Common arity patterns as constants
    public static final PromqlFunctionArity NONE = new PromqlFunctionArity(0, 0);
    public static final PromqlFunctionArity ONE = new PromqlFunctionArity(1, 1);
    public static final PromqlFunctionArity TWO = new PromqlFunctionArity(2, 2);
    public static final PromqlFunctionArity VARIADIC = new PromqlFunctionArity(1, Integer.MAX_VALUE);

    public PromqlFunctionArity {
        if (min < 0) {
            throw new IllegalArgumentException("min must be non-negative");
        }
        if (max < min) {
            throw new IllegalArgumentException("max must be >= min");
        }
    }

    public static PromqlFunctionArity fixed(int count) {
        return switch (count) {
            case 0 -> NONE;
            case 1 -> ONE;
            case 2 -> TWO;
            default -> new PromqlFunctionArity(count, count);
        };
    }

    public static PromqlFunctionArity range(int min, int max) {
        return min == max ? fixed(min) : new PromqlFunctionArity(min, max);
    }

    public static PromqlFunctionArity atLeast(int min) {
        return min == 1 ? VARIADIC : new PromqlFunctionArity(min, Integer.MAX_VALUE);
    }

    public static PromqlFunctionArity optional(int max) {
        return new PromqlFunctionArity(0, max);
    }

    public boolean validate(int paramCount) {
        return paramCount >= min && paramCount <= max;
    }
}
