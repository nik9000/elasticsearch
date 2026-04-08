/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Function definition record for registration and metadata.
 */
public record PromqlFunctionDefinition(
    String name,
    FunctionType functionType,
    PromqlFunctionArity arity,
    FunctionBuilder esqlBuilder,
    String description,
    List<PromqlParamInfo> params,
    List<String> examples,
    CounterSupport counterSupport
) {

    @FunctionalInterface
    public interface FunctionBuilder {
        Expression build(Source source, Expression target, PromqlFunctionRegistry.PromqlContext ctx, List<Expression> extraParams);
    }

    /**
     * Describes whether a PromQL function supports counter metric types.
     * <p>
     * This is an ES|QL implementation detail — in real PromQL, all functions work with any numeric type.
     * ES|QL distinguishes counter types from plain numerics internally, and some ESQL functions only
     * accept one or the other.
     */
    public enum CounterSupport {
        /** Only accepts counter types (e.g., rate, increase, irate). */
        REQUIRED,
        /** Accepts both counter and non-counter types. */
        SUPPORTED,
        /** Only accepts non-counter numeric types. */
        UNSUPPORTED
    }

    /**
     * Create a builder for a {@link FunctionDefinition}.
     */
    public static <T extends Function> Builder<T> def(Class<T> function) {
        return new Builder<>(function);
    }

    public PromqlFunctionDefinition {
        // NOCOMMIT make this private
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(functionType, "functionType cannot be null");
        Objects.requireNonNull(arity, "arity cannot be null");
        Objects.requireNonNull(esqlBuilder, "esqlBuilder cannot be null");
        Objects.requireNonNull(description, "description cannot be null");
        Objects.requireNonNull(params, "params cannot be null");
        Objects.requireNonNull(examples, "examples cannot be null");
        Objects.requireNonNull(counterSupport, "counterSupport cannot be null");
        if (arity.max() != params.size()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Arity max %d does not match number of parameters %d for function %s",
                    arity.max(),
                    params.size(),
                    name
                )
            );
        }
        if (params.isEmpty() == false && params.stream().filter(PromqlParamInfo::child).count() != 1) {
            throw new IllegalArgumentException("If a function takes parameters, there must be exactly one child parameter");
        }
    }

    private static final PromqlParamInfo RANGE_VECTOR = PromqlParamInfo.child(
        "v",
        PromqlDataType.RANGE_VECTOR,
        "Range vector input."
    );
    private static final PromqlParamInfo INSTANT_VECTOR = PromqlParamInfo.child(
        "v",
        PromqlDataType.INSTANT_VECTOR,
        "Instant vector input."
    );
    private static final PromqlParamInfo SCALAR = PromqlParamInfo.child(
        "s",
        PromqlDataType.SCALAR,
        "Scalar value."
    );
    private static final PromqlParamInfo QUANTILE = PromqlParamInfo.of(
        "φ",
        PromqlDataType.SCALAR,
        "Quantile value (0 ≤ φ ≤ 1)."
    );
    private static final PromqlParamInfo TO_NEAREST = PromqlParamInfo.optional(
        "to_nearest",
        PromqlDataType.SCALAR,
        "Round to nearest multiple of this value."
    );
    private static final PromqlParamInfo MIN_SCALAR = PromqlParamInfo.of(
        "min",
        PromqlDataType.SCALAR,
        "Minimum value."
    );
    private static final PromqlParamInfo MAX_SCALAR = PromqlParamInfo.of(
        "max",
        PromqlDataType.SCALAR,
        "Maximum value."
    );

    /**
     * A builder for {@link PromqlFunctionDefinition}s. Get one from {@link #def}.
     */
    public static class Builder<T extends Function> {
        @SuppressWarnings("unused")
        private final Class<T> function;
        private final List<String> examples = new ArrayList<>();
        private FunctionType functionType;
        private PromqlFunctionArity arity;
        private FunctionBuilder builder;
        private String description;
        private List<PromqlParamInfo> params;
        private CounterSupport counterSupport;

        public Builder(Class<T> function) {
            this.function = function;
        }

        public PromqlFunctionDefinition.Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        public PromqlFunctionDefinition.Builder<T> example(String example) {
            this.examples.add(example);
            return this;
        }

        public PromqlFunctionDefinition.Builder<T> unaryValueTransformation(ValueTransformationFunction<T> builder) {
            this.functionType = FunctionType.VALUE_TRANSFORMATION;
            this.arity = PromqlFunctionArity.ONE;
            this.builder = (source, target, ctx, extraParams) -> builder.build(source, target);
            this.params = List.of(INSTANT_VECTOR);
            this.counterSupport = CounterSupport.UNSUPPORTED;
            return this;
        }

        /**
         * Build the {@link PromqlFunctionDefinition} with the given primary name.
         */
        public PromqlFunctionDefinition name(String name) {
            return new PromqlFunctionDefinition(name, functionType, arity, builder, description, params, examples, counterSupport);
        }
    }

    @FunctionalInterface
    public interface ValueTransformationFunction<T extends Expression> {
        T build(Source source, Expression value);
    }
}
