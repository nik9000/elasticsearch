/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

import java.util.List;

/**
 * PromQL built-in function definitions that do not correspond to a dedicated ES|QL function class.
 */
class PromqlBuiltinFunctionDefinitions {

    static final PromqlFunctionDefinition VECTOR = new PromqlFunctionDefinition(
        "vector",
        FunctionType.VECTOR_CONVERSION,
        PromqlFunctionArity.ONE,
        (source, target, ctx, extraParams) -> target,
        "Returns the scalar as a vector with no labels.",
        List.of(PromqlFunctionDefinition.SCALAR),
        List.of("vector(1)"),
        PromqlFunctionDefinition.CounterSupport.SUPPORTED
    );

    static final PromqlFunctionDefinition SCALAR = new PromqlFunctionDefinition(
        "scalar",
        FunctionType.SCALAR_CONVERSION,
        PromqlFunctionArity.ONE,
        (source, target, ctx, extraParams) -> new Scalar(source, target),
        "Returns the sample value of a single-element instant vector as a scalar. "
            + "If the input vector does not have exactly one element, scalar returns NaN.",
        List.of(PromqlFunctionDefinition.INSTANT_VECTOR),
        List.of("scalar(sum(http_requests_total))"),
        PromqlFunctionDefinition.CounterSupport.SUPPORTED
    );

    static final PromqlFunctionDefinition TIME = PromqlFunctionDefinition.def()
        .scalarWithStep((source, step) -> new Div(source, new ToDouble(source, step), Literal.fromDouble(source, 1000.0)))
        .counterSupport(PromqlFunctionDefinition.CounterSupport.SUPPORTED)
        .description(
            "returns the number of seconds since January 1, 1970 UTC."
                + " Note that this does not actually return the current time, but the time at which the expression is to be evaluated."
        )
        .example("time()")
        .name("time");

    private PromqlBuiltinFunctionDefinitions() {}
}
