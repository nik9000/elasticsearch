/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTestCaseHelper<S extends AbstractTestCaseHelper<S>> {
    private static final Logger log = LogManager.getLogger(AbstractTestCaseHelper.class);

    private final int arity;
    private final List<List<TestCaseSupplier.TypedDataSupplier>> testCases;
    private final Function<List<TestCaseSupplier.TypedDataSupplier>, String> name;
    private final String evaluatorToString;
    private final DataType outputType;
    /**
     * Map from inputs to expected value.
     */
    private final Function<List<Object>, Object> expected;
    /**
     * Map from inputs to expected warnings.
     */
    private final Function<List<Object>, List<String>> expectedWarnings;
    private final Supplier<Configuration> configuration;
    private final boolean withoutEvaluator;
    private final Class<? extends Throwable> foldingExceptionClass;
    private final Function<List<Object>, String> foldingExceptionMessage;

    protected AbstractTestCaseHelper(int arity) {
        this.arity = arity;
        this.testCases = List.of();
        this.name = null;
        this.evaluatorToString = null;
        this.outputType = null;
        this.expected = null;
        this.expectedWarnings = v -> List.of();
        this.configuration = null;
        this.withoutEvaluator = false;
        this.foldingExceptionClass = null;
        this.foldingExceptionMessage = null;
    }

    protected AbstractTestCaseHelper(
        int arity,
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String evaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration,
        boolean withoutEvaluator,
        Class<? extends Throwable> foldingExceptionClass,
        Function<List<Object>, String> foldingExceptionMessage
    ) {
        this.arity = arity;
        this.testCases = values;
        this.name = name;
        this.evaluatorToString = evaluatorToString;
        this.outputType = expectedOutputType;
        this.expected = expected;
        this.expectedWarnings = expectedWarnings;
        this.configuration = configuration;
        this.withoutEvaluator = withoutEvaluator;
        this.foldingExceptionClass = foldingExceptionClass;
        this.foldingExceptionMessage = foldingExceptionMessage;
    }

    /**
     * Create a new instance with the given parent fields, preserving subclass-specific state.
     */
    protected abstract S create(
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String expectedEvaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration,
        boolean withoutEvaluator,
        Class<? extends Throwable> foldingExceptionClass,
        Function<List<Object>, String> foldingExceptionMessage
    );

    protected final List<List<TestCaseSupplier.TypedDataSupplier>> testCases() {
        return testCases;
    }

    protected final S addTestCases(List<List<TestCaseSupplier.TypedDataSupplier>> testCases) {
        List<List<TestCaseSupplier.TypedDataSupplier>> newTestCases = new ArrayList<>(this.testCases);
        newTestCases.addAll(testCases);
        return replaceTestCases(List.copyOf(newTestCases));
    }

    protected final S replaceTestCases(List<List<TestCaseSupplier.TypedDataSupplier>> testCases) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    /**
     * Provide a naming function for the cases this will build. If this is not provided
     * then the case will be named
     * <pre>{@code <param1name, param2name, param3name>}</pre>.
     */
    public final S name(Function<List<TestCaseSupplier.TypedDataSupplier>, String> name) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    /**
     * Sets the expected {@link Object#toString} of the {@link ExpressionEvaluator}.
     * Use {@code %0} for the "reader" for the first parameter. Use {@code %1} for the second. Etc.
     * Like:
     * <pre>{@code
     * helper.evaluatorToString("ToBase64Evaluator[field=%0]");
     * }</pre>
     */
    public final S evaluatorToString(String evaluatorToString) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    protected final String evaluatorToString() {
        return evaluatorToString;
    }

    public final S expectedOutputType(DataType expectedOutputType) {
        return create(
            testCases,
            name,
            evaluatorToString,
            expectedOutputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    protected final S expectedFromArgs(Function<List<Object>, Object> expected) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    protected final S expectWarningsFromArgs(Function<List<Object>, List<String>> expectedWarnings) {
        Function<List<Object>, List<String>> wrapped = o -> {
            List<String> e = expectedWarnings.apply(o);
            if (e.isEmpty()) {
                return e;
            }
            List<String> warnings = new ArrayList<>();
            warnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
            warnings.addAll(e);
            return warnings;
        };
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            wrapped,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    /**
     * Provide a {@link Configuration} to the function. If this isn't called
     * then a {@linkplain Configuration} is not available to the function.
     */
    public final S configuration(Supplier<Configuration> configuration) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    /**
     * Build {@link TestCaseSupplier.TestCase}s that can't build an evaluator.
     * <p>
     *     Useful for special cases that can't be executed, but should still be considered.
     * </p>
     */
    public S withoutEvaluator() {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            true,
            foldingExceptionClass,
            foldingExceptionMessage
        );
    }

    /**
     * Set the expected exception type and message that is thrown when folding.
     * Use this for sets of functions that fail completely when folding. Most functions
     * <strong>don't</strong> do this, but a few fail with nice error messages
     * on bad inputs.
     */
    public final S foldingExceptionFromAllArgs(Class<? extends Throwable> clazz, Function<List<Object>, String> message) {
        return create(
            testCases,
            name,
            evaluatorToString,
            outputType,
            expected,
            expectedWarnings,
            configuration,
            withoutEvaluator,
            clazz,
            message
        );
    }

    /**
     * Build the {@link TestCaseSupplier suppliers} and write them into the provided list.
     */
    public final void build(List<TestCaseSupplier> suppliers) {
        if (testCases.isEmpty()) {
            throw new IllegalStateException("values must be provided");
        }
        if (evaluatorToString == null) {
            throw new IllegalStateException("evaluatorToString must be provided");
        }
        if (outputType == null) {
            throw new IllegalStateException("expectedOutputType must be provided");
        }
        if (expected == null) {
            throw new IllegalStateException("expectedValue must be provided");
        }
        for (List<TestCaseSupplier.TypedDataSupplier> valueSuppliers : testCases) {
            List<DataType> types = valueSuppliers.stream().map(TestCaseSupplier.TypedDataSupplier::type).toList();
            Supplier<TestCaseSupplier.TestCase> supplier = () -> buildTestCase(
                valueSuppliers.stream().map(TestCaseSupplier.TypedDataSupplier::get).toList()
            );
            suppliers.add(new TestCaseSupplier(resolveName(valueSuppliers), types, supplier));
        }
    }

    /**
     * Pick a name for the test case represented by the provided parameters.
     */
    private String resolveName(List<TestCaseSupplier.TypedDataSupplier> valueSuppliers) {
        if (name != null) {
            // name pattern was configured, don't use the default
            return name.apply(valueSuppliers);
        }
        StringBuilder b = new StringBuilder();
        for (TestCaseSupplier.TypedDataSupplier s : valueSuppliers) {
            if (b.isEmpty() == false) {
                b.append(", ");
            }
            b.append(s.name());
        }
        return b.toString();
    }

    /**
     * Build a {@link TestCaseSupplier.TestCase} with the provided parameters.
     */
    private TestCaseSupplier.TestCase buildTestCase(List<TestCaseSupplier.TypedData> parameters) {
        log.info("Inputs are {}", parameters);
        List<Object> inputs = parameters.stream().map(TestCaseSupplier.TypedData::getValue).toList();
        assertThat(parameters.size(), equalTo(arity));
        Object expectedValue = this.expected.apply(inputs);
        log.info("expectedValue is {}", expectedValue);
        Matcher<?> matcher = expectedValue instanceof Matcher<?> ? (Matcher<?>) expectedValue : equalTo(expectedValue);
        TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(parameters, resolveEvaluatorToString(), outputType, matcher);
        for (String warning : expectedWarnings.apply(inputs)) {
            testCase = testCase.withWarning(warning);
        }
        if (configuration != null) {
            testCase = testCase.withConfiguration(new Source(new Location(1, 0), "source"), configuration.get());
        }
        if (withoutEvaluator) {
            testCase = testCase.withoutEvaluator();
        }
        if (foldingExceptionClass != null) {
            testCase = testCase.withFoldingException(foldingExceptionClass, foldingExceptionMessage.apply(inputs));
        }
        return testCase;
    }

    private String resolveEvaluatorToString() {
        String resolved = evaluatorToString;
        for (int a = 0; a < arity; a++) {
            resolved = resolved.replace("%" + a, "Attribute[channel=" + a + "]");
        }
        return resolved;
    }
}
