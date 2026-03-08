/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class WarningsTests extends ESTestCase {
    public void testRegisterCollect() {
        List<String> warningsList = new ArrayList<>();
        Warnings warnings = Warnings.createWarnings(warningsList, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
        assertThat(
            warningsList,
            containsInAnyOrder(
                "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: null"
            )
        );
    }

    public void testRegisterCollectFilled() {
        List<String> warningsList = new ArrayList<>();
        Warnings warnings = Warnings.createWarnings(warningsList, new TestWarningsSource("foo"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1: evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1: java.lang.IllegalArgumentException: " + i;
        }

        assertThat(warningsList, containsInAnyOrder(expected));
    }

    public void testRegisterCollectViews() {
        List<String> warningsList = new ArrayList<>();
        Warnings warnings = Warnings.createWarnings(warningsList, new TestWarningsSource("foo", "view1"));
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS + 1000; i++) {
            warnings.registerException(new IllegalArgumentException(Integer.toString(i)));
        }

        String[] expected = new String[21];
        expected[0] = "Line 1:1 (in view [view1]): evaluation of [foo] failed, treating result as null. Only first 20 failures recorded.";
        for (int i = 0; i < Warnings.MAX_ADDED_WARNINGS; i++) {
            expected[i + 1] = "Line 1:1 (in view [view1]): java.lang.IllegalArgumentException: " + i;
        }

        assertThat(warningsList, containsInAnyOrder(expected));
    }

    public void testRegisterIgnore() {
        Warnings warnings = Warnings.createWarnings(null, new TestWarningsSource("foo"));
        warnings.registerException(new IllegalArgumentException());
    }

    public record TestWarningsSource(String text, String viewName, int lineNumber, int columnNumber) implements WarningSourceLocation {
        public TestWarningsSource(String text) {
            this(text, null, 1, 1);
        }

        public TestWarningsSource(String text, String viewName) {
            this(text, viewName, 1, 1);
        }
    }
}
