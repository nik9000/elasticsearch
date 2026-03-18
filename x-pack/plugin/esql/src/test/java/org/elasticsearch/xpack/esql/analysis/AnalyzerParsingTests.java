/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.StatementParserTests;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

/**
 * Parses a plan, builds an AST for it, and then runs logical analysis on it.
 * So if we don't error out in the process,  all references were resolved correctly.
 * Use this class if you want to test parsing <b>and resolution</b> of a query
 *  and especially if you expect to get a ParsingException.
 *  <p>
 *  For testing parsing <b>only</b>, use {@link StatementParserTests} or a subclass of {@link AbstractStatementParserTests}.
 */
public class AnalyzerParsingTests extends ESTestCase {
    private final TestAnalyzer defaultAnalyzer = analyzer().addEmployees("test");

    public void testCaseFunctionInvalidInputs() {
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case()"));
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(a)"));
        assertEquals("1:22: error building [case]: expects at least two arguments", error("row a = 1 | eval x = case(1)"));
    }

    public void testConcatFunctionInvalidInputs() {
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat()"));
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(a)"));
        assertEquals("1:22: error building [concat]: expects at least two arguments", error("row a = 1 | eval x = concat(1)"));
    }

    public void testCoalesceFunctionInvalidInputs() {
        assertEquals("1:22: error building [coalesce]: expects at least one argument", error("row a = 1 | eval x = coalesce()"));
    }

    public void testGreatestFunctionInvalidInputs() {
        assertEquals("1:22: error building [greatest]: expects at least one argument", error("row a = 1 | eval x = greatest()"));
    }

    public void testLeastFunctionInvalidInputs() {
        assertEquals("1:22: error building [least]: expects at least one argument", error("row a = 1 | eval x = least()"));
    }

    private String error(String query) {
        String message = defaultAnalyzer.error(query, ParsingException.class);
        assertTrue(message.startsWith("line "));
        return message.substring("line ".length());
    }
}
