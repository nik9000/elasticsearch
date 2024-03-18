/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.AbstractStringPattern;

import java.util.function.Function;

public class RegexMatch {
    /**
     * Convert a QL regex matcher into an evaluator.
     */
    public static EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator,
        org.elasticsearch.xpack.ql.expression.predicate.regex.RegexMatch<?> expression
    ) {
        Automaton automaton = Operations.determinize(
            new UTF32ToUTF8().convert(((AbstractStringPattern) expression.pattern()).createAutomaton()),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        ByteRunAutomaton run = new ByteRunAutomaton(automaton, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        return new RegexMatchEvaluator.Factory(expression.source(), toEvaluator.apply(expression.field()), run, toDot(automaton));
    }

    @Evaluator
    static boolean process(BytesRef input, @Fixed(includeInToString = false) ByteRunAutomaton automaton, @Fixed String pattern) {
        if (input == null) {
            return false;
        }
        return automaton.run(input.bytes, input.offset, input.length);
    }

    private static final int MAX_LENGTH = 1024 * 64;

    /**
     * Convert an {@link Automaton} to <a href="https://graphviz.org/doc/info/lang.html">dot</a>.
     * <p>
     *  This was borrowed from {@link Automaton#toDot} but has been modified to snip if the length
     *  grows too much and to format the bytes differently.
     * </p>
     */
    public static String toDot(Automaton automaton) {
        StringBuilder b = new StringBuilder();
        b.append("digraph Automaton {\n");
        b.append("  rankdir = LR\n");
        b.append("  node [width=0.2, height=0.2, fontsize=8]\n");
        int numStates = automaton.getNumStates();
        if (numStates > 0) {
            b.append("  initial [shape=plaintext,label=\"\"]\n");
            b.append("  initial -> 0\n");
        }

        Transition t = new Transition();

        too_big: for (int state = 0; state < numStates; ++state) {
            b.append("  ");
            b.append(state);
            if (automaton.isAccept(state)) {
                b.append(" [shape=doublecircle,label=\"").append(state).append("\"]\n");
            } else {
                b.append(" [shape=circle,label=\"").append(state).append("\"]\n");
            }

            int numTransitions = automaton.initTransition(state, t);

            for (int i = 0; i < numTransitions; ++i) {
                automaton.getNextTransition(t);

                assert t.max >= t.min;

                b.append("  ");
                b.append(state);
                b.append(" -> ");
                b.append(t.dest);
                b.append(" [label=\"");
                appendByte(t.min, b);
                if (t.max != t.min) {
                    b.append('-');
                    appendByte(t.max, b);
                }

                b.append("\"]\n");
                if (b.length() >= MAX_LENGTH) {
                    b.append("...snip...");
                    break too_big;
                }
            }
        }

        b.append('}');
        return b.toString();
    }

    static void appendByte(int c, StringBuilder b) {
        if (c > 255) {
            throw new UnsupportedOperationException("can only format bytes but got [" + c + "]");
        }
        if (c == 34) {
            b.append("\\\"");
            return;
        }
        if (c == 92) {
            b.append("\\\\");
            return;
        }
        if (c >= 33 && c <= 126) {
            b.appendCodePoint(c);
            return;
        }
        b.append("0x");
        String hex = Integer.toHexString(c);
        switch (hex.length()) {
            case 1 -> b.append('0').append(hex);
            case 2 -> b.append(hex);
            default -> throw new UnsupportedOperationException("can only format bytes");
        }
    }
}
