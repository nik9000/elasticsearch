/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class Reverse extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Reverse", Reverse::new);

    private final Expression field;

    @FunctionInfo(
        returnType = { "keyword", "text" },
        description = "Returns a new string representing the input string in reverse order.",
        examples = {
            @Example(file = "string", tag = "reverse"),
            @Example(file = "string", tag = "reverseEmoji", description = "`REVERSE` works with unicode, too! It keeps unicode grapheme clusters together during reversal.") }
    )
    public Reverse(
        Source source,
        @Param(
            name = "str",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression field
    ) {
        super(source, field);
        this.field = field;
    }

    private Reverse(StreamInput in) throws IOException {
        this(Source.EMPTY, in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    /**
     * Reverses a unicode string, keeping grapheme clusters together
     * @param str
     * @return
     */
    public static String reverseStringWithUnicodeCharacters(String str) {
        BreakIterator boundary = BreakIterator.getCharacterInstance(Locale.ROOT);
        boundary.setText(str);

        List<String> characters = new ArrayList<>();
        int start = boundary.first();
        for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {
            characters.add(str.substring(start, end));
        }

        StringBuilder reversed = new StringBuilder(str.length());
        for (int i = characters.size() - 1; i >= 0; i--) {
            reversed.append(characters.get(i));
        }

        return reversed.toString();
    }

    private static boolean isOneByteUTF8(BytesRef ref) {
        int end = ref.offset + ref.length;
        for (int i = ref.offset; i < end; i++) {
            if (ref.bytes[i] < 0) {
                return false;
            }
        }
        return true;
    }

    private static void reverseArray(byte[] array, int start, int end) {
        while (start < end) {
            byte temp = array[start];
            array[start] = array[end];
            array[end] = temp;
            start++;
            end--;
        }
    }

    @Evaluator
    static BytesRef process(BytesRef val) {
        if (isOneByteUTF8(val)) {
            // this is the fast path. we know we can just reverse the bytes.
            BytesRef reversed = BytesRef.deepCopyOf(val);
            reverseArray(reversed.bytes, reversed.offset, reversed.offset + reversed.length - 1);
            return reversed;
        }
        return BytesRefs.toBytesRef(reverseStringWithUnicodeCharacters(val.utf8ToString()));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new ReverseEvaluator.Factory(source(), fieldEvaluator);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return new Reverse(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Reverse::new, field);
    }
}
