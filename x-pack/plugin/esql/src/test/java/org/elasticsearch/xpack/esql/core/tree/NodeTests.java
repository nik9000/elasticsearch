/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.NameId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.tree.SourceTests.randomSource;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NodeTests extends ESTestCase {
    public void testToStringNoChildren() {
        assertThat(new NoChildren(randomSource(), "thing"), hasToString("NoChildren[thing]"));
    }

    public void testToStringDepth() {
        ChildrenAreAProperty empty = new ChildrenAreAProperty(randomSource(), emptyList(), "thing");
        assertThat(empty, hasToString("ChildrenAreAProperty[thing]"));
        assertThat(new ChildrenAreAProperty(randomSource(), singletonList(empty), "single"), hasToString("""
            ChildrenAreAProperty[single]
            \\_ChildrenAreAProperty[thing]"""));
        assertThat(new ChildrenAreAProperty(randomSource(), Arrays.asList(empty, empty), "many"), hasToString("""
            ChildrenAreAProperty[many]
            |_ChildrenAreAProperty[thing]
            \\_ChildrenAreAProperty[thing]"""));
    }

    public void testToStringAChildIsAProperty() {
        NoChildren empty = new NoChildren(randomSource(), "thing");
        assertThat(new AChildIsAProperty(randomSource(), empty, "single"), hasToString("""
            AChildIsAProperty[single]
            \\_NoChildren[thing]"""));
    }

    public void testToStringMaxLineWidth() {
        assertThat(new NoChildren(randomSource(), "a".repeat(110)), hasToString("NoChildren[" + "a".repeat(110) + "]"));
    }

    public void testToStringLongProperty() {
        assertThat(
            new NoChildren(randomSource(), "a".repeat(200)),
            hasToString("NoChildren[" + "a".repeat(110) + "\n" + "a".repeat(90) + "]")
        );
    }

    public void testToStringVeryLongProperty() {
        assertThat(
            new NoChildren(randomSource(), "a".repeat(1000)),
            hasToString("NoChildren[" + ("a".repeat(110) + "\n").repeat(9) + "a".repeat(10) + "]")
        );
    }

    public void testToStringTruncatesAtMaxLines() {
        assertThat(
            new NoChildren(randomSource(), "a".repeat(2000)),
            hasToString("NoChildren[" + ("a".repeat(110) + "\n").repeat(9) + "a".repeat(110) + "..." + "]")
        );
    }

    public void testToStringNameId() {
        NameId nameId = new NameId();
        assertThat(new NoChildren(randomSource(), nameId), hasToString("NoChildren[#" + nameId + "]"));
    }

    public void testToStringList() {
        assertThat(
            new NoChildren(randomSource(), List.of("aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj")),
            hasToString("NoChildren[[aaa, bbb, ccc, ddd, eee, fff, ggg, hhh, iii, jjj]]")
        );
    }

    public void testToStringListWrapping() {
        assertThat(
            new NoChildren(
                randomSource(),
                List.of(
                    "aaaaaaaaaa",
                    "bbbbbbbbbb",
                    "cccccccccc",
                    "dddddddddd",
                    "eeeeeeeeee",
                    "ffffffffff",
                    "gggggggggg",
                    "hhhhhhhhhh",
                    "iiiiiiiiii",
                    "jjjjjjjjjj"
                )
            ),
            hasToString("""
                NoChildren[[aaaaaaaaaa, bbbbbbbbbb, cccccccccc, dddddddddd, eeeeeeeeee, ffffffffff, gggggggggg, hhhhhhhhhh, iiiiiiiiii, j
                jjjjjjjjj]]""")
        );
    }

    public void testToStringListPathologicalWrapping() {
        List<String> strings = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            strings.add(String.valueOf((char) ('a' + (i % 26))));
        }
        assertThat(new NoChildren(randomSource(), strings), hasToString("""
            NoChildren[[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k
            , l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u,\s
            v, w, x, y, z, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v]]"""));
    }

    public void testRandomToString() {
        NoChildren node = new NoChildren(randomSource(), randomNestedList(3));
        String[] lines = node.toString().split("\n", -1);
        assertThat(lines.length, lessThanOrEqualTo(Node.TO_STRING_MAX_LINES));
        if (lines.length == 1) {
            assertThat(lines[0].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "NoChildren[]".length()));
            return;
        }
        assertThat(lines[0].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "NoChildren[".length()));
        for (int i = 1; i < lines.length - 1; i++) {
            assertThat(lines[i].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH));
        }
        assertThat(lines[lines.length - 1].length(), lessThanOrEqualTo(Node.TO_STRING_MAX_WIDTH + "...]".length()));
    }

    public void testWithNullChild() {
        List<Dummy> listWithNull = new ArrayList<>();
        listWithNull.add(null);
        var e = expectThrows(QlIllegalArgumentException.class, () -> new ChildrenAreAProperty(randomSource(), listWithNull, "single"));
        assertEquals("Null children are not allowed", e.getMessage());
    }

    public void testWithImmutableChildList() {
        // It's good enough that the node can be created without throwing a NPE
        var node = new ChildrenAreAProperty(randomSource(), List.of(), "single");
        assertEquals(node.children().size(), 0);
    }

    public void testTransformDownAsyncNoChildren() throws InterruptedException {
        NoChildren node = new NoChildren(randomSource(), "original");

        assertAsyncTransform(node, (n, listener) -> {
            NoChildren transformed = new NoChildren(n.source(), "transformed");
            listener.onResponse(transformed);
        }, result -> {
            assertEquals(NoChildren.class, result.getClass());
            assertEquals("transformed", result.thing());
        });
    }

    public void testTransformDownAsyncWithChildren() throws InterruptedException {
        NoChildren child1 = new NoChildren(randomSource(), "child1");
        NoChildren child2 = new NoChildren(randomSource(), "child2");
        ChildrenAreAProperty parent = new ChildrenAreAProperty(randomSource(), Arrays.asList(child1, child2), "parent");

        assertAsyncTransform(parent, (n, listener) -> {
            if (n instanceof NoChildren) {
                NoChildren nc = (NoChildren) n;
                if ("child1".equals(nc.thing())) {
                    listener.onResponse(new NoChildren(nc.source(), "transformed1"));
                } else {
                    listener.onResponse(n);
                }
            } else {
                listener.onResponse(n);
            }
        }, result -> {
            assertEquals(ChildrenAreAProperty.class, result.getClass());
            ChildrenAreAProperty transformed = (ChildrenAreAProperty) result;
            assertEquals(2, transformed.children().size());
            assertEquals("transformed1", transformed.children().get(0).thing());
            assertEquals("child2", transformed.children().get(1).thing());
        });
    }

    public void testTransformDownAsyncNoChange() throws InterruptedException {
        NoChildren node = new NoChildren(randomSource(), "unchanged");
        assertAsyncTransform(node, (n, listener) -> listener.onResponse(n), result -> assertSame(node, result));
    }

    private void assertAsyncTransform(Dummy node, BiConsumer<? super Dummy, ActionListener<Dummy>> rule, Consumer<Dummy> assertions)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<Dummy> listener = new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            assertions.accept(result);
        }), latch);

        node.transformDown(rule, listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

    public abstract static class Dummy extends Node<Dummy> {
        private final String thing;

        public Dummy(Source source, List<Dummy> children, String thing) {
            super(source, children);
            this.thing = thing;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        public String thing() {
            return thing;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Dummy other = (Dummy) obj;
            return thing.equals(other.thing) && children().equals(other.children());
        }

        @Override
        public int hashCode() {
            return Objects.hash(thing, children());
        }
    }

    public static class ChildrenAreAProperty extends Dummy {
        public ChildrenAreAProperty(Source source, List<Dummy> children, String thing) {
            super(source, children, thing);
        }

        @Override
        protected NodeInfo<ChildrenAreAProperty> info() {
            return NodeInfo.create(this, ChildrenAreAProperty::new, children(), thing());
        }

        @Override
        public ChildrenAreAProperty replaceChildren(List<Dummy> newChildren) {
            return new ChildrenAreAProperty(source(), newChildren, thing());
        }
    }

    public static class AChildIsAProperty extends Dummy {
        public AChildIsAProperty(Source source, Dummy child, String thing) {
            super(source, singletonList(child), thing);
        }

        @Override
        protected NodeInfo<AChildIsAProperty> info() {
            return NodeInfo.create(this, AChildIsAProperty::new, child(), thing());
        }

        @Override
        public AChildIsAProperty replaceChildren(List<Dummy> newChildren) {
            return new AChildIsAProperty(source(), newChildren.get(0), thing());
        }

        public Dummy child() {
            return children().get(0);
        }
    }

    public static class NoChildren extends Dummy {
        private final Object thing;

        public NoChildren(Source source, Object thing) {
            super(source, emptyList(), thing.toString());
            this.thing = thing;
        }

        @Override
        protected NodeInfo<NoChildren> info() {
            return NodeInfo.create(this, NoChildren::new, thing);
        }

        @Override
        public Dummy replaceChildren(List<Dummy> newChildren) {
            throw new UnsupportedOperationException("no children to replace");
        }
    }

    private static List<Object> randomNestedList(int allowedDepth) {
        Supplier<Object> element = allowedDepth > 0
            ? () -> randomBoolean() ? randomNestedList(allowedDepth - 1) : randomAlphaOfLengthBetween(0, 10)
            : () -> randomAlphaOfLengthBetween(0, 10);
        return randomList(10, element);
    }

    // Returns an empty list. The returned list may be backed various implementations, some
    // allowing null some not - disallowing null disallows (throws NPE for) contains(null).
    private static <T> List<T> emptyList() {
        return randomFrom(List.of(), Collections.emptyList(), new ArrayList<>(), new LinkedList<>());
    }
}
