/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockTestUtils;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.ComputeTestCase;
import org.elasticsearch.test.ListMatcher;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.equalTo;

public class RightChunkedLeftJoinTests extends ComputeTestCase {
    public void testNoGaps() {
        int size = 100;
        BlockFactory factory = blockFactory();
        try (RightChunkedLeftJoin join = new RightChunkedLeftJoin(buildExampleLeftHand(factory, size), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 0, 1, 2 }, // formatter
                    { 1, 2, 3 }, // formatter
                    { 2, 3, 3 }, // formatter
                    { 3, 9, 9 }, // formatter
                },
                new Object[][] {
                    { "l00", 1, 2 }, // formatter
                    { "l01", 2, 3 }, // formatter
                    { "l02", 3, 3 }, // formatter
                    { "l03", 9, 9 }, // formatter
                }
            );
            assertTrailing(join, size, 4);
        }
    }

    /**
     * Test the first example in the main javadoc of {@link RightChunkedLeftJoin}.
     */
    public void testFirstExample() {
        BlockFactory factory = blockFactory();
        try (RightChunkedLeftJoin join = new RightChunkedLeftJoin(buildExampleLeftHand(factory, 100), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 0, 1, 2 },  // formatter
                    { 1, 2, 3 },  // formatter
                    { 1, 3, 3 },  // formatter
                    { 3, 9, 9 },  // formatter
                },
                new Object[][] {
                    { "l00", 1, 2 },  // formatter
                    { "l01", 2, 3 },  // formatter
                    { "l01", 3, 3 },  // formatter
                    { "l02", null, null },  // formatter
                    { "l03", 9, 9 },  // formatter
                }
            );
        }
    }

    public void testLeadingNulls() {
        int size = 3;
        BlockFactory factory = blockFactory();
        try (RightChunkedLeftJoin join = new RightChunkedLeftJoin(buildExampleLeftHand(factory, size), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] { { 2, 1, 2 } },
                new Object[][] {
                    { "l0", null, null }, // formatter
                    { "l1", null, null }, // formatter
                    { "l2", 1, 2 }, // formatter
                }
            );
            assertTrailing(join, size, 3);
        }
    }

    /**
     * Test the second example in the main javadoc of {@link RightChunkedLeftJoin}.
     */
    public void testSecondExample() {
        int size = 100;
        BlockFactory factory = blockFactory();
        try (RightChunkedLeftJoin join = new RightChunkedLeftJoin(buildExampleLeftHand(factory, size), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 0, 1, 2 },  // formatter
                    { 1, 3, 3 },  // formatter
                },
                new Object[][] {
                    { "l00", 1, 2 },  // formatter
                    { "l01", 3, 3 },  // formatter
                }
            );
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 1, 9, 9 },  // formatter
                    { 2, 9, 9 },  // formatter
                },
                new Object[][] {
                    { "l01", 9, 9 },  // formatter
                    { "l02", 9, 9 },  // formatter
                }
            );
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 5, 10, 10 },  // formatter
                    { 7, 11, 11 },  // formatter
                },
                new Object[][] {
                    { "l03", null, null },  // formatter
                    { "l04", null, null },  // formatter
                    { "l05", 10, 10 },  // formatter
                    { "l06", null, null },  // formatter
                    { "l07", 11, 11 },  // formatter
                }
            );
            assertTrailing(join, size, 8);
        }
    }

    /**
     * Test the third example in the main javadoc of {@link RightChunkedLeftJoin}.
     */
    public void testThirdExample() {
        int size = 100;
        BlockFactory factory = blockFactory();
        try (RightChunkedLeftJoin join = new RightChunkedLeftJoin(buildExampleLeftHand(factory, size), 2)) {
            Page pre = buildPage(factory, IntStream.range(0, 96).mapToObj(p -> new int[] { p, p, p }).toArray(int[][]::new));
            try {
                join.join(pre).releaseBlocks();
            } finally {
                pre.releaseBlocks();
            }
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 96, 1, 2 },  // formatter
                    { 97, 3, 3 },  // formatter
                },
                new Object[][] {
                    { "l96", 1, 2 },  // formatter
                    { "l97", 3, 3 },  // formatter
                }
            );
            assertTrailing(join, size, 98);
        }
    }

    NumberFormat exampleNumberFormat(int size) {
        NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ROOT);
        nf.setMinimumIntegerDigits((int) Math.ceil(Math.log10(size)));
        return nf;
    }

    Page buildExampleLeftHand(BlockFactory factory, int size) {
        NumberFormat nf = exampleNumberFormat(size);
        try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(size)) {
            for (int i = 0; i < size; i++) {
                builder.appendBytesRef(new BytesRef("l" + nf.format(i)));
            }
            return new Page(builder.build().asBlock());
        }
    }

    Page buildPage(BlockFactory factory, int[][] rows) {
        try (
            IntVector.Builder positions = factory.newIntVectorFixedBuilder(rows.length);
            IntVector.Builder r1 = factory.newIntVectorFixedBuilder(rows.length);
            IntVector.Builder r2 = factory.newIntVectorFixedBuilder(rows.length);
        ) {
            for (int[] row : rows) {
                positions.appendInt(row[0]);
                r1.appendInt(row[1]);
                r2.appendInt(row[2]);
            }
            return new Page(positions.build().asBlock(), r1.build().asBlock(), r2.build().asBlock());
        }
    }

    private void assertJoined(Page joined, Object[][] expected) {
        try {
            List<List<Object>> actualColumns = new ArrayList<>();
            BlockTestUtils.readInto(actualColumns, joined);

            for (int r = 0; r < expected.length; r++) {
                List<Object> actualRow = new ArrayList<>();
                for (int c = 0; c < actualColumns.size(); c++) {
                    Object v = actualColumns.get(c).get(r);
                    if (v instanceof BytesRef b) {
                        v = b.utf8ToString();
                    }
                    actualRow.add(v);
                }

                ListMatcher rowMatcher = matchesList();
                for (Object v : expected[r]) {
                    rowMatcher = rowMatcher.item(v);
                }
                assertMap("row " + r, actualRow, rowMatcher);
            }
        } finally {
            joined.releaseBlocks();
        }
    }

    private void assertJoined(BlockFactory factory, RightChunkedLeftJoin join, int[][] rightRows, Object[][] expectRows) {
        Page rightHand = buildPage(factory, rightRows);
        try {
            assertJoined(join.join(rightHand), expectRows);
        } finally {
            rightHand.releaseBlocks();
        }
    }

    private void assertTrailing(RightChunkedLeftJoin join, int size, int next) {
        NumberFormat nf = exampleNumberFormat(size);
        if (size == next) {
            assertThat(join.noMoreRightHandPages(), equalTo(Optional.empty()));
        } else {
            assertJoined(
                join.noMoreRightHandPages().get(),
                IntStream.range(next, size).mapToObj(p -> new Object[] { "l" + nf.format(p), null, null }).toArray(Object[][]::new)
            );
        }
    }
}
