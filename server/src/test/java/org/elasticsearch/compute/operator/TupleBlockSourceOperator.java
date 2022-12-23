/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.Stream;

/**
 * A source operator whose output is the given tuple values. This operator produces pages
 * with two Blocks. The returned pages preserve the order of values as given in the in initial list.
 */
public class TupleBlockSourceOperator extends AbstractBlockSourceOperator {

    private static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;

    private final List<Tuple<Long, Long>> values;

    public TupleBlockSourceOperator(Stream<Tuple<Long, Long>> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public TupleBlockSourceOperator(Stream<Tuple<Long, Long>> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values.toList();
    }

    public TupleBlockSourceOperator(List<Tuple<Long, Long>> values) {
        this(values, DEFAULT_MAX_PAGE_POSITIONS);
    }

    public TupleBlockSourceOperator(List<Tuple<Long, Long>> values, int maxPagePositions) {
        super(maxPagePositions);
        this.values = values;
    }

    @Override
    protected Page createPage(int positionOffset, int length) {
        BlockBuilder blockBuilder1 = BlockBuilder.newLongBlockBuilder(length);
        BlockBuilder blockBuilder2 = BlockBuilder.newLongBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            Tuple<Long, Long> item = values.get(positionOffset + i);
            if (item.v1() == null) {
                blockBuilder1.appendNull();
            } else {
                blockBuilder1.appendLong(item.v1());
            }
            if (item.v2() == null) {
                blockBuilder2.appendNull();
            } else {
                blockBuilder2.appendLong(item.v2());
            }
        }
        currentPosition += length;
        return new Page(blockBuilder1.build(), blockBuilder2.build());
    }

    @Override
    protected int remaining() {
        return values.size() - currentPosition;
    }
}
