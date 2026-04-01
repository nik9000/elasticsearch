/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.operator.PagedBytesRefBuilder;

import static org.hamcrest.Matchers.lessThan;

public class Utf8AscTopNEncoderWithPagedTests extends AbstractUtf8TopNEncoderWithPagedTests {
    public Utf8AscTopNEncoderWithPagedTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return TopNEncoder.UTF8;
    }

    @Override
    protected void assertMinMax(PagedBytesRefBuilder min, PagedBytesRefBuilder max) {
        assertThat(min, lessThan(max));
    }
}
