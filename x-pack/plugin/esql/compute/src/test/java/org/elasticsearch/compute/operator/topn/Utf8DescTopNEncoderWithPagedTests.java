/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.bytes.PagedBytesRefBuilder;

import static org.hamcrest.Matchers.greaterThan;

public class Utf8DescTopNEncoderWithPagedTests extends AbstractUtf8TopNEncoderWithPagedTests {
    public Utf8DescTopNEncoderWithPagedTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return TopNEncoder.UTF8.toSortable(false);
    }

    @Override
    protected void assertMinMax(PagedBytesRefBuilder min, PagedBytesRefBuilder max) {
        assertThat(min, greaterThan(max));
    }
}
