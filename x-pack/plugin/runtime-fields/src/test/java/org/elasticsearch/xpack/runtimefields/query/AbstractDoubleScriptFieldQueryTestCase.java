/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.xpack.runtimefields.DoubleRuntimeValues;

import static org.mockito.Mockito.mock;

public abstract class AbstractDoubleScriptFieldQueryTestCase<T extends AbstractDoubleScriptFieldQuery> extends
    AbstractScriptFieldQueryTestCase<T> {

    protected final DoubleRuntimeValues.LeafFactory leafFactory = mock(DoubleRuntimeValues.LeafFactory.class);

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }

    protected static DoubleRuntimeValues values(double[] values) {
        return new DoubleRuntimeValues() {
            @Override
            public void execute(int docId) {}

            @Override
            public void sort() {}

            @Override
            public double value(int idx) {
                return values[idx];
            }

            @Override
            public int count() {
                return values.length;
            }
        };
    }
}
