/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.xpack.runtimefields.DoubleRuntimeValues;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DoubleScriptFieldExistsQueryTests extends AbstractDoubleScriptFieldQueryTestCase<DoubleScriptFieldExistsQuery> {
    @Override
    protected DoubleScriptFieldExistsQuery createTestInstance() {
        return new DoubleScriptFieldExistsQuery(randomScript(), leafFactory, randomAlphaOfLength(5));
    }

    @Override
    protected DoubleScriptFieldExistsQuery copy(DoubleScriptFieldExistsQuery orig) {
        return new DoubleScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName());
    }

    @Override
    protected DoubleScriptFieldExistsQuery mutate(DoubleScriptFieldExistsQuery orig) {
        if (randomBoolean()) {
            new DoubleScriptFieldExistsQuery(randomValueOtherThan(orig.script(), this::randomScript), leafFactory, orig.fieldName());
        }
        return new DoubleScriptFieldExistsQuery(orig.script(), leafFactory, orig.fieldName() + "modified");
    }

    @Override
    public void testMatches() {
        assertTrue(createTestInstance().matches(values(new double[] { 1 })));
        assertFalse(createTestInstance().matches(values(new double[0])));
        DoubleRuntimeValues big = mock(DoubleRuntimeValues.class);
        when(big.count()).thenReturn(between(2, Integer.MAX_VALUE));
        assertTrue(createTestInstance().matches(big));
    }

    @Override
    protected void assertToString(DoubleScriptFieldExistsQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo("DoubleScriptFieldExistsQuery"));
    }
}
