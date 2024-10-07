/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractBlockHashStatusTests<T extends BlockHash.Status> extends AbstractNamedWriteableTestCase<T> {
    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(BlockHash.getNamedWriteables());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final Class<T> categoryClass() {
        return (Class<T>) BlockHash.Status.class;
    }

    /**
     * A "simple" version of the status on which we can assert without randomization.
     */
    public abstract T simple();

    /**
     * The expected json rendering of {@link #simple()}.
     */
    public abstract String simpleToJson();

    public final void testToXContent() {
        assertThat(Strings.toString(simple(), true, true), equalTo(simpleToJson()));
    }
}
