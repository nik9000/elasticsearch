/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import java.io.IOException;

public class NullBlockHashStatusTests extends AbstractBlockHashStatusTests<NullBlockHash.Status> {
    public static NullBlockHash.Status randomStatus() {
        return new NullBlockHash.Status();
    }

    @Override
    protected NullBlockHash.Status createTestInstance() {
        return randomStatus();
    }

    @Override
    protected NullBlockHash.Status mutateInstance(NullBlockHash.Status instance) throws IOException {
        return null; // can't mutate
    }

    @Override
    public NullBlockHash.Status simple() {
        return new NullBlockHash.Status();
    }

    @Override
    public String simpleToJson() {
        return "{ }";
    }
}
