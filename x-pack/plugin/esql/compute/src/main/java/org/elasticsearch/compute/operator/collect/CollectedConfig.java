/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.collect;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record CollectedConfig(String mainId, CollectedMetadata metadata) implements Writeable {
    public CollectedConfig(StreamInput in) throws IOException {
        this(in.readString(), new CollectedMetadata(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(mainId);
        metadata.writeTo(out);
    }
}
