/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.collect.CollectedConfig;
import org.elasticsearch.compute.operator.collect.CollectedMetadata;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.ESQL_SKIP_ES_INDEX_SERIALIZATION;

public class EsCollectedSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsCollectedSourceExec",
        EsCollectedSourceExec::readFrom
    );

    private final CollectedConfig config;
    private final List<Attribute> attributes;

    public EsCollectedSourceExec(EsRelation relation) {
        this(relation.source(), relation.collectedConfig(), relation.output());
    }

    public EsCollectedSourceExec(Source source, CollectedConfig config, List<Attribute> attributes) {
        super(source);
        if (config == null) {
            throw new IllegalArgumentException("collectedMetadata required");
        }
        this.config = config;
        this.attributes = attributes;
    }

    private static EsCollectedSourceExec readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        CollectedConfig config = new CollectedConfig(in);
        List<Attribute> attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        return new EsCollectedSourceExec(source, config, attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        config.writeTo(out);
        out.writeNamedWriteableCollection(attributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public CollectedConfig config() {
        return config;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EsCollectedSourceExec::new, config, attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, attributes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsCollectedSourceExec other = (EsCollectedSourceExec) obj;
        return config.equals(other.config) && attributes.equals(other.attributes);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + config.mainId() + "]" + NodeUtils.limitedToString(attributes);
    }
}
