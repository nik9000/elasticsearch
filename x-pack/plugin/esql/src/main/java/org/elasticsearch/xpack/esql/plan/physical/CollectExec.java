/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CollectExec extends UnaryExec {
    private final ReferenceAttribute nameAttr;
    private final ReferenceAttribute pageCountAttr;
    private final ReferenceAttribute expirationAttr;
    private final String name;
    private final TimeValue expiration;

    public CollectExec(
        Source source,
        PhysicalPlan child,
        ReferenceAttribute nameAttr,
        ReferenceAttribute pageCountAttr,
        ReferenceAttribute expirationAttr,
        String name,
        TimeValue expiration
    ) {
        super(source, child);
        this.nameAttr = nameAttr;
        this.pageCountAttr = pageCountAttr;
        this.expirationAttr = expirationAttr;
        this.name = name;
        this.expiration = expiration;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodeInfo<CollectExec> info() {
        return NodeInfo.create(this, CollectExec::new, child(), nameAttr, pageCountAttr, expirationAttr, name, expiration);
    }

    @Override
    public CollectExec replaceChild(PhysicalPlan newChild) {
        return new CollectExec(source(), newChild, nameAttr, pageCountAttr, expirationAttr, name, expiration);
    }

    public String name() {
        return name;
    }

    public TimeValue expiration() {
        return expiration;
    }

    @Override
    public List<Attribute> output() {
        return List.of(nameAttr, pageCountAttr, expirationAttr);
    }

    @Override
    protected AttributeSet computeReferences() {
        return child().references();
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), nameAttr, pageCountAttr, expirationAttr, name, expiration);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CollectExec other = (CollectExec) obj;
        return nameAttr.equals(other.nameAttr)
            && pageCountAttr.equals(other.pageCountAttr)
            && expirationAttr.equals(other.expirationAttr)
            && name.equals(other.name)
            && expiration.equals(other.expiration)
            && child().equals(other.child());
    }
}
