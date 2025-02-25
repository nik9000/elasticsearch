/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.collect.CollectedConfig;
import org.elasticsearch.compute.operator.collect.CollectedMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

/**
 * An Elasticsearch index.
 * @param partiallyUnmappedFields Fields mapped only in some (but *not* all) indices.
 *                                Since this is only used by the analyzer, it is not serialized.
 * @param collectedConfig If this is {@code FROM} a {@code | COLLECT}ed result this
 *                        is that result's read configuration.
 */
public record EsIndex(
    String name,
    Map<String, EsField> mapping,
    Map<String, IndexMode> indexNameWithModes,
    Set<String> partiallyUnmappedFields,
    @Nullable CollectedConfig collectedConfig
) implements Writeable {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert partiallyUnmappedFields != null;
    }

    public EsIndex(String name, Map<String, EsField> mapping, Map<String, IndexMode> indexNameWithModes) {
        this(name, mapping, indexNameWithModes, Set.of(), null);
    }

    /**
     * Intended for tests. Returns an index with an empty index mode map.
     */
    public EsIndex(String name, Map<String, EsField> mapping) {
        this(name, mapping, Map.of(), Set.of(), null);
    }

    public static EsIndex readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        Map<String, EsField> mapping = in.readImmutableMap(StreamInput::readString, EsField::readFrom);
        Map<String, IndexMode> indexNameWithModes;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            indexNameWithModes = in.readMap(IndexMode::readFrom);
        } else {
            @SuppressWarnings("unchecked")
            Set<String> indices = (Set<String>) in.readGenericValue();
            assert indices != null;
            indexNameWithModes = indices.stream().collect(toMap(e -> e, e -> IndexMode.STANDARD));
        }
        CollectedConfig collectedConfig = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_COLLECT)
            ? in.readOptional(CollectedConfig::new)
            : null;
        return new EsIndex(
            name,
            mapping,
            indexNameWithModes,
            Set.of(), // Use Set.of() for partially unmapped fields because we don't need this outside of the coordinator.
            collectedConfig
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
        out.writeMap(mapping(), (o, x) -> x.writeTo(out));
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeMap(indexNameWithModes, (o, v) -> IndexMode.writeTo(v, out));
        } else {
            out.writeGenericValue(indexNameWithModes.keySet());
        }
        // partially unmapped fields shouldn't pass the coordinator node anyway, since they are only used by the Analyzer.
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_COLLECT)) {
            out.writeOptional((o, c) -> c.writeTo(o), collectedConfig);
        }
    }

    public boolean isPartiallyUnmappedField(String fieldName) {
        return partiallyUnmappedFields.contains(fieldName);
    }

    public Set<String> concreteIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
