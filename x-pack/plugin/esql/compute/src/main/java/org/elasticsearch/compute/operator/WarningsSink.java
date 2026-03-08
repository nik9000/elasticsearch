/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Collects warning strings, deduplicating and preserving insertion order.
 * Shared by all evaluators within a single driver.
 */
public final class WarningsSink {

    private LinkedHashSet<String> warnings = new LinkedHashSet<>();

    /**
     * Add a warning. Duplicates are silently ignored.
     * Must not be called after {@link #takeWarnings()}.
     */
    public void add(String warning) {
        warnings.add(warning);
    }

    /**
     * Takes ownership of the collected warnings. The sink must not be used after this call.
     */
    public Set<String> takeWarnings() {
        Set<String> result = Collections.unmodifiableSet(warnings);
        warnings = null;
        return result;
    }
    @Override
    public String toString() {
        return warnings.toString();
    }
}
