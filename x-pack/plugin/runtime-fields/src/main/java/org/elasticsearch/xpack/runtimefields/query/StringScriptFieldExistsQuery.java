/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.util.List;

public class StringScriptFieldExistsQuery extends AbstractStringScriptFieldQuery {
    public StringScriptFieldExistsQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName) {
        super(leafFactory, fieldName);
    }

    @Override
    public boolean matches(List<String> values) {
        return false == values.isEmpty();
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return "*";
        }
        return fieldName() + ":*";
    }

    // Superclass's equals and hashCode are great for this class
}
