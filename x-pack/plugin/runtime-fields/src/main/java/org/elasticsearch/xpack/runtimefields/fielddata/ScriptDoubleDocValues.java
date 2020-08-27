/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.fielddata;

import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.xpack.runtimefields.DoubleRuntimeValues;

import java.io.IOException;

public final class ScriptDoubleDocValues extends SortedNumericDoubleValues {
    private final DoubleRuntimeValues doubles;
    private int cursor;

    ScriptDoubleDocValues(DoubleRuntimeValues doubles) {
        this.doubles = doubles;
    }

    @Override
    public boolean advanceExact(int docId) {
        doubles.execute(docId);
        if (doubles.count() == 0) {
            return false;
        }
        doubles.sort();
        cursor = 0;
        return true;
    }

    @Override
    public double nextValue() throws IOException {
        return doubles.value(cursor++);
    }

    @Override
    public int docValueCount() {
        return doubles.count();
    }
}
