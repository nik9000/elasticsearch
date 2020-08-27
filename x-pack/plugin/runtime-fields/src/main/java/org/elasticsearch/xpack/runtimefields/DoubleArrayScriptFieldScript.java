/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class DoubleArrayScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("double_array_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "double_array_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleArrayScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public DoubleArrayScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    private double[] values = new double[1];
    private int count;

    public abstract void execute();

    protected void collectValue(double v) {
        if (values.length < count + 1) {
            values = ArrayUtil.grow(values, count + 1);
        }
        values[count++] = v;
    }

    public final DoubleRuntimeValues asRuntimeDoubles() {
        return new DoubleRuntimeValues() {
            @Override
            public void execute(int docId) {
                setDocument(docId);
                count = 0;
                DoubleArrayScriptFieldScript.this.execute();
            }

            @Override
            public void sort() {
                Arrays.sort(values, 0, count);
            }

            @Override
            public double value(int idx) {
                return values[idx];
            }

            @Override
            public int count() {
                return count;
            }
        };
    }

    public static class Value {
        private final DoubleArrayScriptFieldScript script;

        public Value(DoubleArrayScriptFieldScript script) {
            this.script = script;
        }

        public void value(double v) {
            script.collectValue(v);
        }
    }
}
