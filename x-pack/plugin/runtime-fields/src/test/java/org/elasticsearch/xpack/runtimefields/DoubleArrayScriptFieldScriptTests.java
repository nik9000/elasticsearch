/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.script.ScriptContext;

public class DoubleArrayScriptFieldScriptTests extends ScriptFieldScriptTestCase<DoubleArrayScriptFieldScript.Factory> {
    public static final DoubleArrayScriptFieldScript.Factory DUMMY = (params, lookup) -> ctx -> new DoubleArrayScriptFieldScript(
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            collectValue(1);
            collectValue(2);
        }
    };

    @Override
    protected ScriptContext<DoubleArrayScriptFieldScript.Factory> context() {
        return DoubleArrayScriptFieldScript.CONTEXT;
    }

    @Override
    protected DoubleArrayScriptFieldScript.Factory dummyScript() {
        return DUMMY;
    }
}
