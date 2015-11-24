/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.deletebyquery;

import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.util.Collection;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class IndexBySearchScriptTests extends IndexBySearchTestCase {
    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(DeleteByQueryPlugin.class, NativeTestScriptsPlugin.class);
    }

    public void testScriptedCopy() throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "1").setSource("foo", "a"));

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest");
        copy.script(new Script("replace", ScriptType.INLINE, "native", singletonMap("foo", singletonMap("a", "b"))));
        assertResponse(copy.get(), 1, 0);
        refresh();

        assertHitCount(client().prepareSearch("test").get(), 2);
        assertSearchHits(client().prepareSearch("test").setQuery(termQuery("foo", "b")).get(), "1");
    }

    public void testScriptedUpdate() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"));

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test"); // type defaults to the doc's type
        copy.script(new Script("merge", ScriptType.INLINE, "native", singletonMap("bar", "a")));
        assertResponse(copy.get(), 1, 0);
        refresh();

        assertHitCount(client().prepareSearch("test").get(), 1);
        assertSearchHits(client().prepareSearch("test").setQuery(termQuery("bar", "a")).get(), "1");
    }

    public static class NativeTestScriptsPlugin extends Plugin {
        public static final String NAME = "native-test-scripts";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public String description() {
            return "native scripts for testing";
        }

        public void onModule(ScriptModule scriptModule) {
            scriptModule.registerScript("replace", ReplaceScriptFactory.class);
            scriptModule.registerScript("merge", MergeScriptFactory.class);
        }
    }

    public static class ReplaceScriptFactory extends AbstractBaseNativeScriptFactory {
        @Override
        public ExecutableScript newScript(final Map<String, Object> params) {
            return new AbstractBaseExecutableScript() {
                @Override
                public void run(Map<String, Object> ctx) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    for (Map.Entry<String, Object> field: params.entrySet()) {
                        Object value = source.get(field.getKey());
                        if (value == null || false == (value instanceof String)) {
                            continue;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> replacements = (Map<String, Object>) field.getValue();
                        Object replacement = replacements.get(value);
                        if (replacement == null) {
                            continue;
                        }
                        source.put(field.getKey(), replacement);
                    }
                }
            };
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

    public static class MergeScriptFactory extends AbstractBaseNativeScriptFactory {
        @Override
        public ExecutableScript newScript(final Map<String, Object> params) {
            return new AbstractBaseExecutableScript() {
                @Override
                public void run(Map<String, Object> ctx) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
                    source.putAll(params);
                }
            };
        }
    }

    private static abstract class AbstractBaseNativeScriptFactory implements NativeScriptFactory {
        @Override
        public boolean needsScores() {
            return false;
        }
    }


    private static abstract class AbstractBaseExecutableScript implements ExecutableScript {
        private Map<String, Object> ctx;

        protected abstract void run(Map<String, Object> ctx);

        @Override
        @SuppressWarnings("unchecked")
        public void setNextVar(String name, Object value) {
            if (name.equals("ctx")) {
                ctx = (Map<String, Object>) value;
                return;
            }
            throw new IllegalArgumentException("Unsupported variable");
        }

        @Override
        public Object run() {
            run(ctx);
            return null;
        }

        @Override
        public Object unwrap(Object value) {
            return value;
        }
    }
}
