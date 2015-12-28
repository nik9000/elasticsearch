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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.containsString;

public class UpdateByQueryWithScriptTests extends UpdateByQueryTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SetBarScript.RegistrationPlugin.class);
        plugins.add(SetCtxFieldScript.RegistrationPlugin.class);
        return plugins;
    }

    @Before
    public void createTestData() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "1").setSource("foo", "a"));
    }

    private BulkIndexByScrollResponse updateByQuery(String script, String paramKey, Object paramValue) {
        return request().source("test").refresh(true)
                .script(new Script(script, ScriptType.INLINE, "native", singletonMap(paramKey, paramValue))).get();
    }

    public void testBasicUpdateByQuery() {
        assertThat(updateByQuery("set-bar", "to", "cat"), responseMatcher().updated(1));

        assertSearchHits(client().prepareSearch("test").setQuery(matchQuery("bar", "cat")).get(), "1");
    }

    public void testAddingJunkToScriptCtxIsError() {
        try {
            updateByQuery("set-ctx-field", "junk", "cat");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Invalid fields added to ctx [junk]"));
        }
    }

    public void testModifyingCtxNotAllowed() {
        /*
         * Its important that none of these actually match any of the fields.
         * They don't now, but make sure they still don't match if you add any
         * more. The point of have many is that they should all present the same
         * error message to the user, not some ClassCastException.
         */
        Object[] options = new Object[] {"cat", new Object(), 123, new Date(), Math.PI};
        for (String ctxVar: new String[] {"_index", "_type", "_id", "_version", "_parent", "_routing", "_timestamp", "_ttl"}) {
            try {
                updateByQuery("set-ctx-field", ctxVar, randomFrom(options));
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Modifying [" + ctxVar + "] not allowed"));
            }
        }
    }
}
