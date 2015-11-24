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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchAction;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = SUITE, transportClientRatio = 0) // TODO why?
public class IndexBySearchTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(DeleteByQueryPlugin.class);
    }

    public void testBasicsIntoExistingIndex() throws Exception {
        basics(true);
    }

    public void testBasicsIntoNewIndex() throws Exception {
        basics(false);
    }

    /**
     * Simple, readable test case around copying documents.
     */
    private void basics(boolean destinationIndexIsSourceIndex) throws Exception {
        indexRandom(true,
                client().prepareIndex("test", "source", "1").setSource("foo", "a"),
                client().prepareIndex("test", "source", "2").setSource("foo", "a"),
                client().prepareIndex("test", "source", "3").setSource("foo", "b"),
                client().prepareIndex("test", "source", "4").setSource("foo", "c"));
        assertTestSize("test", "source", 4);

        String destinationIndex = destinationIndexIsSourceIndex ? "test" : "not_test";

        // Copy all the docs
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex(destinationIndex).setType("dest_all");
        assertResponse(copy.get(), 4, 0);
        refresh();
        assertTestSize(destinationIndex, "dest_all", 4);

        // Now none of them
        copy = newIndexBySearch();
        copy.search().setQuery(termQuery("foo", "no_match"));
        copy.index().setIndex(destinationIndex).setType("dest_none");
        assertResponse(copy.get(), 0, 0);
        refresh();
        assertTestSize(destinationIndex, "dest_none", 0);

        // Now half of them
        copy = newIndexBySearch();
        copy.search().setTypes("source").setQuery(termQuery("foo", "a"));
        copy.index().setIndex(destinationIndex).setType("dest_half");
        assertResponse(copy.get(), 2, 0);
        refresh();
        assertTestSize(destinationIndex, "dest_half", 2);
    }

    public void testCopyMany() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("test", "source", Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertTestSize("test", "source", max);

        // Copy all the docs
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest_all");
        assertResponse(copy.get(), max, 0);
        refresh();
        assertTestSize("test", "dest_all", max);
    }

    public void testParentChild() throws Exception {
        QueryBuilder parentIsUS = setupParentChildIndex(true);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(parentIsUS);
        copy.index().setIndex("test").setType("dest");
        assertResponse(copy.get(), 1, 0);
        refresh();

        // Make sure parent/child is intact on that type
        assertSearchHits(client().prepareSearch("test").setTypes("dest").setQuery(parentIsUS).get(), "north carolina");
    }

    public void testErrorMessageWhenBadParentChild() throws Exception {
        QueryBuilder parentIsUS = setupParentChildIndex(false);

        // Copy the child to a new type
        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setQuery(parentIsUS);
        copy.index().setIndex("test").setType("dest");
        try {
            copy.get();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify parent if no parent field has been configured"));
        }
    }

    private IndexBySearchRequestBuilder newIndexBySearch() {
        return new IndexBySearchRequestBuilder(client(), IndexBySearchAction.INSTANCE);
    }

    private void assertResponse(IndexBySearchResponse response, long expectedIndexed, long expectedCreated) {
        assertThat(response.indexed(), equalTo(expectedIndexed));
        assertThat(response.created(), equalTo(expectedCreated));
    }

    /**
     * Setup a parent/child index and return a query that should find the child using the parent.
     */
    private QueryBuilder setupParentChildIndex(boolean addParentMappingForDest) throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("city", "{\"_parent\": {\"type\": \"country\"}}")
                .addMapping(addParentMappingForDest ? "dest" : "not_dest", "{\"_parent\": {\"type\": \"country\"}}"));
        ensureGreen();

        indexRandom(true,
                client().prepareIndex("test", "country", "united states").setSource("foo", "bar"),
                client().prepareIndex("test", "city", "north carolina").setParent("united states").setSource("foo", "bar"));

        // Make sure we build the parent/child relationship
        QueryBuilder parentIsUS = hasParentQuery("country", idsQuery("country").addIds("united states"));
        assertSearchHits(client().prepareSearch("test").setQuery(parentIsUS).get(), "north carolina");

        return parentIsUS;
    }

    private void assertTestSize(String index, String type, int expected) {
        assertHitCount(client().prepareSearch(index).setTypes(type).setSize(0).get(), expected);
    }
}
