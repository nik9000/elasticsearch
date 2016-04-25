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

package org.elasticsearch.index;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class BlockUntilRefreshIT extends ESIntegTestCase {
    /**
     * Setting blockUntilRefresh will cause the request to block until the document is made visible by a refresh.
     */
    public void testBlockUntilRefresh() {
        IndexResponse index = client().prepareIndex("test", "index", "1").setSource("foo", "bar").setBlockUntilRefresh(true).get();
        assertEquals(RestStatus.CREATED, index.status());
        SearchResponse search = client().prepareSearch("test").setQuery(matchQuery("foo", "bar")).get();
        assertSearchHits(search, "1");
        // TODO tests for delete and bulk and ?update?
    }

}
