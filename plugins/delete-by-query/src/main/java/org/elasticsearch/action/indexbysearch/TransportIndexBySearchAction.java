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

package org.elasticsearch.action.indexbysearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportIndexBySearchAction extends HandledTransportAction<IndexBySearchRequest, IndexBySearchResponse> {
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;
    private final TransportBulkAction bulkAction;
    private final TransportClearScrollAction clearScrollAction;

    @Inject
    public TransportIndexBySearchAction(Settings settings, ThreadPool threadPool, TransportSearchAction transportSearchAction,
            TransportSearchScrollAction transportSearchScrollAction, TransportBulkAction bulkAction,
            TransportClearScrollAction clearScrollAction, TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IndexBySearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                IndexBySearchRequest.class);
        this.searchAction = transportSearchAction;
        this.scrollAction = transportSearchScrollAction;
        this.bulkAction = bulkAction;
        this.clearScrollAction = clearScrollAction;
    }

    @Override
    protected void doExecute(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
        new AsyncIndexBySearchAction(request, listener).start();
    }

    class AsyncIndexBySearchAction extends AsyncScrollAction<IndexBySearchRequest, IndexBySearchResponse> {
        public AsyncIndexBySearchAction(IndexBySearchRequest request, ActionListener<IndexBySearchResponse> listener) {
            super(logger, searchAction, scrollAction, bulkAction, clearScrollAction, request, request.search(), listener);
        }

        @Override
        protected BulkRequest buildBulk(SearchHit[] docs) {
            BulkRequest bulkRequest = new BulkRequest(mainRequest);
            for (SearchHit doc : docs) {
                IndexRequest index = new IndexRequest(mainRequest.index(), mainRequest);

                // We want the index from the copied request, not the doc.
                index.id(doc.id());
                // TODO routing?
                index.source(doc.source());
                // TODO version?

                SearchHitField parent = doc.field("_parent");
                if (parent != null) {
                    index.parent((String) parent.value());
                }
                SearchHitField timestamp = doc.field("_timestamp");
                if (timestamp != null) {
                    // Comes back as a Long but needs to be a string
                    index.timestamp(timestamp.value().toString()); // TODO test
                                                                   // me
                }
                SearchHitField ttl = doc.field("_ttl");
                if (ttl != null) {
                    index.ttl((Long) ttl.value()); // TODO test me
                }

                bulkRequest.add(index);
            }
            return bulkRequest;
        }

        @Override
        protected IndexBySearchResponse buildResponse() {
            return new IndexBySearchResponse(indexed(), created());
        }
    }
}
