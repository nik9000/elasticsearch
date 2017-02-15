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

package org.elasticsearch.plugin.log4j.appender;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.log4j.appender.ElasticsearchAppender;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Asks the node to flush its external logging buffer if it has one, otherwise it is a noop.
 */
class RestFlushExternalLoggingHandler extends BaseRestHandler {
    private final Supplier<ElasticsearchAppender> appenderSource;

    RestFlushExternalLoggingHandler(Settings settings, RestController controller, Supplier<ElasticsearchAppender> appenderSource) {
        super(settings);
        this.appenderSource = appenderSource;
        controller.registerHandler(POST, "/_flush_external_logging", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // NOCOMMIT should this be a transport action? Probably.....
        return channel -> {
            ElasticsearchAppender appender = appenderSource.get();
            boolean flushed = false;
            if (appender != null) {
                appender.flush(30, TimeUnit.SECONDS);
                flushed = true;
            }
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("flushed", flushed);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

}
