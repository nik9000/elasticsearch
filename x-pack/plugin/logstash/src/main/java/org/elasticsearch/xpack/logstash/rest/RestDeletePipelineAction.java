/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.xpack.logstash.action.DeletePipelineAction;
import org.elasticsearch.xpack.logstash.action.DeletePipelineRequest;
import org.elasticsearch.xpack.logstash.action.DeletePipelineResponse;

import java.io.IOException;
import java.util.List;

public class RestDeletePipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_delete_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(Method.DELETE, "/_logstash/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        return restChannel -> client.execute(
            DeletePipelineAction.INSTANCE,
            new DeletePipelineRequest(id),
            new RestActionListener<>(restChannel) {
                @Override
                protected void processResponse(DeletePipelineResponse deletePipelineResponse) {
                    final RestStatus status = deletePipelineResponse.isDeleted() ? RestStatus.OK : RestStatus.NOT_FOUND;
                    channel.sendResponse(new BytesRestResponse(status, XContentType.JSON.mediaType(), BytesArray.EMPTY));
                }
            }
        );
    }
}
