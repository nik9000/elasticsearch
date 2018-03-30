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

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Set;

/**
 * A prepared request. Can be executed
 * {@link PreparedHighLevelRequest#perform synchronously}
 * or {@link PreparedHighLevelRequest#performAsync asynchronously}.
 */
public final class PreparedHighLevelRequest<ResponseT extends ActionResponse> {
    private final NamedXContentRegistry registry;
    private final PreparedRequest request;
    private final CheckedFunction<Response, ResponseT, IOException> responseConverter;
    private final Set<Integer> ignores;

    public PreparedHighLevelRequest(NamedXContentRegistry registry, PreparedRequest request,
            CheckedFunction<Response, ResponseT, IOException> responseConverter,
            Set<Integer> ignores) {
        this.registry = registry;
        this.request = request;
        this.responseConverter = responseConverter;
        this.ignores = ignores;
    }

    /**
     * Set the headers to send with this request.
     * @return this for chaining
     */
    public PreparedHighLevelRequest<ResponseT> headers(Header...headers) {
        request.headers(headers);
        return this;
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to.
     * Blocks until the request is completed and returns its response or fails
     * by throwing an exception. Selects a host out of the provided ones in a
     * round-robin fashion. Failing hosts are marked dead and retried after a
     * certain amount of time (minimum 1 minute, maximum 30 minutes), depending
     * on how many times they previously failed (the more failures, the later
     * they will be retried). In case of failures all of the alive nodes (or
     * dead nodes that deserve a retry) are retried until one responds or none
     * of them does, in which case an {@link IOException} will be thrown.
     *
     * This method works by performing an asynchronous call and waiting for
     * the result. If the asynchronous call throws an exception we wrap it and
     * rethrow it so that the stack trace attached to the exception contains
     * the call site. While we attempt to preserve the original exception this
     * isn't always possible and likely haven't covered all of the cases. You
     * can get the original exception from {@link Exception#getCause()}.
     *
     * @return the response returned by Elasticsearch
     * @throws IOException in case of a problem or the connection was aborted
     * @throws ClientProtocolException in case of an http protocol error
     * @throws ResponseException in case Elasticsearch responded with a status
     *      code that indicated an error
     */
    public ResponseT perform() throws IOException {
        Response response;
        try {
            response = request.perform();
        } catch (ResponseException e) {
            if (ignores.contains(e.getResponse().getStatusLine().getStatusCode())) {
                try {
                    return responseConverter.apply(e.getResponse());
                } catch (Exception innerException) {
                    /*
                     * the exception is ignored as we now try to parse the
                     * response as an error. This covers cases like get where
                     * 404 can either be a valid document not found response,
                     * or an error for which parsing is completely different.
                     * We try to consider the 404 response as a valid one
                     * first. If parsing of the response breaks, we fall back
                     * to parsing it as an error.
                     */
                    throw parseResponseException(e);
                }
            }
            throw parseResponseException(e);
        }

        try {
            return responseConverter.apply(response);
        } catch(Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    /**
     * Sends a request to the Elasticsearch cluster that the client points to.
     * The request is executed asynchronously and the provided
     * {@link ResponseListener} gets notified upon request completion or
     * failure. Selects a host out of the provided ones in a round-robin
     * fashion. Failing hosts are marked dead and retried after a certain
     * amount of time (minimum 1 minute, maximum 30 minutes), depending on how
     * many times they previously failed (the more failures, the later they
     * will be retried). In case of failures all of the alive nodes (or dead
     * nodes that deserve a retry) are retried until one responds or none of
     * them does, in which case an {@link IOException} will be thrown.
     *
     * @param responseListener the {@link ResponseListener} to notify when the
     *      request is completed or fails
     */
    public void performAsync(ActionListener<ResponseT> listener) {
        ResponseListener responseListener = wrapResponseListener(responseConverter, listener, ignores);
        request.performAsync(responseListener);
    }

    /**
     * Converts a {@link ResponseException} obtained from the low level REST
     * client into an {@link ElasticsearchException}. If a response body was
     * returned, tries to parse it as an error returned from Elasticsearch.
     * If no response body was returned or anything goes wrong while parsing
     * the error, returns a new {@link ElasticsearchStatusException} that
     * wraps the original {@link ResponseException}. The potential exception
     * obtained while parsing is added to the returned exception as a
     * suppressed exception. This method is guaranteed to not throw any
     * exception eventually thrown while parsing.
     */
    private final ElasticsearchStatusException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        ElasticsearchStatusException elasticsearchException;
        if (entity == null) {
            elasticsearchException = new ElasticsearchStatusException(
                    responseException.getMessage(), RestStatus.fromCode(response.getStatusLine().getStatusCode()), responseException);
        } else {
            try {
                elasticsearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
                elasticsearchException.addSuppressed(responseException);
            } catch (Exception e) {
                RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());
                elasticsearchException = new ElasticsearchStatusException("Unable to parse response body", restStatus, responseException);
                elasticsearchException.addSuppressed(e);
            }
        }
        return elasticsearchException;
    }

    private final <Resp> Resp parseEntity(final HttpEntity entity,
                                      final CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry,
            LoggingDeprecationHandler.INSTANCE, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    private final <Resp> ResponseListener wrapResponseListener(CheckedFunction<Response, Resp, IOException> responseConverter,
                                                        ActionListener<Resp> actionListener, Set<Integer> ignores) {
        return new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch(Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (ignores.contains(response.getStatusLine().getStatusCode())) {
                        try {
                            actionListener.onResponse(responseConverter.apply(response));
                        } catch (Exception innerException) {
                            //the exception is ignored as we now try to parse the response as an error.
                            //this covers cases like get where 404 can either be a valid document not found response,
                            //or an error for which parsing is completely different. We try to consider the 404 response as a valid one
                            //first. If parsing of the response breaks, we fall back to parsing it as an error.
                            actionListener.onFailure(parseResponseException(responseException));
                        }
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }
}
