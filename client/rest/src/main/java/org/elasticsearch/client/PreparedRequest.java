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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLHandshakeException;

import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.conn.ConnectTimeoutException;

/**
 * Request that has yet to be performed so it can still be cusomized.
 */
public class PreparedRequest {
    private static final Header[] EMPTY_HEADERS = new Header[0];
    private final RestClient client;
    private final String method;
    private final String endpoint;
    private Header[] headers = EMPTY_HEADERS;
    private Map<String, String> params = Collections.<String, String>emptyMap();
    private HttpEntity entity;
    private HttpAsyncResponseConsumerFactory responseConsumerFactory =
            HttpAsyncResponseConsumerFactory.DEFAULT;

    PreparedRequest(RestClient client, String method, String endpoint) {
        this.client = client;
        this.method = method;
        this.endpoint = endpoint;
    }

    /**
     * Set the headers to send with this request.
     * @return this for chaining
     */
    public PreparedRequest headers(Header... headers) {
        this.headers = headers;
        return this;
    }

    /**
     * Set the parameters sent on the url with this request.
     * @return this for chaining
     */
    public PreparedRequest params(Map<String, String> params) {
        this.params = params;
        return this;
    }

    /**
     * Set the entity to send with this request.
     * @return this for chaining
     */
    public PreparedRequest entity(HttpEntity entity) {
        this.entity = entity;
        return this;
    }

    /**
     * Customize how we consume the response.
     * @return this for chaining
     */
    public PreparedRequest responseConsumerFactory(HttpAsyncResponseConsumerFactory responseConsumerFactory) {
        this.responseConsumerFactory = responseConsumerFactory;
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
    public Response perform() throws IOException {
        SyncResponseListener listener = new SyncResponseListener(client.getMaxRetryTimeoutMillis());
        client.performRequestAsyncNoCatch(method, endpoint, params, entity, responseConsumerFactory,
            listener, headers);
        return listener.get();
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
    public void performAsync(ResponseListener responseListener) {
        try {
            client.performRequestAsyncNoCatch(method, endpoint, params, entity, responseConsumerFactory,
                responseListener, headers);
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }

    /**
     * Listener used in any sync performRequest calls, it waits for a response or an exception back up to a timeout
     */
    static class SyncResponseListener implements ResponseListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        private final long timeout;

        SyncResponseListener(long timeout) {
            assert timeout > 0;
            this.timeout = timeout;
        }

        @Override
        public void onSuccess(Response response) {
            Objects.requireNonNull(response, "response must not be null");
            boolean wasResponseNull = this.response.compareAndSet(null, response);
            if (wasResponseNull == false) {
                throw new IllegalStateException("response is already set");
            }

            latch.countDown();
        }

        @Override
        public void onFailure(Exception exception) {
            Objects.requireNonNull(exception, "exception must not be null");
            boolean wasExceptionNull = this.exception.compareAndSet(null, exception);
            if (wasExceptionNull == false) {
                throw new IllegalStateException("exception is already set");
            }
            latch.countDown();
        }

        /**
         * Waits (up to a timeout) for some result of the request: either a response, or an exception.
         */
        Response get() throws IOException {
            try {
                //providing timeout is just a safety measure to prevent everlasting waits
                //the different client timeouts should already do their jobs
                if (latch.await(timeout, TimeUnit.MILLISECONDS) == false) {
                    throw new IOException("listener timeout after waiting for [" + timeout + "] ms");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("thread waiting for the response was interrupted", e);
            }

            Exception exception = this.exception.get();
            Response response = this.response.get();
            if (exception != null) {
                if (response != null) {
                    IllegalStateException e = new IllegalStateException("response and exception are unexpectedly set at the same time");
                    e.addSuppressed(exception);
                    throw e;
                }
                /*
                 * Wrap and rethrow whatever exception we received, copying the type
                 * where possible so the synchronous API looks as much as possible
                 * like the asynchronous API. We wrap the exception so that the caller's
                 * signature shows up in any exception we throw.
                 */
                if (exception instanceof ResponseException) {
                    throw new ResponseException((ResponseException) exception);
                }
                if (exception instanceof ConnectTimeoutException) {
                    ConnectTimeoutException e = new ConnectTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SocketTimeoutException) {
                    SocketTimeoutException e = new SocketTimeoutException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof ConnectionClosedException) {
                    ConnectionClosedException e = new ConnectionClosedException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof SSLHandshakeException) {
                    SSLHandshakeException e = new SSLHandshakeException(exception.getMessage());
                    e.initCause(exception);
                    throw e;
                }
                if (exception instanceof IOException) {
                    throw new IOException(exception.getMessage(), exception);
                }
                if (exception instanceof RuntimeException){
                    throw new RuntimeException(exception.getMessage(), exception);
                }
                throw new RuntimeException("error while performing request", exception);
            }

            if (response == null) {
                throw new IllegalStateException("response not set and no exception caught either");
            }
            return response;
        }
    }
}
