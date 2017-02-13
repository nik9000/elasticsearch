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

package org.elasticsearch.log4j.appender;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;

class ElasticsearchClient {
    private final List<Thread> threadCollector = synchronizedList(new ArrayList<>());
    private final BiConsumer<String, Exception> errorLogger;
    private final RestClient client;

    ElasticsearchClient(BiConsumer<String, Exception> errorLogger, String host, int port, String scheme, Map<String, String> headers,
            int socketTimeoutMillis, int connectTimeoutMillis, String username, String password) {
        this.errorLogger = errorLogger;

        Header[] clientHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> header : headers.entrySet()) {
            clientHeaders[i] = new BasicHeader(header.getKey(), header.getValue());
        }
        this.client = RestClient.builder(new HttpHost(host, port, scheme))
                .setDefaultHeaders(clientHeaders)
                .setRequestConfigCallback(c -> {
                    c.setSocketTimeout(socketTimeoutMillis);
                    c.setConnectTimeout(connectTimeoutMillis);
                    return c;
                })
                .setHttpClientConfigCallback(c -> {
                    // Enable basic auth if it is configured
                    if (username != null) {
                        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(username, password);
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, creds);
                        c.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    // Stick the task id in the thread name so we can track down tasks from stack traces
                    AtomicInteger threads = new AtomicInteger();
                    c.setThreadFactory(r -> {
                        String threadName = "es-log-appender-" + host + "-" + threads.getAndIncrement();
                        Thread t = new Thread(r, threadName);
                        threadCollector.add(t);
                        return t;
                    });
                    // Limit ourselves to one reactor thread because we only expect to have a single outstanding request at a time
                    c.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
                    return c;
                }).build();
    }

    public boolean close() {
        try {
            client.close();
        } catch (IOException e) {
            errorLogger.accept("couldn't shut down internal Elasticsearch client", e);
            return false;
        }
        List<String> runningThreads = threadCollector.stream().filter(Thread::isAlive).map(Thread::getName).collect(toList());
        if (false == runningThreads.isEmpty()) {
            errorLogger.accept("failed to properly stop client threads " + runningThreads + "]", null);
            return false;
        }
        return true;
    }

    public void indexExists(String name, Consumer<Boolean> callback) {
        client.performRequestAsync("HEAD", name, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    callback.accept(true);
                } else if (response.getStatusLine().getStatusCode() == 404) {
                    callback.accept(false);
                } else {
                    // NOCOMMIT sleep and retry
                    try {
                        errorLogger.accept("remote failed to check if index exists, status [" + response.getStatusLine().getStatusCode()
                                + "], response: " + EntityUtils.toString(response.getEntity()), null);
                    } catch (IOException e) {
                        errorLogger.accept("remote failed to check if index exists, status [" + response.getStatusLine().getStatusCode()
                                + "] and failed to extract response", null);
                    }
                }
            }

            @Override
            public void onFailure(Exception exception) {
                // NOCOMMIT sleep and retry
                errorLogger.accept("failed to check if index [" + name + "] exists", exception);
            }
        });
    }

    public void createIndex(String name, String config, Runnable callback) {
        HttpEntity entity = new StringEntity(config, ContentType.APPLICATION_JSON);
        client.performRequestAsync("PUT", name, emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
                    callback.run();
                } else {
                    // TODO sleep and retry
                    try {
                        errorLogger.accept("remote failed to create index, status [" + response.getStatusLine().getStatusCode()
                                + "], response: " + EntityUtils.toString(response.getEntity()), null);
                    } catch (IOException e) {
                        errorLogger.accept("remote failed to create index, status [" + response.getStatusLine().getStatusCode()
                                + "] and failed to extract response", null);
                    }
                }
            }

            @Override
            public void onFailure(Exception exception) {
                // TODO sleep and retry
                errorLogger.accept("failed to create index [" + name + "]", exception);
            }
        });
    }

    public void bulk(String path, String requestBody, Runnable callback) {
        HttpEntity entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
        client.performRequestAsync("POST", path, emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    String entity = EntityUtils.toString(response.getEntity());
                    if (response.getStatusLine().getStatusCode() != 200) {
                        errorLogger.accept("remote failed to execute _bulk request [" + response.getStatusLine().getStatusCode()
                                + "], response: " + entity, null);
                    }
                    if (entity.indexOf("\"errors\":true") >= 0) {
                        errorLogger.accept("remote failed portions of the _bulk request, response: " + entity, null);
                    }
                } catch (IOException e) {
                    errorLogger.accept("failed to extract response from _bulk request", e);
                }
                callback.run();
            }

            @Override
            public void onFailure(Exception exception) {
                errorLogger.accept("failed to send _bulk request", exception);
            }
        });
    }
}
