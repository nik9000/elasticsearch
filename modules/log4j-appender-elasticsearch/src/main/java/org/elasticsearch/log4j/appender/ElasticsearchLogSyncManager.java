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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyMap;
import static java.util.Collections.synchronizedList;

public class ElasticsearchLogSyncManager extends AbstractManager {
    public static ElasticsearchLogSyncManager getManager(String name, RemoteInfo remoteInfo) {
        return AbstractManager.getManager(name, ElasticsearchLogSyncManager::new, remoteInfo);
    }

    public static class RemoteInfo {
        /**
         * Default {@link #socketTimeoutMillis}.
         */
        public static final long DEFAULT_SOCKET_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
        /**
         * Default {@link #connectTimeoutMillis}.
         */
        public static final long DEFAULT_CONNECT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

        private final String scheme;
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        private final Map<String, String> headers;
        /**
         * Time to wait for a response from each request.
         */
        private final long socketTimeoutMillis;
        /**
         * Time to wait for a connecting to the remote cluster.
         */
        private final long connectTimeoutMillis;

        private final String index;
        private final String type;
        private final int numberOfReplicas;
        private final int numberOfShards;

        public RemoteInfo(String scheme, String host, int port,
                String username, String password, Map<String, String> headers, long socketTimeoutMillis, long connectTimeoutMillis,
                String index, String type, int numberOfReplicas, int numberOfShards) {
            this.scheme = scheme;
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.headers = headers;
            this.socketTimeoutMillis = socketTimeoutMillis;
            this.connectTimeoutMillis = connectTimeoutMillis;

            this.index = index;
            this.type = type;
            this.numberOfReplicas = numberOfReplicas;
            this.numberOfShards = numberOfShards;
        }
    }

    private final CountDownLatch ready = new CountDownLatch(1); // NOCOMMIT replace with something fancy

    private final List<Thread> threadCollector = synchronizedList(new ArrayList<>());
    private final RemoteInfo remoteInfo;
    private final RestClient client;
    private final String path;

    private ElasticsearchLogSyncManager(String name, RemoteInfo remoteInfo) {
        super(LoggerContext.getContext(false), name);
        if (remoteInfo.index.indexOf('/') >= 0) {
            throw new IllegalArgumentException("Index can't contain '/' but was [" + remoteInfo.index + "]");
        }
        if (remoteInfo.type.indexOf('/') >= 0) {
            throw new IllegalArgumentException("Type can't contain '/' but was [" + remoteInfo.type + "]");
        }
        this.remoteInfo = remoteInfo;
        this.path = remoteInfo.index + "/" + remoteInfo.type + "/" + "_bulk";

        // Now build the client
        Header[] clientHeaders = new Header[remoteInfo.headers.size()];
        int i = 0;
        for (Map.Entry<String, String> header : remoteInfo.headers.entrySet()) {
            clientHeaders[i] = new BasicHeader(header.getKey(), header.getValue());
        }
        this.client = RestClient.builder(new HttpHost(remoteInfo.host, remoteInfo.port, remoteInfo.scheme))
                .setDefaultHeaders(clientHeaders)
                .setRequestConfigCallback(c -> {
                    c.setSocketTimeout(Math.toIntExact(remoteInfo.socketTimeoutMillis));
                    c.setConnectTimeout(Math.toIntExact(remoteInfo.connectTimeoutMillis));
                    return c;
                })
                .setHttpClientConfigCallback(c -> {
                    // Enable basic auth if it is configured
                    if (remoteInfo.username != null) {
                        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(remoteInfo.username,
                                remoteInfo.password);
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, creds);
                        c.setDefaultCredentialsProvider(credentialsProvider);
                    }
                    // Stick the task id in the thread name so we can track down tasks from stack traces
                    AtomicInteger threads = new AtomicInteger();
                    c.setThreadFactory(r -> {
                        String threadName = "es-log-appender-" + name + "-" + threads.getAndIncrement();
                        Thread t = new Thread(r, threadName);
                        threadCollector.add(t);
                        return t;
                    });
                    // Limit ourselves to one reactor thread because we only expect to have a single outstanding request at a time
                    c.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(1).build());
                    return c;
                }).build();

        // Now make sure the index is ready before going anywhere
        createIndexIfMissing();
    }

    @Override
    protected boolean releaseSub(long timeout, TimeUnit timeUnit) {
        boolean superReleased = super.releaseSub(timeout, timeUnit);
        boolean clientClosed = false;
        try {
            try { // NOCOMMIT remove 
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            client.close();  // TODO log4j expects this to have a timeout but we totally don't
            clientClosed = true;
        } catch (IOException e) {
            logError("Couldn't shut down internal Elasticsearch client", e);
        }
        for (Thread thread : threadCollector) {
            if (thread.isAlive()) {
                assert false: "Failed to properly stop client thread [" + thread.getName() + "]";
                logError("Failed to properly stop client thread [" + thread.getName() + "]", null);
            }
        }
        return superReleased && clientClosed;
    }

    void sendLog(LogEvent event, String message) {
        try {
            ready.await();
        } catch (InterruptedException e) { // NOCOMMIT replace
            throw new RuntimeException(e);
        }

        message = message.replace("\"", "\\\"").replace("\n", "\\\n");
        String string = "{\"index\":{}}\n"
                + "{\"time\":" + event.getTimeMillis()
                + ",\"level\":\"" + event.getLevel() + "\""
                + ",\"message\":\"" + message + "\""
                + "}\n";
        HttpEntity entity = new StringEntity(string, ContentType.APPLICATION_JSON);
        try {
            checkBulkResponse(client.performRequest("POST", path, emptyMap(), entity));
        } catch (IOException e) {
            logError("Error sending log", e);
        }
    }

    private void checkBulkResponse(Response response) {
        try {
            String entity = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200 || entity.indexOf("\"errors\":true") >= 0) {
                logError("Error response from _bulk: " + entity, null);
            }
        } catch (IOException e) {
            logError("Error checking response from _bulk", e);
        }
    }

    private void createIndexIfMissing() {
        client.performRequestAsync("HEAD", remoteInfo.index, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    ready();
                } else if (response.getStatusLine().getStatusCode() == 404) {
                    createIndex();
                }
            }

            @Override
            public void onFailure(Exception exception) {
                logError("Failed to check if index exists", exception);
            }
        });
    }

    private void createIndex() {
        String indexConfig = 
                  "{\n"
                + "  \"mapping\": {\n"
                + "    \"" + remoteInfo.type + "\": {\n"
                + "      \"dynamic\": false,\n"
                + "      \"properties\": {\n"
                + "        \"time\": {\n"
                + "          \"type\": \"date\"\n"
                + "        },\n"
                + "        \"level\": {\n"
                + "          \"type\": \"keyword\",\n"
                + "        },\n"
                + "        \"message\": {\n"
                + "          \"type\": \"string\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"settings\": {\n"
                + "    \"number_of_replicas\": " + remoteInfo.numberOfReplicas + "\n"
                + "    \"number_of_shards\": " + remoteInfo.numberOfShards + "\n"
                + "  }\n"
                + "}\n";
        HttpEntity entity = new StringEntity(indexConfig, ContentType.APPLICATION_JSON);
        client.performRequestAsync("PUT", remoteInfo.index, emptyMap(), entity, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                ready();
            }

            @Override
            public void onFailure(Exception exception) {
                logError("Failed to check if index exists", exception);
            }
        });
    }

    private void ready() {
        ready.countDown();
    }
}
