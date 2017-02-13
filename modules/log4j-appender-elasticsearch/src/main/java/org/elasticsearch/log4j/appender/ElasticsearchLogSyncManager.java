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

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractManager;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ElasticsearchLogSyncManager extends AbstractManager {
    public static ElasticsearchLogSyncManager getManager(String name, Spec remoteInfo) {
        return AbstractManager.getManager(name, ElasticsearchLogSyncManager::new, remoteInfo);
    }

    public static class Spec {
        /**
         * Default {@link #socketTimeoutMillis}.
         */
        public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = Math.toIntExact(TimeUnit.SECONDS.toMillis(30));
        /**
         * Default {@link #connectTimeoutMillis}.
         */
        public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = Math.toIntExact(TimeUnit.SECONDS.toMillis(30));

        private final String scheme;
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        private final Map<String, String> headers;
        /**
         * Time to wait for a response from each request.
         */
        private final int socketTimeoutMillis;
        /**
         * Time to wait for a connecting to the remote cluster.
         */
        private final int connectTimeoutMillis;

        private final String index;
        private final String type;
        private final int numberOfReplicas;
        private final int numberOfShards;
        private final int batchSize;

        public Spec(String scheme, String host, int port, String username, String password, Map<String, String> headers,
                int socketTimeoutMillis, int connectTimeoutMillis, String index, String type, int numberOfReplicas, int numberOfShards,
                int batchSize) {
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
            this.batchSize = batchSize;
        }
    }

    private final CountDownLatch ready = new CountDownLatch(1); // NOCOMMIT replace with something fancy

    private final Spec spec;
    private final ElasticsearchClient client;
    private final LogBuffer buffer;
    private final String bulkPath;

    private ElasticsearchLogSyncManager(String name, Spec spec) {
        super(LoggerContext.getContext(false), name);
        if (spec.index.indexOf('/') >= 0) {
            throw new IllegalArgumentException("Index can't contain '/' but was [" + spec.index + "]");
        }
        if (spec.type.indexOf('/') >= 0) {
            throw new IllegalArgumentException("Type can't contain '/' but was [" + spec.type + "]");
        }
        this.spec = spec;
        this.client = new ElasticsearchClient(this::logError, spec.host, spec.port, spec.scheme, spec.headers, spec.socketTimeoutMillis,
                spec.connectTimeoutMillis, spec.username, spec.password);
        this.buffer = new LogBuffer((m, t) -> {
            // NOCOMMIT remove me
            System.err.println(m);
            if (t != null) {
                t.printStackTrace();
            }
            logDebug(m, t);
        }, this::logWarn, this::logError, this::flush, spec.batchSize);
        this.bulkPath = spec.index + "/" + spec.type + "/" + "_bulk";

        // Now make sure the index is ready before going anywhere
        createIndexIfMissing();
    }

    @Override
    protected boolean releaseSub(long timeout, TimeUnit timeUnit) {
        boolean superReleased = super.releaseSub(timeout, timeUnit);
        boolean bufferClosed = buffer.close(timeout, timeUnit);
        boolean clientClosed = client.close();
        return superReleased && bufferClosed && clientClosed;
    }

    void buffer(LogEvent event) {
        buffer.buffer(event);
    }

    private void flush(Iterable<? extends LogEvent> events, Runnable callback) {
        try {
            // NOCOMMIT replace with something nicer
            ready.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        StringBuilder requestBody = new StringBuilder();
        for (LogEvent event : events) {
            requestBody.append("{\"index\":{}}\n");

            // TODO More efficient and more accurate escaping
            String message = event.getMessage().getFormattedMessage().replace("\"", "\\\"").replace("\n", "\\\n");

            requestBody.append("{\"time\":").append(event.getTimeMillis());
            requestBody.append(",\"level\":\"").append(event.getLevel()).append("\"");
            requestBody.append(",\"message\":\"").append(message).append("\"");
            requestBody.append(",\"thread\":"); {
                requestBody.append("{\"id\":").append(event.getThreadId());
                requestBody.append(",\"name\":\"").append(event.getThreadName()).append("\"");
                requestBody.append(",\"priority\":").append(event.getThreadPriority()).append("}");
            }
            requestBody.append("}\n");
        }

        // TODO streaming or something
        client.bulk(bulkPath, requestBody.toString(), callback);
    }

    private void createIndexIfMissing() {
        client.indexExists(spec.index, exists -> {
            if (exists) {
                ready();
            } else {
                createIndex();
            }
        });
    }

    private void createIndex() {
        String config = 
                  "{\n"
                + "  \"mappings\": {\n"
                + "    \"" + spec.type + "\": {\n"
                + "      \"dynamic\": false,\n"
                + "      \"properties\": {\n"
                + "        \"time\": {\n"
                + "          \"type\": \"date\"\n"
                + "        },\n"
                + "        \"level\": {\n"
                + "          \"type\": \"keyword\"\n"
                + "        },\n"
                + "        \"message\": {\n"
                + "          \"type\": \"text\"\n"
                + "        },\n"
                + "        \"thread\": {\n"
                + "          \"properties\": {\n"
                + "            \"id\": {\n"
                + "              \"type\": \"long\"\n"
                + "            },\n"
                + "            \"name\": {\n"
                + "              \"type\": \"keyword\"\n"
                + "            },\n"
                + "            \"priority\": {\n"
                + "              \"type\": \"integer\"\n"
                + "            }\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"settings\": {\n"
                + "    \"number_of_replicas\": " + spec.numberOfReplicas + ",\n"
                + "    \"number_of_shards\": " + spec.numberOfShards + "\n"
                + "  }\n"
                + "}\n";
        client.createIndex(spec.index, config, this::ready);
    }

    private void ready() {
        ready.countDown();
    }
}
