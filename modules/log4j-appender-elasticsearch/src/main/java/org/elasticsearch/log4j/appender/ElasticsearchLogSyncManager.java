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
import org.apache.logging.log4j.core.impl.ThrowableProxy;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;

class ElasticsearchLogSyncManager extends AbstractManager {
    static ElasticsearchLogSyncManager getManager(String name, Spec remoteInfo) {
        return AbstractManager.getManager(name, ElasticsearchLogSyncManager::new, remoteInfo);
    }

    static class Spec {
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
        /**
         * Number of shards to give the log index when creating it. If the index already exists then this has no effect.
         */
        private final int numberOfShards;
        /**
         * Number of logs that must be buffered before we start flushing the buffer to Elasticsearch. Defaults to {@code 100} which seems
         * like as good a number as any.
         */
        private final int targetBatchSize;
        /**
         * Maximum number of logs that can be buffered at any point before we start overwriting the last buffered log. Defaults to
         * {@code 1000} which means that the buffer takes up about a megabyte of heap.
         */
        private final int maxBatchSize;
        /**
         * Number of milliseconds that a log event can stay in the buffer before we initiate a flush even if the buffer is under the
         * {@link #targetBatchSize}.
         */
        private final long flushAfterMillis;

        /**
         * Executor used to schedule polling for logs to flush. Defaults to using starting a thread.
         */
        private final DelayedExecutor executor;

        Spec(String scheme, String host, int port, String username, String password, Map<String, String> headers,
                int socketTimeoutMillis, int connectTimeoutMillis, String index, String type, int numberOfReplicas, int numberOfShards,
                int targetBatchSize, int maxBatchSize, long flushAfterMillis, DelayedExecutor executor) {
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
            this.targetBatchSize = targetBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.flushAfterMillis = flushAfterMillis;

            this.executor = executor;
        }
    }

    private final Spec spec;
    private final ElasticsearchClient client;
    private final LogBuffer buffer;
    private final String bulkPath;

    /**
     * Executor used to schedule polling for logs to flush. Defaults to using starting a thread.
     */
    private final DelayedExecutor executor;
    /**
     * Single-thread thread pool that backs {@link #executor} if none is provided to the constructor.
     */
    private final ScheduledThreadPoolExecutor builtinExecutorBacking;

    private volatile ScheduledFuture<?> nextPollForOldAgeFlush;
    private volatile boolean ready = false;

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
        this.buffer = new LogBuffer(this::logDebug, this::logWarn, this::logError, this::flushReady, this::flush, spec.targetBatchSize,
                spec.maxBatchSize, spec.flushAfterMillis);
        this.bulkPath = spec.index + "/" + spec.type + "/" + "_bulk";

        // Now make sure the index is ready before going anywhere
        createIndexIfMissing();

        if (spec.executor == null) {
            builtinExecutorBacking = new ScheduledThreadPoolExecutor(0, r -> {
                Thread t = new Thread(r);
                t.setName("elasticsearch-manager-" + name);
                return t;
            });
            executor = (d, r) -> builtinExecutorBacking.schedule(r, d, TimeUnit.MILLISECONDS);
        } else {
            builtinExecutorBacking = null;
            executor = spec.executor;
        }

        nextPollForOldAgeFlush = executor.submit(spec.flushAfterMillis, this::pollForOldAgeFlush);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        // Note: do not override releaseSub for cleanup as that'll do it under a lock.....
        boolean supetStopped = super.stop(timeout, timeUnit);
        boolean bufferClosed = buffer.close(timeout, timeUnit);
        boolean clientClosed = client.close();
        boolean executorBackingClosed = true;

        ScheduledFuture<?> toCancel = nextPollForOldAgeFlush;
        if (toCancel != null) {
            // TODO back this out when we move this out of Elasticsearch.
            FutureUtils.cancel(toCancel);
        }
        if (builtinExecutorBacking != null) {
            builtinExecutorBacking.shutdown();
            try {
                builtinExecutorBacking.awaitTermination(timeout, timeUnit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executorBackingClosed = false;
            }
        }
        return supetStopped && bufferClosed && clientClosed && executorBackingClosed;
    }

    void flush(long timeout, TimeUnit unit) throws InterruptedException {
        buffer.flush(timeout, unit);
    }

    void buffer(LogEvent event) {
        buffer.buffer(event);
    }

    private boolean flushReady() {
        return ready;
    }

    private void flush(Iterable<? extends LogEvent> events, Runnable callback) {
        if (false == ready) {
            return;
        }

        StringBuilder requestBody = new StringBuilder();
        for (LogEvent event : events) {
            requestBody.append("{\"index\":{}}\n");

            requestBody.append("{\"time\":").append(event.getTimeMillis());
            requestBody.append(",\"level\":\"").append(event.getLevel()).append("\"");
            requestBody.append(",\"message\":\"").append(jsonEscape(event.getMessage().getFormattedMessage())).append("\"");
            requestBody.append(",\"thread\":"); {
                requestBody.append("{\"id\":").append(event.getThreadId());
                requestBody.append(",\"name\":\"").append(jsonEscape(event.getThreadName())).append("\"");
                requestBody.append(",\"priority\":").append(event.getThreadPriority()).append("}");
            }
            if (event.getSource() != null) {
                requestBody.append(",\"source\":\"").append(jsonEscape(event.getSource().toString())).append("\"");
            }
            if (event.getLoggerName() != null) {
                requestBody.append(",\"logger\":\"").append(jsonEscape(event.getLoggerName())).append("\"");
            }
            ThrowableProxy thrown = event.getThrownProxy();
            if (thrown != null) {
                requestBody.append(",\"exception\":"); {
                    requestBody.append("{\"message\":\"").append(jsonEscape(thrown.getMessage())).append("\"");
                    requestBody.append(",\"trace\":\"").append(jsonEscape(thrown.getExtendedStackTraceAsString())).append("\"}");
                }
            }
            requestBody.append("}\n");
        }

        // TODO streaming or something
        client.bulk(bulkPath, requestBody.toString(), callback);
    }

    private String jsonEscape(String string) {
        // TODO More efficient and more accurate escaping
        return string.replace("\"", "\\\"").replace("\n", "\\n").replaceAll("\t", "\\t");
    }

    private void createIndexIfMissing() {
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
              + "        },\n"
              + "        \"source\": {\n"
              + "          \"type\": \"keyword\"\n"
              + "        },\n"
              + "        \"logger\": {\n"
              + "          \"type\": \"keyword\"\n"
              + "        },\n"
              + "        \"exception\": {\n"
              + "          \"properties\": {\n"
              + "            \"message\": {\n"
              + "              \"type\": \"text\"\n"
              + "            },\n"
              + "            \"trace\": {\n"
              + "              \"type\": \"text\"\n"
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

        client.createIndexIfMissing(spec.index, config, () -> {this.ready = true;});
    }

    private void pollForOldAgeFlush() {
        long toWait = spec.flushAfterMillis;
        try {
            toWait = buffer.pollForFlushMillis(System.currentTimeMillis());
            /* We don't want to get into the situation where we are busy polling because Elasticsearch is barely able to keep up so
             * we limit ourselves to polling at most once every second.*/
            toWait = max(1000, toWait);
        } finally {
            if (toWait > 0) {
                nextPollForOldAgeFlush = executor.submit(toWait, this::pollForOldAgeFlush);
            } else {
                nextPollForOldAgeFlush = null;
            }
        }
    }
}
