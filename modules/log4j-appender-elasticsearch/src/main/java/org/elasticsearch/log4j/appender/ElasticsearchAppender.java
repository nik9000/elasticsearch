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

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;

public class ElasticsearchAppender extends AbstractAppender {
    public static final String PLUGIN_NAME = "Elasticsearch";

    public static class Builder implements org.apache.logging.log4j.core.util.Builder<ElasticsearchAppender> {
        private String name;
        private String scheme = "http";
        private String host;
        private int port = 9200;
        private String username;
        private String password;
        private Map<String, String> headers = emptyMap();
        private int socketTimeoutMillis = ElasticsearchLogSyncManager.Spec.DEFAULT_SOCKET_TIMEOUT_MILLIS;
        private int connectTimeoutMillis = ElasticsearchLogSyncManager.Spec.DEFAULT_CONNECT_TIMEOUT_MILLIS;

        private String index;
        private String type;
        /**
         * Number of replicas to give the log index when creating it. If the idnex already exists then this has no effect.
         */
        private int numberOfReplicas = 2;
        /**
         * Number of shards to give the log index when creating it. If the index already exists then this has no effect.
         */
        private int numberOfShards = 1;

        /**
         * Number of logs that must be buffered before we start flushing the buffer to Elasticsearch. Defaults to {@code 100} which seems
         * like as good a number as any.
         */
        private int targetBatchSize = 100;
        /**
         * Maximum number of logs that can be buffered at any point before we start overwriting the last buffered log. Defaults to
         * {@code 1000} which means that the buffer takes up about a megabyte of heap.
         */
        private int maxBatchSize = 1000;
        /**
         * Number of milliseconds that a log event can stay in the buffer before we initiate a flush even if the buffer is under the
         * {@link #targetBatchSize}.
         */
        private long flushAfterMillis = TimeUnit.SECONDS.toMillis(30);
        /**
         * Executor used to schedule polling for logs to flush. Defaults to using starting a thread.
         */
        private DelayedExecutor executor;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withIndex(String index) {
            this.index = index;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        /**
         * Number of replicas to give the log index when creating it. If the idnex already exists then this has no effect.
         */
        public Builder withNumberOfReplicas(int numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        /**
         * Number of shards to give the log index when creating it. If the index already exists then this has no effect.
         */
        public Builder withNumberOfShards(int numberOfShards) {
            this.numberOfShards = numberOfShards;
            return this;
        }

        /**
         * Number of logs that must be buffered before we start flushing the buffer to Elasticsearch. Defaults to {@code 100} which seems
         * like as good a number as any.
         */
        public Builder withTargetBatchSize(int targetBatchSize) {
            this.targetBatchSize = targetBatchSize;
            return this;
        }

        /**
         * Maximum number of logs that can be buffered at any point before we start overwriting the last buffered log. Defaults to
         * {@code 1000} which means that the buffer takes up about a megabyte of heap.
         */
        public Builder withMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        /**
         * Number of milliseconds that a log event can stay in the buffer before we initiate a flush even if the buffer is under the
         * {@link #targetBatchSize}.
         */
        public Builder withFlushAfterMillis(long flushAfterMillis) {
            this.flushAfterMillis = flushAfterMillis;
            return this;
        }

        /**
         * Executor used to schedule polling for logs to flush. Defaults to using starting a thread.
         */
        public Builder withExecutor(DelayedExecutor executor) {
            this.executor = executor;
            return this;
        }

        @Override
        public ElasticsearchAppender build() {
            ElasticsearchLogSyncManager.Spec remoteInfo = new ElasticsearchLogSyncManager.Spec(scheme, host, port, 
                    username, password, headers, socketTimeoutMillis, connectTimeoutMillis, index, type, numberOfReplicas, numberOfShards,
                    targetBatchSize, maxBatchSize, flushAfterMillis, executor);
            // TODO be smarter about how managers are shared
            ElasticsearchLogSyncManager manager = ElasticsearchLogSyncManager.getManager(name, remoteInfo);
            return new ElasticsearchAppender(name, null, null, false, manager);
        }
    }

    private final ElasticsearchLogSyncManager manager;

    private ElasticsearchAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
            ElasticsearchLogSyncManager manager) {
        super(name, filter, layout, ignoreExceptions);
        this.manager = manager;
    }

    @Override
    public void append(LogEvent event) {
        manager.buffer(event);
    }

    @Override
    public void stop() {
        /* Override the default implementation of stop to give us a reasonable time to stop. We don't really expect this to be called
         * outside of unit tests but we may as well get it right. */
        stop(30, TimeUnit.SECONDS);
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        boolean superStopped = super.stop(timeout, timeUnit);
        // Stop the manager if it is not still in use by some other appender
        boolean managerStopped = manager.stop(timeout, timeUnit);
        return superStopped && managerStopped;
    }
}
