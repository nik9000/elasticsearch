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
        private int socketTimeoutMillis = ElasticsearchLogSyncManager.RemoteInfo.DEFAULT_SOCKET_TIMEOUT_MILLIS;
        private int connectTimeoutMillis = ElasticsearchLogSyncManager.RemoteInfo.DEFAULT_CONNECT_TIMEOUT_MILLIS;

        private String index;
        private String type;
        private int numberOfReplicas = 2;
        private int numberOfShards = 1;
        private int batchSize = 100;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
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

        @Override
        public ElasticsearchAppender build() {
            ElasticsearchLogSyncManager.RemoteInfo remoteInfo = new ElasticsearchLogSyncManager.RemoteInfo(scheme, host, port, 
                    username, password, headers, socketTimeoutMillis, connectTimeoutMillis, index, type, numberOfReplicas, numberOfShards,
                    batchSize);
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
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        boolean superStopped = super.stop(timeout, timeUnit);
        boolean managerStopped = manager.stop(timeout, timeUnit);
        return superStopped && managerStopped;
    }
}
