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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.log4j.appender.DelayedExecutor;
import org.elasticsearch.log4j.appender.ElasticsearchAppender;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class ElasticsearchAppenderPlugin extends Plugin implements Closeable, ActionPlugin {
    private static final Pattern HOST_PATTERN = Pattern.compile(
            "(?<scheme>[^:]+)://(?<host>[^:]+):(?<port>\\d+)/(?<index>[^/]+)/(?<type>[^/]+)");
    public static final Setting<String> HOST = Setting.simpleString("external_logging.host", Property.Dynamic, Property.NodeScope);

    private static final Logger logger = ESLoggerFactory.getLogger(ElasticsearchAppenderPlugin.class);

    private final SetOnce<ThreadPool> threadPool = new SetOnce<>();
    private volatile ElasticsearchAppender appender;

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(HOST);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            shutdownAppenderIfRunning();
        }
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        this.threadPool.set(threadPool);
        return emptySet();
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        // NOCOMMIT make ClusterSettings more widely available
        clusterSettings.addSettingsUpdateConsumer(HOST, this::setupAppender, host -> {
            if (Strings.hasLength(host) && false == HOST_PATTERN.matcher(host).matches()) {
                throw new IllegalArgumentException(
                        "[" + HOST.getKey() + "] must be of the form [scheme]://[host]:[port]/[index]/[type] but was [" + host + "]");
            }
        });
        return emptyList();
    }

    private void setupAppender(String host) {
        synchronized (this) {
            // NOCOMMIT whitelist
            shutdownAppenderIfRunning();

            if (Strings.hasLength(host)) {
                Matcher m = HOST_PATTERN.matcher(host);
                if (false == m.matches()) {
                    throw new IllegalArgumentException(
                            "[" + HOST.getKey() + "] must be of the form [scheme]://[host]:[port]/[index]/[type] but was [" + host + "]");
                }
                String scheme = m.group("scheme");
                host = m.group("host");
                int port = Integer.parseInt(m.group("port"));
                String index = m.group("index");
                String type = m.group("type");
                logger.info("enabling external logging to [{}://{}:{}/{}/{}]", scheme, host, port, index, type);
                DelayedExecutor executor = (afterMillis, runnable) -> threadPool.get().schedule(timeValueMillis(afterMillis),
                        ThreadPool.Names.GENERIC, runnable);
                appender = new ElasticsearchAppender.Builder().withName("remote_es").withScheme(scheme).withHost(host).withPort(port)
                        .withIndex(index).withType(type).withExecutor(executor).build();
                appender.start();
                LoggerContext context = (LoggerContext) LogManager.getContext(false);
                context.getRootLogger().addAppender(appender);
                logger.info("enabled external logging to [{}://{}:{}/{}/{}]", scheme, host, port, index, type);
            }
        }
    }

    private void shutdownAppenderIfRunning() {
        if (appender == null) {
            return;
        }
        logger.info("shutting down external logging");
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.getRootLogger().removeAppender(appender);
        appender.stop(30, TimeUnit.SECONDS);
        logger.info("shut down external logging");
    }
}