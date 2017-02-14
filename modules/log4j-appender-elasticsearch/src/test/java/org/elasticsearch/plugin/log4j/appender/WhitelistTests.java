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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.plugin.log4j.appender.ElasticsearchAppenderPlugin.HOST_WHITELIST;

public class WhitelistTests extends ESTestCase {
    public void testWhitelisted() {
        List<String> whitelist = randomWhitelist();
        String[] inList = whitelist.iterator().next().split(":");
        String host = inList[0];
        int port = Integer.valueOf(inList[1]);
        plugin(whitelist).checkWhitelist(host, port);
    }

    public void testNotWhitelisted() {
        String host = randomAsciiOfLength(5);
        int port = between(1, Integer.MAX_VALUE);
        Exception e = expectThrows(IllegalArgumentException.class, () ->
            plugin(emptyList()).checkWhitelist(host, port));
        assertEquals("[" + host + ":" + port + "] not whitelisted in external_logging.whitelist", e.getMessage());
    }

    private ElasticsearchAppenderPlugin plugin(List<String> whitelist) {
        return new ElasticsearchAppenderPlugin(Settings.builder().put(HOST_WHITELIST.getKey(), String.join(",", whitelist)).build());
    }

    private List<String> randomWhitelist() {
        int size = between(1, 100);
        List<String> whitelist = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            whitelist.add(randomAsciiOfLength(5) + ':' + between(1, Integer.MAX_VALUE));
        }
        return whitelist;
    }
}
