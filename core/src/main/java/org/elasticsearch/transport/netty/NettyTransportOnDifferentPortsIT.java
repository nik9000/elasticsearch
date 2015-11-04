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

package org.elasticsearch.transport.netty;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportInfo;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Checks that Elasticsearch produces a sane publish_address when it binds to
 * different ports on ipv4 and ipv6.
 */
@ESIntegTestCase.SuppressLocalMode
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NettyTransportOnDifferentPortsIT extends ESIntegTestCase {
    public void testDifferentPorts() throws Exception {
        logger.info("--> starting a node on ipv4 only");
        Settings ipv4Settings = Settings.builder().put("network.host", "127.0.0.1").build();
        String ipv4Node = internalCluster().startNode(ipv4Settings);

        logger.info("--> starting a node on ipv4 and ipv6");
        Settings bothSettings = Settings.builder().put("network.host", "_local_").build();
        String bothNode = internalCluster().startNode(bothSettings);

        logger.info("--> waiting for the clsuter to declare itself stable");
        ensureStableCluster(2); // This will timeout if the publish_address is funky

        logger.info("--> checking that we reproduced the funky port bindings");
        // These will fail if we didn't reproduce the problem properly
        BoundPorts ipv4Ports = new BoundPorts(ipv4Node);
        assertThat("ipv4 node should bind ipv4", ipv4Ports.ipv4, not(equalTo(0)));
        assertThat("ipv4 node shouldn't bind ipv6", ipv4Ports.ipv6, equalTo(0));

        BoundPorts bothPorts = new BoundPorts(bothNode);
        assertThat("both node should bind ipv4", bothPorts.ipv4, not(equalTo(0)));
        assertThat("both node should bind ipv6", bothPorts.ipv6, not(equalTo(0)));
        assertThat("both node shouldn't bind ipv4 and ipv6 to the same port", bothPorts.ipv6, not(equalTo(bothPorts.ipv4)));
    }

    public class BoundPorts {
        private int ipv4;
        private int ipv6;

        public BoundPorts(String nodeName) {
            TransportInfo info = client().admin().cluster().prepareNodesInfo(nodeName).get().getAt(0).getTransport();
            for (TransportAddress address : info.getAddress().boundAddresses()) {
                switch (address.getAddress()) {
                case "127.0.0.1":
                    ipv4 = address.getPort();
                    break;
                case "[::1]":
                    ipv6 = address.getPort();
                    break;
                }
            }
        }
    }
}
