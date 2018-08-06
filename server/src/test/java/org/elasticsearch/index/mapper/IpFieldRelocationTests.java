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

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpFieldRelocationTests extends AbstractFieldRelocationTestCase {
    @Override
    protected String fieldType() {
        return "ip";
    }

    @Override
    protected void writeRandomValue(XContentBuilder builder) throws IOException {
        builder.value(randomIp());
    }

    private static String randomIp() {
        if (rarely()) {
            // ::1 is so common I want it to come up more than random
            return "::1";
        }
        try {
            InetAddress address;
            if (randomBoolean()) {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                address = InetAddress.getByAddress(ipv4);
            } else {
                byte[] ipv6 = new byte[16];
                random().nextBytes(ipv6);
                address = InetAddress.getByAddress(ipv6);
            }
            // Sometimes sorten with .format, sometime don't with getHostAddress
            return randomBoolean() ? NetworkAddress.format(address) : address.getHostAddress();
        } catch (UnknownHostException e) {
            throw new AssertionError();
        }
    }

}
