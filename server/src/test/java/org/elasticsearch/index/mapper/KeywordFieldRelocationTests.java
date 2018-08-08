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
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.joda.time.DateTime;
import org.joda.time.Instant;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

public class KeywordFieldRelocationTests extends AbstractFieldRelocationTestCase {
    @Override
    protected String fieldType() {
        return "keyword";
    }

    @Override
    protected void extraMappingConfiguration(XContentBuilder builder) throws IOException {
        builder.field("ignore_above", 2000);
    }

    @Override
    protected void writeRandomValue(XContentBuilder builder) throws IOException {
        builder.value(randomRealisticUnicodeOfLengthBetween(0, 1000));
    }
}
