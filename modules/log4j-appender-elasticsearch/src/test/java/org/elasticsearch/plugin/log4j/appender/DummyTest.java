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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.log4j.appender.ElasticsearchAppender;
import org.elasticsearch.test.ESTestCase;

public class DummyTest extends ESTestCase {
    public void testDummy() {
        Appender appender = new ElasticsearchAppender.Builder().withName("remote_es")
                .withHost("localhost").withIndex("test").withType("log").build();
        appender.start();
        try {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            context.getRootLogger().addAppender(appender);
            try {    
                ESLoggerFactory.getLogger("test").info("ASDFADF");
            } finally {
                context.getRootLogger().removeAppender(appender);
            }
        } finally {
            appender.stop();
        }
        assert true;
    }
}
