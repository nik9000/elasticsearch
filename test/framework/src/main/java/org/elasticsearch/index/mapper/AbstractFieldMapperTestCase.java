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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fieldvisitor.SourceLoader;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;

/**
 * Infrastructure for testing mappers.
 */
public abstract class AbstractFieldMapperTestCase extends ESSingleNodeTestCase {
    protected IndexService indexService;
    protected DocumentMapperParser parser;

    @Before
    public final void setupDummyIndex() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    /**
     * Round trips some json as though it were loaded from the translog
     * so we can verify that it is normalized as though it were loaded
     * from doc values.
     */
    protected final void asThoughRelocatedTestCase(DocumentMapper docMapper, String input) throws IOException {
        asThoughRelocatedTestCase(docMapper, input, input);
    }

    /**
     * Round trips some json as though it were loaded from the translog
     * so we can verify that it is normalized as though it were loaded
     * from doc values.
     */
    protected final void asThoughRelocatedTestCase(DocumentMapper docMapper, String expected, String input) throws IOException {
        SourceLoader loader = SourceLoader.forReadingFromTranslog(docMapper.sourceRelocationHandlers());
        loader.setLoadedSource(new BytesArray(input));
        loader.load(null, 1);
        assertEquals(expected, loader.source().utf8ToString());
    }
}
