/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class DeepAggsIT extends ESRestTestCase {
    private static final int DEPTH = 10;
    private static final int VALUE_COUNT = 4; // NOCOMMIT 4?
    private static final long TOTAL_DOCS = (long) Math.pow(VALUE_COUNT, DEPTH);

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().module("aggregations").build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testTerms() throws Exception {
        Request request = new Request("POST", "/deep/_search");
        XContentBuilder body = JsonXContent.contentBuilder().prettyPrint().startObject();
        body.field("timeout", "500ms");
        body.field("size", 0);
        agg(body, "terms", 10);
        request.setJsonEntity(Strings.toString(body.endObject()));
        Map<?, ?> response = responseAsMap(client().performRequest(request));
        assertMap(response, matchesMap());
    }

    private void agg(XContentBuilder body, String type, int depth) throws IOException {
        if (depth == 0) {
            return;
        }
        body.startObject("aggs");
        body.startObject(field("agg", depth));
        {
            body.startObject(type);
            body.field("field", field("kwd", depth - 1));
            body.endObject();
        }
        agg(body, type, depth - 1);
        body.endObject().endObject();
    }

    @Before
    public void createDeep() throws IOException {
        if (indexExists("deep")) {
            return;
        }
        logger.info("creating deep");
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        for (int f = 0; f < DEPTH; f++) {
            mapping.startObject(field("kwd", f)).field("type", "keyword").endObject();
        }
        CreateIndexResponse createIndexResponse = createIndex(
            "deep",
            Settings.builder().put("index.number_of_replicas", 0).build(),
            Strings.toString(mapping.endObject().endObject())
        );
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        Bulk bulk = new Bulk();
        bulk.doc(new StringBuilder("{"), 0);
        bulk.flush();

        Map<?, ?> response = responseAsMap(client().performRequest(new Request("POST", "/_refresh")));
        assertMap(response, matchesMap().entry("_shards", Map.of("total", 1, "failed", 0, "successful", 1)));
    }

    private String field(String prefix, int field) {
        return String.format(Locale.ROOT, "%s%03d", prefix, field);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    class Bulk {
        private static final int BULK_SIZE = Math.toIntExact(ByteSizeValue.ofMb(2).getBytes());

        StringBuilder bulk = new StringBuilder();
        int current = 0;
        int total = 0;

        void doc(StringBuilder doc, int field) throws IOException {
            if (field != 0) {
                doc.append(',');
            }
            int len = doc.length();
            for (int value = 0; value < VALUE_COUNT; value++) {
                doc.append('"').append(field("kwd", field)).append("\":\"").append(value).append('"');
                if (field == DEPTH - 1) {
                    addToBulk(doc.append('}'));
                } else {
                    doc(doc, field + 1);
                }
                doc.setLength(len);
            }
        }

        void addToBulk(StringBuilder doc) throws IOException {
            current++;
            total++;
            bulk.append("{\"index\":{}}\n");
            bulk.append(doc).append('\n');
            if (bulk.length() > BULK_SIZE) {
                flush();
            }
        }

        void flush() throws IOException {
            logger.info(
                "Flushing to deep {} docs/{}. Total {}% {}/{}",
                current,
                ByteSizeValue.ofBytes(bulk.length()),
                String.format("%04.1f", 100.0 * total / TOTAL_DOCS),
                total,
                TOTAL_DOCS
            );
            Request request = new Request("POST", "/deep/_bulk");
            request.setJsonEntity(bulk.toString());
            Map<?, ?> response = responseAsMap(client().performRequest(request));
            assertMap(response, matchesMap().extraOk().entry("errors", false));
            bulk.setLength(0);
            current = 0;
        }
    }
}
