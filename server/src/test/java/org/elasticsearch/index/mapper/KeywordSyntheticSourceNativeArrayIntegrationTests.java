/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class KeywordSyntheticSourceNativeArrayIntegrationTests extends ESSingleNodeTestCase {

    public void testSynthesizeArray() throws Exception {
        var arrayValues = new Object[][] {
            new Object[] { "z", "y", null, "x", null, "v" },
            new Object[] { null, "b", null, "a" },
            new Object[] { null },
            new Object[] { null, null, null },
            new Object[] { "c", "b", "a" } };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeEmptyArray() throws Exception {
        var arrayValues = new Object[][] { new Object[] {} };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayRandom() throws Exception {
        var arrayValues = new Object[][] { generateRandomStringArray(64, 8, false, true) };
        verifySyntheticArray(arrayValues);
    }

    public void testSynthesizeArrayIgnoreAbove() throws Exception {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .field("ignore_above", 4)
            .endObject()
            .endObject()
            .endObject();
        // Note values that would be ignored are added at the end of arrays,
        // this makes testing easier as ignored values are always synthesized after regular values:
        var arrayValues = new Object[][] {
            new Object[] { null, "a", "ab", "abc", "abcd", null, "abcde" },
            new Object[] { "12345", "12345", "12345" },
            new Object[] { "123", "1234", "12345" },
            new Object[] { null, null, null, "blabla" },
            new Object[] { "1", "2", "3", "blabla" } };
        verifySyntheticArray(arrayValues, mapping, 4, "_id", "field._original");
    }

    public void testSynthesizeObjectArray() throws Exception {
        List<List<Object[]>> documents = new ArrayList<>();
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "z", "y", "x" });
            document.add(new Object[] { "m", "l", "m" });
            document.add(new Object[] { "c", "b", "a" });
            documents.add(document);
        }
        {
            List<Object[]> document = new ArrayList<>();
            document.add(new Object[] { "9", "7", "5" });
            document.add(new Object[] { "2", "4", "6" });
            document.add(new Object[] { "7", "6", "5" });
            documents.add(document);
        }
        verifySyntheticObjectArray(documents);
    }

    public void testSynthesizeArrayInObjectField() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        documents.add(new Object[] { "z", "y", "x" });
        documents.add(new Object[] { "m", "l", "m" });
        documents.add(new Object[] { "c", "b", "a" });
        documents.add(new Object[] { "9", "7", "5" });
        documents.add(new Object[] { "2", "4", "6" });
        documents.add(new Object[] { "7", "6", "5" });
        verifySyntheticArrayInObject(documents);
    }

    public void testSynthesizeArrayInObjectFieldRandom() throws Exception {
        List<Object[]> documents = new ArrayList<>();
        int numDocs = randomIntBetween(8, 256);
        for (int i = 0; i < numDocs; i++) {
            documents.add(generateRandomStringArray(64, 8, false, true));
        }
        verifySyntheticArrayInObject(documents);
    }

    private void verifySyntheticArray(Object[][] arrays) throws IOException {
        var mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        verifySyntheticArray(arrays, mapping, null, "_id");
    }

    private void verifySyntheticArray(Object[][] arrays, XContentBuilder mapping, Integer ignoreAbove, String... expectedStoredFields)
        throws IOException {
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            mapping
        );
        for (int i = 0; i < arrays.length; i++) {
            var array = arrays[i];

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            if (array != null) {
                source.startArray("field");
                for (Object arrayValue : array) {
                    source.value(arrayValue);
                }
                source.endArray();
            } else {
                source.field("field").nullValue();
            }
            indexRequest.source(source.endObject());
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                var sourceAsMap = hit.getSourceAsMap();
                assertThat(sourceAsMap, hasKey("field"));
                var actualArray = (List<?>) sourceAsMap.get("field");
                if (array == null) {
                    assertThat(actualArray, nullValue());
                } else if (array.length == 0) {
                    assertThat(actualArray, empty());
                } else {
                    assertThat(actualArray, Matchers.contains(array));
                }
            } finally {
                searchResponse.decRef();
            }
        }

        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < arrays.length; i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is no ignored source:
                Set<String> storedFieldNames = new LinkedHashSet<>(document.getFields().stream().map(IndexableField::name).toList());
                if (IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE.isEnabled()) {
                    assertThat(storedFieldNames, contains(expectedStoredFields));
                } else {
                    String[] copyExpectedStoredFields = new String[expectedStoredFields.length + 1];
                    System.arraycopy(expectedStoredFields, 0, copyExpectedStoredFields, 0, expectedStoredFields.length);
                    copyExpectedStoredFields[copyExpectedStoredFields.length - 1] = "_recovery_source";
                    assertThat(storedFieldNames, containsInAnyOrder(copyExpectedStoredFields));
                }
            }
            var fieldInfo = FieldInfos.getMergedFieldInfos(reader).fieldInfo("field.offsets");
            assertThat(fieldInfo.getDocValuesType(), equalTo(DocValuesType.SORTED));
        }
    }

    private void verifySyntheticObjectArray(List<List<Object[]>> documents) throws IOException {
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        for (int i = 0; i < documents.size(); i++) {
            var document = documents.get(i);

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            source.startArray("object");
            for (Object[] arrayValue : document) {
                source.startObject();
                source.array("field", arrayValue);
                source.endObject();
            }
            source.endArray();
            indexRequest.source(source.endObject());
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                var sourceAsMap = hit.getSourceAsMap();
                var objectArray = (List<?>) sourceAsMap.get("object");
                for (int j = 0; j < document.size(); j++) {
                    var expected = document.get(j);
                    List<?> actual = (List<?>) ((Map<?, ?>) objectArray.get(j)).get("field");
                    assertThat(actual, Matchers.contains(expected));
                }
            } finally {
                searchResponse.decRef();
            }
        }

        indexService.getShard(0).forceMerge(new ForceMergeRequest("test-index").maxNumSegments(1));
        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < documents.size(); i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is ignored source because of leaf array being wrapped by object array:
                List<String> storedFieldNames = document.getFields().stream().map(IndexableField::name).toList();
                if (IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE.isEnabled()) {
                    assertThat(storedFieldNames, contains("_id", "_ignored_source"));
                } else {
                    assertThat(storedFieldNames, containsInAnyOrder("_id", "_ignored_source", "_recovery_source"));
                }

                // Verify that there is no offset field:
                LeafReader leafReader = reader.leaves().get(0).reader();
                for (FieldInfo fieldInfo : leafReader.getFieldInfos()) {
                    String name = fieldInfo.getName();
                    assertFalse("expected no field that contains [offsets] in name, but found [" + name + "]", name.contains("offsets"));
                }

                var binaryDocValues = leafReader.getBinaryDocValues("object.field.offsets");
                assertThat(binaryDocValues, nullValue());
            }
        }
    }

    private void verifySyntheticArrayInObject(List<Object[]> documents) throws IOException {
        var indexService = createIndex(
            "test-index",
            Settings.builder().put("index.mapping.source.mode", "synthetic").put("index.mapping.synthetic_source_keep", "arrays").build(),
            jsonBuilder().startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        for (int i = 0; i < documents.size(); i++) {
            var arrayValue = documents.get(i);

            var indexRequest = new IndexRequest("test-index");
            indexRequest.id("my-id-" + i);
            var source = jsonBuilder().startObject();
            source.startObject("object");
            source.array("field", arrayValue);
            source.endObject();
            indexRequest.source(source.endObject());
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client().index(indexRequest).actionGet();

            var searchRequest = new SearchRequest("test-index");
            searchRequest.source().query(new IdsQueryBuilder().addIds("my-id-" + i));
            var searchResponse = client().search(searchRequest).actionGet();
            try {
                var hit = searchResponse.getHits().getHits()[0];
                assertThat(hit.getId(), equalTo("my-id-" + i));
                var sourceAsMap = hit.getSourceAsMap();
                var objectArray = (Map<?, ?>) sourceAsMap.get("object");

                List<?> actual = (List<?>) objectArray.get("field");
                if (arrayValue == null) {
                    assertThat(actual, nullValue());
                } else if (arrayValue.length == 0) {
                    assertThat(actual, empty());
                } else {
                    assertThat(actual, Matchers.contains(arrayValue));
                }
            } finally {
                searchResponse.decRef();
            }
        }

        indexService.getShard(0).forceMerge(new ForceMergeRequest("test-index").maxNumSegments(1));
        try (var searcher = indexService.getShard(0).acquireSearcher(getTestName())) {
            var reader = searcher.getDirectoryReader();
            for (int i = 0; i < documents.size(); i++) {
                var document = reader.storedFields().document(i);
                // Verify that there is no ignored source:
                Set<String> storedFieldNames = new LinkedHashSet<>(document.getFields().stream().map(IndexableField::name).toList());
                if (IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE.isEnabled()) {
                    assertThat(storedFieldNames, contains("_id"));
                } else {
                    assertThat(storedFieldNames, containsInAnyOrder("_id", "_recovery_source"));
                }
            }
            var fieldInfo = FieldInfos.getMergedFieldInfos(reader).fieldInfo("object.field.offsets");
            assertThat(fieldInfo.getDocValuesType(), equalTo(DocValuesType.SORTED));
        }
    }

}
