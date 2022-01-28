/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.DataStreamsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class IndicesPermissionsWithAliasesWildcardsAndRegexsTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + "user1:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "role1:user1\n";
    }

    @Override
    protected String configRoles() {
        return """
            %s
            role1:
              cluster: [ all ]
              indices:
                  - names: 't*'
                    privileges: [ALL]
                    field_security:
                       grant: [ field1 ]
                  - names: 'my_alias'
                    privileges: [ALL]
                    field_security:
                       grant: [ field2 ]
                  - names: '/an_.*/'
                    privileges: [ALL]
                    field_security:
                       grant: [ field3 ]
            """.formatted(super.configRoles());
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> lst = new ArrayList<>(super.nodePlugins());
        lst.add(DataStreamsPlugin.class);
        return lst;
    }

    public void testGetResolveWildcardsRegexs() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text")
                .addAlias(new Alias("my_alias"))
                .addAlias(new Alias("an_alias"))
        );
        client().prepareIndex("test")
            .setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        GetResponse getResponse = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareGet("test", "1").get();
        assertThat(getResponse.getSource().size(), equalTo(1));
        assertThat((String) getResponse.getSource().get("field1"), equalTo("value1"));

        getResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareGet("my_alias", "1")
            .get();
        assertThat(getResponse.getSource().size(), equalTo(1));
        assertThat((String) getResponse.getSource().get("field2"), equalTo("value2"));

        getResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareGet("an_alias", "1")
            .get();
        assertThat(getResponse.getSource().size(), equalTo(1));
        assertThat((String) getResponse.getSource().get("field3"), equalTo("value3"));
    }

    public void testSearchResolveWildcardsRegexs() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text")
                .addAlias(new Alias("my_alias"))
                .addAlias(new Alias("an_alias"))
        );
        client().prepareIndex("test")
            .setId("1")
            .setSource("field1", "value1", "field2", "value2", "field3", "value3")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareSearch("test").setQuery(QueryBuilders.termQuery("_id", "1")).get();
        assertThat(response.getHits().getHits().length, equalTo(1));
        Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
        assertThat(source.size(), equalTo(1));
        assertThat((String) source.get("field1"), equalTo("value1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareSearch("my_alias")
            .setQuery(QueryBuilders.termQuery("_id", "1"))
            .get();
        assertThat(response.getHits().getHits().length, equalTo(1));
        source = response.getHits().getHits()[0].getSourceAsMap();
        assertThat(source.size(), equalTo(1));
        assertThat((String) source.get("field2"), equalTo("value2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareSearch("an_alias")
            .setQuery(QueryBuilders.termQuery("_id", "1"))
            .get();
        assertThat(response.getHits().getHits().length, equalTo(1));
        source = response.getHits().getHits()[0].getSourceAsMap();
        assertThat(source.size(), equalTo(1));
        assertThat((String) source.get("field3"), equalTo("value3"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareSearch("*_alias")
            .setQuery(QueryBuilders.termQuery("_id", "1"))
            .get();
        assertThat(response.getHits().getHits().length, equalTo(1));
        source = response.getHits().getHits()[0].getSourceAsMap();
        assertThat(source.size(), equalTo(2));
        assertThat((String) source.get("field2"), equalTo("value2"));
        assertThat((String) source.get("field3"), equalTo("value3"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
            .prepareSearch("*_alias", "t*")
            .setQuery(QueryBuilders.termQuery("_id", "1"))
            .get();
        assertThat(response.getHits().getHits().length, equalTo(1));
        source = response.getHits().getHits()[0].getSourceAsMap();
        assertThat(source.size(), equalTo(3));
        assertThat((String) source.get("field1"), equalTo("value1"));
        assertThat((String) source.get("field2"), equalTo("value2"));
        assertThat((String) source.get("field3"), equalTo("value3"));
    }

    public void testSearchResolveDataStreams() throws Exception {
        putComposableIndexTemplate("id1", List.of("test*"));
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request("test");
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        aliasesRequest.addAliasAction(
            new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).aliases("my_alias", "an_alias")
                .index("test")
        );
        assertAcked(client().admin().indices().aliases(aliasesRequest).actionGet());

        try {
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            client().prepareIndex("test")
                .setCreate(true)
                .setId("1")
                .setSource(DEFAULT_TIMESTAMP_FIELD, value, "field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

            SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareSearch("test").setQuery(QueryBuilders.termQuery("_id", "1")).get();
            assertThat(response.getHits().getHits().length, equalTo(1));
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            assertThat(source.size(), equalTo(1));
            assertThat((String) source.get("field1"), equalTo("value1"));

            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("my_alias")
                .setQuery(QueryBuilders.termQuery("_id", "1"))
                .get();
            assertThat(response.getHits().getHits().length, equalTo(1));
            source = response.getHits().getHits()[0].getSourceAsMap();
            assertThat(source.size(), equalTo(1));
            assertThat((String) source.get("field2"), equalTo("value2"));

            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("an_alias")
                .setQuery(QueryBuilders.termQuery("_id", "1"))
                .get();
            assertThat(response.getHits().getHits().length, equalTo(1));
            source = response.getHits().getHits()[0].getSourceAsMap();
            assertThat(source.size(), equalTo(1));
            assertThat((String) source.get("field3"), equalTo("value3"));

            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("*_alias")
                .setQuery(QueryBuilders.termQuery("_id", "1"))
                .get();
            assertThat(response.getHits().getHits().length, equalTo(1));
            source = response.getHits().getHits()[0].getSourceAsMap();
            assertThat(source.size(), equalTo(2));
            assertThat((String) source.get("field2"), equalTo("value2"));
            assertThat((String) source.get("field3"), equalTo("value3"));

            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("*_alias", "t*")
                .setQuery(QueryBuilders.termQuery("_id", "1"))
                .get();
            assertThat(response.getHits().getHits().length, equalTo(1));
            source = response.getHits().getHits()[0].getSourceAsMap();
            assertThat(source.size(), equalTo(3));
            assertThat((String) source.get("field1"), equalTo("value1"));
            assertThat((String) source.get("field2"), equalTo("value2"));
            assertThat((String) source.get("field3"), equalTo("value3"));
        } finally {
            AcknowledgedResponse response = client().execute(
                DeleteDataStreamAction.INSTANCE,
                new DeleteDataStreamAction.Request(new String[] { "*" })
            ).actionGet();
            assertAcked(response);

            DeleteDataStreamAction.Request deleteDSRequest = new DeleteDataStreamAction.Request(new String[] { "*" });
            client().execute(DeleteDataStreamAction.INSTANCE, deleteDSRequest).actionGet();
            DeleteComposableIndexTemplateAction.Request deleteTemplateRequest = new DeleteComposableIndexTemplateAction.Request("*");
            client().execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteTemplateRequest).actionGet();
        }
    }

    private void putComposableIndexTemplate(String id, List<String> patterns) throws IOException {
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(id);
        request.indexTemplate(
            new ComposableIndexTemplate(
                patterns,
                new Template(null, null, null),
                null,
                null,
                null,
                null,
                new ComposableIndexTemplate.DataStreamTemplate(),
                null
            )
        );
        client().execute(PutComposableIndexTemplateAction.INSTANCE, request).actionGet();
    }
}
