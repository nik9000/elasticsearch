/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static org.elasticsearch.test.ESTestCase.expectThrows;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Builder for constructing {@link MutableAnalyzerContext} instances in tests.
 * Provides "empty" defaults.
 */
public class TestAnalyzerBuilder {
    TestAnalyzerBuilder() {}

    private Configuration configuration = EsqlTestUtils.TEST_CFG;
    private EsqlFunctionRegistry functionRegistry = EsqlTestUtils.TEST_FUNCTION_REGISTRY;
    private final Map<IndexPattern, IndexResolution> indexResolutions = new HashMap<>();
    private final Map<String, IndexResolution> lookupResolution = new HashMap<>();
    private EnrichResolution enrichResolution = new EnrichResolution();
    private InferenceResolution inferenceResolution = InferenceResolution.EMPTY;
    private UnmappedResolution unmappedResolution = UNMAPPED_FIELDS.defaultValue();
    private TimestampBounds timestampBounds;

    public TestAnalyzerBuilder configuration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public TestAnalyzerBuilder functionRegistry(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
        return this;
    }

    public TestAnalyzerBuilder addIndex(String pattern, IndexResolution resolution) {
        this.indexResolutions.put(new IndexPattern(Source.EMPTY, pattern), resolution);
        return this;
    }

    public TestAnalyzerBuilder addIndex(EsIndex index) {
        return addIndex(IndexResolution.valid(index));
    }

    public TestAnalyzerBuilder addIndex(IndexResolution resolution) {
        return addIndex(resolution.get().name(), resolution);
    }

    /**
     * Adds the standard set of subquery index resolutions used by many analyzer tests.
     */
    public TestAnalyzerBuilder addAnalysisTestsIndexResolutions() {
        String noFieldsIndexName = "no_fields_index";
        EsIndex noFieldsIndex = new EsIndex(
            noFieldsIndexName,
            Map.of(),
            Map.of(noFieldsIndexName, IndexMode.STANDARD),
            Map.of("", List.of(noFieldsIndexName)),
            Map.of("", List.of(noFieldsIndexName)),
            Set.of()
        );
        addIndex("languages", "mapping-languages.json");
        addIndex("sample_data", "mapping-sample_data.json");
        addIndex("test_mixed_types", "mapping-default-incompatible.json");
        addIndex("colors", "mapping-colors.json");
        addIndex("k8s", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES);
        addIndex("remote:missingIndex", IndexResolution.EMPTY_SUBQUERY);
        addIndex("empty_index", IndexResolution.empty("empty_index"));
        addIndex(noFieldsIndexName, IndexResolution.valid(noFieldsIndex));
        return this;
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestAnalyzerBuilder addIndex(String name, String mappingFile) {
        return addIndex(name, mappingFile, IndexMode.STANDARD);
    }

    public TestAnalyzerBuilder addIndex(String name, String mappingFile, IndexMode indexMode) {
        return addIndex(name, loadMapping(mappingFile, name, indexMode));
    }

    /**
     * Adds our traditional "employees" index.
     */
    public TestAnalyzerBuilder addEmployees() {
        return addEmployees("employees");
    }

    /**
     * Add our traditional "employees" index with a custom name.
     */
    public TestAnalyzerBuilder addEmployees(String name) {
        return addIndex(name, "mapping-basic.json");
    }

    private static IndexResolution loadMapping(String resource, String indexName, IndexMode indexMode) {
        return IndexResolution.valid(
            new EsIndex(indexName, EsqlTestUtils.loadMapping(resource), Map.of(indexName, indexMode), Map.of(), Map.of(), Set.of())
        );
    }

    public TestAnalyzerBuilder lookupResolution(String name, IndexResolution resolution) {
        this.lookupResolution.put(name, resolution);
        return this;
    }

    /**
     * Adds the standard set of lookup resolutions used by many analyzer tests.
     */
    public TestAnalyzerBuilder addAnalysisTestsLookupResolutions() {
        lookupResolution("languages_lookup", loadMapping("mapping-languages.json", "languages_lookup", IndexMode.LOOKUP));
        lookupResolution("test_lookup", loadMapping("mapping-basic.json", "test_lookup", IndexMode.LOOKUP));
        lookupResolution("spatial_lookup", loadMapping("mapping-multivalue_geometries.json", "spatial_lookup", IndexMode.LOOKUP));
        return this;
    }

    public TestAnalyzerBuilder enrichResolution(EnrichResolution enrichResolution) {
        this.enrichResolution = enrichResolution;
        return this;
    }

    public TestAnalyzerBuilder addEnrichError(String policyName, Enrich.Mode mode, String reason) {
        enrichResolution.addError(policyName, mode, reason);
        return this;
    }

    /**
     * Adds the standard set of enrich policy resolutions used by many analyzer tests.
     */
    public TestAnalyzerBuilder addAnalysisTestsEnrichResolution() {
        addEnrichPolicy(EnrichPolicy.MATCH_TYPE, "languages", "language_code", "languages_idx", "mapping-languages.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "client_cidr", "client_cidr", "client_cidr", "mapping-client_cidr.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "ages_policy", "age_range", "ages", "mapping-ages.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "heights_policy", "height_range", "heights", "mapping-heights.json");
        addEnrichPolicy(EnrichPolicy.RANGE_TYPE, "decades_policy", "date_range", "decades", "mapping-decades.json");
        addEnrichPolicy(
            EnrichPolicy.GEO_MATCH_TYPE,
            "city_boundaries",
            "city_boundary",
            "airport_city_boundaries",
            "mapping-airport_city_boundaries.json"
        );
        addEnrichPolicy(
            Enrich.Mode.COORDINATOR,
            EnrichPolicy.MATCH_TYPE,
            "languages_coord",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        addEnrichPolicy(
            Enrich.Mode.REMOTE,
            EnrichPolicy.MATCH_TYPE,
            "languages_remote",
            "language_code",
            "languages_idx",
            "mapping-languages.json"
        );
        return this;
    }

    /**
     * Adds an enrich policy resolution by loading the mapping from a resource file.
     */
    public TestAnalyzerBuilder addEnrichPolicy(String policyType, String policy, String field, String index, String mapping) {
        return addEnrichPolicy(Enrich.Mode.ANY, policyType, policy, field, index, mapping);
    }

    /**
     * Adds an enrich policy resolution with a specific mode by loading the mapping from a resource file.
     */
    public TestAnalyzerBuilder addEnrichPolicy(
        Enrich.Mode mode,
        String policyType,
        String policy,
        String field,
        String index,
        String mapping
    ) {
        IndexResolution indexResolution = loadMapping(mapping, index, IndexMode.STANDARD);
        List<String> enrichFields = new ArrayList<>(indexResolution.get().mapping().keySet());
        enrichFields.remove(field);
        enrichResolution.addResolvedPolicy(
            policy,
            mode,
            new ResolvedEnrichPolicy(field, policyType, enrichFields, Map.of("", index), indexResolution.get().mapping())
        );
        return this;
    }

    public TestAnalyzerBuilder inferenceResolution(InferenceResolution inferenceResolution) {
        this.inferenceResolution = inferenceResolution;
        return this;
    }

    public TestAnalyzerBuilder unmappedResolution(UnmappedResolution unmappedResolution) {
        this.unmappedResolution = unmappedResolution;
        return this;
    }

    public TestAnalyzerBuilder timestampBounds(TimestampBounds timestampBounds) {
        this.timestampBounds = timestampBounds;
        return this;
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query) {
        return buildAnalyzer().analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query));
    }

    /**
     * Build the analyzer, parse the query, and analyze it.
     */
    public LogicalPlan query(String query, Object... params) {
        return buildAnalyzer().analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query, EsqlTestUtils.toQueryParams(params)));
    }

    /**
     * Build the analyzer, parse the query, analyze it, and assert that it throws.
     * Returns the error message with the "Found N problem(s)" prefix stripped.
     */
    public String error(String query, Object... params) {
        return error(query, VerificationException.class, params);
    }

    /**
     * Build the analyzer, parse the query, analyze it, and assert that it throws the given exception.
     * Returns the error message with the "Found N problem(s)" prefix stripped.
     */
    public String error(String query, Class<? extends Exception> exception, Object... params) {
        return error(buildAnalyzer(), query, exception, params);
    }

    /**
     * Like {@link #error(String, Object...)} but with a minimum transport version override.
     */
    public String error(String query, TransportVersion transportVersion, Object... params) {
        return error(query, transportVersion, VerificationException.class, params);
    }

    /**
     * Like {@link #error(String, Class, Object...)} but with a minimum transport version override.
     */
    public String error(String query, TransportVersion transportVersion, Class<? extends Exception> exception, Object... params) {
        Analyzer analyzer = buildAnalyzer();
        MutableAnalyzerContext mutableContext = (MutableAnalyzerContext) analyzer.context();
        try (var restore = mutableContext.setTemporaryTransportVersionOnOrAfter(transportVersion)) {
            return error(analyzer, query, exception, params);
        }
    }

    private static String error(Analyzer analyzer, String query, Class<? extends Exception> exception, Object... params) {
        Throwable e = expectThrows(
            exception,
            "Expected error for query [" + query + "] but no error was raised",
            () -> analyzer.analyze(EsqlTestUtils.TEST_PARSER.parseQuery(query, EsqlTestUtils.toQueryParams(params)))
        );
        assertThat(e, instanceOf(exception));

        String message = e.getMessage();
        if (e instanceof VerificationException) {
            assertTrue(message.startsWith("Found "));
        }
        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    public Analyzer buildAnalyzer() {
        return buildAnalyzer(EsqlTestUtils.TEST_VERIFIER);
    }

    public Analyzer buildAnalyzer(Verifier verifier) {
        return new Analyzer(buildContext(), verifier);
    }

    public MutableAnalyzerContext buildContext() {
        return new MutableAnalyzerContext(
            configuration,
            functionRegistry,
            indexResolutions,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            EsqlTestUtils.randomMinimumVersion(),
            unmappedResolution,
            timestampBounds
        );
    }
}
