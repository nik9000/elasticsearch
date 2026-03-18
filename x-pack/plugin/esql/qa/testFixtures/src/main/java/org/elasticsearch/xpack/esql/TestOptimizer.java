/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;

/**
 * Helper for testing all things related to {@link LogicalPlanOptimizer#optimize}
 * and {@link LocalLogicalPlanOptimizer#localOptimize}.
 */
public class TestOptimizer {
    private final TestAnalyzer analyzer = new TestAnalyzer();
    private SearchStats searchStats = TEST_SEARCH_STATS;

    public TestOptimizer searchStats(SearchStats searchStats) {
        this.searchStats = searchStats;
        return this;
    }

    public LogicalPlanOptimizer buildLogicalOptimizer() {
        return new LogicalPlanOptimizer(unboundLogicalOptimizerContext());
    }

    public LocalLogicalPlanOptimizer buildLocalLogicalOptimizer() {
        var localContext = new LocalLogicalOptimizerContext(analyzer.configuration(), FoldContext.small(), searchStats);
        return new LocalLogicalPlanOptimizer(localContext);
    }

    /**
     * Build the optimizer, parse the query, analyze it, and optimize it.
     */
    public LogicalPlan coordinator(String query) {
        return buildLogicalOptimizer().optimize(analyzer.query(query));
    }

    /**
     * Build the optimizer, parse the query, analyze it, and optimize it.
     */
    public LogicalPlan local(String query) {
        return buildLocalLogicalOptimizer().localOptimize(coordinator(query));
    }

    public record LogicalAndPhysical(LogicalPlan logical, PhysicalPlan physical) {}

    /**
     * Build a {@link LogicalPlan} and a {@link PhysicalPlan}.
     */
    public LogicalAndPhysical physical(String query) {
        Analyzer analyzer = this.analyzer.buildAnalyzer();
        LogicalPlan logical = buildLogicalOptimizer().optimize(analyzer.analyze(TEST_PARSER.parseQuery(query)));
        logical = buildLocalLogicalOptimizer().localOptimize(logical);
        var mapper = new Mapper();
        PhysicalPlan physical = mapper.map(new Versioned<>(logical, analyzer.context().minimumVersion()));
        return new LogicalAndPhysical(logical, physical);
    }

    // ==================
    // Below this is delegates to TestAnalyzer

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addIndex(String name, String mappingFile) {
        analyzer.addIndex(name, mappingFile);
        return this;
    }

    /**
     * Adds an index resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addIndex(String name, String mappingFile, IndexMode indexMode) {
        analyzer.addIndex(name, mappingFile, indexMode);
        return this;
    }

    /**
     * Add an index to the query.
     */
    public TestOptimizer addIndex(EsIndex index) {
        analyzer.addIndex(index);
        return this;
    }

    /**
     * Adds the standard set of lookup resolutions used by many analyzer tests.
     */
    public TestOptimizer addAnalysisTestsLookupResolutions() {
        analyzer.addAnalysisTestsLookupResolutions();
        return this;
    }

    /**
     * Adds an enrich policy resolution by loading the mapping from a resource file.
     */
    public TestOptimizer addEnrichPolicy(String policyType, String policy, String field, String index, String mapping) {
        analyzer.addEnrichPolicy(policyType, policy, field, index, mapping);
        return this;
    }
}
