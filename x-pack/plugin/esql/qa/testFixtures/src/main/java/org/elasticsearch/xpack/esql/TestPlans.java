/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.function.Function;

import static org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize.estimateRowSize;

/**
 * Helper for building optimized coordinating node and local plans.
 */
public class TestPlans {
    private final Analyzer analyzer;
    private final LogicalPlan coordinatorLogicalUnoptimized;
    private SearchStats searchStats = EsqlTestUtils.TEST_SEARCH_STATS;
    private boolean stringLikeOnIndex = EsqlFlags.ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY);
    private int roundToPushdownThreshold = EsqlFlags.ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY);
    @Nullable
    private QueryBuilder esFilter;
    private Function<LogicalOptimizerContext, LogicalPlanOptimizer> localPlanOptimizerBuilder = LogicalPlanOptimizer::new;

    private LogicalPlan coordinatorLogicalOptimized;
    private PhysicalPlan coordinatorPhysicalPlanUnoptimized;
    private PhysicalPlan coordinatorPhysicalPlanOptimized;
    private PhysicalPlan dataNodePlanOptimized;

    TestPlans(Analyzer analyzer, LogicalPlan coordinatorLogicalUnoptimized) {
        this.analyzer = analyzer;
        this.coordinatorLogicalUnoptimized = coordinatorLogicalUnoptimized;
    }

    public TestPlans searchStats(SearchStats searchStats) {
        this.searchStats = searchStats;
        coordinatorLogicalOptimized = null;
        coordinatorPhysicalPlanUnoptimized = null;
        coordinatorPhysicalPlanOptimized = null;
        dataNodePlanOptimized = null;
        return this;
    }

    public TestPlans roundToPushdownThreshold(int roundToPushdownThreshold) {
        this.roundToPushdownThreshold = roundToPushdownThreshold;
        dataNodePlanOptimized = null;
        return this;
    }

    /**
     * Add a filter.
     */
    public TestPlans esFilter(QueryBuilder esFilter) {
        this.esFilter = esFilter;
        coordinatorPhysicalPlanOptimized = null;
        dataNodePlanOptimized = null;
        return this;
    }

    /**
     * Replace the default {@link LogicalPlanOptimizer}.
     */
    public TestPlans localPlanOptimizerBuilder(Function<LogicalOptimizerContext, LogicalPlanOptimizer> localPlanOptimizerBuilder) {
        this.localPlanOptimizerBuilder = localPlanOptimizerBuilder;
        return this;
    }

    public LogicalPlan coordinatorLogicalOptimized() {
        if (coordinatorLogicalOptimized == null) {
            LogicalPlanOptimizer optimizer = localPlanOptimizerBuilder.apply(
                new LogicalOptimizerContext(analyzer.context().configuration(), FoldContext.small(), analyzer.context().minimumVersion())
            );
            coordinatorLogicalOptimized = optimizer.optimize(coordinatorLogicalUnoptimized);
        }
        return coordinatorLogicalOptimized;
    }

    public PhysicalPlan coordinatorPhysicalPlanUnoptimized() {
        if (coordinatorPhysicalPlanUnoptimized == null) {
            coordinatorPhysicalPlanUnoptimized = new Mapper().map(
                new Versioned<>(coordinatorLogicalOptimized(), analyzer.context().minimumVersion())
            );
        }
        return coordinatorPhysicalPlanUnoptimized;
    }

    public PhysicalPlan coordinatorPhysicalPlanOptimized() {
        if (coordinatorPhysicalPlanOptimized == null) {
            PhysicalPlan plan = PlannerUtils.integrateEsFilterIntoFragment(coordinatorPhysicalPlanUnoptimized(), esFilter);
            PhysicalPlanOptimizer optimizer = new PhysicalPlanOptimizer(
                new PhysicalOptimizerContext(analyzer.context().configuration(), analyzer.context().minimumVersion())
            );
            coordinatorPhysicalPlanOptimized = estimateRowSize(0, optimizer.optimize(plan));
        }
        return coordinatorPhysicalPlanOptimized;
    }

    public PhysicalPlan dataNodePlanOptimized() {
        if (dataNodePlanOptimized == null) {
            var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
                new LocalLogicalOptimizerContext(analyzer.context().configuration(), FoldContext.small(), searchStats)
            );
            var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
                new LocalPhysicalOptimizerContext(
                    PlannerSettings.DEFAULTS,
                    new EsqlFlags(stringLikeOnIndex, roundToPushdownThreshold),
                    analyzer.context().configuration(),
                    FoldContext.small(),
                    searchStats
                ),
                true
            );
            PhysicalPlan plan = PlannerUtils.localPlan(
                coordinatorPhysicalPlanOptimized(),
                logicalTestOptimizer,
                physicalTestOptimizer,
                null
            );
            dataNodePlanOptimized = localRelationshipAlignment(plan);
        }
        return dataNodePlanOptimized;
    }

    static PhysicalPlan localRelationshipAlignment(PhysicalPlan l) {
        // handle local reduction alignment
        return l.transformUp(ExchangeExec.class, exg -> {
            PhysicalPlan pl = exg;
            if (exg.inBetweenAggs() && exg.child() instanceof LocalSourceExec lse) {
                var output = exg.output();
                if (lse.output().equals(output) == false) {
                    pl = exg.replaceChild(new LocalSourceExec(lse.source(), output, lse.supplier()));
                }
            }
            return pl;
        });
    }
}
