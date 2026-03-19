/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTests.withInlinestatsWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class AnalyzerUnmappedTests extends ESTestCase {
    public void testFailKeepAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailKeepAndMatchingAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailAfterKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """, "Unknown column [does_not_exist_field]");
    }

    public void testFailDropWithNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailDropWithMatchingAndNonMatchingStar() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP emp_*, does_not_exist_field*
            """, "No matches found for pattern [does_not_exist_field*]");
    }

    public void testFailEvalAfterDrop() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """, "3:12: Unknown column [does_not_exist_field]");
    }

    public void testFailFilterAfterDrop() {
        assertUnmappedFailure(test(), """
            FROM test
            | WHERE emp_no > 1000
            | DROP emp_no
            | WHERE emp_no < 2000
            """, "line 4:9: Unknown column [emp_no]");
    }

    public void testFailDropThenKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailDropThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailEvalThenDropThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field::LONG + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field::LONG + 2
            """, "line 6:8: Unknown column [does_not_exist_field]");
    }

    public void testFailStatsThenKeep() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """, "line 3:8: Unknown column [does_not_exist_field]");
    }

    public void testFailStatsThenKeepShadowing() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS count(*)
            | EVAL foo = emp_no
            """, "line 3:14: Unknown column [emp_no]");
    }

    public void testFailStatsThenEval() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """, "line 3:12: Unknown column [does_not_exist_field]");
    }

    public void testFailAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(test(), """
            FROM
                (FROM test
                 | STATS c = COUNT(*))
            | SORT does_not_exist
            """, "line 4:8: Unknown column [does_not_exist]");
    }

    // unmapped_fields="load" disallows subqueries and LOOKUP JOIN (see #142033)
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        String msg = test().addLanguages()
            .addSampleData()
            .addLanguagesLookup()
            .statementError(setUnmappedLoad("""
                FROM test,
                    (FROM languages
                     | WHERE language_code > 10
                     | RENAME language_name as languageName),
                    (FROM sample_data
                    | STATS max(@timestamp)),
                    (FROM test
                    | EVAL language_code = languages
                    | LOOKUP JOIN languages_lookup ON language_code)
                | WHERE emp_no > 10000 OR does_not_exist1::LONG < 10
                | STATS COUNT(*) BY emp_no, language_code, does_not_exist2
                | RENAME emp_no AS empNo, language_code AS languageCode
                | MV_EXPAND languageCode
                """));
        assertThat(msg, containsString("Found 4 problems"));
        assertThat(msg, containsString("Subqueries and views are not supported with unmapped_fields=\"load\""));
        assertThat(msg, containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
        assertThat(msg, not(containsString("FORK is not supported")));
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyNullify() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(analyzer().addLanguages(), """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """, "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?");
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(analyzer().addLanguages(), """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """, "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?");
    }

    public void testFailAfterForkOfStats() {
        assertUnmappedFailure(test(), """
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary))
                   (DISSECT hire_date::KEYWORD "%{year}-%{month}-%{day}T"
                    | STATS x = MIN(year::LONG), y = MAX(month::LONG) WHERE year::LONG > 1000 + does_not_exist2::DOUBLE)
            | EVAL e = does_not_exist3 + 1
            """, "line 7:12: Unknown column [does_not_exist3]");
    }

    public void testFailMetadataFieldInKeep() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | KEEP " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInEval() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | EVAL x = " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInWhere() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | WHERE " + field + " IS NOT NULL", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInSort() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | SORT " + field, "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInStats() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | STATS x = COUNT(" + field + ")", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldInRename() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            assertUnmappedFailure(test(), "FROM test | RENAME " + field + " AS renamed", "Unknown column [" + field + "]");
        }
    }

    public void testFailMetadataFieldAfterStats() {
        assertUnmappedFailure(test(), """
            FROM test
            | STATS c = COUNT(*)
            | KEEP _score
            """, "Unknown column [_score]");
    }

    public void testFailMetadataFieldInFork() {
        assertUnmappedFailure(test(), """
            FROM test
            | FORK (WHERE _score > 1)
                   (WHERE salary > 50000)
            """, "Unknown column [_score]");
    }

    public void testFailMetadataFieldInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assertUnmappedFailure(test(), """
            FROM
                (FROM test
                 | WHERE _score > 1)
            """, "Unknown column [_score]");
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredNullify() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = test().statement(setUnmappedNullify("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // No Eval(NULL) — the field was resolved via METADATA, not nullified
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[_score{m}#5]]
     *   \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, ...]
     */
    public void testMetadataFieldDeclaredLoad() {
        // This isn't gilded since it would just create a bunch of clutter due to nesting.
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var plan = test().statement(setUnmappedLoad("FROM test METADATA " + field + " | KEEP " + field));

            var limit = as(plan, Limit.class);
            assertThat(limit.limit().fold(FoldContext.small()), is(1000));

            var project = as(limit.child(), Project.class);
            assertThat(project.projections(), hasSize(1));
            assertThat(Expressions.name(project.projections().getFirst()), is(field));
            assertThat(project.projections().getFirst(), instanceOf(MetadataAttribute.class));

            // The field was resolved via METADATA, not loaded as an unmapped field into EsRelation
            var relation = as(project.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("test"));
        }
    }

    public void testChangedTimestmapFieldWithRate() {
        assertThat(analyzer().addK8sDownsampled().statementError(setUnmappedNullify("""
            TS k8s
            | RENAME @timestamp AS newTs
            | STATS max(rate(network.total_cost)) BY tbucket = BUCKET(newTs, 1hour)
            """)), containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX));

        assertThat(analyzer().addK8sDownsampled().statementError(setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """)), containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX));
    }

    public void testLoadModeDisallowsFork() {
        var stmt = setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)");
        assertThat(test().statementError(stmt), containsString("FORK is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsForkWithStats() {
        var stmt = setUnmappedLoad("FROM test | FORK (STATS c = COUNT(*)) (STATS d = AVG(salary))");
        assertThat(test().statementError(stmt), containsString("FORK is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsForkWithMultipleBranches() {
        var stmt = setUnmappedLoad("""
            FROM test
            | FORK (WHERE emp_no > 1)
                   (WHERE emp_no < 100)
                   (WHERE salary > 50000)
            """);
        assertThat(test().statementError(stmt), containsString("FORK is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsLookupJoin() {
        var stmt = setUnmappedLoad("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code");
        assertThat(
            test().addLanguagesLookup().statementError(stmt),
            containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsLookupJoinAfterFilter() {
        var stmt = setUnmappedLoad("""
            FROM test
            | WHERE emp_no > 1
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | KEEP emp_no, language_name
            """);
        assertThat(
            test().addLanguagesLookup().statementError(stmt),
            containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("FROM test, (FROM languages | WHERE language_code > 1)");
        assertThat(
            test().addLanguages().statementError(stmt),
            containsString("Subqueries and views are not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsMultipleSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("""
            FROM test,
                (FROM languages | WHERE language_code > 1),
                (FROM sample_data | STATS max(@timestamp))
            """);
        assertThat(
            test().addLanguages().addSampleData().statementError(stmt),
            containsString("Subqueries and views are not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsNestedSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)");
        assertThat(
            test().addLanguages().addSampleData().statementError(stmt),
            containsString("Subqueries and views are not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsSubqueryWithLookupJoin() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("""
            FROM test,
                (FROM test
                | EVAL language_code = languages
                | LOOKUP JOIN languages_lookup ON language_code)
            """);
        assertThat(
            test().addLanguagesLookup().statementError(stmt),
            containsString("Subqueries and views are not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkAndLookupJoin() {
        var query = setUnmappedLoad("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        var analyzer = test().addLanguagesLookup();
        assertThat(analyzer.statementError(query), containsString("FORK is not supported with unmapped_fields=\"load\""));
        assertThat(analyzer.statementError(query), containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsSubqueryAndFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = setUnmappedLoad("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        var analyzer = test().addLanguages();
        assertThat(analyzer.statementError(query), containsString("Subqueries and views are not supported with unmapped_fields=\"load\""));
        assertThat(analyzer.statementError(query), containsString("FORK is not supported with unmapped_fields=\"load\""));
    }

    private static final String UNMAPPED_TIMESTAMP_SUFFIX = UnresolvedTimestamp.UNRESOLVED_SUFFIX + Verifier.UNMAPPED_TIMESTAMP_SUFFIX;

    public void testTbucketWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | WHERE trange(1 hour)", "[trange(1 hour)] ");
    }

    public void testTbucketAndTrangeWithUnmappedTimestamp() {
        unmappedTimestampFailure(
            "FROM test | WHERE trange(1 hour) | STATS c = COUNT(*) BY tbucket(1 hour)",
            "[tbucket(1 hour)] ",
            "[trange(1 hour)] "
        );
    }

    public void testRateWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS rate(salary)", "[rate(salary)] ");
    }

    public void testIrateWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS irate(salary)", "[irate(salary)] ");
    }

    public void testDeltaWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS delta(salary)", "[delta(salary)] ");
    }

    public void testIdeltaWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS idelta(salary)", "[idelta(salary)] ");
    }

    public void testIncreaseWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS increase(salary)", "[increase(salary)] ");
    }

    public void testDerivWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS deriv(salary)", "[deriv(salary)] ");
    }

    public void testFirstOverTimeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS first_over_time(salary)", "[first_over_time(salary)] ");
    }

    public void testLastOverTimeWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS last_over_time(salary)", "[last_over_time(salary)] ");
    }

    public void testRateAndTbucketWithUnmappedTimestamp() {
        unmappedTimestampFailure("FROM test | STATS rate(salary) BY tbucket(1 hour)", "[rate(salary)] ", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterWhere() {
        unmappedTimestampFailure("FROM test | WHERE emp_no > 10 | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterEval() {
        unmappedTimestampFailure("FROM test | EVAL x = salary + 1 | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampMultipleGroupings() {
        unmappedTimestampFailure("FROM test | STATS c = COUNT(*) BY tbucket(1 hour), emp_no", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterRename() {
        unmappedTimestampFailure("FROM test | RENAME emp_no AS e | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampAfterDrop() {
        unmappedTimestampFailure("FROM test | DROP emp_no | STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestampCompoundWhere() {
        unmappedTimestampFailure("FROM test | WHERE trange(1 hour) AND emp_no > 10", "[trange(1 hour)] ");
    }

    public void testTrangeWithUnmappedTimestampAfterEval() {
        unmappedTimestampFailure("FROM test | EVAL x = salary + 1 | WHERE trange(1 hour)", "[trange(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampInInlineStats() {
        unmappedTimestampFailure("FROM test | INLINE STATS c = COUNT(*) BY tbucket(1 hour)", "[tbucket(1 hour)] ");
    }

    public void testTbucketWithUnmappedTimestampWithFork() {
        var query = "FROM test | FORK (STATS c = COUNT(*) BY tbucket(1 hour)) (STATS d = COUNT(*) BY emp_no)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            String err = test().statementError(statement);
            assertThat(err, containsString("[tbucket(1 hour)] "));
            assertThat(err, not(containsString("FORK is not supported")));
        }
    }

    public void testTbucketWithUnmappedTimestampWithLookupJoin() {
        var query = """
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | STATS c = COUNT(*) BY tbucket(1 hour)
            """;
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            String err = test().addLanguagesLookup().statementError(statement);
            assertThat(err, containsString("[tbucket(1 hour)] "));
            assertThat(err, not(containsString("LOOKUP JOIN is not supported")));
        }
    }

    public void testTbucketWithTimestampPresent() {
        var query = "FROM sample_data | STATS c = COUNT(*) BY tbucket(1 hour)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var plan = analyzer().addSampleData().statement(statement);
            var limit = as(plan, Limit.class);
            var aggregate = as(limit.child(), Aggregate.class);
            var relation = as(aggregate.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("sample_data"));
            assertTimestampInOutput(relation);
        }
    }

    public void testTrangeWithTimestampPresent() {
        var query = "FROM sample_data | WHERE trange(1 hour)";
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var plan = analyzer().addSampleData().statement(statement);
            var limit = as(plan, Limit.class);
            var filter = as(limit.child(), Filter.class);
            var relation = as(filter.child(), EsRelation.class);
            assertThat(relation.indexPattern(), is("sample_data"));
            assertTimestampInOutput(relation);
        }
    }

    public void testTbucketTimestampPresentButDroppedNullify() {
        String err = analyzer().addSampleData()
            .statementError(setUnmappedNullify("FROM sample_data | DROP @timestamp | STATS c = COUNT(*) BY tbucket(1 hour)"));
        assertThat(err, containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX));
        assertThat(err, not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)));
    }

    public void testTbucketTimestampPresentButRenamedNullify() {
        String err = analyzer().addSampleData()
            .statementError(setUnmappedNullify("FROM sample_data | RENAME @timestamp AS ts | STATS c = COUNT(*) BY tbucket(1 hour)"));
        assertThat(err, containsString(UnresolvedTimestamp.UNRESOLVED_SUFFIX));
        assertThat(err, not(containsString(Verifier.UNMAPPED_TIMESTAMP_SUFFIX)));
    }

    private static void assertTimestampInOutput(EsRelation relation) {
        assertTrue(
            "@timestamp field should be present in the EsRelation output",
            relation.output().stream().anyMatch(a -> MetadataAttribute.TIMESTAMP_FIELD.equals(a.name()))
        );
    }

    private void unmappedTimestampFailure(String query, String... expectedFailures) {
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var err = test().statementError(statement);
            for (String expected : expectedFailures) {
                assertThat(err, containsString(expected + UNMAPPED_TIMESTAMP_SUFFIX));
            }
        }
    }

    private void assertUnmappedFailure(TestAnalyzer analyzer, String query, String... failure) {
        for (var statement : List.of(setUnmappedNullify(query), setUnmappedLoad(query))) {
            var err = analyzer.statementError(statement);
            for (String expected : failure) {
                assertThat(err, containsString(expected));
            }
        }
    }

    private static TestAnalyzer test() {
        return analyzer().addEmployees("test");
    }

    private static String setUnmappedNullify(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        return "SET unmapped_fields=\"nullify\"; " + query;
    }

    private static String setUnmappedLoad(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_V2", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V2.isEnabled());
        return "SET unmapped_fields=\"load\"; " + query;
    }

    /**
     * Reproducer for #141927: with unmapped_fields=load, full-text search (MATCH, match operator, MATCH_PHRASE, etc.)
     * must fail at analysis instead of returning empty results.
     * <p>
     * One assertion per forbidden full-text function so that re-enabling any of them (e.g. QSTR, KNN, MATCH_PHRASE)
     * would cause this test to fail. When full-text function support grows, this test will need updates; see #144121.
     */
    public void testUnmappedFieldsLoadWithFullTextSearchFails() {
        // Assert the new message format and that the specific full-text function is named in brackets
        // Function names in error messages use Function.functionName() (class simple name upper-cased) or override (e.g. QSTR, :)
        var analyzer = test();
        assertThat(
            analyzer.statementError(setUnmappedLoad("FROM test | WHERE first_name:\"foo\" | KEEP first_name")),
            containsString("does not support full-text search function [:]")
        );
        assertThat(
            analyzer.statementError(setUnmappedLoad("FROM test | WHERE match(first_name, \"foo\") | KEEP first_name")),
            containsString("does not support full-text search function [MATCH]")
        );
        assertThat(
            analyzer.statementError(setUnmappedLoad("FROM test | WHERE match_phrase(first_name, \"foo bar\") | KEEP first_name")),
            containsString("does not support full-text search function [MatchPhrase]")
        );
        if (EsqlCapabilities.Cap.MULTI_MATCH_FUNCTION.isEnabled()) {
            assertThat(
                analyzer.statementError(setUnmappedLoad("FROM test | WHERE multi_match(\"foo\", first_name) | KEEP first_name")),
                containsString("does not support full-text search function [MultiMatch]")
            );
        }
        if (EsqlCapabilities.Cap.QSTR_FUNCTION.isEnabled()) {
            assertThat(
                analyzer.statementError(setUnmappedLoad("FROM test | WHERE qstr(\"first_name: foo\") | KEEP first_name")),
                containsString("does not support full-text search function [QSTR]")
            );
        }
        if (EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled()) {
            assertThat(
                analyzer.statementError(setUnmappedLoad("FROM test | WHERE kql(\"first_name: foo\") | KEEP first_name")),
                containsString("does not support full-text search function [KQL]")
            );
        }
        assertThat(
            analyzer().addIndex("test", "mapping-full_text_search.json")
                .statementError(setUnmappedLoad("FROM test | WHERE knn(vector, [1, 2, 3]) | KEEP vector")),
            containsString("does not support full-text search function [KNN]")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
