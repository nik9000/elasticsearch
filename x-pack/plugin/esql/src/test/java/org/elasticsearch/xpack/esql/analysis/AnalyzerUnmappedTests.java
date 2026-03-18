/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
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
        var query = """
            FROM test
            | KEEP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailKeepAndMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | KEEP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailAfterKeep() {
        var query = """
            FROM test
            | KEEP emp_*
            | EVAL x = does_not_exist_field + 1
            """;
        var failure = "Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailDropWithNonMatchingStar() {
        var query = """
            FROM test
            | DROP does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailDropWithMatchingAndNonMatchingStar() {
        var query = """
            FROM test
            | DROP emp_*, does_not_exist_field*
            """;
        var failure = "No matches found for pattern [does_not_exist_field*]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailEvalAfterDrop() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL x = does_not_exist_field + 1
            """;

        var failure = "3:12: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailFilterAfterDrop() {
        var query = """
            FROM test
            | WHERE emp_no > 1000
            | DROP emp_no
            | WHERE emp_no < 2000
            """;

        var failure = "line 4:9: Unknown column [emp_no]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailDropThenKeep() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailDropThenEval() {
        var query = """
            FROM test
            | DROP does_not_exist_field
            | EVAL does_not_exist_field + 2
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailEvalThenDropThenEval() {
        var query = """
            FROM test
            | KEEP does_not_exist_field
            | EVAL x = does_not_exist_field::LONG + 1
            | WHERE x IS NULL
            | DROP does_not_exist_field
            | EVAL does_not_exist_field::LONG + 2
            """;
        var failure = "line 6:8: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailStatsThenKeep() {
        var query = """
            FROM test
            | STATS cnd = COUNT(*)
            | KEEP does_not_exist_field
            """;
        var failure = "line 3:8: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailStatsThenKeepShadowing() {
        var query = """
            FROM test
            | STATS count(*)
            | EVAL foo = emp_no
            """;

        var failure = "line 3:14: Unknown column [emp_no]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailStatsThenEval() {
        var query = """
            FROM test
            | STATS cnt = COUNT(*)
            | EVAL x = does_not_exist_field + cnt
            """;
        var failure = "line 3:12: Unknown column [does_not_exist_field]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailAfterUnionAllOfStats() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM test
                 | STATS c = COUNT(*))
            | SORT does_not_exist
            """;
        var failure = "line 4:8: Unknown column [does_not_exist]";
        assertUnmappedFailure(test(), query, failure);
    }

    // unmapped_fields="load" disallows subqueries and LOOKUP JOIN (see #142033)
    public void testSubquerysMixAndLookupJoinLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("""
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
            """);
        String msg = test()
            .addAnalysisTestsIndexResolutions()
            .addAnalysisTestsLookupResolutions()
            .statementError(stmt);
        assertThat(msg, containsString("Found 4 problems"));
        assertThat(msg, containsString("Subqueries and views are not supported with unmapped_fields=\"load\""));
        assertThat(msg, containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
        assertThat(msg, not(containsString("FORK is not supported")));
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyNullify() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        assertUnmappedFailure(EsqlTestUtils.analyzer().addAnalysisTestsIndexResolutions(), query, failure);
    }

    public void testFailSubquerysWithNoMainAndStatsOnlyLoad() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM languages
                 | STATS c = COUNT(*) BY emp_no, does_not_exist1),
                (FROM languages
                 | STATS a = AVG(salary::LONG))
            | WHERE does_not_exist2::LONG < 10
            """;
        var failure = "line 6:9: Unknown column [does_not_exist2], did you mean [does_not_exist1]?";
        assertUnmappedFailure(EsqlTestUtils.analyzer().addAnalysisTestsIndexResolutions(), query, failure);
    }

    public void testFailAfterForkOfStats() {
        var query = """
            FROM test
            | WHERE does_not_exist1 IS NULL
            | FORK (STATS c = COUNT(*))
                   (STATS d = AVG(salary))
                   (DISSECT hire_date::KEYWORD "%{year}-%{month}-%{day}T"
                    | STATS x = MIN(year::LONG), y = MAX(month::LONG) WHERE year::LONG > 1000 + does_not_exist2::DOUBLE)
            | EVAL e = does_not_exist3 + 1
            """;
        var failure = "line 7:12: Unknown column [does_not_exist3]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailMetadataFieldInKeep() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | KEEP " + field;
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldInEval() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | EVAL x = " + field;
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldInWhere() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | WHERE " + field + " IS NOT NULL";
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldInSort() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | SORT " + field;
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldInStats() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | STATS x = COUNT(" + field + ")";
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldInRename() {
        for (String field : MetadataAttribute.ATTRIBUTES_MAP.keySet()) {
            var query = "FROM test | RENAME " + field + " AS renamed";
            var failure = "Unknown column [" + field + "]";
            assertUnmappedFailure(test(), query, failure);
        }
    }

    public void testFailMetadataFieldAfterStats() {
        var query = """
            FROM test
            | STATS c = COUNT(*)
            | KEEP _score
            """;
        var failure = "Unknown column [_score]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailMetadataFieldInFork() {
        var query = """
            FROM test
            | FORK (WHERE _score > 1)
                   (WHERE salary > 50000)
            """;
        var failure = "Unknown column [_score]";
        assertUnmappedFailure(test(), query, failure);
    }

    public void testFailMetadataFieldInSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = """
            FROM
                (FROM test
                 | WHERE _score > 1)
            """;
        var failure = "Unknown column [_score]";
        assertUnmappedFailure(test(), query, failure);
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
        var stmt1 = setUnmappedNullify("""
            TS k8s
            | RENAME @timestamp AS newTs
            | STATS max(rate(network.total_cost)) BY tbucket = BUCKET(newTs, 1hour)
            """);
        assertThat(
            EsqlTestUtils.analyzer().addAnalysisTestsIndexResolutions().statementError(stmt1),
            containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );

        var stmt2 = setUnmappedNullify("""
            TS k8s
            | DROP @timestamp
            | STATS max(rate(network.total_cost))
            """);
        assertThat(
            EsqlTestUtils.analyzer().addAnalysisTestsIndexResolutions().statementError(stmt2),
            containsString("3:13: [rate(network.total_cost)] " + UnresolvedTimestamp.UNRESOLVED_SUFFIX)
        );
    }

    public void testLoadModeDisallowsFork() {
        var stmt = setUnmappedLoad("FROM test | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)");
        assertThat(
            test().statementError(stmt),
            containsString("FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkWithStats() {
        var stmt = setUnmappedLoad("FROM test | FORK (STATS c = COUNT(*)) (STATS d = AVG(salary))");
        assertThat(
            test().statementError(stmt),
            containsString("FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsForkWithMultipleBranches() {
        var stmt = setUnmappedLoad("""
            FROM test
            | FORK (WHERE emp_no > 1)
                   (WHERE emp_no < 100)
                   (WHERE salary > 50000)
            """);
        assertThat(
            test().statementError(stmt),
            containsString("FORK is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsLookupJoin() {
        var stmt = setUnmappedLoad("FROM test | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code");
        assertThat(
            test().addAnalysisTestsLookupResolutions().statementError(stmt),
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
            test().addAnalysisTestsLookupResolutions().statementError(stmt),
            containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsSubquery() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("FROM test, (FROM languages | WHERE language_code > 1)");
        assertThat(
            test().addAnalysisTestsIndexResolutions().statementError(stmt),
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
            test().addAnalysisTestsIndexResolutions().statementError(stmt),
            containsString("Subqueries and views are not supported with unmapped_fields=\"load\"")
        );
    }

    public void testLoadModeDisallowsNestedSubqueries() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var stmt = setUnmappedLoad("FROM test, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)");
        assertThat(
            test().addAnalysisTestsIndexResolutions().statementError(stmt),
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
            test().addAnalysisTestsLookupResolutions().statementError(stmt),
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
        var analyzer = test().addAnalysisTestsLookupResolutions();
        assertThat(analyzer.statementError(query), containsString("FORK is not supported with unmapped_fields=\"load\""));
        assertThat(analyzer.statementError(query), containsString("LOOKUP JOIN is not supported with unmapped_fields=\"load\""));
    }

    public void testLoadModeDisallowsSubqueryAndFork() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        var query = setUnmappedLoad("""
            FROM test, (FROM languages | WHERE language_code > 1)
            | FORK (WHERE emp_no > 1) (WHERE emp_no < 100)
            """);
        var analyzer = test().addAnalysisTestsIndexResolutions();
        assertThat(analyzer.statementError(query), containsString("Subqueries and views are not supported with unmapped_fields=\"load\""));
        assertThat(analyzer.statementError(query), containsString("FORK is not supported with unmapped_fields=\"load\""));
    }

    private void assertUnmappedFailure(TestAnalyzer analyzer, String query, String failure) {
        assertThat(analyzer.statementError(setUnmappedNullify(query)), containsString(failure));
        assertThat(analyzer.statementError(setUnmappedLoad(query)), containsString(failure));
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
            EsqlTestUtils.analyzer().addIndex("test", "mapping-full_text_search.json")
                .statementError(setUnmappedLoad("FROM test | WHERE knn(vector, [1, 2, 3]) | KEEP vector")),
            containsString("does not support full-text search function [KNN]")
        );
    }

    @Override
    protected List<String> filteredWarnings() {
        return withInlinestatsWarning(withDefaultLimitWarning(super.filteredWarnings()));
    }
}
