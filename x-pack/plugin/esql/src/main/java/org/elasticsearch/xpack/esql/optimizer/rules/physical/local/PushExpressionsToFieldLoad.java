/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Replaces {@link Expression}s that can be pushed to field loading with a field attribute
 * that calculates the expression during value loading. See {@link BlockLoaderExpression}
 * for more about how these loads are implemented and why we do this.
 * <p>
 *     This rule runs in one downward (aka output-to-read) pass, making four sorts
 *     of transformations:
 * </p>
 * <ul>
 *     <li>
 *         When we see a use of a <strong>new</strong> pushable function we build an
 *         attribute for the function, record that attribute, and discard it after use.
 *         For example, {@code EVAL l = LENGTH(message)} becomes
 *         {@code EVAL l = $$message$LENGTH$1324$$ | DROP $$message$LENGTH$1324$$ }.
 *         We need the {@code DROP} so we don't change the output schema.
 *     </li>
 *     <li>
 *         When we see a use of pushable function for which we already have an attribute
 *         we just use it. This looks like the {@code l} attribute in
 *         {@code EVAL l = LENGTH(message) | EVAL l2 = LENGTH(message)}
 *     </li>
 *     <li>
 *         When we see a PROJECT, add any new attributes to the projection so we can use
 *         them on previously visited nodes. So {@code KEEP foo | EVAL l = LENGTH(message)}
 *         becomes
 *         <pre>{@code
 *           | KEEP foo, $$message$LENGTH$1324$$
 *           | EVAL l = $$message$LENGTH$1324$$
 *           | DROP $$message$LENGTH$1324$$}
 *         }</pre>
 *     </li>
 *     <li>
 *         When we see a relation, add the attribute to it.
 *     </li>
 * </ul>
 */
public class PushExpressionsToFieldLoad extends ParameterizedRule<PhysicalPlan, PhysicalPlan, LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        ScanForPotential scan = new ScanForPotential(context);
        plan.forEachDown(scan::scan);
        // TODO in the future take more care when pushing many things to the same field.
        // for now we just always push
        if (scan.toPush.isEmpty()) {
            return plan;
        }
        return plan.transformDown(PhysicalPlan.class, new Rule(scan.toPush)::doRule);
    }

    /**
     * Lazily scans the plan for "primaries". A "primary" here is an {@link EsRelation}
     * we can push a field load into.
     * NOCOMMIT explain
     */
    private class Extracts {
        /**
         * A map from each field to how it is extracted.
         */
        private final Map<FieldAttribute, FieldExtractExec> sources = new IdentityHashMap<>();

        private final Set<PhysicalPlan> scanned = Collections.newSetFromMap(new IdentityHashMap<>());

        /**
         * Find "primaries" for a node. Returning the empty list is special here - it
         * means that the node's ancestors contain a node to which we cannot push.
         */
        FieldExtractExec extractFor(PhysicalPlan node, FieldAttribute attr) {
            scan(node);
            return sources.get(attr);
        }

        /**
         * Recursively scan {@code plan}, visiting ancestors before children, and
         * ignoring any trees we've scanned before.
         */
        private void scan(PhysicalPlan plan) {
            if (scanned.add(plan) == false) {
                return;
            }
            for (PhysicalPlan child : plan.children()) {
                scan(child);
            }
            if (plan instanceof FieldExtractExec extract) {
                onExtract(extract);
            }
        }

        private void onExtract(FieldExtractExec plan) {
            for (Attribute attr : plan.attributesToExtract()) {
                if (attr instanceof FieldAttribute fa) {
                    sources.put(fa, plan);
                }
            }
        }
    }

    private class ScanForPotential {
        private final Extracts extracts = new Extracts();
        private final Map<FieldExtractExec, Map<FieldAttribute, List<ConfigAndOrig>>> toPush = new IdentityHashMap<>();

        private final LocalPhysicalOptimizerContext context;

        private ScanForPotential(LocalPhysicalOptimizerContext context) {
            this.context = context;
        }

        void scan(PhysicalPlan plan) {
            if (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof AggregateExec) {
                scanPotentialInvocation(plan);
            }
        }

        private void scanPotentialInvocation(PhysicalPlan plan) {
            plan.forEachExpression(Expression.class, e -> {
                if (e instanceof BlockLoaderExpression ble) {
                    BlockLoaderExpression.PushedBlockLoaderExpression fuse = ble.tryPushToFieldLoading(context.searchStats());
                    if (fuse != null) {
                        scanPotentialFuse(plan, e, fuse);
                    }
                }
            });
        }

        private void scanPotentialFuse(
            PhysicalPlan nodeWithExpression,
            Expression e,
            BlockLoaderExpression.PushedBlockLoaderExpression fuse
        ) {
            FieldExtractExec extract = extracts.extractFor(nodeWithExpression, fuse.field());
            log.trace("found extract {} {}", nodeWithExpression, extract);
            if (extract == null) {
                return;
            }
            var preference = context.configuration().pragmas().fieldExtractPreference();
            if (context.searchStats().supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), preference) == false) {
                return;
            }
            Map<FieldAttribute, List<ConfigAndOrig>> added = toPush.computeIfAbsent(extract, unused -> new IdentityHashMap<>());
            added.computeIfAbsent(fuse.field(), unused -> new ArrayList<>()).add(new ConfigAndOrig(fuse.config(), e));
        }
    }

    private record ConfigAndOrig(BlockLoaderFunctionConfig config, Expression orig) {}

    private class Rule {
        private final Map<FieldExtractExec, List<Attribute>> newExtractConfig = new IdentityHashMap<>();
        private final Map<Expression, FieldAttribute> expressionSubstitutions = new IdentityHashMap<>();

        private boolean addedNewAttribute;

        private Rule(Map<FieldExtractExec, Map<FieldAttribute, List<ConfigAndOrig>>> toPush) {
            for (var scanned : toPush.entrySet()) {
                List<Attribute> toExtract = new ArrayList<>(scanned.getKey().attributesToExtract());
                for (Map.Entry<FieldAttribute, List<ConfigAndOrig>> e : scanned.getValue().entrySet()) {
                    FieldAttribute attr = e.getKey();
                    toExtract.remove(attr); // How can we tell if it's still used?! Only remove if not.
                    for (ConfigAndOrig cao : e.getValue()) {
                        FunctionEsField functionEsField = new FunctionEsField(attr.field(), cao.orig.dataType(), cao.config);
                        var name = rawTemporaryName(attr.name(), cao.config.function().toString(), String.valueOf(cao.config.hashCode()));
                        FieldAttribute added = new FieldAttribute(
                            attr.source(),
                            attr.parentName(),
                            attr.qualifier(),
                            name,
                            functionEsField,
                            attr.nullable(),
                            new NameId(),
                            true
                        );
                        expressionSubstitutions.put(cao.orig, added);
                        toExtract.add(added);
                    }
                }
                newExtractConfig.put(scanned.getKey(), toExtract);
            }
        }

        private PhysicalPlan doRule(PhysicalPlan plan) {
            if (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof AggregateExec) {
                return transformPotentialInvocation(plan);
            }
            if (plan instanceof FieldExtractExec exec) {
                return transformExtract(exec);
            }
            return plan;
        }

        private PhysicalPlan transformPotentialInvocation(PhysicalPlan plan) {
            addedNewAttribute = false;
            PhysicalPlan transformedPlan = plan.transformExpressionsOnly(Expression.class, e -> {
                FieldAttribute s = expressionSubstitutions.get(e);
                if (s != null) {
                    addedNewAttribute = true;
                    return s;
                }
                return e;
            });
            if (addedNewAttribute == false) {
                // Either didn't see anything pushable or everything has already been pushed
                return plan;
            }
            // Found a new pushable attribute, discard it *after* use so we don't modify the output.
            return new ProjectExec(Source.EMPTY, transformedPlan, transformedPlan.output());
        }

        private LogicalPlan transformProject(Project project) {
            // Preserve any pushed attributes so we can use them later
            // List<NamedExpression> projections = new ArrayList<>(project.projections());
            // projections.addAll(addedAttrs.values());
            // return project.withProjections(projections);
            return null;
        }

        private FieldExtractExec transformExtract(FieldExtractExec plan) {
            List<Attribute> newConfig = newExtractConfig.get(plan);
            if (newConfig == null) {
                return plan;
            }
            return plan.withAttributesToExtract(newConfig);
        }
    }
}
