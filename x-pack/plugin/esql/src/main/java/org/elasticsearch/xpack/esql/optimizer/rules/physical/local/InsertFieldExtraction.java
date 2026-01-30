/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Materialize the concrete fields that need to be extracted from the storage until the last possible moment.
 * Expects the local plan to already have a projection containing the fields needed upstream.
 * <p>
 * 1. add the materialization right before usage inside the local plan
 * 2. materialize any missing fields needed further up the chain
 *
 * @see ProjectAwayColumns
 */
public class InsertFieldExtraction extends ParameterizedRule<PhysicalPlan, PhysicalPlan, LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        Scan scan = new Scan(context);
        plan.forEachUp(scan::scan);
        return plan.transformUp(new InsertLoads(context, scan.loads)::insertLoads);
    }

    private static class Scan {
        private final Map<PhysicalPlan, LoadsForPlan> loads = new IdentityHashMap<>();
        private final LocalPhysicalOptimizerContext context;

        private Scan(LocalPhysicalOptimizerContext context) {
            this.context = context;
        }

        void scan(PhysicalPlan plan) {
            // Source nodes never load anything
            if (plan instanceof LeafExec) {
                return;
            }
            LoadsForPlan loadsForPlan = loadsForPlan(plan);
            if (loadsForPlan.loads.isEmpty() == false) {
                loads.put(plan, loadsForPlan);
            }
        }

        private LoadsForPlan loadsForPlan(PhysicalPlan plan) {
            LoadsForPlan loadsForPlan = new LoadsForPlan();
            if (false == (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof AggregateExec)) {
                scanForAttributeLoads(plan, loadsForPlan);
                return loadsForPlan;
            }

            // Try the most aggressive field fusion plan
            scanForAttributeLoads(pushAll(plan, loadsForPlan), loadsForPlan);
            return loadsForPlan;
        }

        private void scanForAttributeLoads(PhysicalPlan plan, LoadsForPlan loadsForPlan) {
            AttributeSet input = plan.inputSet();
            Set<Attribute> pushed = loadsForPlan.pushedExpressionsSet();
            plan.references().forEach(a -> {
                if (a instanceof FieldAttribute || a instanceof MetadataAttribute) {
                    if (input.contains(a) == false && pushed.contains(a) == false) {
                        loadsForPlan.attr(a).loadWithoutFuse = true;
                    }
                }
            });
        }

        private PhysicalPlan pushAll(PhysicalPlan plan, LoadsForPlan loadsForPlan) {
            return plan.transformExpressionsOnly(Expression.class, e -> {
                if (e instanceof BlockLoaderExpression ble) {
                    BlockLoaderExpression.PushedBlockLoaderExpression fuse = ble.tryPushToFieldLoading(context.searchStats());
                    if (fuse != null) {
                        return toPushedIfPossible(plan, e, fuse, loadsForPlan);
                    }
                }
                return e;
            });
        }

        private Expression toPushedIfPossible(
            PhysicalPlan plan,
            Expression e,
            BlockLoaderExpression.PushedBlockLoaderExpression fuse,
            LoadsForPlan loadsForPlan
        ) {
            var preference = context.configuration().pragmas().fieldExtractPreference();
            if (context.searchStats().supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), preference) == false) {
                return e;
            }
            // NOCOMMIT don't attempt if already loaded
            PushedExpression pushed = new PushedExpression(fuse.field(), fuse.config(), e);
            loadsForPlan.attr(fuse.field()).pushedExpression(pushed);
            return pushed.attr;
        }
    }

    private static class LoadsForPlan {
        private final Map<Attribute, LoadsForAttribute> loads = new IdentityHashMap<>();

        LoadsForAttribute attr(Attribute attr) {
            return loads.computeIfAbsent(attr, unused -> new LoadsForAttribute());
        }

        @Override
        public String toString() {
            return loads.toString();
        }

        public Set<Attribute> pushedExpressionsSet() {
            Set<Attribute> attributes = Collections.newSetFromMap(new IdentityHashMap<>());
            for (LoadsForAttribute l : loads.values()) {
                for (PushedExpression e : l.pushedExpressions) {
                    attributes.add(e.attr);
                }
            }
            return attributes;
        }

        public List<Attribute> attributesToExtract() {
            List<Attribute> attributesToExtract = new ArrayList<>(loads.size());
            for (Map.Entry<Attribute, LoadsForAttribute> l : loads.entrySet()) {
                Attribute attr = l.getKey();
                LoadsForAttribute loads = l.getValue();
                if (loads.loadWithoutFuse) {
                    attributesToExtract.add(attr);
                }
                if (loads.pushedExpressions != null) {
                    for (PushedExpression push : loads.pushedExpressions) {
                        attributesToExtract.add(push.attr);
                    }
                }
            }
            return attributesToExtract;
        }

        public boolean hasPushedExpressions() {
            for (LoadsForAttribute l : loads.values()) {
                if (l.pushedExpressions != null) {
                    return true;
                }
            }
            return false;
        }

        public PhysicalPlan replacePushedExpressions(PhysicalPlan plan) {
            Map<Expression, Attribute> replaced = new IdentityHashMap<>();
            for (LoadsForAttribute l : loads.values()) {
                if (l.pushedExpressions != null) {
                    for (PushedExpression p : l.pushedExpressions) {
                        replaced.put(p.orig, p.attr);
                    }
                }
            }
            return plan.transformExpressionsOnly(e -> {
                Attribute replacement = replaced.get(e);
                return replacement == null ? e : replacement;
            });
        }
    }

    private static class LoadsForAttribute {
        private List<PushedExpression> pushedExpressions;
        private boolean loadWithoutFuse = false;

        void pushedExpression(PushedExpression exp) {
            if (pushedExpressions == null) {
                pushedExpressions = new ArrayList<>(2);
            }

            pushedExpressions.add(exp);
        }

        @Override
        public String toString() {
            if (pushedExpressions == null) {
                return "load";
            }
            StringBuilder b = new StringBuilder();
            b.append("{fused").append(pushedExpressions);
            if (loadWithoutFuse) {
                b.append(", and unfused");
            }
            return b.append('}').toString();
        }
    }

    private static class PushedExpression {
        // NOCOMMIT just use the new attribute?
        private final BlockLoaderFunctionConfig config;
        private final Expression orig;
        private final FieldAttribute attr;

        private PushedExpression(FieldAttribute field, BlockLoaderFunctionConfig config, Expression orig) {
            this.config = config;
            this.orig = orig;
            FunctionEsField functionEsField = new FunctionEsField(field.field(), orig.dataType(), config);
            String name = rawTemporaryName(field.name(), config.function().toString(), String.valueOf(config.hashCode()));
            this.attr = new FieldAttribute(
                field.source(),
                field.parentName(),
                field.qualifier(),
                name,
                functionEsField,
                field.nullable(),
                new NameId(),
                true
            );
        }

        @Override
        public String toString() {
            return attr.toString();
        }
    }

    private record InsertLoads(LocalPhysicalOptimizerContext context, Map<PhysicalPlan, LoadsForPlan> loads) {
        private PhysicalPlan insertLoads(PhysicalPlan plan) {
            LoadsForPlan loadsForPlan = loads.get(plan);
            if (loadsForPlan == null) {
                return plan;
            }
            plan = plan.replaceChildren(addLoadsToChildren(plan, loadsForPlan));
            if (loadsForPlan.hasPushedExpressions() == false) {
                return plan;
            }
            PhysicalPlan replaced = loadsForPlan.replacePushedExpressions(plan);
            return new ProjectExec(plan.source(), replaced, plan.output());
        }

        private List<PhysicalPlan> addLoadsToChildren(PhysicalPlan plan, LoadsForPlan loadsForPlan) {
            List<PhysicalPlan> newChildren = new ArrayList<>(plan.children().size());
            boolean found = false;
            for (PhysicalPlan child : plan.children()) {
                if (found == false) {
                    if (child.outputSet().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                        found = true;
                        // collect source attributes and add the extractor
                        child = new FieldExtractExec(
                            plan.source(),
                            child,
                            loadsForPlan.attributesToExtract(),
                            context.configuration().pragmas().fieldExtractPreference()
                        );

                    }
                }
                newChildren.add(child);
            }
            // somehow no doc id
            if (found == false) {
                throw new IllegalArgumentException("No child with doc id found");
            }
            return newChildren;
        }
    }
}
