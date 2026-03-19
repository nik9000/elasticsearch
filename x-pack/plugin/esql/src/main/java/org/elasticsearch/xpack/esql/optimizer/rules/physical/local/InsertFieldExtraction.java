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
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
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
        return plan.transformUp(scan::scan);
        // return plan.transformUp(new InsertLoads(context, scan.loads)::insertLoads);
    }

    private static class Scan {
        private final Map<PhysicalPlan, LoadsForPlan> loads = new IdentityHashMap<>();
        private final LocalPhysicalOptimizerContext context;
        private Map<Attribute.IdIgnoringWrapper, PushedExpressionFirstUse> alreadyPushed;

        private Scan(LocalPhysicalOptimizerContext context) {
            this.context = context;
        }

        PhysicalPlan scan(PhysicalPlan plan) {
            if (plan instanceof LeafExec) {
                // Source nodes never load anything
                return plan;
            }

            LoadsForPlan loadsForPlan = loadsForPlan(plan);
            System.err.println("loading " + loadsForPlan + " for " + plan.nodeName());
            if (loadsForPlan.loads.isEmpty()) {
                // No loads, nothing to index
                return plan;
            }

            // Index the loads
            loads.put(plan, loadsForPlan);
            PhysicalPlan withLoad = plan.replaceChildren(
                loadsForPlan.addLoadsToChildren(plan, context.configuration().pragmas().fieldExtractPreference())
            );
            PhysicalPlan replaced = loadsForPlan.replacePushedExpressions(withLoad);
            return replaced;
        }

        private LoadsForPlan loadsForPlan(PhysicalPlan plan) {
            LoadsForPlan loadsForPlan = new LoadsForPlan();
            if (false == (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof AggregateExec)) {
                scanForAttributeLoads(plan, loadsForPlan);
                return loadsForPlan;
            }

            // Try the most aggressive field fusion plan
            scanForAttributeLoads(pushAll(plan, loadsForPlan), loadsForPlan);

            // TODO prune back any bad choices
            loadsForPlan.indexPushedExpressions(this);
            return loadsForPlan;
        }

        private void scanForAttributeLoads(PhysicalPlan plan, LoadsForPlan loadsForPlan) {
            AttributeSet input = plan.inputSet();
            Set<Attribute> pushed = loadsForPlan.pushedAttributes();
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
            if (plan.inputSet().contains(fuse.field())) {
                /*
                 * If the target field is already loaded it does us no good to try and fuse to it.
                 * In fact, it's disastrous because it might not be coming from the index at all.
                 */
                return e;
            }
            var preference = context.configuration().pragmas().fieldExtractPreference();
            if (context.searchStats().supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), preference) == false) {
                return e;
            }
            PushedExpressionFirstUse pushed = new PushedExpressionFirstUse(fuse.field(), fuse.config(), e);
            loadsForPlan.attr(fuse.field()).pushedExpression(referenceAlreadyPushed(pushed));
            return pushed.attr;
        }

        /**
         * If the proposed push was <strong>already</strong> pushed, then reference it
         * instead.
         */
        private PushedExpression referenceAlreadyPushed(PushedExpressionFirstUse push) {
            if (alreadyPushed == null) {
                return push;
            }
            PushedExpressionFirstUse firstPush = alreadyPushed.get(push.key);
            return firstPush == null ? push : firstPush.reference(push.orig);
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

        /**
         * A {@link Set} of the attributes that are pushed expressions.
         */
        public Set<Attribute> pushedAttributes() {
            Set<Attribute> attributes = Collections.newSetFromMap(new IdentityHashMap<>());
            for (LoadsForAttribute l : loads.values()) {
                l.buildPushedAttributes(attributes);
            }
            return attributes;
        }

        public List<Attribute> attributesToExtract() {
            List<Attribute> attributesToExtract = new ArrayList<>(loads.size());
            for (Map.Entry<Attribute, LoadsForAttribute> l : loads.entrySet()) {
                l.getValue().buildAttributesToExtract(attributesToExtract, l.getKey());
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
            if (false == hasPushedExpressions()) {
                return plan;
            }
            Map<Expression, Attribute> replaced = new IdentityHashMap<>();
            for (LoadsForAttribute l : loads.values()) {
                l.buildPushedExpressionsToReplace(replaced);
            }
            return plan.transformExpressionsOnly(e -> {
                Attribute replacement = replaced.get(e);
                return replacement == null ? e : replacement;
            });
        }

        public void indexPushedExpressions(Scan scan) {
            for (LoadsForAttribute load : loads.values()) {
                load.indexPushedExpressions(scan);
            }
        }

        public List<PhysicalPlan> addLoadsToChildren(PhysicalPlan plan, MappedFieldType.FieldExtractPreference extractPreference) {
            List<PhysicalPlan> newChildren = new ArrayList<>(plan.children().size());
            boolean found = false;
            for (PhysicalPlan child : plan.children()) {
                if (found == false) {
                    if (child.outputSet().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                        found = true;
                        // collect source attributes and add the extractor
                        child = new FieldExtractExec(plan.source(), child, attributesToExtract(), extractPreference);

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

        public void indexPushedExpressions(Scan scan) {
            if (pushedExpressions == null) {
                return;
            }
            for (PushedExpression push : pushedExpressions) {
                push.indexPushedExpressions(scan);
            }
        }

        public void buildPushedAttributes(Set<Attribute> attributes) {
            if (pushedExpressions == null) {
                return;
            }
            for (PushedExpression e : pushedExpressions) {
                e.buildPushedAttributes(attributes);
            }
        }

        public void buildAttributesToExtract(List<Attribute> attributesToExtract, Attribute attr) {
            if (loadWithoutFuse) {
                attributesToExtract.add(attr);
            }
            if (pushedExpressions == null) {
                return;
            }
            for (PushedExpression push : pushedExpressions) {
                push.buildAttributesToExtract(attributesToExtract);
            }
        }

        public void buildPushedExpressionsToReplace(Map<Expression, Attribute> replaced) {
            if (pushedExpressions == null) {
                return;
            }
            for (PushedExpression p : pushedExpressions) {
                p.buildPushedExpressionToReplace(replaced);
            }
        }
    }

    private interface PushedExpression {
        void indexPushedExpressions(Scan scan);

        void buildPushedAttributes(Set<Attribute> attributes);

        void buildAttributesToExtract(List<Attribute> attributesToExtract);

        void buildPushedExpressionToReplace(Map<Expression, Attribute> replaced);
    }

    private static class PushedExpressionFirstUse implements PushedExpression {
        private final Expression orig;
        private final FieldAttribute attr;
        private final Attribute.IdIgnoringWrapper key;

        private PushedExpressionFirstUse(FieldAttribute field, BlockLoaderFunctionConfig config, Expression orig) {
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
            this.key = attr.ignoreId();
        }

        public PushedExpressionReference reference(Expression orig) {
            return new PushedExpressionReference(orig, this);
        }

        @Override
        public void indexPushedExpressions(Scan scan) {
            if (scan.alreadyPushed == null) {
                scan.alreadyPushed = new HashMap<>();
            }
            scan.alreadyPushed.put(key, this);
        }

        @Override
        public void buildPushedAttributes(Set<Attribute> attributes) {
            attributes.add(attr);
        }

        @Override
        public void buildAttributesToExtract(List<Attribute> attributesToExtract) {
            attributesToExtract.add(attr);
        }

        @Override
        public void buildPushedExpressionToReplace(Map<Expression, Attribute> replaced) {
            replaced.put(orig, attr);
        }

        @Override
        public String toString() {
            return attr.toString();
        }
    }

    private record PushedExpressionReference(Expression orig, PushedExpressionFirstUse firstUse) implements PushedExpression {
        @Override
        public void indexPushedExpressions(Scan scan) {
            // Only index first push
        }

        @Override
        public void buildPushedAttributes(Set<Attribute> attributes) {
            attributes.add(firstUse.attr);
        }

        @Override
        public void buildAttributesToExtract(List<Attribute> attributesToExtract) {
            // Only extract on first push
        }

        @Override
        public void buildPushedExpressionToReplace(Map<Expression, Attribute> replaced) {
            replaced.put(orig, firstUse.attr);
        }

        @Override
        public String toString() {
            return "->" + firstUse;
        }
    }

    private record InsertLoads(LocalPhysicalOptimizerContext context, Map<PhysicalPlan, LoadsForPlan> loads) {
        private PhysicalPlan insertLoads(PhysicalPlan plan) {
            LoadsForPlan loadsForPlan = loads.get(plan);
            if (loadsForPlan == null) {
                return plan;
            }
            plan = plan.replaceChildren(loadsForPlan.addLoadsToChildren(plan, context.configuration().pragmas().fieldExtractPreference()));
            if (loadsForPlan.hasPushedExpressions() == false) {
                return plan;
            }
            PhysicalPlan replaced = loadsForPlan.replacePushedExpressions(plan);
            // return new ProjectExec(plan.source(), replaced, plan.output());
            return replaced;
        }
    }
}
