/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.lucene.spatial.CartesianShapeIndexer;
import org.elasticsearch.lucene.spatial.CoordinateEncoder;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.function.Function;

public class SpatialIntersects extends SpatialRelatesFunction {
    protected static final SpatialRelations GEO = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.GEO,
        CoordinateEncoder.GEO,
        new GeoShapeIndexer(Orientation.CCW, "ST_Intersects")
    );
    protected static final SpatialRelations CARTESIAN = new SpatialRelations(
        ShapeField.QueryRelation.INTERSECTS,
        SpatialCoordinateTypes.CARTESIAN,
        CoordinateEncoder.CARTESIAN,
        new CartesianShapeIndexer("ST_Intersects")
    );

    @FunctionInfo(returnType = { "boolean" }, description = "Returns whether the two geometries or geometry columns intersect.")
    public SpatialIntersects(
        Source source,
        @Param(
            name = "field",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" },
            description = "Geometry column name or variable of geometry type"
        ) Expression left,
        @Param(
            name = "other",
            type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" },
            description = "Geometry column name or variable of geometry type"
        ) Expression right
    ) {
        this(source, left, right, false);
    }

    private SpatialIntersects(Source source, Expression left, Expression right, boolean useDocValues) {
        super(source, left, right, useDocValues);
    }

    @Override
    public ShapeField.QueryRelation queryRelation() {
        return ShapeField.QueryRelation.INTERSECTS;
    }

    @Override
    public SpatialIntersects withDocValues() {
        return new SpatialIntersects(source(), left(), right(), true);
    }

    @Override
    protected SpatialIntersects replaceChildren(Expression newLeft, Expression newRight) {
        return new SpatialIntersects(source(), newLeft, newRight, useDocValues);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, SpatialIntersects::new, left(), right());
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory geoEvaluator(
        EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory rhs
    ) {
        return new SpatialIntersectsGeoEvaluator.Factory(source(), lhs, rhs);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory cartesianEvaluator(
        EvalOperator.ExpressionEvaluator.Factory lhs,
        EvalOperator.ExpressionEvaluator.Factory rhs
    ) {
        return new SpatialIntersectsCartesianEvaluator.Factory(source(), lhs, rhs);
    }

    @Evaluator(extraName = "Geo", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processGeo(BytesRef lhs, BytesRef rhs) throws IOException {
        return GEO.geometryRelatesGeometry(lhs, rhs);
    }

    @Evaluator(extraName = "Cartesian", warnExceptions = { IllegalArgumentException.class, IOException.class })
    static boolean processCartesian(BytesRef lhs, BytesRef rhs) throws IOException {
        return CARTESIAN.geometryRelatesGeometry(lhs, rhs);
    }
}
