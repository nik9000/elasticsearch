/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects.CARTESIAN;
import static org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects.GEO;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;

public class PointToGeometry {
    /**
     * Promotes an evaluator that returns a point to one that returns a geometry.
     * Returns unchanged evaluators that return geometries.
     */
    static EvalOperator.ExpressionEvaluator.Factory promote(
        Source source,
        boolean useDocValues,
        DataType type,
        EvalOperator.ExpressionEvaluator.Factory evaluator
    ) {
        if (type.equals(GEO_POINT)) {
            if (useDocValues) {
                return new PointToGeometryGeoEvaluator.Factory(source, evaluator);
            }
            return evaluator;
        }
        if (type.equals(CARTESIAN_POINT)) {
            if (useDocValues) {
                return new PointToGeometryCartesianEvaluator.Factory(source, evaluator);
            }
            return evaluator;
        }
        if (type.equals(GEO_SHAPE)) {
            return evaluator;
        }
        if (type.equals(CARTESIAN_SHAPE)) {
            return evaluator;
        }
        throw new UnsupportedOperationException("type not supported [" + type.esType() + "]");
    }

    @Evaluator(extraName = "Cartesian")
    static BytesRef processCartesian(long v) {
        Point point = CARTESIAN.spatialCoordinateType.longAsPoint(v);
        return CARTESIAN.spatialCoordinateType.asWkb(point);
    }

    @Evaluator(extraName = "Geo")
    static BytesRef processGeo(long v) {
        Point point = GEO.spatialCoordinateType.longAsPoint(v);
        return GEO.spatialCoordinateType.asWkb(point);
    }
}
