/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedView;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ViewService {
    private final ClusterService clusterService;

    public ViewService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public LogicalPlan resolve(UnresolvedView unresolvedView, PlanTelemetry telementry) {
        ViewMetadata views = clusterService.state().metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
        View view = views.views().get(unresolvedView.name());
        if (view == null) {
            return unresolvedView;
        }
        // TODO don't reparse every time. Store parsed? Or cache parsing? dunno
        // NOCOMMIT this will make super-wrong Source. the _source should be the view.
        return new EsqlParser().createStatement(view.query(), new QueryParams(), telementry);
    }

    /**
     * Adds or modifies a view by name. This method can only be invoked on the elected master node.
     */
    public void put(String name, View view, ActionListener<Void> callback) {
        assert clusterService.localNode().isMasterNode();
        // TODO validate the view parses

        updateClusterState(callback, current -> {
            Map<String, View> original = current.metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY).views();
            Map<String, View> updated = new HashMap<>(original);
            updated.put(name, view);
            return updated;
        });
    }

    /**
     * Removes a view from the cluster state. This method can only be invoked on the master node.
     */
    public void delete(String name, ActionListener<Void> callback) {
        assert clusterService.localNode().isMasterNode();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        updateClusterState(callback, current -> {
            Map<String, View> original = current.metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY).views();
            if (original.containsKey(name) == false) {
                throw new ResourceNotFoundException("policy [{}] not found", name);
            }
            Map<String, View> updated = new HashMap<>(original);
            updated.remove(name);
            return updated;
        });
    }

    private void updateClusterState(ActionListener<Void> callback, Function<ClusterState, Map<String, View>> function) {
        submitUnbatchedTask("update-esql-view-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Map<String, View> policies = function.apply(currentState);
                Metadata metadata = Metadata.builder(currentState.metadata())
                    .putCustom(ViewMetadata.TYPE, new ViewMetadata(policies))
                    .build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                callback.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                callback.onFailure(e);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
