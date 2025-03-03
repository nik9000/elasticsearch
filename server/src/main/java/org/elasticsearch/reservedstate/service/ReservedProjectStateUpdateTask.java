/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class ReservedProjectStateUpdateTask extends ReservedStateUpdateTask<ProjectMetadata> {
    private final ProjectId projectId;

    public ReservedProjectStateUpdateTask(
        ProjectId projectId,
        String namespace,
        ReservedStateChunk stateChunk,
        ReservedStateVersionCheck versionCheck,
        Map<String, ReservedClusterStateHandler<ProjectMetadata, ?>> handlers,
        Collection<String> orderedHandlers,
        Consumer<ErrorState> errorReporter,
        ActionListener<ActionResponse.Empty> listener
    ) {
        super(namespace, stateChunk, versionCheck, handlers, orderedHandlers, errorReporter, listener);
        this.projectId = projectId;
    }

    @Override
    protected Optional<ProjectId> projectId() {
        return Optional.of(projectId);
    }

    @Override
    protected ClusterState execute(ClusterState currentState) {
        if (currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // If cluster state has become blocked, this task was submitted while the node was master but is now not master.
            // The new master will re-read file settings, so whatever update was to be written here will be handled
            // by the new master.
            return currentState;
        }

        // use an empty project if it doesnt exist, this is then added to ClusterState below.
        ProjectMetadata project = ReservedClusterStateService.getPotentiallyNewProject(currentState, projectId);

        var result = execute(project, project.reservedStateMetadata());
        if (result == null) {
            return currentState;
        }

        return ClusterState.builder(currentState).putProjectMetadata(ProjectMetadata.builder(result.v1()).put(result.v2())).build();
    }
}
