/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.collect;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CollectOperator implements Operator {
    public record Factory(CollectResultsService service, String sessionId, List<CollectedMetadata.Field> fields, Instant expirationTime)
        implements
            OperatorFactory {
        @Override
        public CollectOperator get(DriverContext driverContext) {
            return new CollectOperator(driverContext, service, sessionId, fields, expirationTime);
        }

        @Override
        public String describe() {
            return "CollectOperator";
        }
    }

    private final FailureCollector failureCollector = new FailureCollector();

    private final CollectResultsService service;
    private final DriverContext driverContext;
    private final AsyncExecutionId mainId;
    private final List<CollectedMetadata.Field> fields;
    private final Instant expirationTime;

    private int pageCount;

    private volatile Phase phase = Phase.COLLECTING;
    private volatile IsBlockedResult blocked = NOT_BLOCKED;

    public CollectOperator(
        DriverContext driverContext,
        CollectResultsService service,
        String sessionId,
        List<CollectedMetadata.Field> fields,
        Instant expirationTime
    ) {
        this.driverContext = driverContext;
        this.service = service;
        this.mainId = new AsyncExecutionId(UUIDs.randomBase64UUID(), new TaskId(sessionId.split("/")[0]));
        this.fields = fields;
        this.expirationTime = expirationTime;
    }

    @Override
    public boolean needsInput() {
        return failureCollector.hasFailure() == false && phase == Phase.COLLECTING && blocked.listener().isDone();
    }

    @Override
    public void addInput(Page page) {
        assert needsInput();
        checkFailure();
        SubscribableListener<Void> blockedFuture = new SubscribableListener<>();
        LogManager.getLogger(CollectOperator.class).error("indexing");
        driverContext.addAsyncAction();
        blocked = new IsBlockedResult(blockedFuture, "indexing");
        service.savePage(mainId, pageCount, page, expirationTime, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse docWriteResponse) {
                try {
                    pageCount++;
                    LogManager.getLogger(CollectOperator.class).error("done indexing");
                    blocked = NOT_BLOCKED;
                } finally {
                    Releasables.closeExpectNoException(Releasables.wrap(page::releaseBlocks, docWriteResponse::decRef));
                }
                driverContext.removeAsyncAction();
                blockedFuture.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                driverContext.removeAsyncAction();
                blockedFuture.onFailure(e);
            }
        });
    }

    @Override
    public void finish() {
        if (phase != Phase.COLLECTING) {
            return;
        }
        phase = Phase.WAITING_TO_FINISH;
        checkFailure();
        blocked.listener().addListener(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                writeMetadata();
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
            }
        });
    }

    private void writeMetadata() {
        SubscribableListener<Void> blockedFuture = new SubscribableListener<>();
        LogManager.getLogger(CollectOperator.class).error("writing finished");
        driverContext.addAsyncAction();
        blocked = new IsBlockedResult(blockedFuture, "finishing");
        service.saveMetadata(mainId, new CollectedMetadata(fields, pageCount), expirationTime, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse docWriteResponse) {
                phase = Phase.READY_TO_OUTPUT;
                LogManager.getLogger(CollectOperator.class).error("done finishing");
                blocked = NOT_BLOCKED;
                driverContext.removeAsyncAction();
                blockedFuture.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                failureCollector.unwrapAndCollect(e);
                driverContext.removeAsyncAction();
                blockedFuture.onFailure(e);
            }
        });
    }

    @Override
    public boolean isFinished() {
        return phase == Phase.FINISHED;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return blocked;
    }

    @Override
    public Page getOutput() {
        checkFailure();
        if (phase != Phase.READY_TO_OUTPUT) {
            return null;
        }
        LogManager.getLogger(CollectOperator.class).error("outputting");
        Block nameBlock = null;
        Block pageCountBlock = null;
        Block expiration = null;
        try {
            nameBlock = driverContext.blockFactory().newConstantBytesRefBlockWith(new BytesRef(mainId.getEncoded()), 1);
            pageCountBlock = driverContext.blockFactory().newConstantIntBlockWith(pageCount, 1);
            expiration = driverContext.blockFactory().newConstantLongBlockWith(expirationTime.toEpochMilli(), 1);
            Page result = new Page(nameBlock, pageCountBlock, expiration);
            nameBlock = null;
            pageCountBlock = null;
            expiration = null;
            phase = Phase.FINISHED;
            return result;
        } finally {
            Releasables.close(nameBlock, pageCountBlock, expiration);
        }
    }

    @Override
    public void close() {}

    private void checkFailure() {
        Exception e = failureCollector.getFailure();
        if (e != null) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private enum Phase {
        COLLECTING,
        WAITING_TO_FINISH,
        FINISHING,
        READY_TO_OUTPUT,
        FINISHED;
    }
}
