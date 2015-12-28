/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract base for scrolling across a search and executing bulk indexes on all
 * results.
 */
public abstract class AbstractAsyncBulkIndexByScrollAction<Request extends AbstractBulkIndexByScrollRequest<Request>, Response extends BulkIndexByScrollResponse>
        extends AbstractAsyncBulkByScrollAction<Request, Response> {

    private final AtomicLong noops = new AtomicLong(0);
    private final ScriptService scriptService;
    private final CompiledScript script;

    public AbstractAsyncBulkIndexByScrollAction(ESLogger logger, ScriptService scriptService, Client client, ThreadPool threadPool,
            Request mainRequest, SearchRequest firstSearchRequest, ActionListener<Response> listener) {
        super(logger, client, threadPool, mainRequest, firstSearchRequest, listener);
        this.scriptService = scriptService;
        if (mainRequest.script() == null) {
            script = null;
        } else {
            script = scriptService.compile(mainRequest.script(), ScriptContext.Standard.UPDATE, mainRequest);
        }
    }

    /**
     * Build the IndexRequest for a single search hit. This shouldn't handle
     * metadata or the script. That will be handled by copyMetadata and
     * applyScript functions that can be overridden.
     */
    protected abstract IndexRequest buildIndexRequest(SearchHit doc);

    /**
     * The number of noops (skipped bulk items) as part of this request.
     */
    public long noops() {
        return noops.get();
    }

    @Override
    protected BulkRequest buildBulk(Iterable<SearchHit> docs) {
        BulkRequest bulkRequest = new BulkRequest(mainRequest);
        ExecutableScript executableScript = null;
        Map<String, Object> scriptCtx = null;

        for (SearchHit doc : docs) {
            IndexRequest index = buildIndexRequest(doc);
            copyMetadata(index, doc);
            if (script != null) {
                if (executableScript == null) {
                    executableScript = scriptService.executable(script, mainRequest.script().getParams());
                    scriptCtx = new HashMap<>(16);
                }
                if (applyScript(index, doc, executableScript, scriptCtx) == false) {
                    continue;
                }
            }
            bulkRequest.add(index);
        }

        return bulkRequest;
    }

    /**
     * Copies the metadata from a hit to the index request.
     */
    protected void copyMetadata(IndexRequest index, SearchHit doc) {
        index.parent(fieldValue(doc, "_parent"));
        copyRouting(index, doc);
        // Comes back as a Long but needs to be a string
        Long timestamp = fieldValue(doc, "_timestamp");
        if (timestamp != null) {
            index.timestamp(timestamp.toString());
        }
        index.ttl(fieldValue(doc, "_ttl"));
    }

    /**
     * Part of copyMetadata but called out individual for easy overwriting.
     */
    protected void copyRouting(IndexRequest index, SearchHit doc) {
        index.routing(fieldValue(doc, "_routing"));
    }

    protected <T> T fieldValue(SearchHit doc, String fieldName) {
        SearchHitField field = doc.field(fieldName);
        return field == null ? null : field.value();
    }

    /**
     * Apply a script to the request.
     *
     * @return is this request still ok to apply (true) or is it a noop (false)
     */
    @SuppressWarnings("unchecked")
    protected boolean applyScript(IndexRequest index, SearchHit doc, ExecutableScript script, final Map<String, Object> ctx) {
        if (script == null) {
            return true;
        }
        ctx.put("_index", doc.index());
        ctx.put("_type", doc.type());
        ctx.put("_id", doc.id());
        Long oldVersion = doc.getVersion();
        ctx.put("_version", oldVersion);
        String oldParent = fieldValue(doc, "_parent");
        ctx.put("_parent", oldParent);
        String oldRouting = fieldValue(doc, "_routing");
        ctx.put("_routing", oldRouting);
        Long oldTimestamp = fieldValue(doc, "_timestamp");
        ctx.put("_timestamp", oldTimestamp);
        Long oldTtl = fieldValue(doc, "_ttl");
        ctx.put("_ttl", oldTtl);
        ctx.put("_source", index.sourceAsMap());
        ctx.put("op", "update");
        script.setNextVar("ctx", ctx);
        script.run();
        Map<String, Object> resultCtx = (Map<String, Object>) script.unwrap(ctx);
        String newOp = (String) resultCtx.remove("op");
        if (newOp == null) {
            throw new IllegalArgumentException("Script cleared op!");
        }
        if ("noop".equals(newOp)) {
            noops.incrementAndGet();
            return false;
        }
        if ("update".equals(newOp) == false) {
            throw new IllegalArgumentException("Invalid op [" + newOp + ']');
        }

        /*
         * It'd be lovely to only set the source if we know its been
         * modified but it isn't worth keeping two copies of it
         * around just to check!
         */
        index.source((Map<String, Object>) resultCtx.remove("_source"));

        Object newValue = ctx.remove("_index");
        if (false == doc.index().equals(newValue)) {
            scriptChangedIndex(index, newValue);
        }
        newValue = ctx.remove("_type");
        if (false == doc.type().equals(newValue)) {
            scriptChangedType(index, newValue);
        }
        newValue = ctx.remove("_id");
        if (false == doc.id().equals(newValue)) {
            scriptChangedId(index, newValue);
        }
        newValue = ctx.remove("_version");
        if (false == Objects.equals(oldVersion, newValue)) {
            scriptChangedVersion(index, newValue);
        }
        newValue = ctx.remove("_parent");
        if (false == Objects.equals(oldParent, newValue)) {
            scriptChangedParent(index, newValue);
        }
        /*
         * Its important that routing comes after parent in case you want to
         * change them both.
         */
        newValue = ctx.remove("_routing");
        if (false == Objects.equals(oldRouting, newValue)) {
            scriptChangedRouting(index, newValue);
        }
        newValue = ctx.remove("_timestamp");
        if (false == Objects.equals(oldTimestamp, newValue)) {
            scriptChangedTimestamp(index, newValue);
        }
        newValue = ctx.remove("_ttl");
        if (false == Objects.equals(oldTtl, newValue)) {
            scriptChangedTtl(index, newValue);
        }
        if (false == ctx.isEmpty()) {
            throw new IllegalArgumentException("Invalid fields added to ctx [" + String.join(",", ctx.keySet()) + ']');
        }
        return true;
    }

    protected void scriptChangedIndex(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_index] not allowed");
    }

    protected void scriptChangedType(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_type] not allowed");
    }

    protected void scriptChangedId(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_id] not allowed");
    }

    protected void scriptChangedVersion(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_version] not allowed");
    }

    protected void scriptChangedRouting(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_routing] not allowed");
    }

    protected void scriptChangedParent(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_parent] not allowed");
    }

    protected void scriptChangedTimestamp(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_timestamp] not allowed");
    }

    protected void scriptChangedTtl(IndexRequest index, Object to) {
        throw new IllegalArgumentException("Modifying [_ttl] not allowed");
    }
}
