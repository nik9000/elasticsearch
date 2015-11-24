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

package org.elasticsearch.action.indexbysearch;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IndexBySearchRequest extends ActionRequest<IndexBySearchRequest> {
    private static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(20);

    /**
     * The search to be executed.
     */
    private SearchRequest search;

    /**
     * Prototype for index requests.
     */
    private IndexRequest index;

    private Script script;

    public IndexBySearchRequest() {
    }

    public IndexBySearchRequest(SearchRequest search, IndexRequest index) {
        this.search = search;
        this.index = index;

        search.scroll(DEFAULT_SCROLL_TIMEOUT);
    }

    public Script script() {
        return script;
    }

    public IndexBySearchRequest script(Script script) {
        this.script = script;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = search.validate();
        if (index.source() != null) {
            e = addValidationError("source is specified", e);
        }
        for (String validationError: index.validate().validationErrors()) {
            // TODO these are hacky
            if ("source is missing".equals(validationError)) {
                continue;
            }
            if ("type is missing".equals(validationError)) {
                continue;
            }
            e = addValidationError(validationError, e);
        }
        return e;
    }

    protected SearchRequest search() {
        return search;
    }

    protected IndexRequest index() {
        return index;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        search.readFrom(in);
        index.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        search.writeTo(out);
        index.writeTo(out);
    }

    @Override
    public String toString() {
        return LoggerMessageFormat.format("index-by-search from {}{} to [{}][{}]",
                search.indices() == null ? "all indices" : search.indices(),
                search.types() == null || search.types().length == 0 ? "" : search.types(),
                index.index(),
                index.type());
    }
}
