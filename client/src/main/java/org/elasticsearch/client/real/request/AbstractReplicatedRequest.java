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

package org.elasticsearch.client.real.request;

import java.util.concurrent.TimeUnit;

/**
 * Abstract base for requests that are replicated.
 */
public abstract class AbstractReplicatedRequest<Request extends AbstractReplicatedRequest> extends AbstractRequest<Request> {
    /**
     * The index this request is targeting.
     */
    private String index;
    /**
     * A timeout. Hopefully in Elasticsearch's timeout format but not validated.
     */
    private String timeout;

    /**
     * The index this request is targeting.
     */
    public final Request setIndex(String index) {
        this.index = index;
        return self();
    }

    /**
     * The timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>. The format of
     * the string is documented TODO here. This format is not validated by the client.
     */
    public final Request setTimeout(String timeout) {
        this.timeout = timeout;
        return self();
    }

    /**
     * Set the timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>. This is a
     * convenience wrapper around {@link #setTimeout(String)} with parameters more familiar to Java developers.
     */
    public final Request setTimeout(long duration, TimeUnit unit) {
        switch (unit) {
            case DAYS:
                return setTimeout(duration + "d");
            case MINUTES:
                return setTimeout(duration + "m");
            case SECONDS:
                return setTimeout(duration + "s");
            case MILLISECONDS:
                return setTimeout(duration + "ms");
            default:
                throw new IllegalArgumentException("Unsupported TimeUnit:  " + unit);
        }
    }
}
