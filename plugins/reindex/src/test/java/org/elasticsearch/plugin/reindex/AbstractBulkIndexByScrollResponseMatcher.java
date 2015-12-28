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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractBulkIndexByScrollResponseMatcher<Response extends BulkIndexByScrollResponse, Self extends AbstractBulkIndexByScrollResponseMatcher<Response, Self>>
        extends TypeSafeMatcher<Response> {
    private Matcher<Long> updatedMatcher = equalTo(0l);
    /**
     * Matches for number of batches. Optional.
     */
    private Matcher<Integer> batchesMatcher;
    private Matcher<Long> versionConflictsMatcher = equalTo(0l);
    private Matcher<Integer> failuresMatcher = equalTo(0);

    protected abstract Self self();

    public Self updated(Matcher<Long> updatedMatcher) {
        this.updatedMatcher = updatedMatcher;
        return self();
    }

    public Self updated(long updated) {
        return updated(equalTo(updated));
    }

    /**
     * Set the matches for the number of batches. Defaults to matching any
     * integer because we usually don't care about how many batches the job
     * takes.
     */
    public Self batches(Matcher<Integer> batchesMatcher) {
        this.batchesMatcher = batchesMatcher;
        return self();
    }

    public Self batches(int batches) {
        return batches(equalTo(batches));
    }

    public Self batches(int total, int batchSize) {
        // Round up
        return batches((total + batchSize - 1) / batchSize);
    }

    public Self versionConflicts(Matcher<Long> versionConflictsMatcher) {
        this.versionConflictsMatcher = versionConflictsMatcher;
        return self();
    }

    public Self versionConflicts(long versionConflicts) {
        return versionConflicts(equalTo(versionConflicts));
    }

    /**
     * Set the matcher for the size of the failures list. For more in depth
     * matching do it by hand. The type signatures required to match the
     * actual failures list here just don't work.
     */
    public Self failures(Matcher<Integer> failuresMatcher) {
        this.failuresMatcher = failuresMatcher;
        return self();
    }

    /**
     * Set the expected size of the failures list.
     */
    public Self failures(int failures) {
        return failures(equalTo(failures));
    }


    @Override
    protected boolean matchesSafely(Response item) {
        return updatedMatcher.matches(item.updated()) &&
            (batchesMatcher == null || batchesMatcher.matches(item.batches())) &&
            versionConflictsMatcher.matches(item.versionConflicts()) &&
            failuresMatcher.matches(item.failures().size());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("indexed matches ").appendDescriptionOf(updatedMatcher);
        if (batchesMatcher != null) {
            description.appendText(" and batches matches ").appendDescriptionOf(batchesMatcher);
        }
        description.appendText(" and versionConflicts matches ").appendDescriptionOf(versionConflictsMatcher);
        description.appendText(" and failures size matches ").appendDescriptionOf(failuresMatcher);
    }
}