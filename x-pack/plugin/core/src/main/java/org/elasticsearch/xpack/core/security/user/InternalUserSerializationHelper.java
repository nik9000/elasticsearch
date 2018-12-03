/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class InternalUserSerializationHelper {
    public static User readFrom(StreamInput input) throws IOException {
        final boolean isInternalUser = input.readBoolean();
        final String username = input.readString();
        if (isInternalUser) {
            User user = InternalUsers.resolve(username);
            if (user == null) {
                throw new IllegalStateException("user [" + username + "] is not an internal user");
            }
        }
        return User.partialReadFrom(username, input);
    }
    public static void writeTo(User user, StreamOutput output) throws IOException {
        if (InternalUsers.isInternal(user)) {
            output.writeBoolean(true);
            output.writeString(user.principal());
        } else {
            User.writeTo(user, output);
        }
    }
}
