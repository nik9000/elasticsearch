/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

/**
 * Utilities for internal users.
 */
public class InternalUsers {
    /**
     * Is the provided user internal?
     */
    public static boolean isInternal(User user) {
        return USER_TO_ROLE.containsKey(user);
    }

    /**
     * Is the provided user internal?
     */
    public static boolean isInternal(String principal) {
        return PRINCIPAL_TO_USER.containsKey(principal);
    }


    /**
     * Resolve the principal into an internal user or null if there is no such
     * internal user.
     */
    public static User resolve(String principal) {
        return PRINCIPAL_TO_USER.get(principal);
    }

    /**
     * Synchronously resolves roles for internal users which is important to
     * prevent deadlocks elsewhere.
     */
    public static Role resolveRole(User user) {
        if (SystemUser.is(user)) {
            throw new IllegalArgumentException("the user [" + user.principal() + "] is the system user and we should never try to get its" +
                " roles");
        }
        return USER_TO_ROLE.get(user);
    }

    /**
     * Collection of all internal users, mostly used by tests when we nedd to
     * pick a random internal user.
     */
    public static Collection<User> users() {
        return USER_TO_ROLE.keySet();
    }

    public static final Map<User, Role> USER_TO_ROLE;
    static {
        Map<User, Role> users = new HashMap<User, Role>();
        users.put(XPackUser.INSTANCE, XPackUser.ROLE);
        users.put(XPackSecurityUser.INSTANCE, ReservedRolesStore.SUPERUSER_ROLE);
        /*
         * The SystemUser is funny and deprecated and it doesn't resolve roles
         * using this Map and will be removed.
         */
        users.put(SystemUser.INSTANCE, null);
        USER_TO_ROLE = unmodifiableMap(users);
    }

    private static final Map<String, User> PRINCIPAL_TO_USER = unmodifiableMap(users().stream()
            .collect(toMap(User::principal, Function.identity())));
}