/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.examples.SimpleSecurityManager;

/**
 * this is a test security manager that will authenticate credentials when username matches the
 * password. It will authorize all operations. It keeps a list of expired users, and will throw
 * AuthenticationExpiredException if the user is in that list. This security manager is usually used
 * with NewCredentialAuthInitialize.
 *
 * make sure to call reset after each test to clean things up.
 */
public class ExpirableSecurityManager extends SimpleSecurityManager {
  private static final Set<String> EXPIRED_USERS = ConcurrentHashMap.newKeySet();
  private static final Map<String, List<String>> AUTHORIZED_OPS =
      new ConcurrentHashMap<>();
  private static final List<String> UNAUTHORIZED_USERS = new ArrayList<>();

  @Override
  public boolean authorize(Object principal, ResourcePermission permission) {
    addToMap(AUTHORIZED_OPS, principal, permission);
    // always authorized
    return true;
  }

  @Override
  public boolean isExpired(Object principal) {
    boolean expired = EXPIRED_USERS.contains((String) principal);
    if (expired) {
      UNAUTHORIZED_USERS.add((String) principal);
    }
    return expired;
  }

  public static void addExpiredUser(String user) {
    EXPIRED_USERS.add(user);
  }

  public static Set<String> getExpiredUsers() {
    return EXPIRED_USERS;
  }

  public static Map<String, List<String>> getAuthorizedOps() {
    return AUTHORIZED_OPS;
  }

  public static List<String> getUnauthorizedUsers() {
    return UNAUTHORIZED_USERS;
  }

  private static void addToMap(Map<String, List<String>> maps, Object user,
      ResourcePermission permission) {
    List<String> list = maps.get(user);
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(permission.toString());
    maps.put(user.toString(), list);
  }

  public static void reset() {
    EXPIRED_USERS.clear();
    AUTHORIZED_OPS.clear();
    UNAUTHORIZED_USERS.clear();
  }
}
