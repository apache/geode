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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
public class ExpirableSecurityManager extends SimpleSecurityManager implements Serializable {
  // use static field for ease of testing since there is only one instance of this in each VM
  // we only need ConcurrentHashSet here, but map is only construct available in the library
  private final Set<String> expired_users = ConcurrentHashMap.newKeySet();
  private final Map<String, List<String>> authorizedOps =
      new ConcurrentHashMap<>();
  private final Map<String, List<String>> unauthorizedOps =
      new ConcurrentHashMap<>();

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    Object user = super.authenticate(credentials);
    if (expired_users.contains((String) user)) {
      throw new AuthenticationFailedException("User already expired.");
    }
    return user;
  }

  @Override
  public boolean authorize(Object principal, ResourcePermission permission) {
    if (expired_users.contains((String) principal)) {
      addToMap(unauthorizedOps, principal, permission);
      throw new AuthenticationExpiredException("User authentication expired.");
    }
    addToMap(authorizedOps, principal, permission);

    // always authorized
    return true;
  }

  public void addExpiredUser(String user) {
    expired_users.add(user);
  }

  public Set<String> getExpiredUsers() {
    return expired_users;
  }

  public Map<String, List<String>> getAuthorizedOps() {
    return authorizedOps;
  }

  public Map<String, List<String>> getUnAuthorizedOps() {
    return unauthorizedOps;
  }

  private void addToMap(Map<String, List<String>> maps, Object user,
      ResourcePermission permission) {
    List<String> list = maps.get(user);
    if (list == null) {
      list = new ArrayList<>();
    }
    if (!list.contains(permission.toString())) {
      list.add(permission.toString());
    }
    maps.put(user.toString(), list);
  }
}
