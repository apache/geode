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

package org.apache.geode.redis;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;

public class ExpiringSecurityManager extends SimpleSecurityManager {
  private final Set<String> expiredUsers = new HashSet<>();

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    String user = (String) super.authenticate(credentials);
    if (expiredUsers.remove(user)) {
      throw new AuthenticationExpiredException("User has expired.");
    }

    return user;
  }

  @Override
  public boolean authorize(Object principal, ResourcePermission permission) {
    String user = (String) principal;
    boolean authorized = super.authorize(principal, permission) && !expiredUsers.contains(user);
    if (expiredUsers.remove(user)) {
      throw new AuthenticationExpiredException("User has expired.");
    }

    return authorized;
  }

  public void addExpiredUser(String user) {
    expiredUsers.add(user);
  }

  public void reset() {
    expiredUsers.clear();
  }
}
