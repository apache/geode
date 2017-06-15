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

import java.util.Properties;

/**
 * This class provides a simple implementation of {@link SecurityManager} for authentication and
 * authorization solely based on the username and password provided.
 *
 * It is meant for demo purpose, not for production.
 *
 * Authentication: All users whose password matches the username are authenticated. e.g.
 * username/password = test/test, user/user, admin/admin
 *
 * Authorization: users whose username is a substring (case insensitive) of the permission required
 * are authorized. e.g. username = data: is authorized for all data operations: data; data:manage
 * data:read data:write username = dataWrite: is authorized for data writes on all regions:
 * data:write data:write:regionA username = cluster: authorized for all cluster operations username
 * = clusterRead: authorized for all cluster read operations
 *
 * a user could be a comma separated list of roles as well.
 */
public class SimpleTestSecurityManager implements SecurityManager {

  @Override
  public void init(final Properties securityProps) {
    // nothing
  }

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    String username = credentials.getProperty("security-username");
    String password = credentials.getProperty("security-password");
    if (username != null && username.equals(password)) {
      return username;
    }
    throw new AuthenticationFailedException("invalid username/password");
  }

  @Override
  public boolean authorize(final Object principal, final ResourcePermission permission) {
    String[] principals = principal.toString().toLowerCase().split(",");
    for (String role : principals) {
      String permissionString = permission.toString().replace(":", "").toLowerCase();
      if (permissionString.startsWith(role))
        return true;
    }
    return false;
  }

  @Override
  public void close() {
    // nothing
  }
}
