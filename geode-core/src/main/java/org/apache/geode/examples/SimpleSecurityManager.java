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
package org.apache.geode.examples;

import java.util.Properties;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

/**
 * Intended for implementation testing, this class authenticates a user when the username matches
 * the password, which also represents the permissions the user is granted.
 */
public class SimpleSecurityManager implements SecurityManager {

  @Override
  public void init(final Properties securityProps) {
    // nothing
  }

  @Override
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    String token = credentials.getProperty("security-token");
    if (token != null) {
      return "Bearer " + token;
    }
    String username = credentials.getProperty("security-username");
    String password = credentials.getProperty("security-password");
    if (username != null && username.equals(password)) {
      return username;
    }
    throw new AuthenticationFailedException("invalid username/password");
  }

  @Override
  public boolean authorize(final Object principal, final ResourcePermission permission) {
    if (principal.toString().startsWith("Bearer ")) {
      return true;
    }
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
