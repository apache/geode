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

import static org.apache.geode.security.SecurityManager.PASSWORD;
import static org.apache.geode.security.SecurityManager.TOKEN;
import static org.apache.geode.security.SecurityManager.USER_NAME;

import java.util.Properties;

import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

/**
 * Intended for example and demo purpose, this class authenticates a user when the username matches
 * the password, which also represents the permissions the user is granted.
 *
 * It also validate an auth token if it's present
 */
public class SimpleSecurityManager implements SecurityManager {
  /**
   * the valid token string that will be authenticated. Any other token string will be rejected.
   */
  public static final String VALID_TOKEN = "FOO_BAR";

  @Override
  public void init(final Properties securityProps) {
    // nothing
  }

  @Override
  /*
   * these following users will be authenticated:
   * 1. auth token defined as SimpleSecurityManager.VALID_TOKEN
   * 2. username and password that are the same
   */
  public Object authenticate(final Properties credentials) throws AuthenticationFailedException {
    String token = credentials.getProperty(TOKEN);
    if (token != null) {
      if (VALID_TOKEN.equalsIgnoreCase(token) || token.length() > 20) {
        return "Bearer " + token;
      } else {
        throw new AuthenticationFailedException("Invalid token");
      }
    }
    String username = credentials.getProperty(USER_NAME);
    String password = credentials.getProperty(PASSWORD);
    if (username != null && username.equals(password)) {
      return username;
    }
    throw new AuthenticationFailedException("invalid username/password");
  }

  @Override
  /*
   * these following users will be authorized for the permission:
   * 1. All authtoken users will be authorized
   * 2. users with passwords, if the permission string (without the :) starts with the username,
   * then it will be authorized, e.g. if the permission string cluster:manage, then username
   * "cluster" or "clusterManage" will be authorized
   */
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
