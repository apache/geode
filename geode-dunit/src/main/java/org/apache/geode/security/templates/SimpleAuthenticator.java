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

package org.apache.geode.security.templates;

import java.security.Principal;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.Authenticator;

/**
 * A test implementation of the legacy {@link org.apache.geode.security.Authenticator}.
 * Authenticates a user when the username matches the password, which in turn will match the
 * user's permissions in {@link org.apache.geode.security.templates.SimpleAccessController}.
 */
@SuppressWarnings("deprecation")
public class SimpleAuthenticator implements Authenticator {
  @Override
  public void init(Properties securityProps, LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {}

  public static Authenticator create() {
    return new SimpleAuthenticator();
  }

  @Override
  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    // Expect "security-username" and "security-password" to (a) match and (b) define permissions.
    String username = props.getProperty("security-username");
    String password = props.getProperty("security-password");
    if (StringUtils.isNotBlank(username) && !username.equals(password)) {
      throw new AuthenticationFailedException(
          "SimpleAuthenticator expects username to match password.");
    }
    return new UsernamePrincipal(username);
  }

  @Override
  public void close() {}
}
