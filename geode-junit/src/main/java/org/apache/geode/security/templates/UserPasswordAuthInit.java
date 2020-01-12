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

import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * An {@link AuthInitialize} implementation that obtains the user name and password as the
 * credentials from the given set of properties.
 *
 * To use this class the {@code security-client-auth-init} property should be set to the fully
 * qualified name the static {@code create} method viz.
 * {@code org.apache.geode.security.templates.UserPasswordAuthInit.create}
 *
 * @since GemFire 5.5
 */
public class UserPasswordAuthInit implements AuthInitialize {

  public static final String USER_NAME = "security-username";
  public static final String PASSWORD = "security-password";

  protected LogWriter systemLogWriter;
  protected LogWriter securityLogWriter;

  public static AuthInitialize create() {
    return new UserPasswordAuthInit();
  }

  @Override
  public void init(final LogWriter systemLogWriter, final LogWriter securityLogWriter)
      throws AuthenticationFailedException {
    this.systemLogWriter = systemLogWriter;
    this.securityLogWriter = securityLogWriter;
  }

  @Override
  public Properties getCredentials(final Properties securityProperties,
      final DistributedMember server, final boolean isPeer) throws AuthenticationFailedException {
    String userName = securityProperties.getProperty(USER_NAME);
    if (userName == null) {
      throw new AuthenticationFailedException(
          "UserPasswordAuthInit: user name property [" + USER_NAME + "] not set.");
    }

    String password = securityProperties.getProperty(PASSWORD);
    if (password == null) {
      password = "";
    }

    Properties securityPropertiesCopy = new Properties();
    securityPropertiesCopy.setProperty(USER_NAME, userName);
    securityPropertiesCopy.setProperty(PASSWORD, password);
    return securityPropertiesCopy;
  }

  @Override
  public void close() {}
}
