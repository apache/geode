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
import org.apache.geode.security.SecurityManager;

/**
 * An {@link AuthInitialize} implementation that obtains a bearer token as credentials from the
 * given set of properties.
 *
 * To use this class the {@code security-client-auth-init} property should be set to the fully
 * qualified name of the static {@code create} method viz.
 * {@code org.apache.geode.security.templates.TokenAuthInit.create}
 */
public class TokenAuthInit implements AuthInitialize {

  public static final String BEARER_TOKEN = "security-bearer-token";

  protected LogWriter systemLogWriter;
  protected LogWriter securityLogWriter;

  public static AuthInitialize create() {
    return new TokenAuthInit();
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
    String token = securityProperties.getProperty(BEARER_TOKEN);
    if (token == null) {
      throw new AuthenticationFailedException(
          "TokenAuthInit: bearer token property [" + BEARER_TOKEN + "] not set.");
    }

    Properties securityPropertiesCopy = new Properties();
    // SecurityManager expects TOKEN property
    securityPropertiesCopy.setProperty(SecurityManager.TOKEN, token);
    return securityPropertiesCopy;
  }

  @Override
  public void close() {}
}
