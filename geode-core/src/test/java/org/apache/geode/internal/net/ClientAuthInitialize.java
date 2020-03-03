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
package org.apache.geode.internal.net;

import java.util.Properties;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;

public class ClientAuthInitialize implements AuthInitialize {

  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    Properties credentials = new Properties();

    String username = securityProps.getProperty(SECURITY_USERNAME);
    String password = securityProps.getProperty(SECURITY_PASSWORD);

    if (username == null) {
      throw new AuthenticationFailedException(
          "unable to find " + SECURITY_USERNAME + " [gfsecurity.properties]");
    }
    if (password == null) {
      throw new AuthenticationFailedException(
          "unable to find " + SECURITY_PASSWORD + " [gfsecurity.properties]");
    }

    credentials.setProperty(SECURITY_USERNAME, securityProps.getProperty(SECURITY_USERNAME));
    credentials.setProperty(SECURITY_PASSWORD, securityProps.getProperty(SECURITY_PASSWORD));
    return credentials;
  }


  @Override
  public void close() {}
}
