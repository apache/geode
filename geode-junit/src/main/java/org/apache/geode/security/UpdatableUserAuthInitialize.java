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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.distributed.DistributedMember;

/**
 * this is used in conjunction with ExpirableSecurityManager. It will create a new set of
 * credentials every time getCredentials are called, and they will always be authenticated
 * and authorized by the ExpirableSecurityManager.
 *
 * make sure reset is called after each test to clean things up.
 */
public class UpdatableUserAuthInitialize implements AuthInitialize {
  // use static field for ease of testing since there is only one instance of this in each VM
  private static final AtomicReference<String> user = new AtomicReference<>();

  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    Properties credentials = new Properties();
    if (securityProps.size() < 2) {
      credentials.put("security-username", user.get());
      credentials.put("security-password", user.get());
    } else {
      Object userName = securityProps.get("security-username");
      if (userName != null && !((String) userName).isEmpty()) {
        credentials.put("security-username", userName);
        credentials.put("security-password", userName);
      } else {
        throw new AuthenticationFailedException("No username provided.");
      }
    }
    return credentials;
  }

  public static String getUser() {
    return user.get();
  }

  public static void setUser(String newValue) {
    user.set(newValue);
  }

  public static void reset() {
    user.set(null);
  }
}
