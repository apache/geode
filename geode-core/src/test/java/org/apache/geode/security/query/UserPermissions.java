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
package org.apache.geode.security.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.internal.RestrictedMethodInvocationAuthorizer;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.TestSecurityManager;

public class UserPermissions {
  private TestSecurityManager manager = new TestSecurityManager();
  public HashMap<String, List<String>> queryPermissions;

  public UserPermissions() {
    if (!manager.initializeFromJsonResource(
        "org/apache/geode/management/internal/security/clientServer.json"))
      throw new RuntimeException(
          "Something bad happened while trying to load the TestSecurityManager with the org/apache/geode/management/internal/security/clientServer.json");
  }


  public String getUserPassword(String user) {
    return manager.getUser(user).getPassword();
  }

}
