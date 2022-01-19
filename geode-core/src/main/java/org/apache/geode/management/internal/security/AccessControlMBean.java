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
package org.apache.geode.management.internal.security;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * AccessControlMBean Implementation. This retrieves JMXPrincipal from AccessController and performs
 * authorization for given role using gemfire AccessControl Plugin
 *
 * @since Geode 1.0
 */
public class AccessControlMBean implements AccessControlMXBean {

  private final SecurityService securityService;

  public AccessControlMBean(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public boolean authorize(String resource, String permission) {
    try {
      securityService.authorize(Resource.valueOf(resource), Operation.valueOf(permission),
          ResourcePermission.ALL, ResourcePermission.ALL);
      return true;
    } catch (GemFireSecurityException e) {
      return false;
    }
  }

}
