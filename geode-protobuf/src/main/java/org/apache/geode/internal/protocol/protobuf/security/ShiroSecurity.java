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
package org.apache.geode.internal.protocol.protobuf.security;

import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission;

public class ShiroSecurity implements Security {
  private final Subject subject;
  private final SecurityService securityService;

  public ShiroSecurity(SecurityService securityService, Subject subject) {
    this.securityService = securityService;
    this.subject = subject;
  }

  @Override
  public void authorize(ResourcePermission permission) {
    securityService.authorize(permission, subject);
  }

  @Override
  public Object postProcess(String regionPath, Object key, Object value) {
    return securityService.postProcess(subject.getPrincipal(), regionPath, key, value, false);
  }

  @Override
  public boolean needsPostProcessing() {
    return securityService.getPostProcessor() != null;
  }
}
