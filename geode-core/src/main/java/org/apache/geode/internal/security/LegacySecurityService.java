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
package org.apache.geode.internal.security;

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.SecurityManager;

/**
 * implementing SecurityService when only legacy authenticators are specified
 */
public class LegacySecurityService implements SecurityService {

  private final boolean hasClientAuthenticator;
  private final boolean hasPeerAuthenticator;

  LegacySecurityService() {
    this.hasClientAuthenticator = false;
    this.hasPeerAuthenticator = false;
  }

  LegacySecurityService(final String clientAuthenticator, final String peerAuthenticator) {
    this.hasClientAuthenticator = StringUtils.isNotBlank(clientAuthenticator);
    this.hasPeerAuthenticator = StringUtils.isNotBlank(peerAuthenticator);
  }

  public boolean isClientSecurityRequired() {
    return this.hasClientAuthenticator;
  }

  public boolean isIntegratedSecurity() {
    return false;
  }

  public boolean isPeerSecurityRequired() {
    return this.hasPeerAuthenticator;
  }

  public ThreadState bindSubject(Subject subject) {
    return null;
  }

  public Subject getSubject() {
    return null;
  }

  public Subject login(Properties credentials) {
    return null;
  }

  public void logout() {}

  public Callable associateWith(Callable callable) {
    return callable;
  }

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation) {}

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, ResourcePermission.Target target) {}

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String target) {}

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, String target, String key) {}

  public void authorize(ResourcePermission.Resource resource,
      ResourcePermission.Operation operation, ResourcePermission.Target target, String key) {}

  public void authorize(ResourcePermission context) {}

  public void authorize(ResourcePermission context, Subject currentUser) {}

  public void close() {}

  public boolean needPostProcess() {
    return false;
  }

  public Object postProcess(String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return value;
  }

  public Object postProcess(Object principal, String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return value;
  }

  public SecurityManager getSecurityManager() {
    return null;
  }

  public PostProcessor getPostProcessor() {
    return null;
  }
}
