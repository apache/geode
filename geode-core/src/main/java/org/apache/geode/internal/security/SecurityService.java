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

import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.security.SecurityManager;

public interface SecurityService {

  ThreadState bindSubject(Subject subject);

  Subject getSubject();

  Object getPrincipal();

  Subject login(Properties credentials);

  void logout();

  Callable associateWith(Callable callable);

  void authorize(Resource resource, Operation operation);

  void authorize(Resource resource, Operation operation, Target target);

  void authorize(Resource resource, Operation operation, String target);

  void authorize(Resource resource, Operation operation, String target, Object key);

  void authorize(Resource resource, Operation operation, Target target, String key);

  void authorize(ResourcePermission context);

  void authorize(ResourcePermission context, Subject currentUser);

  void close();

  boolean needPostProcess();

  Object postProcess(String regionPath, Object key, Object value, boolean valueIsSerialized);

  Object postProcess(Object principal, String regionPath, Object key, Object value,
      boolean valueIsSerialized);

  boolean isClientSecurityRequired();

  boolean isIntegratedSecurity();

  boolean isPeerSecurityRequired();

  SecurityManager getSecurityManager();

  PostProcessor getPostProcessor();
}
