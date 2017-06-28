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

import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.security.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadState;

import java.util.Properties;
import java.util.concurrent.Callable;

public interface SecurityService {

  default ThreadState bindSubject(Subject subject) {
    return null;
  }

  default Subject getSubject() {
    return null;
  }

  default Subject login(Properties credentials) {
    return null;
  }

  default void logout() {}

  default Callable associateWith(Callable callable) {
    return callable;
  }

  default void authorize(Resource resource, Operation operation, String target, String key) {}

  default void authorize(Resource resource, Operation operation, Target target, String key) {}

  default void authorize(Resource resource, Operation operation, Target target) {}

  default void authorizeClusterManage() {}

  default void authorizeClusterWrite() {}

  default void authorizeClusterRead() {}

  default void authorizeDataManage() {}

  default void authorizeDataWrite() {}

  default void authorizeDataRead() {}

  default void authorizeDiskManage() {}

  default void authorizeGatewayManage() {}

  default void authorizeJarManage() {}

  default void authorizeQueryManage() {}

  default void authorizeRegionManage(String regionName) {}

  default void authorizeRegionManage(String regionName, String key) {}

  default void authorizeRegionWrite(String regionName) {}

  default void authorizeRegionWrite(String regionName, String key) {}

  default void authorizeRegionRead(String regionName) {}

  default void authorizeRegionRead(String regionName, String key) {}

  default void authorize(ResourcePermission context) {}

  default void close() {}

  default boolean needPostProcess() {
    return false;
  }

  default Object postProcess(String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return value;
  }

  default Object postProcess(Object principal, String regionPath, Object key, Object value,
      boolean valueIsSerialized) {
    return value;
  }

  default boolean isClientSecurityRequired() {
    return false;
  }

  default boolean isIntegratedSecurity() {
    return false;
  }

  default boolean isPeerSecurityRequired() {
    return false;
  }

  default SecurityManager getSecurityManager() {
    return null;
  }

  default PostProcessor getPostProcessor() {
    return null;
  }
}
