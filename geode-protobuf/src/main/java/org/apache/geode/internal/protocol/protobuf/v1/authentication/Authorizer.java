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
package org.apache.geode.internal.protocol.protobuf.v1.authentication;

import org.apache.geode.security.ResourcePermission;

/**
 * Authorization checker. When security is enabled, this wraps a subject and checks that the
 * subject is allowed to perform the requested operation
 */
public interface Authorizer {
  default void authorize(ResourcePermission.Resource data, ResourcePermission.Operation read,
      String regionName, Object key) {
    authorize(new ResourcePermission(data, read, regionName, key.toString()));
  }

  void authorize(ResourcePermission resourcePermission);
}
