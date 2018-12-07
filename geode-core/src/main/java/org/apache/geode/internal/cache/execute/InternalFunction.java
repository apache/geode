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
package org.apache.geode.internal.cache.execute;

import java.util.Collection;
import java.util.Collections;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Defines a marker interface to be used by all internal functions and expected to require
 * ResourcePermissions.ALL to execute the function. Internal Functions are functions that are
 * executed by members with in the distributed system, if a function is required by a client API
 * then it shouldn't be an InternalFunction.
 */
public interface InternalFunction<T> extends Function<T>, InternalEntity {
  /**
   * InternalFunction do require ResourcePermissions.ALL so that it only allows super users to
   * invoke from Clients. So don't override this in implementations.
   */
  default Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.ALL);
  }
}
