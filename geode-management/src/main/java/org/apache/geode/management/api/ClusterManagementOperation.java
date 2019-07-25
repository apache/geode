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
package org.apache.geode.management.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.runtime.OperationResult;

/**
 * Geode modules, extensions, or user code can define cluster management operations to use with
 * {@link ClusterManagementService#start(ClusterManagementOperation)} by subclassing this
 * abstract base class and adding any necessary parameters. For example, a CompactDiskStore
 * operation might need a parameter for the name of the disk store to compact.
 *
 * The implementation of the operation is not to be included here; instead it must be registered
 * using OperationManager.registerOperation on the locator.
 *
 * @param <V> the result type of the operation
 */
@SuppressWarnings("unused")
public interface ClusterManagementOperation<V extends OperationResult> {
  /**
   * must match the REST controller's RequestMapping
   *
   * @return the portion after /management/experimental, e.g. /operations/name
   */
  @JsonIgnore
  String getEndpoint();
}
