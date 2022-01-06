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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Interface for cluster management operations that can be used with
 * {@link ClusterManagementService#start(ClusterManagementOperation)}
 *
 * Classes implementing this interface should hold only the parameters for the operation.
 *
 * Implementations must be registered in the locator using OperationManager.registerOperation.
 *
 * @param <V> the result type of the operation
 */
@SuppressWarnings("unused")
@Experimental
public interface ClusterManagementOperation<V extends OperationResult>
    extends JsonSerializable, Serializable {
  /**
   * must match the REST controller's RequestMapping
   *
   * @return the portion after /management/v1, e.g. /operations/name
   */
  @JsonIgnore
  String getEndpoint();

  String getOperator();
}
