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

import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.internal.Dormant;

/**
 * This is normally returned by
 * {@link ClusterManagementService#startOperation(ClusterManagementOperation)} to convey status of
 * launching the async operation, and if successful, the {@link CompletableFuture} to access the
 * status
 * and result of the async operation.
 *
 * @param <V> the type of the operation's result
 */
@Experimental
public class ClusterManagementOperationResult<V extends JsonSerializable>
    extends ClusterManagementResult {
  @JsonIgnore
  private final CompletableFuture<V> operationResult;

  /**
   * normally called by {@link ClusterManagementService#startOperation(ClusterManagementOperation)}
   */
  public ClusterManagementOperationResult(ClusterManagementResult result,
      CompletableFuture<V> operationResult) {
    super(result);
    this.operationResult = operationResult;
  }

  /**
   * @return the future result of the async operation
   */
  @JsonIgnore
  public CompletableFuture<V> getResult() {
    if (operationResult instanceof Dormant)
      ((Dormant) operationResult).wakeUp();
    return operationResult;
  }
}
