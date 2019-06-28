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

package org.apache.geode.management.internal.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.geode.management.internal.api.LocatorClusterManagementService;
import org.apache.geode.management.operation.OperationResult;

/**
 * Interface which any Management service operation must implement. {@code OperationExecutor}s are
 * registered in the {@link LocatorClusterManagementService}
 */
public interface OperationExecutor {

  /**
   * Return the id of the {@code OperationExecutor}. This id will also be used in the background
   * thread performing the operation.
   *
   * @return the id
   */
  String getId();

  /**
   * An optional description of the operation
   *
   * @return the description or empty string if not overridden
   */
  default String getDescription() {
    return "";
  }

  /**
   * Method which actually performs the operation. For example:
   *
   * <pre>
   *   public CompletableFuture&lt;OperationResult&gt; run(
   *       Map<String, String> arguments, OperationResult operationResult,
   *       Executor exe) {
   *     final CompletableFuture&lt;OperationResult&gt; future = new CompletableFuture&lt;&gt;();
   *     exe.execute(() -> {
   *       operationResult.setStartTime(System.currentTimeMillis());
   *       operationResult.setEndTime(System.currentTimeMillis());
   *       operationResult.setStatus(OperationResult.Status.COMPLETED);
   *     });
   *
   *     return future;
   *   }
   * </pre>
   *
   * @param arguments possible arguments to the operation
   * @param operationResult a {@code OperationResult} instance which must be used to complete the
   *        returned {@code CompletableFuture}
   * @param executor an {@link Executor} which must be used to execute the background task.
   */
  CompletableFuture<OperationResult> run(Map<String, String> arguments,
      OperationResult operationResult, Executor executor);

}
