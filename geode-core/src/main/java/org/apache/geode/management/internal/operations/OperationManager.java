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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.management.operation.OperationResult;

/**
 * Maintains a list of known, possible operations and can run them when requested. Maintains state
 * of running operations
 */
public class OperationManager {

  private final Map<String, OperationExecutor> operations = new HashMap<>();

  private final Map<String, OperationResult> operationResults = new HashMap<>();

  public void registerOperation(OperationExecutor executor) {
    operations.put(executor.getId(), executor);
  }

  /**
   * Launch an operation...
   */
  public synchronized CompletableFuture<OperationResult> run(String operation,
      Map<String, String> arguments) {
    OperationExecutor opExecutor = operations.get(operation);
    if (opExecutor == null) {
      throw new IllegalArgumentException("No operation for " + operation + " exists");
    }

    Executor executor = LoggingExecutors.newThreadOnEachExecute(operation);
    OperationResult result = new OperationResult(operation);
    CompletableFuture<OperationResult> future = opExecutor.run(arguments, result, executor);

    operationResults.put(operation, result);

    return future;
  }

  /**
   * Return a copy of the latest result of an operation
   */
  public synchronized OperationResult getResult(String operation) {
    OperationResult result = operationResults.get(operation);
    return result != null ? result.clone() : null;
  }

}
