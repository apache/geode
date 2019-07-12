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
package org.apache.geode.management.internal.operation;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.util.Strings;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.AsyncOperation;
import org.apache.geode.management.api.AsyncOperationResult;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.api.ReturnType;
import org.apache.geode.management.operation.RebalanceOperation;

@Experimental
public class AsyncExecutorManager {
  private ExecutorService executorService;
  private Map<Class, AsyncOperationExecutor> executors;
  private Map<String, Future> history;

  public AsyncExecutorManager() {
    executorService = Executors.newFixedThreadPool(10);
    history = new HashMap<>();

    // initialize the list of operation executors
    executors = new HashMap<>();
    executors.put(RebalanceOperation.class, new RebalanceOperationExecutor());
  }

  public <A extends AsyncOperation & ReturnType<V>, V extends JsonSerializable> AsyncOperationResult<V> submit(
      A op) {
    if (!Strings.isBlank(op.getId()))
      throw new IllegalArgumentException(
          String.format("Operation type %s should not supply its own id",
              op.getClass().getSimpleName()));
    op.setId(UUID.randomUUID().toString());
    tidyHistory();
    AsyncOperationExecutor<A, V> executor = getExecutor(op);
    if (executor == null) {
      throw new IllegalArgumentException(String.format("Operation type %s is not supported",
          op.getClass().getSimpleName()));
    }
    validate(op);
    Future<V> future = executorService.submit(executor.createCallable(op));

    // save the Future so we can check on it later
    history.put(op.getId(), future);

    return new LocatorAsyncOperationResult<V>(future);
  }

  private <T extends AsyncOperation> void validate(T op) {
    // TODO: look up a suitable validator for this op type and call it
  }

  private void tidyHistory() {
    // TODO: decide if we're keeping a max number, a max age, or only most recent
    // TODO: decide if that policy applies only to completed or also in-progress jobs
    // TODO: decide if that policy applies per job type
  }

  @SuppressWarnings("unchecked")
  private <T extends AsyncOperation, R> AsyncOperationExecutor<T, R> getExecutor(
      T op) {
    return executors.get(op.getClass());
  }

  /**
   * looks up the future for an async operation by id
   */
  @SuppressWarnings("unchecked")
  public <V extends JsonSerializable> Future<V> getStatus(String opId) {
    return history.get(opId);
  }
}
