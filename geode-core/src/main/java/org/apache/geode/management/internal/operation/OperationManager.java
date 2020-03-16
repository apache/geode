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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.OperationResult;

@Experimental
public class OperationManager implements AutoCloseable {
  private final Map<Class<? extends ClusterManagementOperation>, BiFunction> performers;
  private final OperationHistoryManager historyManager;
  private final Executor executor;
  private final InternalCache cache;

  public OperationManager(InternalCache cache, OperationHistoryManager historyManager) {
    this.cache = cache;
    this.historyManager = historyManager;
    this.executor = LoggingExecutors.newThreadOnEachExecute("CMSOpPerformer");

    // initialize the list of operation performers
    performers = new ConcurrentHashMap<>();
    registerOperation(RebalanceOperation.class, RebalanceOperationPerformer::perform);
  }

  /**
   * for use by modules/extensions to install custom cluster management operations
   */
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> void registerOperation(
      Class<A> operationClass, BiFunction<Cache, A, V> operationPerformer) {
    performers.put(operationClass, operationPerformer);
  }

  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> submit(
      A op) {
    BiFunction<Cache, A, V> performer = getPerformer(op);
    if (performer == null) {
      throw new IllegalArgumentException(String.format("%s is not supported.",
          op.getClass().getSimpleName()));
    }

    String opId = historyManager.recordStart(op);
    // get the operationState BEFORE we start the async thread
    // so that start will return a result that is not influenced
    // by how far the async thread gets in its execution.
    OperationState<A, V> operationState = historyManager.get(opId);

    CompletableFuture.supplyAsync(() -> performer.apply(cache, op), executor)
        .whenComplete((result, exception) -> {
          Throwable cause = exception == null ? null : exception.getCause();
          historyManager.recordEnd(opId, result, cause);
        });

    return operationState;
  }

  @SuppressWarnings("unchecked")
  private <C extends Cache, A extends ClusterManagementOperation<V>, V extends OperationResult> BiFunction<C, A, V> getPerformer(
      A op) {
    return performers.get(op.getClass());
  }

  /**
   * looks up the future for an async operation by id
   */
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> get(
      String opId) {
    return historyManager.get(opId);
  }

  public <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationState<A, V>> list(
      A opType) {
    return historyManager.list(opType);
  }

  @Override
  public void close() {}
}
