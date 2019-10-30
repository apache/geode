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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.internal.operation.OperationHistoryManager.OperationInstance;
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

  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> submit(
      A op) {
    String opId = UUID.randomUUID().toString();

    BiFunction<Cache, A, V> performer = getPerformer(op);
    if (performer == null) {
      throw new IllegalArgumentException(String.format("%s is not supported.",
          op.getClass().getSimpleName()));
    }

    CompletableFuture<V> future =
        CompletableFuture.supplyAsync(() -> performer.apply(cache, op), executor);

    OperationInstance<A, V> inst = new OperationInstance<>(future, opId, op, new Date());

    // save the Future so we can check on it later
    return historyManager.save(inst);
  }

  @SuppressWarnings("unchecked")
  private <C extends Cache, A extends ClusterManagementOperation<V>, V extends OperationResult> BiFunction<C, A, V> getPerformer(
      A op) {
    Class<? extends ClusterManagementOperation> aClass = op.getClass();

    if (op instanceof TaggedWithOperator
        && ClusterManagementOperation.class.isAssignableFrom(aClass.getSuperclass())) {
      aClass = (Class<? extends ClusterManagementOperation>) aClass.getSuperclass();
    }

    return performers.get(aClass);
  }

  /**
   * looks up the future for an async operation by id
   */
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> getOperationInstance(
      String opId) {
    return historyManager.getOperationInstance(opId);
  }

  public <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationInstance<A, V>> listOperationInstances(
      A opType) {
    return historyManager.listOperationInstances(opType);
  }

  @Override
  public void close() {
    if (executor instanceof ExecutorService) {
      ((ExecutorService) executor).shutdownNow();
    }
  }
}
