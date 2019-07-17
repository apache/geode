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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.util.Strings;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.operation.RebalanceOperation;

@Experimental
public class OperationManager implements AutoCloseable {
  private final Map<Class<? extends ClusterManagementOperation>, OperationPerformer> performers;
  private final OperationHistoryManager historyManager;
  private final Executor executor;

  public OperationManager(OperationHistoryManager historyManager) {
    this.historyManager = historyManager;
    this.executor = LoggingExecutors.newThreadOnEachExecute("CMSOpPerformer");

    // initialize the list of operation performers
    performers = new HashMap<>();
    performers.put(RebalanceOperation.class, new RebalanceOperationPerformer());
  }

  public <A extends ClusterManagementOperation<V>, V extends JsonSerializable> CompletableFuture<V> submit(
      A op) {
    if (!Strings.isBlank(op.getId())) {
      throw new IllegalArgumentException(
          String.format("Operation type %s should not supply its own id",
              op.getClass().getSimpleName()));
    }
    op.setId(UUID.randomUUID().toString());
    OperationPerformer<A, V> performer = getPerformer(op);
    if (performer == null) {
      throw new IllegalArgumentException(String.format("Operation type %s is not supported",
          op.getClass().getSimpleName()));
    }

    CompletableFuture<V> future =
        CompletableFuture.supplyAsync(() -> performer.perform(op), executor);

    // save the Future so we can check on it later
    historyManager.save(op, future);

    return future;
  }

  @SuppressWarnings("unchecked")
  private <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationPerformer<A, V> getPerformer(
      A op) {
    return performers.get(op.getClass());
  }

  /**
   * looks up the future for an async operation by id
   */
  @SuppressWarnings("unchecked")
  public <V extends JsonSerializable> CompletableFuture<V> getStatus(String opId) {
    return historyManager.getStatus(opId);
  }

  @Override
  public void close() {
    if (executor instanceof ExecutorService) {
      ((ExecutorService) executor).shutdownNow();
    }
  }
}
