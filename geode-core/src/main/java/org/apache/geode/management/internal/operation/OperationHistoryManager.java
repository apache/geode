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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;

/**
 * Retains references to all running and some recently-completed operations.
 *
 * The policy for expiring completed operations is subject to change, but may be based on age,
 * count, when it was last accessed, or some combination thereof.
 */
@Experimental
public class OperationHistoryManager {
  private final ConcurrentMap<String, OperationInstance> history;
  private final long keepCompletedMillis;

  /**
   * set a default retention policy to keep results for 2 hours after completion
   */
  public OperationHistoryManager() {
    this(2, TimeUnit.HOURS);
  }

  /**
   * set a custom retention policy to keep results for X amount of time after completion
   */
  public OperationHistoryManager(long keepCompleted, TimeUnit timeUnit) {
    history = new ConcurrentHashMap<>();
    keepCompletedMillis = timeUnit.toMillis(keepCompleted);
  }

  /**
   * look up the specified key
   */
  @SuppressWarnings("unchecked")
  <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> getOperationInstance(
      String opId) {
    expireHistory();
    return history.get(opId);
  }

  private void expireHistory() {
    final long purgeThreshold = System.currentTimeMillis() - keepCompletedMillis;
    Set<String> expiredKeys = history.entrySet().stream()
        .filter(e -> e.getValue().getFutureOperationEnded().isDone()
            && getOperationEnded(e.getValue()).getTime() < purgeThreshold)
        .map(e -> e.getKey()).collect(Collectors.toSet());
    expiredKeys.forEach(history::remove);
  }

  /**
   * @return null if unavailable
   */
  private static Date getOperationEnded(OperationInstance<?, ?> operationInstance) {
    try {
      return operationInstance.futureOperationEnded.get();
    } catch (ExecutionException ignore) {
      return null;
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Stores a new operation in the history and installs a trigger to move the event from the
   * in-progress map to the completed map upon completion.
   */
  public <A extends ClusterManagementOperation<V>, V extends JsonSerializable> OperationInstance<A, V> save(
      OperationInstance<A, V> operationInstance) {
    String opId = operationInstance.getId();
    CompletableFuture<V> future = operationInstance.getFutureResult();

    CompletableFuture<V> newFuture = future.whenComplete((result, exception) -> {
      operationInstance.setFutureOperationEnded(new Date());
    });

    OperationInstance<A, V> newOperationInstance = new OperationInstance<>(newFuture, opId,
        operationInstance.getOperation(), operationInstance.getOperationStart());

    history.put(opId, operationInstance);
    expireHistory();

    return newOperationInstance;
  }

  /**
   * struct for holding information pertinent to a specific instance of an operation
   *
   * all fields are immutable, however note that {@link #setFutureOperationEnded(Date)} completes
   * {@link #getFutureOperationEnded()}
   */
  public static class OperationInstance<A extends ClusterManagementOperation<V>, V extends JsonSerializable>
      implements Identifiable<String> {
    private final CompletableFuture<V> future;
    private final String opId;
    private final A operation;
    private final Date operationStart;
    private final CompletableFuture<Date> futureOperationEnded;

    public OperationInstance(CompletableFuture<V> future, String opId, A operation,
        Date operationStart) {
      this.future = future;
      this.opId = opId;
      this.operation = operation;
      this.operationStart = operationStart;
      this.futureOperationEnded = new CompletableFuture<>();
    }

    @Override
    public String getId() {
      return opId;
    }

    public CompletableFuture<V> getFutureResult() {
      return future;
    }

    public A getOperation() {
      return operation;
    }

    public Date getOperationStart() {
      return operationStart;
    }

    public CompletableFuture<Date> getFutureOperationEnded() {
      return futureOperationEnded;
    }

    public void setFutureOperationEnded(Date futureOperationEnded) {
      this.futureOperationEnded.complete(futureOperationEnded);
    }
  }
}
