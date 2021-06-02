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
import org.apache.geode.management.runtime.OperationResult;

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
  <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> getOperationInstance(
      String opId) {
    expireHistory();
    return (OperationInstance<A, V>) history.get(opId);
  }

  private void expireHistory() {
    final long expirationDate = now() - keepCompletedMillis;
    Set<String> expiredKeys =
        history.entrySet().stream().filter(e -> isExpired(expirationDate, e.getValue()))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    expiredKeys.forEach(history::remove);
  }

  long now() {
    return System.currentTimeMillis();
  }

  private static boolean isExpired(long expirationDate, OperationInstance<?, ?> operationInstance) {
    CompletableFuture<Date> futureOperationEnded = operationInstance.getFutureOperationEnded();

    if (!futureOperationEnded.isDone()) {
      return false; // always keep while still in-progress
    }

    final long endTime;
    try {
      endTime = futureOperationEnded.get().getTime();
    } catch (ExecutionException ignore) {
      // cannot ever happen because we've already checked isDone above
      return false;
    } catch (InterruptedException ignore) {
      // cannot ever happen because we've already checked isDone above
      Thread.currentThread().interrupt();
      return false;
    }

    return endTime <= expirationDate;
  }

  /**
   * Stores a new operation in the history and installs a trigger to record the operation end time.
   */
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> save(
      OperationInstance<A, V> operationInstance) {
    String opId = operationInstance.getId();
    CompletableFuture<V> future = operationInstance.getFutureResult();

    future.whenComplete((result, exception) -> operationInstance.setOperationEnded(new Date()));

    history.put(opId, operationInstance);
    expireHistory();

    return operationInstance;
  }

  @SuppressWarnings("unchecked")
  <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationInstance<A, V>> listOperationInstances(
      A opType) {
    expireHistory();
    return history.values().stream().filter(oi -> opType.getClass().isInstance(oi.getOperation()))
        .map(oi -> (OperationInstance<A, V>) oi).collect(Collectors.toList());
  }

  /**
   * struct for holding information pertinent to a specific instance of an operation
   *
   * all fields are immutable, however note that {@link #setOperationEnded(Date)} completes
   * {@link #getFutureOperationEnded()}
   */
  public static class OperationInstance<A extends ClusterManagementOperation<V>, V extends OperationResult>
      implements Identifiable<String> {
    private final CompletableFuture<V> future;
    private final String opId;
    private final A operation;
    private final Date operationStart;
    private final CompletableFuture<Date> futureOperationEnded;
    private String operator;

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

    public void setOperationEnded(Date operationEnded) {
      this.futureOperationEnded.complete(operationEnded);
    }

    public String getOperator() {
      return operator;
    }

    public void setOperator(String operator) {
      this.operator = operator;
    }
  }
}
