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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
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
  private final long keepCompletedMillis;
  private final OperationHistoryPersistenceService historyPersistenceService;

  /**
   * set a default retention policy to keep results for 2 hours after completion
   */
  public OperationHistoryManager() {
    this(2, TimeUnit.HOURS, new InternalOperationHistoryPersistenceService());
  }

  /**
   * set a custom retention policy to keep results for X amount of time after completion
   */
  public OperationHistoryManager(long keepCompleted, TimeUnit timeUnit,
      OperationHistoryPersistenceService historyPersistenceService) {
    keepCompletedMillis = timeUnit.toMillis(keepCompleted);
    this.historyPersistenceService = historyPersistenceService;
  }

  /**
   * look up the specified key
   */
  @SuppressWarnings("unchecked")
  <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> getOperationInstance(
      String opId) {
    expireHistory();

    return (OperationInstance<A, V>) historyPersistenceService.getOperationInstance(opId);
  }

  private void expireHistory() {
    final long expirationDate = now() - keepCompletedMillis;
    Set<String> expiredKeys = historyPersistenceService.listOperationInstances()
        .stream()
        .filter(operationInstance -> isExpired(expirationDate, operationInstance))
        .map(OperationInstance::getId)
        .collect(Collectors.toSet());

    expiredKeys.forEach(historyPersistenceService::remove);
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private static boolean isExpired(long expirationDate, OperationInstance<?, ?> operationInstance) {
    Date operationEnd = operationInstance.getOperationEnd();

    if (operationEnd == null) {
      return false; // always keep while still in-progress
    }

    return operationEnd.getTime() <= expirationDate;
  }

  /**
   * Stores a new operation in the history and installs a trigger to record the operation end time.
   */
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationInstance<A, V> save(
      A op, BiFunction<Cache, A, V> performer, Cache cache, Executor executor) {
    String opId = UUID.randomUUID().toString();
    OperationInstance<A, V> operationInstance = new OperationInstance<>(opId, op, new Date());
    historyPersistenceService.create(operationInstance);

    CompletableFuture.supplyAsync(() -> performer.apply(cache, op), executor)
        .whenComplete((result, exception) -> {
          OperationInstance<A, V> opInstance =
              historyPersistenceService.getOperationInstance(opId);
          if (opInstance != null) {
            Throwable cause = exception == null ? null : exception.getCause();
            opInstance.setOperationEnd(new Date(), result, cause);
            historyPersistenceService.update(opInstance);
          }
        });


    expireHistory();

    return operationInstance;
  }

  <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationInstance<A, V>> listOperationInstances(
      A opType) {
    expireHistory();

    return historyPersistenceService.listOperationInstances()
        .stream()
        .filter(instance -> instance.operation.getClass().equals(opType.getClass()))
        .map(fi -> (OperationInstance<A, V>) fi)
        .collect(Collectors.toList());
  }

  /**
   * struct for holding information pertinent to a specific instance of an operation*
   */
  public static class OperationInstance<A extends ClusterManagementOperation<V>, V extends OperationResult>
      implements Identifiable<String> {
    private final String opId;
    private final A operation;
    private final String operator;
    private final Date operationStart;
    private Date operationEnd;
    private V result;
    private Throwable throwable;

    public OperationInstance(String opId, A operation,
        Date operationStart) {
      this.opId = opId;
      this.operation = operation;
      this.operationStart = operationStart;
      if (operation instanceof TaggedWithOperator) {
        this.operator = ((TaggedWithOperator) operation).getOperator();
      } else {
        this.operator = null;
      }
    }

    @Override
    public String getId() {
      return opId;
    }

    public A getOperation() {
      return operation;
    }

    public Date getOperationStart() {
      return operationStart;
    }

    public String getOperator() {
      return operator;
    }

    public void setOperationEnd(Date operationEnd, V result, Throwable exception) {
      this.result = result;
      this.throwable = exception;
      this.operationEnd = operationEnd;
    }

    public Date getOperationEnd() {
      return this.operationEnd;
    }

    public V getResult() {
      return this.result;
    }

    public Throwable getThrowable() {
      return this.throwable;
    }
  }
}
