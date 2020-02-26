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

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Retains references to all running and some recently-completed operations.
 *
 * Completed operations can expire after 7 days.
 * Expiration is only checked for when get, list, or recordStart are called.
 */
@Experimental
public class OperationHistoryManager {
  private final long keepCompletedMillis;
  private final OperationStateStore operationStateStore;

  /**
   * set a default retention policy to keep results for 2 hours after completion
   */
  public OperationHistoryManager(
      OperationStateStore operationStateStore) {
    this(Duration.ofDays(7), operationStateStore);
  }

  /**
   * set a custom retention policy to keep results for X amount of time after completion
   */
  public OperationHistoryManager(Duration keepCompleted,
      OperationStateStore operationStateStore) {
    keepCompletedMillis = keepCompleted.toMillis();
    this.operationStateStore = operationStateStore;
  }

  /**
   * look up the specified key
   */
  @SuppressWarnings("unchecked")
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> get(
      String opId) {
    expireHistory();

    return (OperationState<A, V>) operationStateStore.get(opId);
  }

  @VisibleForTesting
  void expireHistory() {
    final long expirationTime = now() - keepCompletedMillis;
    Set<String> expiredKeys = operationStateStore.list()
        .stream()
        .filter(operationInstance -> isExpired(expirationTime, operationInstance))
        .map(OperationState::getId)
        .collect(Collectors.toSet());

    expiredKeys.forEach(operationStateStore::remove);
  }

  private long now() {
    return System.currentTimeMillis();
  }

  private static boolean isExpired(long expirationTime, OperationState<?, ?> operationInstance) {
    Date operationEnd = operationInstance.getOperationEnd();

    if (operationEnd == null) {
      return false; // always keep while still in-progress
    }

    return operationEnd.getTime() <= expirationTime;
  }

  /**
   * Stores a new operation in the history and returns its unique identifier.
   */
  public String recordStart(ClusterManagementOperation<?> op) {
    expireHistory();

    return operationStateStore.recordStart(op);
  }

  /**
   * Records that a previously started operation has ended with the given result or cause.
   */
  public void recordEnd(String opId, OperationResult result, Throwable cause) {
    operationStateStore.recordEnd(opId, result, cause);
  }

  public <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationState<A, V>> list(
      A opType) {
    expireHistory();

    return operationStateStore.list()
        .stream()
        .filter(instance -> opType.getClass().isAssignableFrom(instance.getOperation().getClass()))
        .map(fi -> (OperationState<A, V>) fi)
        .collect(Collectors.toList());
  }

}
