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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections.map.LRUMap;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.api.JsonSerializable;

@Experimental
public class OperationHistoryManager {
  private final Map<String, CompletableFuture> history;

  public OperationHistoryManager() {
    this(10);
  }

  public OperationHistoryManager(int historySize) {
    history = new LRUMap(historySize);
  }

  /**
   * looks up the future for an async operation by id
   */
  @SuppressWarnings("unchecked")
  public <V extends JsonSerializable> CompletableFuture<V> getStatus(String opId) {
    return history.get(opId);
  }

  public <A extends ClusterManagementOperation<V>, V extends JsonSerializable> void save(
      A operation,
      CompletableFuture<V> future) {
    history.put(operation.getId(), future);
  }
}
