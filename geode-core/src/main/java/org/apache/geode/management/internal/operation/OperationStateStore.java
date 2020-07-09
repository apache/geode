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

import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

/**
 * Stores operation state.
 * The typical life cycle of an operation on this store is:
 * 1. recordStart: adds the state to this store and sets its start time
 * 2. recordEnd: updates the state in this store and sets its end time
 * 3. get/list: to see one or more states in this store
 * 4. remove: removes a state from this store
 */
public interface OperationStateStore {
  /**
   * Records the state of the given in-progress operation.
   *
   * @return an identifier for the state of the operation
   */
  <A extends ClusterManagementOperation<?>> String recordStart(A operation, String locator);

  /**
   * Returns a single instance of an {@link OperationState} for a given
   * id.
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> get(
      String id);

  /**
   * Returns a list of all persisted {@link OperationState}.
   */
  <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationState<A, V>> list();

  /**
   * Records that the given operation has completed with the given result or exception.
   */
  <V extends OperationResult> void recordEnd(String opId, V result, Throwable exception);

  /**
   * Removes an existing {@link OperationState}.
   * If the instance is not found this is a no-op.
   */
  void remove(String opId);
}
