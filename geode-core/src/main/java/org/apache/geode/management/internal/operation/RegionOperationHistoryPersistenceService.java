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
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class RegionOperationHistoryPersistenceService
    implements OperationHistoryPersistenceService {
  private final Supplier<String> uniqueIdSupplier;
  private final Region<String, OperationState> region;

  @VisibleForTesting
  RegionOperationHistoryPersistenceService(
      Supplier<String> uniqueIdSupplier,
      Region<String, OperationState> region) {
    this.uniqueIdSupplier = uniqueIdSupplier;
    this.region = region;
  }

  public RegionOperationHistoryPersistenceService() {
    this(() -> UUID.randomUUID().toString(), null);
  }

  @Override
  public <A extends ClusterManagementOperation<?>> String recordStart(A operation) {
    String opId = uniqueIdSupplier.get();

    OperationState operationInstance = new OperationState(opId, operation, new Date());
    region.put(opId, operationInstance);

    return opId;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> getOperationInstance(
      String id) {
    return null;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationState<A, V>> listOperationInstances() {
    return null;
  }

  @Override
  public <V extends OperationResult> void recordEnd(String opId, V result, Throwable exception) {
    OperationState operationState = region.get(opId);
    operationState.setOperationEnd(new Date(), result, exception);
    region.put(opId, operationState);
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> void update(
      OperationState<A, V> operationInstance) {
  }

  @Override
  public void remove(String id) {

  }
}
