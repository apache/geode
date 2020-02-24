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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

/**
 * This store uses a geode region to implement the storage of operation state.
 * It is a REPLICATE region so that the storage will be shared on each member
 * that hosts this region. Currently each locator hosts this region.
 */
public class RegionOperationStateStore
    implements OperationStateStore {
  private final Supplier<String> uniqueIdSupplier;
  private final Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region;

  public static final String OPERATION_STATE_REGION_NAME = "__OperationStateRegion";

  @VisibleForTesting
  RegionOperationStateStore(
      Supplier<String> uniqueIdSupplier,
      Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region) {
    this.uniqueIdSupplier = uniqueIdSupplier;
    this.region = region;
  }

  public RegionOperationStateStore(InternalCache cache) {
    this(() -> UUID.randomUUID().toString(), getRegion(cache));
  }

  private static Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> getRegion(
      InternalCache cache) {
    Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region =
        cache.getRegion(OPERATION_STATE_REGION_NAME);

    if (region != null) {
      return region;
    }

    InternalRegionFactory<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> regionFactory =
        cache.createInternalRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.setIsUsedForMetaRegion(true);
    regionFactory.setMetaRegionWithTransactions(false);
    return regionFactory.create(OPERATION_STATE_REGION_NAME);
  }

  @Override
  public <A extends ClusterManagementOperation<?>> String recordStart(A operation) {
    String opId = uniqueIdSupplier.get();

    OperationState operationInstance = new OperationState(opId, operation, new Date());
    region.put(opId, operationInstance);

    return opId;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> OperationState<A, V> get(
      String opId) {
    return (OperationState<A, V>) region.get(opId);
  }

  @VisibleForTesting
  Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> getRegion() {
    return region;
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> List<OperationState<A, V>> list() {
    return new ArrayList(region.values());
  }

  @Override
  public <V extends OperationResult> void recordEnd(String opId, V result, Throwable exception) {
    OperationState<ClusterManagementOperation<OperationResult>, OperationResult> operationState =
        region.get(opId);
    operationState.setOperationEnd(new Date(), result, exception);
    region.put(opId, operationState);
  }

  @Override
  public void remove(String opId) {
    region.remove(opId);
  }
}
