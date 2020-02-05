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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class RegionOperationHistoryPersistenceService
    implements OperationHistoryPersistenceService {
  private final Supplier<String> uniqueIdSupplier;
  private final Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region;

  private static final String OPERATION_HISTORY_DISK_STORE_NAME = "OperationHistory";
  public static final String OPERATION_HISTORY_REGION_NAME = "OperationHistoryRegion";

  @VisibleForTesting
  RegionOperationHistoryPersistenceService(
      Supplier<String> uniqueIdSupplier,
      Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region) {
    this.uniqueIdSupplier = uniqueIdSupplier;
    this.region = region;
  }

  public RegionOperationHistoryPersistenceService(InternalCache cache) {
    this(() -> UUID.randomUUID().toString(), getRegion(cache));
  }

  private static Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> getRegion(
      InternalCache cache) {
    Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region =
        cache.getRegion(OPERATION_HISTORY_REGION_NAME);

    if (region != null) {
      return region;
    }

    try {
      File diskDir = Paths.get("operation_history").toFile(); // TODO: something better

      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory at " + "operation_history"); // TODO:
                                                                                    // something
                                                                                    // better
      }

      File[] diskDirs = {diskDir};
      cache.createDiskStoreFactory().setDiskDirs(diskDirs).setAutoCompact(true)
          .setMaxOplogSize(10).create(OPERATION_HISTORY_DISK_STORE_NAME);

      AttributesFactory<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> regionAttrsFactory =
          new AttributesFactory<>(); // TODO non-deprecated approach
      regionAttrsFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      regionAttrsFactory.setDiskStoreName(OPERATION_HISTORY_DISK_STORE_NAME);
      regionAttrsFactory.setScope(Scope.DISTRIBUTED_ACK);
      InternalRegionArguments internalArgs = new InternalRegionArguments();
      internalArgs.setIsUsedForMetaRegion(true);
      internalArgs.setMetaRegionWithTransactions(false);

      return cache.createVMRegion(OPERATION_HISTORY_REGION_NAME, regionAttrsFactory.create(),
          internalArgs);
    } catch (RuntimeException e) {
      // throw RuntimeException as is
      throw e;
    } catch (Exception e) {
      // turn all other exceptions into runtime exceptions
      throw new RuntimeException("Error occurred while initializing operation history region", e);
    }
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
    return null;
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
