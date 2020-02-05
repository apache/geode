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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.RegionFactoryImpl;
import org.apache.geode.management.api.ClusterManagementOperation;
import org.apache.geode.management.runtime.OperationResult;

public class RegionOperationHistoryPersistenceService
    implements OperationHistoryPersistenceService {
  private final Supplier<String> uniqueIdSupplier;
  private final Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region;

  private static final String OPERATION_HISTORY_DISK_STORE_NAME = "OperationHistory";
  public static final String OPERATION_HISTORY_REGION_NAME = "OperationHistoryRegion";
  public static final String OPERATION_HISTORY_DIR_NAME = "operationHistory";

  @VisibleForTesting
  RegionOperationHistoryPersistenceService(
      Supplier<String> uniqueIdSupplier,
      Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region) {
    this.uniqueIdSupplier = uniqueIdSupplier;
    this.region = region;
  }

  @VisibleForTesting
  public RegionOperationHistoryPersistenceService(InternalCache cache, Path workingDir) {
    this(() -> UUID.randomUUID().toString(), getRegion(cache, workingDir));
  }

  public RegionOperationHistoryPersistenceService(InternalCache cache) {
    this(cache, Paths.get(System.getProperty("user.dir")));
  }

  private static Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> getRegion(
      InternalCache cache, Path workingDir) {
    Region<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> region =
        cache.getRegion(OPERATION_HISTORY_REGION_NAME);

    if (region != null) {
      return region;
    }

    try {
      File diskDir = workingDir.resolve(OPERATION_HISTORY_DIR_NAME).toFile();

      if (!diskDir.exists() && !diskDir.mkdirs()) {
        throw new IOException("Cannot create directory at " + diskDir);
      }

      File[] diskDirs = {diskDir};
      cache.createDiskStoreFactory().setDiskDirs(diskDirs).setAutoCompact(true)
          .setMaxOplogSize(10).create(OPERATION_HISTORY_DISK_STORE_NAME);

      RegionFactory<String, OperationState<ClusterManagementOperation<OperationResult>, OperationResult>> regionFactory =
          cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
      regionFactory.setDiskStoreName(OPERATION_HISTORY_DISK_STORE_NAME);

      InternalRegionArguments internalArgs = new InternalRegionArguments();
      internalArgs.setIsUsedForMetaRegion(true);
      internalArgs.setMetaRegionWithTransactions(false);
      ((RegionFactoryImpl)regionFactory).setInternalRegionArguments(internalArgs);

      return regionFactory.create(OPERATION_HISTORY_REGION_NAME);
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
