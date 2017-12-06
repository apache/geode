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
package org.apache.geode.internal.cache;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

/**
 * A disk region that is created when doing offline compaction.
 */
public class OfflineCompactionDiskRegion extends DiskRegion implements DiskRecoveryStore {
  private OfflineCompactionDiskRegion(DiskStoreImpl ds, DiskRegionView drv) {
    super(ds, drv.getName(), drv.isBucket(), true, false, true,
        new DiskRegionStats(ds.getCache().getDistributedSystem(), drv.getName()),
        new DummyCancelCriterion(), new DummyDiskExceptionHandler(), null, drv.getFlags(),
        drv.getPartitionName(), drv.getStartingBucketId(), drv.getCompressorClassName(),
        drv.getOffHeap());
    setConfig(drv.getLruAlgorithm(), drv.getLruAction(), drv.getLruLimit(),
        drv.getConcurrencyLevel(), drv.getInitialCapacity(), drv.getLoadFactor(),
        drv.getStatisticsEnabled(), drv.isBucket(), drv.getFlags(), drv.getPartitionName(),
        drv.getStartingBucketId(), drv.getCompressorClassName(), drv.getOffHeap());
  }

  static OfflineCompactionDiskRegion create(DiskStoreImpl dsi, DiskRegionView drv) {
    assert dsi != null;
    OfflineCompactionDiskRegion result = new OfflineCompactionDiskRegion(dsi, drv);
    result.register();
    return result;
  }

  ///////////// DiskRecoveryStore methods ////////////////
  public DiskRegionView getDiskRegionView() {
    return this;
  }

  public DiskEntry getDiskEntry(Object key) {
    return null;
  }

  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    throw new IllegalStateException(
        "updateRecoveredEntry should not be called when offline compacting");
  }

  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    throw new IllegalStateException(
        "updateRecoveredEntry should not be called when offline compacting");
  }

  public void destroyRecoveredEntry(Object key) {}

  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException(
        "foreachRegionEntry should not be called when offline compacting");
  }

  public boolean lruLimitExceeded() {
    return false;
  }

  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException(
        "copyRecoveredEntries should not be called on OfflineCompactionDiskRegion");
  }

  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    throw new IllegalStateException(
        "updateSizeOnFaultIn should not be called on OfflineCompactionDiskRegion");
  }

  @Override
  public int calculateValueSize(Object val) {
    return 0;
  }

  @Override
  public int calculateRegionEntryValueSize(RegionEntry re) {
    return 0;
  }

  public RegionMap getRegionMap() {
    throw new IllegalStateException(
        "getRegionMap should not be called on OfflineCompactionDiskRegion");
  }

  public void handleDiskAccessException(DiskAccessException dae) {
    throw new IllegalStateException(
        "handleDiskAccessException should not be called on OfflineCompactionDiskRegion");
  }

  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    throw new IllegalStateException(
        "initializeStats should not be called on OfflineCompactionDiskRegion");
  }



  public static class DummyDiskExceptionHandler implements DiskExceptionHandler {
    public void handleDiskAccessException(DiskAccessException dae) {
      // nothing needed
    }

    public boolean shouldStopServer() {
      return false;
    }
  }

  private static class DummyCancelCriterion extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      return new RuntimeException(e);
    }

  }
}
