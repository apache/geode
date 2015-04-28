/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.internal.cache.persistence.DiskExceptionHandler;
import com.gemstone.gemfire.internal.cache.persistence.DiskRecoveryStore;
import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView;

/**
 * A disk region that is created when doing offline compaction.
 */
public class OfflineCompactionDiskRegion extends DiskRegion implements DiskRecoveryStore {
  private OfflineCompactionDiskRegion(DiskStoreImpl ds,
                               DiskRegionView drv) {
    super(ds, drv.getName(), drv.isBucket(), true, false, true,
          new DiskRegionStats(ds.getCache().getDistributedSystem(), drv.getName()),
          new DummyCancelCriterion(),
          new DummyDiskExceptionHandler(),
          null, drv.getFlags(), drv.getPartitionName(), drv.getStartingBucketId(),
          drv.getCompressorClassName());
    setConfig(drv.getLruAlgorithm(), drv.getLruAction(), drv.getLruLimit(),
              drv.getConcurrencyLevel(), drv.getInitialCapacity(),
              drv.getLoadFactor(), drv.getStatisticsEnabled(),
              drv.isBucket(), drv.getFlags(), drv.getPartitionName(), drv.getStartingBucketId(),
              drv.getCompressorClassName());
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
    throw new IllegalStateException("updateRecoveredEntry should not be called when offline compacting");
  }

  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    throw new IllegalStateException("updateRecoveredEntry should not be called when offline compacting");
  }
  public void destroyRecoveredEntry(Object key) {
  }
  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException("foreachRegionEntry should not be called when offline compacting");
  }
  public boolean lruLimitExceeded() {
    return false;
  }
  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException("copyRecoveredEntries should not be called on OfflineCompactionDiskRegion");
  }
  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    throw new IllegalStateException("updateSizeOnFaultIn should not be called on OfflineCompactionDiskRegion");
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
    throw new IllegalStateException("getRegionMap should not be called on OfflineCompactionDiskRegion");
  }
  public void handleDiskAccessException(DiskAccessException dae) {
    throw new IllegalStateException("handleDiskAccessException should not be called on OfflineCompactionDiskRegion");
  }
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    throw new IllegalStateException("initializeStats should not be called on OfflineCompactionDiskRegion");
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
