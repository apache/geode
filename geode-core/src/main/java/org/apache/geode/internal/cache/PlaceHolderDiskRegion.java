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
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Used to represent a recovered disk region. Once the region actually exists this instance will be
 * thrown away and a real DiskRegion instance will replace it. This class needs to keep track of any
 * information that can be recovered from the DiskInitFile.
 *
 * @since GemFire prPersistSprint2
 */
public class PlaceHolderDiskRegion extends AbstractDiskRegion implements DiskRecoveryStore {

  private final String name;

  /**
   * This constructor is used when creating a region found during recovery of the DiskInitFile.
   */
  PlaceHolderDiskRegion(DiskStoreImpl ds, long id, String name) {
    super(ds, id);
    this.name = name;
    // do the remaining init in setConfig
  }

  /**
   * This constructor is used when we are closing an existing region. We want to remember that it is
   * still present in the disk store.
   */
  PlaceHolderDiskRegion(DiskRegionView drv) {
    super(drv);
    this.name = drv.getName();
  }

  LRUStatistics prlruStats;

  public LRUStatistics getPRLRUStats() {
    return this.prlruStats;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getPrName() {
    assert isBucket();
    String bn = PartitionedRegionHelper.getBucketName(this.name);
    return PartitionedRegionHelper.getPRPath(bn);
  }

  @Override
  void beginDestroyRegion(LocalRegion region) {
    // nothing needed
  }

  public void finishPendingDestroy() {
    // nothing needed
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("name=").append(this.name).append(" id=").append(getId())
    // .append(" ").append(super.toString())
    ;
    return sb.toString();
  }

  // DiskRecoveryStore methods
  public DiskRegionView getDiskRegionView() {
    return this;
  }

  public DiskEntry getDiskEntry(Object key) {
    RegionEntry re = getRecoveredEntryMap().getEntry(key);
    if (re != null && re.isRemoved() && !re.isTombstone()) {
      re = null;
    }
    return (DiskEntry) re;
  }

  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    RegionEntry re = getRecoveredEntryMap().initRecoveredEntry(key, value);
    if (re == null) {
      throw new InternalGemFireError(
          LocalizedStrings.LocalRegion_ENTRY_ALREADY_EXISTED_0.toLocalizedString(key));
    }
    return (DiskEntry) re;
  }

  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    return (DiskEntry) getRecoveredEntryMap().updateRecoveredEntry(key, value);
  }

  public void destroyRecoveredEntry(Object key) {
    throw new IllegalStateException("destroyRecoveredEntry should not be called during recovery");
  }

  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException("copyRecoveredEntries should not be called during recovery");
  }

  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException("foreachRegionEntry should not be called during recovery");
  }

  public boolean lruLimitExceeded() {
    return getRecoveredEntryMap().lruLimitExceeded(this);
  }

  public DiskStoreID getDiskStoreID() {
    return getDiskStore().getDiskStoreID();
  }

  public void acquireReadLock() {
    // not needed. The only thread
    // using this method is the async recovery thread.
    // If this placeholder is replaced with a real region,
    // this is done under a different lock

  }

  public void releaseReadLock() {
    // not needed
  }

  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    // only used by bucket regions
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
    return getRecoveredEntryMap();
  }

  @Override
  public void close() {
    RegionMap rm = getRecoveredEntryMap();
    if (rm != null) {
      rm.close();
    }
  }

  public void handleDiskAccessException(DiskAccessException dae) {
    getDiskStore().getCache().getLoggerI18n().error(
        LocalizedStrings.PlaceHolderDiskRegion_A_DISKACCESSEXCEPTION_HAS_OCCURRED_WHILE_RECOVERING_FROM_DISK,
        getName(), dae);

  }

  public boolean didClearCountChange() {
    return false;
  }

  public CancelCriterion getCancelCriterion() {
    return getDiskStore().getCancelCriterion();
  }

  public boolean isSync() {
    return true;
  }

  public void endRead(long start, long end, long bytesRead) {
    // do nothing
  }

  public boolean isRegionClosed() {
    return false;
  }

  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    this.numEntriesInVM.set(numEntriesInVM);
    this.numOverflowOnDisk.set(numOverflowOnDisk);
    this.numOverflowBytesOnDisk.set(numOverflowBytesOnDisk);
  }
}
