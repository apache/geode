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
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreID;

/**
 * Used to represent a recovered disk region. Once the region actually exists this instance will be
 * thrown away and a real DiskRegion instance will replace it. This class needs to keep track of any
 * information that can be recovered from the DiskInitFile.
 *
 * @since GemFire prPersistSprint2
 */
public class PlaceHolderDiskRegion extends AbstractDiskRegion
    implements DiskRecoveryStore, RegionMapOwner {

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

  @Override
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
  @Override
  public DiskRegionView getDiskRegionView() {
    return this;
  }

  @Override
  public DiskEntry getDiskEntry(Object key) {
    RegionEntry re = getRecoveredEntryMap().getEntry(key);
    if (re != null && re.isRemoved() && !re.isTombstone()) {
      re = null;
    }
    return (DiskEntry) re;
  }

  @Override
  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    RegionEntry re = getRecoveredEntryMap().initRecoveredEntry(key, value);
    if (re == null) {
      throw new InternalGemFireError(
          String.format("Entry already existed: %s", key));
    }
    return (DiskEntry) re;
  }

  @Override
  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry value) {
    return (DiskEntry) getRecoveredEntryMap().updateRecoveredEntry(key, value);
  }

  @Override
  public void destroyRecoveredEntry(Object key) {
    throw new IllegalStateException("destroyRecoveredEntry should not be called during recovery");
  }

  @Override
  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException("copyRecoveredEntries should not be called during recovery");
  }

  @Override
  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException("foreachRegionEntry should not be called during recovery");
  }

  @Override
  public boolean lruLimitExceeded() {
    return getRecoveredEntryMap().lruLimitExceeded(this);
  }

  @Override
  public DiskStoreID getDiskStoreID() {
    return getDiskStore().getDiskStoreID();
  }

  @Override
  public void acquireReadLock() {
    // not needed. The only thread
    // using this method is the async recovery thread.
    // If this placeholder is replaced with a real region,
    // this is done under a different lock

  }

  @Override
  public void releaseReadLock() {
    // not needed
  }

  @Override
  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    // only used by bucket regions
  }

  @Override
  public int calculateValueSize(Object val) {
    return 0;
  }

  @Override
  public RegionMap getRegionMap() {
    return getRecoveredEntryMap();
  }

  @Override
  public void close() {
    RegionMap rm = getRecoveredEntryMap();
    if (rm != null) {
      rm.close(null);
    }
  }

  @Override
  public void handleDiskAccessException(DiskAccessException dae) {
    getDiskStore().getCache().getLogger().error(
        String.format(
            "A DiskAccessException has occurred while recovering values asynchronously from disk for region %s.",
            getName()),
        dae);

  }

  @Override
  public boolean didClearCountChange() {
    return false;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return getDiskStore().getCancelCriterion();
  }

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public void endRead(long start, long end, long bytesRead) {
    // do nothing
  }

  @Override
  public boolean isRegionClosed() {
    return false;
  }

  @Override
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    this.numEntriesInVM.set(numEntriesInVM);
    this.numOverflowOnDisk.set(numOverflowOnDisk);
    this.numOverflowBytesOnDisk.set(numOverflowBytesOnDisk);
  }

  @Override
  public EvictionController getExistingController(InternalRegionArguments internalArgs) {
    if (isBucket()) {
      return this.getDiskStore().getOrCreatePRLRUStats(this);
    }
    return null;
  }
}
