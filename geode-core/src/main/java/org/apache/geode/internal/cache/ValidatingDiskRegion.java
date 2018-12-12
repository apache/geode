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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DistributedRegion.DiskPosition;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * A disk region that is created when doing offline validation.
 *
 * @since GemFire prPersistSprint3
 */
public class ValidatingDiskRegion extends DiskRegion implements DiskRecoveryStore {
  protected ValidatingDiskRegion(DiskStoreImpl ds, DiskRegionView drv) {
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

  static ValidatingDiskRegion create(DiskStoreImpl dsi, DiskRegionView drv) {
    assert dsi != null;
    ValidatingDiskRegion result = new ValidatingDiskRegion(dsi, drv);
    result.register();
    return result;
  }

  private final ConcurrentMap<Object, DiskEntry> map = new ConcurrentHashMap<Object, DiskEntry>();

  ///////////// DiskRecoveryStore methods ////////////////
  @Override
  public DiskRegionView getDiskRegionView() {
    return this;
  }

  @Override
  public DiskEntry getDiskEntry(Object key) {
    return this.map.get(key);
  }

  @Override
  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    ValidatingDiskEntry de = new ValidatingDiskEntry(key, re);
    if (this.map.putIfAbsent(key, de) != null) {
      throw new InternalGemFireError(
          String.format("Entry already existed: %s", key));
    }
    return de;
  }

  @Override
  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    ValidatingDiskEntry de = new ValidatingDiskEntry(key, re);
    this.map.put(key, de);
    return de;
  }

  @Override
  public void destroyRecoveredEntry(Object key) {
    this.map.remove(key);
  }

  @Override
  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException(
        "foreachRegionEntry should not be called when validating disk store");
  }

  @Override
  public boolean lruLimitExceeded() {
    return false;
  }

  @Override
  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException(
        "copyRecoveredEntries should not be called on ValidatingDiskRegion");
  }

  @Override
  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    throw new IllegalStateException(
        "updateSizeOnFaultIn should not be called on ValidatingDiskRegion");
  }

  @Override
  public int calculateValueSize(Object val) {
    return 0;
  }

  @Override
  public RegionMap getRegionMap() {
    throw new IllegalStateException("getRegionMap should not be called on ValidatingDiskRegion");
  }

  @Override
  public void handleDiskAccessException(DiskAccessException dae) {
    throw new IllegalStateException(
        "handleDiskAccessException should not be called on ValidatingDiskRegion");
  }

  @Override
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    throw new IllegalStateException("initializeStats should not be called on ValidatingDiskRegion");
  }

  public int size() {
    return this.map.size();
  }

  static class ValidatingDiskEntry implements DiskEntry, RegionEntry {
    private final Object key;
    private final DiskId diskId;

    public ValidatingDiskEntry(Object key, DiskEntry.RecoveredEntry re) {
      this.key = key;
      this.diskId = DiskId.createDiskId(1, true, false);
      this.diskId.setKeyId(re.getRecoveredKeyId());
      this.diskId.setOffsetInOplog(re.getOffsetInOplog());
      this.diskId.setOplogId(re.getOplogId());
      this.diskId.setUserBits(re.getUserBits());
      this.diskId.setValueLength(re.getValueLength());
    }

    @Override
    public Object getKey() {
      return this.key;
    }

    @Override
    public Object getValue() {
      return null;
    }

    @Override
    public Token getValueAsToken() {
      return null;
    }

    @Override
    public Object getValueRetain(RegionEntryContext context, boolean decompress) {
      return null;
    }

    @Override
    public boolean isValueNull() {
      throw new IllegalStateException("should never be called");
    }

    public void _setValue(RegionEntryContext context, Object value) {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public void setValueWithContext(RegionEntryContext context, Object value) {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public void handleValueOverflow(RegionEntryContext context) {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext context, Object value,
        boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    public void _removePhase1() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public DiskId getDiskId() {
      return this.diskId;
    }

    @Override
    public long getLastModified() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public int updateAsyncEntrySize(EvictionController capacityController) {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public DiskEntry getPrev() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public DiskEntry getNext() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public void setPrev(DiskEntry v) {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public void setNext(DiskEntry v) {
      throw new IllegalStateException("should never be called");
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.cache.DiskEntry#getVersionStamp()
     */
    @Override
    public VersionStamp getVersionStamp() {
      return null;
    }

    @Override
    public boolean isRemovedFromDisk() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public boolean hasStats() {
      return false;
    }

    @Override
    public long getLastAccessed() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public long getHitCount() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public long getMissCount() throws InternalStatisticsDisabledException {
      return 0;
    }

    @Override
    public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
      // nothing
    }

    @Override
    public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
        InternalRegion region, EntryEventImpl event) {
      return null;
    }

    @Override
    public boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException {
      return false;
    }

    @Override
    public void setRecentlyUsed(RegionEntryContext context) {
      // nothing
    }

    @Override
    public void updateStatsForGet(boolean hit, long time) {
      // nothing
    }

    @Override
    public void txDidDestroy(long currentTime) {
      // Tnothing
    }

    @Override
    public void resetCounts() throws InternalStatisticsDisabledException {
      // nothing
    }

    @Override
    public void makeTombstone(InternalRegion region, VersionTag version)
        throws RegionClearedException {
      // nothing
    }

    @Override
    public void removePhase1(InternalRegion region, boolean clear) throws RegionClearedException {
      // nothing
    }

    @Override
    public void removePhase2() {
      // nothing
    }

    @Override
    public boolean isRemoved() {
      return false;
    }

    @Override
    public boolean isRemovedPhase2() {
      return false;
    }

    @Override
    public boolean isTombstone() {
      return false;
    }

    @Override
    public boolean fillInValue(InternalRegion region, Entry entry, ByteArrayDataInput in,
        DistributionManager distributionManager, final Version version) {
      return false;
    }

    @Override
    public boolean isOverflowedToDisk(InternalRegion region, DiskPosition diskPosition) {
      return false;
    }

    @Override
    public Object getValue(RegionEntryContext context) {
      return null;
    }

    @Override
    public Object getValueRetain(RegionEntryContext context) {
      return null;
    }

    @Override
    public void setValue(RegionEntryContext context, Object value) throws RegionClearedException {
      // nothing
    }

    @Override
    public void setValueWithTombstoneCheck(Object value, EntryEvent event)
        throws RegionClearedException {
      // nothing
    }

    @Override
    public Object getTransformedValue() {
      return null;
    }

    @Override
    public Object getValueInVM(RegionEntryContext context) {
      return null;
    }

    @Override
    public Object getValueOnDisk(InternalRegion region) throws EntryNotFoundException {
      return null;
    }

    @Override
    public Object getValueOnDiskOrBuffer(InternalRegion region) throws EntryNotFoundException {
      return null;
    }

    @Override
    public boolean initialImagePut(InternalRegion region, long lastModified, Object newValue,
        boolean wasRecovered, boolean acceptedVersionTag) throws RegionClearedException {
      return false;
    }

    @Override
    public boolean initialImageInit(InternalRegion region, long lastModified, Object newValue,
        boolean create, boolean wasRecovered, boolean acceptedVersionTag)
        throws RegionClearedException {
      return false;
    }

    @Override
    public boolean destroy(InternalRegion region, EntryEventImpl event, boolean inTokenMode,
        boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
        boolean removeRecoveredEntry) throws CacheWriterException, EntryNotFoundException,
        TimeoutException, RegionClearedException {
      return false;
    }

    @Override
    public boolean getValueWasResultOfSearch() {
      return false;
    }

    @Override
    public void setValueResultOfSearch(boolean value) {
      // nothing
    }

    @Override
    public Object getSerializedValueOnDisk(InternalRegion region) {
      return null;
    }

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(InternalRegion region) {
      return null;
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(InternalRegion region) {
      return null;
    }

    @Override
    public boolean isUpdateInProgress() {
      return false;
    }

    @Override
    public void setUpdateInProgress(boolean underUpdate) {
      // nothing
    }

    @Override
    public boolean isInvalid() {
      return false;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public boolean isDestroyedOrRemoved() {
      return false;
    }

    @Override
    public boolean isDestroyedOrRemovedButNotTombstone() {
      return false;
    }

    @Override
    public boolean isInvalidOrRemoved() {
      return false;
    }

    @Override
    public void setValueToNull() {
      // nothing
    }

    @Override
    public void returnToPool() {
      // nothing
    }

    @Override
    public boolean isCacheListenerInvocationInProgress() {
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(boolean isListenerInvoked) {
      // nothing
    }

    @Override
    public void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
        throws RegionClearedException {}

    @Override
    public boolean isInUseByTransaction() {
      return false;
    }

    @Override
    public void incRefCount() {
      // nothing
    }

    @Override
    public void decRefCount(EvictionList lruList, InternalRegion region) {
      // nothing
    }

    @Override
    public void resetRefCount(EvictionList lruList) {
      // nothing
    }

    @Override
    public Object prepareValueForCache(RegionEntryContext context, Object value,
        EntryEventImpl event, boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    @Override
    public boolean isEvicted() {
      return false;
    }
  }


  //////////////// DiskExceptionHandler methods ////////////////////

  public static class DummyDiskExceptionHandler implements DiskExceptionHandler {
    @Override
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
    public RuntimeException generateCancelledException(Throwable throwable) {
      return new RuntimeException(throwable);
    }

  }
}
