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
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalStatisticsDisabledException;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DistributedRegion.DiskPosition;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.lru.EnableLRU;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;

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
  public DiskRegionView getDiskRegionView() {
    return this;
  }

  public DiskEntry getDiskEntry(Object key) {
    return this.map.get(key);
  }

  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    ValidatingDiskEntry de = new ValidatingDiskEntry(key, re);
    if (this.map.putIfAbsent(key, de) != null) {
      throw new InternalGemFireError(
          LocalizedStrings.LocalRegion_ENTRY_ALREADY_EXISTED_0.toLocalizedString(key));
    }
    return de;
  }

  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    ValidatingDiskEntry de = new ValidatingDiskEntry(key, re);
    this.map.put(key, de);
    return de;
  }

  public void destroyRecoveredEntry(Object key) {
    this.map.remove(key);
  }

  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback) {
    throw new IllegalStateException(
        "foreachRegionEntry should not be called when validating disk store");
  }

  public boolean lruLimitExceeded() {
    return false;
  }

  public void copyRecoveredEntries(RegionMap rm) {
    throw new IllegalStateException(
        "copyRecoveredEntries should not be called on ValidatingDiskRegion");
  }

  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    throw new IllegalStateException(
        "updateSizeOnFaultIn should not be called on ValidatingDiskRegion");
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
    throw new IllegalStateException("getRegionMap should not be called on ValidatingDiskRegion");
  }

  public void handleDiskAccessException(DiskAccessException dae) {
    throw new IllegalStateException(
        "handleDiskAccessException should not be called on ValidatingDiskRegion");
  }

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

    public Object getKey() {
      return this.key;
    }

    public Object _getValue() {
      return null;
    }

    @Override
    public Token getValueAsToken() {
      return null;
    }

    public Object _getValueRetain(RegionEntryContext context, boolean decompress) {
      return null;
    }

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
    public Object prepareValueForCache(RegionEntryContext r, Object val, boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }

    public void _removePhase1() {
      throw new IllegalStateException("should never be called");
    }

    public DiskId getDiskId() {
      return this.diskId;
    }

    public long getLastModified() {
      throw new IllegalStateException("should never be called");
    }

    public int updateAsyncEntrySize(EnableLRU capacityController) {
      throw new IllegalStateException("should never be called");
    }

    public DiskEntry getPrev() {
      throw new IllegalStateException("should never be called");
    }

    public DiskEntry getNext() {
      throw new IllegalStateException("should never be called");
    }

    public void setPrev(DiskEntry v) {
      throw new IllegalStateException("should never be called");
    }

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
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isRemovedFromDisk() {
      throw new IllegalStateException("should never be called");
    }

    @Override
    public boolean hasStats() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long getLastAccessed() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getHitCount() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getMissCount() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
      // TODO Auto-generated method stub
    }

    @Override
    public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
        LocalRegion region, EntryEventImpl event) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean dispatchListenerEvents(EntryEventImpl event) throws InterruptedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setRecentlyUsed() {
      // TODO Auto-generated method stub
    }

    @Override
    public void updateStatsForGet(boolean hit, long time) {
      // TODO Auto-generated method stub
    }

    @Override
    public void txDidDestroy(long currTime) {
      // TODO Auto-generated method stub
    }

    @Override
    public void resetCounts() throws InternalStatisticsDisabledException {
      // TODO Auto-generated method stub
    }

    @Override
    public void makeTombstone(LocalRegion r, VersionTag version) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void removePhase1(LocalRegion r, boolean clear) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void removePhase2() {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isRemovedPhase2() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isTombstone() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean fillInValue(LocalRegion r, Entry entry, ByteArrayDataInput in, DM mgr,
        final Version version) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isOverflowedToDisk(LocalRegion r, DiskPosition dp) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Object getValue(RegionEntryContext context) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueRetain(RegionEntryContext context) {
      return null;
    }

    @Override
    public void setValue(RegionEntryContext context, Object value) throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public void setValueWithTombstoneCheck(Object value, EntryEvent event)
        throws RegionClearedException {
      // TODO Auto-generated method stub
    }

    @Override
    public Object getTransformedValue() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueInVM(RegionEntryContext context) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean initialImagePut(LocalRegion region, long lastModified, Object newValue,
        boolean wasRecovered, boolean acceptedVersionTag) throws RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean initialImageInit(LocalRegion region, long lastModified, Object newValue,
        boolean create, boolean wasRecovered, boolean acceptedVersionTag)
        throws RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean destroy(LocalRegion region, EntryEventImpl event, boolean inTokenMode,
        boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
        boolean removeRecoveredEntry) throws CacheWriterException, EntryNotFoundException,
        TimeoutException, RegionClearedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean getValueWasResultOfSearch() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setValueResultOfSearch(boolean v) {
      // TODO Auto-generated method stub
    }

    @Override
    public Object getSerializedValueOnDisk(LocalRegion localRegion) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean isUpdateInProgress() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setUpdateInProgress(boolean underUpdate) {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isInvalid() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyed() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyedOrRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isDestroyedOrRemovedButNotTombstone() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean isInvalidOrRemoved() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setValueToNull() {
      // TODO Auto-generated method stub
    }

    @Override
    public void returnToPool() {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean isCacheListenerInvocationInProgress() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void setCacheListenerInvocationInProgress(boolean isListenerInvoked) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setValue(RegionEntryContext context, Object value, EntryEventImpl event)
        throws RegionClearedException {}

    @Override
    public boolean isInUseByTransaction() {
      return false;
    }

    @Override
    public void setInUseByTransaction(boolean v) {}

    @Override
    public void incRefCount() {}

    @Override
    public void decRefCount(NewLRUClockHand lruList, LocalRegion lr) {}

    @Override
    public void resetRefCount(NewLRUClockHand lruList) {}

    @Override
    public Object prepareValueForCache(RegionEntryContext r, Object val, EntryEventImpl event,
        boolean isEntryUpdate) {
      throw new IllegalStateException("Should never be called");
    }
  }


  //////////////// DiskExceptionHandler methods ////////////////////

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
