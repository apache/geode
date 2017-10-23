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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.lru.NewLRUClockHand;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;

public class NonLocalRegionEntry implements RegionEntry, VersionStamp {

  private long lastModified;
  private boolean isRemoved;
  private Object key;
  private Object value;
  private VersionTag versionTag;

  /**
   * Create one of these in the local case so that we have a snapshot of the state and can allow the
   * bucket to move out from under us.
   */
  public NonLocalRegionEntry(RegionEntry re, LocalRegion br, boolean allowTombstones) {
    this.key = re.getKey();
    if (allowTombstones && re.isTombstone()) { // client get() operations need to see tombstone
                                               // values
      this.value = Token.TOMBSTONE;
    } else {
      this.value = re.getValue(br); // OFFHEAP: copy into heap cd
    }
    Assert.assertTrue(this.value != Token.NOT_AVAILABLE,
        "getEntry did not fault value in from disk");
    this.lastModified = re.getLastModified();
    this.isRemoved = re.isRemoved();
    VersionStamp stamp = re.getVersionStamp();
    if (stamp != null) {
      this.versionTag = stamp.asVersionTag();
    }
  }

  /**
   * Create one of these in the local case so that we have a snapshot of the state and can allow the
   * bucket to move out from under us.
   */
  public NonLocalRegionEntry(LocalRegion br, Object key, Object value) {
    this.key = key;
    this.value = value;
    Assert.assertTrue(this.value != Token.NOT_AVAILABLE,
        "getEntry did not fault value in from disk");
    // this.lastModified = re.getLastModified();
    // this.isRemoved = re.isRemoved();
  }



  /**
   * Create one of these in the local case so that we have a snapshot of the state and can allow the
   * bucket to move out from under us.
   */
  public NonLocalRegionEntry(Region.Entry re, LocalRegion br) {
    this.key = re.getKey();
    this.value = re.getValue();
    if (this.value instanceof CachedDeserializable) {
      // We make a copy of the CachedDeserializable.
      // That way the NonLocalRegionEntry will be disconnected
      // from the CachedDeserializable that is in our cache and
      // will not modify its state.
      this.value = CachedDeserializableFactory.create((CachedDeserializable) this.value);
    }
    Assert.assertTrue(this.value != Token.NOT_AVAILABLE,
        "getEntry did not fault value in from disk");
    this.lastModified = 0l;// re.getStatistics().getLastModifiedTime();
    this.isRemoved = Token.isRemoved(value);
    if (re instanceof EntrySnapshot) {
      this.versionTag = ((EntrySnapshot) re).getVersionTag();
    } else {
      // TODO need to get version information from transaction entries
    }
  }

  @Override
  public String toString() {
    return "NonLocalRegionEntry(" + this.key + "; value=" + this.value + "; version="
        + this.versionTag;
  }

  public void makeTombstone(LocalRegion r, VersionTag isOperationRemote) {
    throw new UnsupportedOperationException();
  }

  public boolean dispatchListenerEvents(EntryEventImpl event) {
    throw new UnsupportedOperationException();
  }

  public VersionStamp getVersionStamp() {
    return this;
  }

  public boolean hasValidVersion() {
    return this.versionTag != null && this.versionTag.hasValidVersion();
  }

  public void setVersionTimeStamp(long time) {
    throw new UnsupportedOperationException();
  }

  public void processVersionTag(EntryEvent ev) {
    throw new UnsupportedOperationException();
  }

  public NonLocalRegionEntry() {
    // for fromData
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.key, out);
    DataSerializer.writeObject(this.value, out);
    out.writeLong(this.lastModified);
    out.writeBoolean(this.isRemoved);
    DataSerializer.writeObject(this.versionTag, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.key = DataSerializer.readObject(in);
    this.value = DataSerializer.readObject(in);
    this.lastModified = in.readLong();
    this.isRemoved = in.readBoolean();
    this.versionTag = (VersionTag) DataSerializer.readObject(in);
  }

  public long getLastModified() {
    return this.lastModified;
  }

  public long getLastAccessed() throws StatisticsDisabledException {
    return -1;
  }

  public long getHitCount() throws StatisticsDisabledException {
    return -1;
  }

  public long getMissCount() throws StatisticsDisabledException {
    return -1;
  }

  public boolean isRemoved() {
    return this.isRemoved;
  }

  public boolean isRemovedPhase2() {
    return this.isRemoved;
  }

  public boolean isTombstone() {
    return false;
  }

  public boolean fillInValue(LocalRegion r, Entry entry, ByteArrayDataInput in, DM mgr,
      final Version version) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public boolean isOverflowedToDisk(LocalRegion r, DistributedRegion.DiskPosition dp) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public Object getKey() {
    return this.key;
  }

  public Object getValue(RegionEntryContext context) {
    return this.value;
  }

  @Override
  public Object getValueRetain(RegionEntryContext context) {
    return this.value;
  }

  /** update the value held in this non-local region entry */
  public void setCachedValue(Object newValue) {
    this.value = newValue;
  }

  // now for the fun part
  public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void setRecentlyUsed() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void updateStatsForGet(boolean hit, long time) throws StatisticsDisabledException {
    // this method has been made a noop to fix bug 37436
  }

  public void txDidDestroy(long currTime) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void resetCounts() throws StatisticsDisabledException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void removePhase1(LocalRegion r, boolean isClear) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void removePhase2() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  public void setValue(RegionEntryContext context, Object value) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public Object _getValue() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public Token getValueAsToken() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public Object _getValueRetain(RegionEntryContext context, boolean decompress) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public Object getTransformedValue() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public Object getValueInVM(RegionEntryContext context) {
    return this.value;
  }

  public Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public boolean initialImagePut(LocalRegion region, long lastModified1, Object newValue,
      boolean wasRecovered, boolean versionTagAccepted) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public boolean initialImageInit(LocalRegion region, long lastModified1, Object newValue,
      boolean create, boolean wasRecovered, boolean versionTagAccepted) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public boolean destroy(LocalRegion region, EntryEventImpl event, boolean inTokenMode,
      boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
      boolean removeRecoveredEntry)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public boolean getValueWasResultOfSearch() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public void setValueResultOfSearch(boolean v) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.RegionEntry#getValueOnDiskOrBuffer(org.apache.geode.internal.
   * cache.LocalRegion)
   */
  public Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  public Object getSerializedValueOnDisk(LocalRegion localRegion) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }


  public boolean hasStats() {
    return false;
  }

  public Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
    return this.value;
  }

  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
    return this.value;
  }

  public void setKey(Object key2) {
    this.key = key2;
  }

  // VersionStamp methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.RegionEntry#generateVersionTag(org.apache.geode.distributed.
   * DistributedMember, boolean)
   */
  public VersionTag generateVersionTag(VersionSource member, boolean withDelta, LocalRegion region,
      EntryEventImpl event) {
    throw new UnsupportedOperationException(); // no text needed - not a customer visible method
  }


  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.RegionEntry#concurrencyCheck(org.apache.geode.internal.cache.
   * LocalRegion, org.apache.geode.internal.cache.versions.VersionTag,
   * org.apache.geode.distributed.internal.membership.InternalDistributedMember,
   * org.apache.geode.distributed.internal.membership.InternalDistributedMember)
   */
  public void processVersionTag(LocalRegion r, VersionTag tag, InternalDistributedMember thisVM,
      InternalDistributedMember sender) {
    throw new UnsupportedOperationException();
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#getEntryVersion()
   */
  public int getEntryVersion() {
    if (this.versionTag != null) {
      return this.versionTag.getEntryVersion();
    }
    return 0;
  }

  public long getRegionVersion() {
    if (this.versionTag != null) {
      return this.versionTag.getRegionVersion();
    }
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#getMemberID()
   */
  public VersionSource getMemberID() {
    if (this.versionTag != null) {
      return this.versionTag.getMemberID();
    }
    return null;
  }

  public int getDistributedSystemId() {
    if (this.versionTag != null) {
      return this.versionTag.getDistributedSystemId();
    }
    return -1;
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#setEntryVersion(int)
   */
  public void setVersions(VersionTag tag) {
    throw new UnsupportedOperationException();
  }


  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.versions.VersionStamp#setMemberID(org.apache.geode.distributed.
   * DistributedMember)
   */
  public void setMemberID(VersionSource memberID) {
    throw new UnsupportedOperationException();
  }


  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.versions.VersionStamp#setPreviousMemberID(org.apache.geode.
   * distributed.DistributedMember)
   */
  public void setPreviousMemberID(DistributedMember previousMemberID) {
    throw new UnsupportedOperationException();
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#asVersionTag()
   */
  public VersionTag asVersionTag() {
    return this.versionTag;
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#processVersionTag(org.apache.geode.
   * internal.cache.LocalRegion, org.apache.geode.internal.cache.versions.VersionTag, boolean,
   * org.apache.geode.distributed.internal.membership.InternalDistributedMember,
   * org.apache.geode.distributed.internal.membership.InternalDistributedMember)
   */
  public void processVersionTag(LocalRegion r, VersionTag tag, boolean isTombstoneFromGII,
      boolean hasDelta, VersionSource thisVM, InternalDistributedMember sender,
      boolean checkForConflicts) {
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.geode.internal.cache.versions.VersionStamp#getVersionTimeStamp()
   */
  @Override
  public long getVersionTimeStamp() {
    return this.versionTag != null ? this.versionTag.getVersionTimeStamp() : 0;
  }

  /** get rvv internal high byte. Used by region entries for transferring to storage */
  public short getRegionVersionHighBytes() {
    return this.versionTag != null ? this.versionTag.getRegionVersionHighBytes() : 0;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  public int getRegionVersionLowBytes() {
    return this.versionTag != null ? this.versionTag.getRegionVersionLowBytes() : 0;
  }

  @Override
  public boolean isUpdateInProgress() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public void setUpdateInProgress(boolean underUpdate) {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public boolean isValueNull() {
    return (null == getValueAsToken());
  }

  @Override
  public boolean isInvalid() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public boolean isDestroyed() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public void setValueToNull() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public boolean isInvalidOrRemoved() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public boolean isDestroyedOrRemoved() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public boolean isDestroyedOrRemovedButNotTombstone() {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

  @Override
  public void returnToPool() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setValueWithTombstoneCheck(Object value, EntryEvent event)
      throws RegionClearedException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
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
      throws RegionClearedException {
    throw new UnsupportedOperationException(
        LocalizedStrings.PartitionedRegion_NOT_APPROPRIATE_FOR_PARTITIONEDREGIONNONLOCALREGIONENTRY
            .toLocalizedString());
  }

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
  public Object prepareValueForCache(RegionEntryContext r, Object val, boolean isEntryUpdate) {
    throw new IllegalStateException("Should never be called");
  }

  @Override
  public Object prepareValueForCache(RegionEntryContext r, Object val, EntryEventImpl event,
      boolean isEntryUpdate) {
    throw new IllegalStateException("Should never be called");
  }
}
