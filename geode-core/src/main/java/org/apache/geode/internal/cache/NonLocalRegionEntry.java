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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.eviction.EvictionList;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;

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
  public NonLocalRegionEntry(RegionEntry re, InternalRegion br, boolean allowTombstones) {
    this.key = re.getKey();
    if (allowTombstones && re.isTombstone()) {
      // client get() operations need to see tombstone values
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
  public NonLocalRegionEntry(InternalRegion br, Object key, Object value) {
    this.key = key;
    this.value = value;
    Assert.assertTrue(this.value != Token.NOT_AVAILABLE,
        "getEntry did not fault value in from disk");
  }

  /**
   * Create one of these in the local case so that we have a snapshot of the state and can allow the
   * bucket to move out from under us.
   */
  public NonLocalRegionEntry(Region.Entry re, InternalRegion br) {
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

  @Override
  public void makeTombstone(InternalRegion region, VersionTag version) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dispatchListenerEvents(EntryEventImpl event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionStamp getVersionStamp() {
    return this;
  }

  @Override
  public boolean hasValidVersion() {
    return this.versionTag != null && this.versionTag.hasValidVersion();
  }

  @Override
  public void setVersionTimeStamp(long time) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processVersionTag(EntryEvent event) {
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

  @Override
  public long getLastModified() {
    return this.lastModified;
  }

  @Override
  public long getLastAccessed() throws StatisticsDisabledException {
    return -1;
  }

  @Override
  public long getHitCount() throws StatisticsDisabledException {
    return -1;
  }

  @Override
  public long getMissCount() throws StatisticsDisabledException {
    return -1;
  }

  @Override
  public boolean isRemoved() {
    return this.isRemoved;
  }

  @Override
  public boolean isRemovedPhase2() {
    return this.isRemoved;
  }

  @Override
  public boolean isTombstone() {
    return false;
  }

  @Override
  public boolean fillInValue(InternalRegion region, Entry entry, ByteArrayDataInput in,
      DistributionManager distributionManager, final Version version) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isOverflowedToDisk(InternalRegion region,
      DistributedRegion.DiskPosition diskPosition) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getKey() {
    return this.key;
  }

  @Override
  public Object getValue(RegionEntryContext context) {
    return this.value;
  }

  @Override
  public Object getValueRetain(RegionEntryContext context) {
    return this.value;
  }

  /** update the value held in this non-local region entry */
  void setCachedValue(Object newValue) {
    this.value = newValue;
  }

  // now for the fun part
  @Override
  public void updateStatsForPut(long lastModifiedTime, long lastAccessedTime) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void setRecentlyUsed(RegionEntryContext context) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void updateStatsForGet(boolean hit, long time) throws StatisticsDisabledException {
    // this method has been made a noop to fix bug 37436
  }

  @Override
  public void txDidDestroy(long currentTime) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void resetCounts() throws StatisticsDisabledException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void removePhase1(InternalRegion region, boolean clear) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void removePhase2() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void setValue(RegionEntryContext context, Object value) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getValue() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Token getValueAsToken() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getValueRetain(RegionEntryContext context, boolean decompress) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getTransformedValue() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getValueInVM(RegionEntryContext context) {
    return this.value;
  }

  @Override
  public Object getValueOnDisk(InternalRegion region) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean initialImagePut(InternalRegion region, long lastModified, Object newValue,
      boolean wasRecovered, boolean acceptedVersionTag) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean initialImageInit(InternalRegion region, long lastModified, Object newValue,
      boolean create, boolean wasRecovered, boolean acceptedVersionTag) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean destroy(InternalRegion region, EntryEventImpl event, boolean inTokenMode,
      boolean cacheWrite, Object expectedOldValue, boolean forceDestroy,
      boolean removeRecoveredEntry)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean getValueWasResultOfSearch() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void setValueResultOfSearch(boolean value) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getValueOnDiskOrBuffer(InternalRegion region) throws EntryNotFoundException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public Object getSerializedValueOnDisk(InternalRegion region) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }


  @Override
  public boolean hasStats() {
    return false;
  }

  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(InternalRegion region) {
    return this.value;
  }

  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(InternalRegion region) {
    return this.value;
  }

  public void setKey(Object key2) {
    this.key = key2;
  }

  @Override
  public VersionTag generateVersionTag(VersionSource member, boolean withDelta,
      InternalRegion region, EntryEventImpl event) {
    throw new UnsupportedOperationException(); // no text needed - not a customer visible method
  }

  public void processVersionTag(InternalRegion r, VersionTag tag, InternalDistributedMember thisVM,
      InternalDistributedMember sender) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getEntryVersion() {
    if (this.versionTag != null) {
      return this.versionTag.getEntryVersion();
    }
    return 0;
  }

  @Override
  public long getRegionVersion() {
    if (this.versionTag != null) {
      return this.versionTag.getRegionVersion();
    }
    return 0;
  }

  @Override
  public VersionSource getMemberID() {
    if (this.versionTag != null) {
      return this.versionTag.getMemberID();
    }
    return null;
  }

  @Override
  public int getDistributedSystemId() {
    if (this.versionTag != null) {
      return this.versionTag.getDistributedSystemId();
    }
    return -1;
  }

  @Override
  public void setVersions(VersionTag tag) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMemberID(VersionSource memberID) {
    throw new UnsupportedOperationException();
  }

  public void setPreviousMemberID(DistributedMember previousMemberID) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VersionTag asVersionTag() {
    return this.versionTag;
  }

  @Override
  public void processVersionTag(InternalRegion region, VersionTag tag, boolean isTombstoneFromGII,
      boolean hasDelta, VersionSource versionSource, InternalDistributedMember sender,
      boolean checkConflicts) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getVersionTimeStamp() {
    return this.versionTag != null ? this.versionTag.getVersionTimeStamp() : 0;
  }

  /** get rvv internal high byte. Used by region entries for transferring to storage */
  @Override
  public short getRegionVersionHighBytes() {
    return this.versionTag != null ? this.versionTag.getRegionVersionHighBytes() : 0;
  }

  /** get rvv internal low bytes. Used by region entries for transferring to storage */
  @Override
  public int getRegionVersionLowBytes() {
    return this.versionTag != null ? this.versionTag.getRegionVersionLowBytes() : 0;
  }

  @Override
  public boolean isUpdateInProgress() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void setUpdateInProgress(boolean underUpdate) {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isValueNull() {
    return (null == getValueAsToken());
  }

  @Override
  public boolean isInvalid() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isDestroyed() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void setValueToNull() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isInvalidOrRemoved() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isDestroyedOrRemoved() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public boolean isDestroyedOrRemovedButNotTombstone() {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

  @Override
  public void returnToPool() {
    // nothing
  }

  @Override
  public void setValueWithTombstoneCheck(Object value, EntryEvent event)
      throws RegionClearedException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
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
      throws RegionClearedException {
    throw new UnsupportedOperationException(
        "Not appropriate for PartitionedRegion.NonLocalRegionEntry");
  }

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
      boolean isEntryUpdate) {
    throw new IllegalStateException("Should never be called");
  }

  @Override
  public Object prepareValueForCache(RegionEntryContext context, Object value, EntryEventImpl event,
      boolean isEntryUpdate) {
    throw new IllegalStateException("Should never be called");
  }

  @Override
  public boolean isEvicted() {
    return false;
  }
}
