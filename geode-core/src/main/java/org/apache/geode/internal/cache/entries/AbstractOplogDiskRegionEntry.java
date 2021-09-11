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
package org.apache.geode.internal.cache.entries;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryBits;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Abstract implementation class of RegionEntry interface. This is adds Disk support behavior
 *
 * @since GemFire 3.5.1
 */
public abstract class AbstractOplogDiskRegionEntry extends AbstractDiskRegionEntry {

  protected AbstractOplogDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }

  public abstract void setDiskId(RegionEntry oldRe);

  @Override
  public void removePhase1(InternalRegion region, boolean clear) throws RegionClearedException {
    synchronized (this) {
      Helper.removeFromDisk(this, region, clear);
      _removePhase1();
    }
  }

  @Override
  public void removePhase2() {
    Object syncObj = getDiskId();
    if (syncObj == null) {
      syncObj = this;
    }
    synchronized (syncObj) {
      super.removePhase2();
    }
  }

  @Override
  public boolean fillInValue(InternalRegion region, Entry entry, ByteArrayDataInput in,
      DistributionManager mgr, final KnownVersion version) {
    return Helper.fillInValue(this, entry, region.getDiskRegion(), region, version);
  }

  @Override
  public boolean isOverflowedToDisk(InternalRegion region,
      DistributedRegion.DiskPosition diskPosition) {
    return Helper.isOverflowedToDisk(this, region.getDiskRegion(), diskPosition);
  }

  @Override
  public Object getValue(RegionEntryContext context) {
    return Helper.faultInValue(this, (InternalRegion) context); // OFFHEAP returned to callers
  }

  @Override
  @Retained
  public Object getValueRetain(RegionEntryContext context) {
    return Helper.faultInValueRetain(this, (InternalRegion) context);
  }

  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(InternalRegion region) {
    return Helper.getValueInVMOrDiskWithoutFaultIn(this, region);
  }

  @Retained
  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(InternalRegion region) {
    return Helper.getValueOffHeapOrDiskWithoutFaultIn(this, region);
  }

  @Override
  public Object getValueOnDisk(InternalRegion region) throws EntryNotFoundException {
    return Helper.getValueOnDisk(this, region.getDiskRegion());
  }

  @Override
  public Object getSerializedValueOnDisk(InternalRegion region) throws EntryNotFoundException {
    return Helper.getSerializedValueOnDisk(this, region.getDiskRegion());
  }

  @Override
  public Object getValueOnDiskOrBuffer(InternalRegion region) throws EntryNotFoundException {
    // @todo darrel if value is Token.REMOVED || Token.DESTROYED throw
    // EntryNotFoundException
    return Helper.getValueOnDiskOrBuffer(this, region.getDiskRegion(), region);
  }

  @Override
  public DiskEntry getPrev() {
    return getDiskId().getPrev();
  }

  @Override
  public DiskEntry getNext() {
    return getDiskId().getNext();
  }

  @Override
  public void setPrev(DiskEntry v) {
    getDiskId().setPrev(v);
  }

  @Override
  public void setNext(DiskEntry v) {
    getDiskId().setNext(v);
  }

  /*
   * If detected a conflict event, persist region needs to persist both the golden copy and conflict
   * tag
   */
  @Override
  public void persistConflictingTag(InternalRegion region, VersionTag tag) {
    // only persist region needs to persist conflict tag
    Helper.updateVersionOnly(this, region, tag);
    setRecentlyUsed(region);
  }

  /**
   * Process a version tag. This overrides AbtractRegionEntry so we can check to see if the old
   * value was recovered from disk. If so, we don't check for conflicts.
   */
  @Override
  public void processVersionTag(EntryEvent cacheEvent) {
    DiskId did = getDiskId();
    boolean checkConflicts = true;
    if (did != null) {
      InternalRegion lr = (InternalRegion) cacheEvent.getRegion();
      if (lr != null && lr.getDiskRegion().isReadyForRecovery()) {
        synchronized (did) {
          checkConflicts = !EntryBits.isRecoveredFromDisk(did.getUserBits());
        }
      }
    }

    processVersionTag(cacheEvent, checkConflicts);
  }

  /**
   * Returns true if the DiskEntry value is equal to {@link Token#DESTROYED},
   * {@link Token#REMOVED_PHASE1}, or {@link Token#REMOVED_PHASE2}.
   */
  @Override
  public boolean isRemovedFromDisk() {
    return Token.isRemovedFromDisk(getValueAsToken());
  }

  // Do not add any instance variables to this class.
  // Instead add them to the DISK section of LeafRegionEntry.cpp.
}
