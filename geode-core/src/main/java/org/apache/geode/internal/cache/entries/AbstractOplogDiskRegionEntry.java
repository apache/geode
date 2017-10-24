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
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryBits;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Retained;

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
  public void removePhase1(LocalRegion r, boolean isClear) throws RegionClearedException {
    synchronized (this) {
      Helper.removeFromDisk(this, r, isClear);
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
  public boolean fillInValue(LocalRegion r, Entry entry, ByteArrayDataInput in, DM mgr,
      final Version version) {
    return Helper.fillInValue(this, entry, r.getDiskRegion(), mgr, in, r, version);
  }

  @Override
  public boolean isOverflowedToDisk(LocalRegion r, DistributedRegion.DiskPosition dp) {
    return Helper.isOverflowedToDisk(this, r.getDiskRegion(), dp, r);
  }

  @Override
  public Object getValue(RegionEntryContext context) {
    return Helper.faultInValue(this, (LocalRegion) context); // OFFHEAP returned to callers
  }

  @Override
  @Retained
  public Object getValueRetain(RegionEntryContext context) {
    return Helper.faultInValueRetain(this, (LocalRegion) context);
  }

  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
    return Helper.getValueInVMOrDiskWithoutFaultIn(this, owner);
  }

  @Retained
  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
    return Helper.getValueOffHeapOrDiskWithoutFaultIn(this, owner);
  }

  @Override
  public Object getValueOnDisk(LocalRegion r) throws EntryNotFoundException {
    return Helper.getValueOnDisk(this, r.getDiskRegion());
  }

  @Override
  public Object getSerializedValueOnDisk(LocalRegion r) throws EntryNotFoundException {
    return Helper.getSerializedValueOnDisk(this, r.getDiskRegion());
  }

  @Override
  public Object getValueOnDiskOrBuffer(LocalRegion r) throws EntryNotFoundException {
    // @todo darrel if value is Token.REMOVED || Token.DESTROYED throw
    // EntryNotFoundException
    return Helper.getValueOnDiskOrBuffer(this, r.getDiskRegion(), r);
  }

  public DiskEntry getPrev() {
    return getDiskId().getPrev();
  }

  public DiskEntry getNext() {
    return getDiskId().getNext();
  }

  public void setPrev(DiskEntry v) {
    getDiskId().setPrev(v);
  }

  public void setNext(DiskEntry v) {
    getDiskId().setNext(v);
  }

  /*
   * If detected a conflict event, persist region needs to persist both the golden copy and conflict
   * tag
   */
  @Override
  public void persistConflictingTag(LocalRegion region, VersionTag tag) {
    // only persist region needs to persist conflict tag
    Helper.updateVersionOnly(this, region, tag);
    setRecentlyUsed();
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
      LocalRegion lr = (LocalRegion) cacheEvent.getRegion();
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
