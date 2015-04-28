/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * Abstract implementation class of RegionEntry interface.
 * This is adds Disk support behavior
 *
 * @since 3.5.1
 *
 * @author Darrel Schneider
 *
 */
public abstract class AbstractOplogDiskRegionEntry
  extends AbstractDiskRegionEntry
{
  protected AbstractOplogDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }

  abstract void setDiskId(RegionEntry oldRe);
  
  @Override
  public final void removePhase1(LocalRegion r, boolean isClear) throws RegionClearedException
  {
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
  public final boolean fillInValue(LocalRegion r, InitialImageOperation.Entry entry, ByteArrayDataInput in, DM mgr) {
    return Helper.fillInValue(this, entry, r.getDiskRegion(), mgr, in, r);
  }

  @Override
  public final boolean isOverflowedToDisk(LocalRegion r, DistributedRegion.DiskPosition dp) {
    return Helper.isOverflowedToDisk(this, r.getDiskRegion(), dp, r);
  }
  
  @Override
  public final Object getValue(RegionEntryContext context) {   
    return Helper.faultInValue(this, (LocalRegion) context);  // OFFHEAP returned to callers
  }
  
  @Override
  public final Object getValueInVMOrDiskWithoutFaultIn(LocalRegion owner) {
    return Helper.getValueInVMOrDiskWithoutFaultIn(this, owner);
  }
  @Override
  public Object getValueOffHeapOrDiskWithoutFaultIn(LocalRegion owner) {
    return Helper.getValueOffHeapOrDiskWithoutFaultIn(this, owner);
  }

  @Override
  public final Object getValueOnDisk(LocalRegion r)
  throws EntryNotFoundException
  {
    return Helper.getValueOnDisk(this, r.getDiskRegion());
  }

  @Override
  public final Object getSerializedValueOnDisk(LocalRegion r)
    throws EntryNotFoundException
  {
    return Helper.getSerializedValueOnDisk(this, r.getDiskRegion());
  }

  @Override
  public final Object getValueOnDiskOrBuffer(LocalRegion r)
    throws EntryNotFoundException
  {
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
   * If detected a conflict event, persist region needs to persist both the
   * golden copy and conflict tag
   */
  @Override
  public void persistConflictingTag(LocalRegion region, VersionTag tag) {
    // only persist region needs to persist conflict tag
    Helper.updateVersionOnly(this, region, tag);
    setRecentlyUsed();
  }
  
  /**
   * Process a version tag. This overrides AbtractRegionEntry so
   * we can check to see if the old value was recovered from disk.
   * If so, we don't check for conflicts.
   */
  @Override
  public void processVersionTag(EntryEvent cacheEvent) {
    DiskId did = getDiskId();
    boolean checkConflicts = true;
    if(did != null) {
      LocalRegion lr = (LocalRegion)cacheEvent.getRegion();
      if (lr != null && lr.getDiskRegion().isReadyForRecovery()) {
        synchronized(did) {
          checkConflicts = !EntryBits.isRecoveredFromDisk(did.getUserBits());
        }
      }
    }
    
    processVersionTag(cacheEvent, checkConflicts);
  }

  /**
   * Returns true if the DiskEntry value is equal to {@link Token#DESTROYED}, {@link Token#REMOVED_PHASE1}, or {@link Token#REMOVED_PHASE2}.
   */
  @Override
  public boolean isRemovedFromDisk() {
    return Token.isRemovedFromDisk(getValueAsToken());
  }

  // Do not add any instance variables to this class.
  // Instead add them to the DISK section of LeafRegionEntry.cpp.
}
