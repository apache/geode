/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * State object used during getInitialImage Locked during clean up of destroyed
 * tokens.
 * 
 * @author Eric Zoerner
 */
public interface ImageState /* extends Lock */ {

  public boolean getRegionInvalidated();

  public void setRegionInvalidated(boolean b);

  public void setInRecovery(boolean b);

  public boolean getInRecovery();

  public void addDestroyedEntry(Object key);
  
  public void removeDestroyedEntry(Object key);

  public boolean hasDestroyedEntry(Object key);

  public java.util.Iterator getDestroyedEntries();

  /**
   *  returns count of entries that have been destroyed by concurrent operations
   *  while in token mode
   */
  public int getDestroyedEntriesCount();
  
  public void setClearRegionFlag(boolean isClearOn, RegionVersionVector rvv);

  public boolean getClearRegionFlag();
  public RegionVersionVector getClearRegionVersionVector();
  public boolean wasRegionClearedDuringGII();
  
  public void addVersionTag(Object key, VersionTag<?> tag);
  public Iterator<VersionTagEntry> getVersionTags();
  
  public void addLeftMember(VersionSource<?> mbr);
  public Set<VersionSource> getLeftMembers();
  public boolean hasLeftMembers();

  public void lockGII();
  public void unlockGII();
  public void readLockRI();
  public void readUnlockRI();
  public void writeLockRI();
  public void writeUnlockRI();

  public boolean isReplicate();
  public boolean isClient();
  
  public void init();
  
  public interface VersionTagEntry {
    public Object getKey();
    public VersionSource getMemberID();
    public long getRegionVersion();
  }
  
}
