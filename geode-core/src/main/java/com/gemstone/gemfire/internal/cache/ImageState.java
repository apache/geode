/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
