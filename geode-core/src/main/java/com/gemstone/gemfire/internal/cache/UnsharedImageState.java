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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.concurrent.StoppableNonReentrantLock;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock;

/**
 * Used on distributed replicated regions to track GII and various state.
 * Also used on pool regions to track register interest.
 * Note that currently a region will never have both a GII and RI in progress
 * at the same time.
 */
public class UnsharedImageState implements ImageState {
  private static final Logger logger = LogService.getLogger();
  
  private final StoppableNonReentrantLock giiLock; // used for gii
  private final StoppableReentrantReadWriteLock riLock; // used for ri
  /**
   * Using CM as a Set of keys
   */
  private volatile ConcurrentMap destroyedEntryKeys;
  private volatile boolean regionInvalidated = false;
  private volatile boolean mayDoRecovery = false;
  private volatile boolean inRecovery = false;
  private volatile boolean clearRegionFlag = false;
  private volatile RegionVersionVector clearRVV;
  private volatile boolean wasRegionClearedDuringGII = false;
  private volatile DiskAccessException dae = null;
  private volatile ConcurrentHashSet<VersionTagEntry> versionTags;
  private volatile ConcurrentHashSet<VersionSource> leftMembers;



  UnsharedImageState(final boolean isClient,
                     final boolean isReplicate,
                     final boolean mayDoRecovery,
                     CancelCriterion stopper) {
    this.riLock = isClient ? new StoppableReentrantReadWriteLock(stopper) : null;
    
    this.giiLock = isReplicate ? new StoppableNonReentrantLock(stopper) : null;
    this.destroyedEntryKeys = new ConcurrentHashMap();
    initVersionTagsSet();
    initFailedMembersSet();
    this.mayDoRecovery = mayDoRecovery;
    if (mayDoRecovery) {
      this.inRecovery = true; // default to true to fix 41147
    }
  }

  public boolean isReplicate() {
    return this.giiLock != null;
  }
  public boolean isClient() {
    return this.riLock != null;
  }
                              
  public void init() {
    if (isReplicate()) {
      this.wasRegionClearedDuringGII = false;
    }
  }
  
  public boolean getRegionInvalidated() {
    if (isReplicate()) {
      return this.regionInvalidated;
    } else {
      return false;
    }
  }
  
  public void setRegionInvalidated(boolean b) {
    if (isReplicate()) {
      this.regionInvalidated = b;
    }
  }  
  
  public void setInRecovery(boolean b) {
    if (this.mayDoRecovery) {
      this.inRecovery = b;
    }
  }
  
  public boolean getInRecovery() {
    if (this.mayDoRecovery) {
      return this.inRecovery;
    } else {
      return false;
    }
  }

  public void addDestroyedEntry(Object key) {
    // assert if ri then readLock held
    // assert if gii then lock held
    if (isReplicate() || isClient()) {
      this.destroyedEntryKeys.put(key, Boolean.TRUE);
    }
  }
  
  public void removeDestroyedEntry(Object key) {
    this.destroyedEntryKeys.remove(key);
  }
  
  public boolean hasDestroyedEntry(Object key) {
    return this.destroyedEntryKeys.containsKey(key);
  }

  public Iterator getDestroyedEntries() {
    // assert if ri then writeLock held
    // assert if gii then lock held
    Iterator result = this.destroyedEntryKeys.keySet().iterator();
    this.destroyedEntryKeys = new ConcurrentHashMap();
    return result;
  }
  
  private void initVersionTagsSet() {
    this.versionTags = new ConcurrentHashSet<VersionTagEntry>(16);
  }
  
  public void addVersionTag(Object key, VersionTag<?> tag) {
    this.versionTags.add(new VersionTagEntryImpl(key, tag.getMemberID(), tag.getRegionVersion()));
  }
  
  public Iterator<VersionTagEntry> getVersionTags() {
    Iterator<VersionTagEntry> result = this.versionTags.iterator();
    initVersionTagsSet();
    return result;
  }
  
  private void initFailedMembersSet() {
    this.leftMembers = new ConcurrentHashSet<VersionSource>(16);
  }
  
  public void addLeftMember(VersionSource<?> mbr) {
    this.leftMembers.add(mbr);
  }
  
  public Set<VersionSource> getLeftMembers() {
    Set<VersionSource> result = this.leftMembers;
    initFailedMembersSet();
    return result;
  }
  
  public boolean hasLeftMembers() {
    return this.leftMembers.size() > 0;
  }

  public void dumpDestroyedEntryKeys() {
    if (this.destroyedEntryKeys == null) {
      logger.info("region has no destroyedEntryKeys in its image state");
    } else {
      logger.info("dump of image state destroyed entry keys of size {}", this.destroyedEntryKeys.size());
      for (Iterator it = this.destroyedEntryKeys.keySet().iterator(); it.hasNext(); ) {
        Object key = it.next();
        logger.info("key={}", key);
      }
    }
  }

  /**
   *  returns count of entries that have been destroyed by concurrent operations
   *  while in token mode
   */
  public int getDestroyedEntriesCount() {
    return this.destroyedEntryKeys.size();
  }
  
  public void setClearRegionFlag(boolean isClearOn, RegionVersionVector rvv) {
    if (isReplicate()) {
      this.clearRegionFlag = isClearOn;
      if (isClearOn) {
        this.clearRVV = rvv;  // will be used to selectively clear content
        this.wasRegionClearedDuringGII = true;
      }
    }
  }

  public boolean getClearRegionFlag() {
    if (isReplicate()) {
      return this.clearRegionFlag;
    } else {
      return false;
    }
  }
  
  public RegionVersionVector getClearRegionVersionVector() {
    if (isReplicate()) {
      return this.clearRVV;
    }
    return null;
  }
  
  /**
   * Returns true if a region clear was received on the region during a GII.
   * If true is returned the the flag is cleared.
   * This method is used by unit tests.
   */
  public boolean wasRegionClearedDuringGII() {
    if (isReplicate()) {
      boolean result = this.wasRegionClearedDuringGII;
      if (result) {
        this.wasRegionClearedDuringGII = false;
      }
      return result;
    } else {
      return false;
    }
  }

  public void lockGII() {
    this.giiLock.lock();
  }

  public void unlockGII() {
    this.giiLock.unlock();
  }

  public void readLockRI() {
    this.riLock.readLock().lock();
  }
  
  public void readUnlockRI() {
    this.riLock.readLock().unlock();
  }

  public void writeLockRI() {
    this.riLock.writeLock().lock();
  }
  
  public void writeUnlockRI() {
    this.riLock.writeLock().unlock();
  }
  
  /** tracks RVV versions applied to the region during GII */
  private static final class VersionTagEntryImpl implements ImageState.VersionTagEntry {
    Object key;
    VersionSource member;
    long regionVersion;

    VersionTagEntryImpl(Object key, VersionSource<?> member, long regionVersion) {
      this.key = key;
      this.member = member;
      this.regionVersion = regionVersion;
    }
    public Object getKey() {
      return key;
    }
    public VersionSource getMemberID() {
      return member;
    }

    @Override
    public long getRegionVersion() {
      return regionVersion;
    }
    
    @Override
    public String toString() {
      return "{rv"+regionVersion+"; mbr="+member+"}";
    }
  }
}
