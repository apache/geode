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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.StoppableNonReentrantLock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;

/**
 * Used on distributed replicated regions to track GII and various state. Also used on pool regions
 * to track register interest. Note that currently a region will never have both a GII and RI in
 * progress at the same time.
 */
public class UnsharedImageState implements ImageState {

  private final StoppableNonReentrantLock giiLock; // used for gii
  private final StoppableReentrantReadWriteLock riLock; // used for ri
  /**
   * Using CM as a Set of keys
   */
  private volatile ConcurrentMap<Object, Boolean> destroyedEntryKeys;
  private volatile boolean regionInvalidated = false;
  private final boolean mayDoRecovery;
  private volatile boolean inRecovery = false;
  private volatile boolean clearRegionFlag = false;
  private volatile RegionVersionVector clearRVV;
  private volatile boolean wasRegionClearedDuringGII = false;
  private volatile Set<VersionTagEntry> versionTags;
  private volatile Set<VersionSource> leftMembers;

  UnsharedImageState(final boolean isClient, final boolean isReplicate, final boolean mayDoRecovery,
      CancelCriterion stopper) {
    riLock = isClient ? new StoppableReentrantReadWriteLock(stopper) : null;
    giiLock = isReplicate ? new StoppableNonReentrantLock(stopper) : null;
    destroyedEntryKeys = new ConcurrentHashMap<>();
    initVersionTagsSet();
    initFailedMembersSet();
    this.mayDoRecovery = mayDoRecovery;
    if (mayDoRecovery) {
      inRecovery = true; // default to true to fix 41147
    }
  }

  @Override
  public boolean isReplicate() {
    return giiLock != null;
  }

  @Override
  public boolean isClient() {
    return riLock != null;
  }

  @Override
  public void init() {
    if (isReplicate()) {
      wasRegionClearedDuringGII = false;
    }
  }

  @Override
  public boolean getRegionInvalidated() {
    if (isReplicate()) {
      return regionInvalidated;
    } else {
      return false;
    }
  }

  @Override
  public void setRegionInvalidated(boolean b) {
    if (isReplicate()) {
      regionInvalidated = b;
    }
  }

  @Override
  public void setInRecovery(boolean b) {
    if (mayDoRecovery) {
      inRecovery = b;
    }
  }

  @Override
  public boolean getInRecovery() {
    if (mayDoRecovery) {
      return inRecovery;
    } else {
      return false;
    }
  }

  @Override
  public void addDestroyedEntry(Object key) {
    // assert if ri then readLock held
    // assert if gii then lock held
    if (isReplicate() || isClient()) {
      destroyedEntryKeys.put(key, Boolean.TRUE);
    }
  }

  @Override
  public void removeDestroyedEntry(Object key) {
    destroyedEntryKeys.remove(key);
  }

  @Override
  public boolean hasDestroyedEntry(Object key) {
    return destroyedEntryKeys.containsKey(key);
  }

  @Override
  public Iterator<Object> getDestroyedEntries() {
    // assert if ri then writeLock held
    // assert if gii then lock held
    Iterator<Object> result = destroyedEntryKeys.keySet().iterator();
    destroyedEntryKeys = new ConcurrentHashMap<>();
    return result;
  }

  private void initVersionTagsSet() {
    versionTags = ConcurrentHashMap.newKeySet(16);
  }

  @Override
  public void addVersionTag(Object key, VersionTag<?> tag) {
    versionTags.add(new VersionTagEntryImpl(key, tag.getMemberID(), tag.getRegionVersion()));
  }

  @Override
  public Iterator<VersionTagEntry> getVersionTags() {
    Iterator<VersionTagEntry> result = versionTags.iterator();
    initVersionTagsSet();
    return result;
  }

  private void initFailedMembersSet() {
    leftMembers = ConcurrentHashMap.newKeySet(16);
  }

  @Override
  public void addLeftMember(VersionSource<?> mbr) {
    leftMembers.add(mbr);
  }

  @Override
  public Set<VersionSource> getLeftMembers() {
    Set<VersionSource> result = leftMembers;
    initFailedMembersSet();
    return result;
  }

  @Override
  public boolean hasLeftMembers() {
    return leftMembers.size() > 0;
  }

  /**
   * returns count of entries that have been destroyed by concurrent operations while in token mode
   */
  @Override
  public int getDestroyedEntriesCount() {
    return destroyedEntryKeys.size();
  }

  @Override
  public void setClearRegionFlag(boolean isClearOn, RegionVersionVector rvv) {
    if (isReplicate()) {
      clearRegionFlag = isClearOn;
      if (isClearOn) {
        clearRVV = rvv; // will be used to selectively clear content
        wasRegionClearedDuringGII = true;
      }
    }
  }

  @Override
  public boolean getClearRegionFlag() {
    if (isReplicate()) {
      return clearRegionFlag;
    } else {
      return false;
    }
  }

  @Override
  public RegionVersionVector getClearRegionVersionVector() {
    if (isReplicate()) {
      return clearRVV;
    }
    return null;
  }

  /**
   * Returns true if a region clear was received on the region during a GII. If true is returned the
   * the flag is cleared. This method is used by unit tests.
   */
  @Override
  public boolean wasRegionClearedDuringGII() {
    if (isReplicate()) {
      boolean result = wasRegionClearedDuringGII;
      if (result) {
        wasRegionClearedDuringGII = false;
      }
      return result;
    } else {
      return false;
    }
  }

  @Override
  public void lockGII() {
    giiLock.lock();
  }

  @Override
  public void unlockGII() {
    giiLock.unlock();
  }

  @Override
  public void readLockRI() {
    riLock.readLock().lock();
  }

  @Override
  public void readUnlockRI() {
    riLock.readLock().unlock();
  }

  @Override
  public void writeLockRI() {
    riLock.writeLock().lock();
  }

  @Override
  public void writeUnlockRI() {
    riLock.writeLock().unlock();
  }

  /** tracks RVV versions applied to the region during GII */
  private static class VersionTagEntryImpl implements ImageState.VersionTagEntry {
    Object key;
    VersionSource member;
    long regionVersion;

    VersionTagEntryImpl(Object key, VersionSource<?> member, long regionVersion) {
      this.key = key;
      this.member = member;
      this.regionVersion = regionVersion;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public VersionSource getMemberID() {
      return member;
    }

    @Override
    public long getRegionVersion() {
      return regionVersion;
    }

    @Override
    public String toString() {
      return "{rv" + regionVersion + "; mbr=" + member + "}";
    }
  }
}
