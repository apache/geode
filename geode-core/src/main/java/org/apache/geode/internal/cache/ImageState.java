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

import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * State object used during getInitialImage Locked during clean up of destroyed tokens.
 *
 */
public interface ImageState /* extends Lock */ {

  boolean getRegionInvalidated();

  void setRegionInvalidated(boolean b);

  void setInRecovery(boolean b);

  boolean getInRecovery();

  void addDestroyedEntry(Object key);

  void removeDestroyedEntry(Object key);

  boolean hasDestroyedEntry(Object key);

  java.util.Iterator<Object> getDestroyedEntries();

  /**
   * returns count of entries that have been destroyed by concurrent operations while in token mode
   */
  int getDestroyedEntriesCount();

  void setClearRegionFlag(boolean isClearOn, RegionVersionVector rvv);

  boolean getClearRegionFlag();

  RegionVersionVector getClearRegionVersionVector();

  boolean wasRegionClearedDuringGII();

  void addVersionTag(Object key, VersionTag<?> tag);

  Iterator<VersionTagEntry> getVersionTags();

  void addLeftMember(VersionSource<?> mbr);

  Set<VersionSource> getLeftMembers();

  boolean hasLeftMembers();

  void lockGII();

  void unlockGII();

  void readLockRI();

  void readUnlockRI();

  void writeLockRI();

  void writeUnlockRI();

  boolean isReplicate();

  boolean isClient();

  void init();

  interface VersionTagEntry {
    Object getKey();

    VersionSource getMemberID();

    long getRegionVersion();
  }

}
