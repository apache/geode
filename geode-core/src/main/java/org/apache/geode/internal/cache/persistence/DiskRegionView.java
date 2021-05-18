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
package org.apache.geode.internal.cache.persistence;

import java.util.EnumSet;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.EvictableRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.versions.RegionVersionVector;

/**
 * Contract DiskInitFile needs a DiskRegion to provide.
 */
public interface DiskRegionView extends PersistentMemberView, RegionEntryContext, EvictableRegion {
  DiskStoreImpl getDiskStore();

  String getName();

  long getId();

  long getClearOplogEntryId();

  void setClearOplogEntryId(long v);

  RegionVersionVector<DiskStoreID> getClearRVV();

  void setClearRVV(RegionVersionVector clearRVV);

  PersistentMemberID addMyInitializingPMID(PersistentMemberID pmid);

  void markInitialized();

  boolean addOnlineMember(PersistentMemberID pmid);

  boolean addOfflineMember(PersistentMemberID pmid);

  boolean addOfflineAndEqualMember(PersistentMemberID pmid);

  boolean rmOnlineMember(PersistentMemberID pmid);

  boolean rmEqualMember(PersistentMemberID pmid);

  boolean rmOfflineMember(PersistentMemberID pmid);

  void markBeginDestroyRegion();

  void markBeginDestroyDataStorage();

  void markEndDestroyDataStorage();

  void markEndDestroyRegion();

  boolean isBackup();

  boolean hasConfigChanged();

  void setConfigChanged(boolean v);

  void setConfig(byte lruAlgorithm, byte lruAction, int lruLimit, int concurrencyLevel,
      int initialCapacity, float loadFactor, boolean statisticsEnabled, boolean isBucket,
      EnumSet<DiskRegionFlag> flags, String partitionName, int startingBucketId,
      String compressorClassName, boolean offHeap);

  byte getLruAlgorithm();

  byte getLruAction();

  int getLruLimit();

  int getConcurrencyLevel();

  int getInitialCapacity();

  float getLoadFactor();

  boolean getStatisticsEnabled();

  boolean isBucket();

  EnumSet<DiskRegionFlag> getFlags();

  String getPartitionName();

  int getStartingBucketId();

  /**
   * Return true if this region has data in disk to allow it be be recovered. Return false if it is
   * a brand new region that has not yet written data to disk.
   */
  boolean isRecreated();

  void prepareForRecovery();

  RegionMap getRecoveredEntryMap();

  boolean isReadyForRecovery();

  int getRecoveredEntryCount();

  void incRecoveredEntryCount();

  void initRecoveredEntryCount();

  void copyExistingRegionMap(LocalRegion drs);

  void incNumOverflowOnDisk(long delta);

  void incNumEntriesInVM(long delta);

  long getNumOverflowOnDisk();

  long getNumOverflowBytesOnDisk();

  long getNumEntriesInVM();

  void acquireReadLock();

  Object getRaw(DiskId did);

  void releaseReadLock();

  boolean didClearCountChange();

  CancelCriterion getCancelCriterion();

  boolean isSync();

  /** Update stats */
  void endRead(long start, long end, long bytesRead);

  boolean isRegionClosed();

  void incNumOverflowBytesOnDisk(long overflowBytesOnDiskDelta);

  RegionVersionVector getRegionVersionVector();

  boolean isEntriesMapIncompatible();

  String getCompressorClassName();

  void oplogRecovered(long oplogId);

  void close();
}
