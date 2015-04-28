/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.util.EnumSet;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.cache.DiskId;
import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.cache.RegionMap;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;

/**
 * Contract DiskInitFile needs a DiskRegion to provide.
 */
public interface DiskRegionView extends PersistentMemberView, RegionEntryContext {
  public DiskStoreImpl getDiskStore();
  public String getName();
  public long getId();
  public long getClearOplogEntryId();
  public void setClearOplogEntryId(long v);
  public RegionVersionVector getClearRVV();
  public void setClearRVV(RegionVersionVector clearRVV);
  public PersistentMemberID addMyInitializingPMID(PersistentMemberID pmid);
  public void markInitialized();
  public boolean addOnlineMember(PersistentMemberID pmid);
  public boolean addOfflineMember(PersistentMemberID pmid);
  public boolean addOfflineAndEqualMember(PersistentMemberID pmid);
  public boolean rmOnlineMember(PersistentMemberID pmid);
  public boolean rmEqualMember(PersistentMemberID pmid);
  public boolean rmOfflineMember(PersistentMemberID pmid);
  public void markBeginDestroyRegion();
  public void markBeginDestroyDataStorage();
  public void markEndDestroyDataStorage();
  public void markEndDestroyRegion();
  public boolean isBackup();
  public boolean hasConfigChanged();
  public void setConfigChanged(boolean v);
  public void setConfig(byte lruAlgorithm, byte lruAction, int lruLimit,
                        int concurrencyLevel, int initialCapacity,
                        float loadFactor, boolean statisticsEnabled,
                        boolean isBucket, EnumSet<DiskRegionFlag> flags,
                        String partitionName, int startingBucketId,
                        String compressorClassName);
  public byte getLruAlgorithm();
  public byte getLruAction();
  public int getLruLimit();
  public int getConcurrencyLevel();
  public int getInitialCapacity();
  public float getLoadFactor();
  public boolean getStatisticsEnabled();
  public boolean isBucket();
  public EnumSet<DiskRegionFlag> getFlags();
  public String getPartitionName();
  public int getStartingBucketId();
  
  /**
   * Return true if this region has data in disk to allow it be be recovered.
   * Return false if it is a brand new region that has not yet written data to disk.
   */
  public boolean isRecreated();
  public void prepareForRecovery();
  public RegionMap getRecoveredEntryMap();
  public boolean isReadyForRecovery();
  public int getRecoveredEntryCount();
  public void incRecoveredEntryCount();
  public void initRecoveredEntryCount();
  public void copyExistingRegionMap(LocalRegion drs);

  public void incNumOverflowOnDisk(long delta);
  public void incNumEntriesInVM(long delta);
  public long getNumOverflowOnDisk();
  public long getNumOverflowBytesOnDisk();
  public long getNumEntriesInVM();
  public void acquireReadLock();
  public Object getRaw(DiskId did);
  public void releaseReadLock();
  public boolean didClearCountChange();
  public CancelCriterion getCancelCriterion();
  public boolean isSync();
  /** Update stats*/
  public void endRead(long start, long end, long bytesRead);
  public boolean isRegionClosed();
  public void incNumOverflowBytesOnDisk(long overflowBytesOnDiskDelta);
  public RegionVersionVector getRegionVersionVector();
  public boolean isEntriesMapIncompatible();
  public String getCompressorClassName();
  public void oplogRecovered(long oplogId);
}
