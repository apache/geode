/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;

/**
 * @author dsmith
 *
 */
public interface DiskInitFileInterpreter {

  boolean isClosing();

  String getNameForError();

  /**
   * @param id
   * @param c
   * @param ic
   */
  void cmnInstantiatorId(int id, Class c, Class ic);

  /**
   * @param id
   * @param cn
   * @param icn
   */
  void cmnInstantiatorId(int id, String cn, String icn);

  /**
   * @param dsc
   */
  void cmnDataSerializerId(Class dsc);

  /**
   * @param drId
   * @param pmid
   */
  void cmnOnlineMemberId(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   * @param pmid
   */
  void cmnOfflineMemberId(long drId, PersistentMemberID pmid);

  void cmdOfflineAndEqualMemberId(long drId, PersistentMemberID pmid);
  /**
   * @param drId
   * @param pmid
   */
  void cmnRmMemberId(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   * @param pmid
   */
  void cmnAddMyInitializingPMID(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   */
  void cmnMarkInitialized(long drId);

  /**
   * @param drId
   * @param regName
   */
  void cmnCreateRegion(long drId, String regName);

  /**
   * @param drId
   */
  void cmnBeginDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnEndDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnBeginPartialDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnEndPartialDestroyRegion(long drId);

  /**
   * @param drId
   * @param clearOplogEntryId
   */
  void cmnClearRegion(long drId, long clearOplogEntryId);
  
  /**
   * @param drId
   * @param clearRVV
   */
  void cmnClearRegion(long drId, ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> clearRVV);

  public void cmnCrfCreate(long oplogId);
  public void cmnDrfCreate(long oplogId);
  public boolean cmnCrfDelete(long oplogId);
  public boolean cmnDrfDelete(long oplogId);
  
  public void cmnRegionConfig(long drId, byte lruAlgorithm, byte lruAction, int lruLimit,
                              int concurrencyLevel, int initialCapacity,
                              float loadFactor, boolean statisticsEnabled,
                              boolean isBucket, EnumSet<DiskRegionFlag> flags,
                              String partitionName, int startingBucketId,
                              String compressorClassName);

  void cmnKrfCreate(long oplogId);
  
  boolean cmnPRCreate(String name, PRPersistentConfig config);
  
  boolean cmnPRDestroy(String name);

  void cmnAddCanonicalMemberId(int id, Object object);

  void cmnDiskStoreID(DiskStoreID result);

  boolean cmnRevokeDiskStoreId(PersistentMemberPattern id);
  
  void cmnGemfireVersion(Version version);
}
