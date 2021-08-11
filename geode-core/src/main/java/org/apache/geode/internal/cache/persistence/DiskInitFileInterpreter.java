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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.serialization.KnownVersion;

public interface DiskInitFileInterpreter {

  boolean isClosing();

  String getNameForError();

  void cmnInstantiatorId(int id, Class<?> c, Class<?> ic);

  void cmnInstantiatorId(int id, String cn, String icn);

  void cmnDataSerializerId(Class<? extends DataSerializer> dsc);

  void cmnOnlineMemberId(long drId, PersistentMemberID pmid);

  void cmnOfflineMemberId(long drId, PersistentMemberID pmid);

  void cmdOfflineAndEqualMemberId(long drId, PersistentMemberID pmid);

  void cmnRmMemberId(long drId, PersistentMemberID pmid);

  void cmnAddMyInitializingPMID(long drId, PersistentMemberID pmid);

  void cmnMarkInitialized(long drId);

  void cmnCreateRegion(long drId, String regName);

  void cmnBeginDestroyRegion(long drId);

  void cmnEndDestroyRegion(long drId);

  void cmnBeginPartialDestroyRegion(long drId);

  void cmnEndPartialDestroyRegion(long drId);

  void cmnClearRegion(long drId, long clearOplogEntryId);

  void cmnClearRegion(long drId,
      ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> clearRVV);

  void cmnCrfCreate(long oplogId);

  void cmnDrfCreate(long oplogId);

  boolean cmnCrfDelete(long oplogId);

  boolean cmnDrfDelete(long oplogId);

  void cmnRegionConfig(long drId, byte lruAlgorithm, byte lruAction, int lruLimit,
      int concurrencyLevel, int initialCapacity, float loadFactor, boolean statisticsEnabled,
      boolean isBucket, EnumSet<DiskRegionFlag> flags, String partitionName, int startingBucketId,
      String compressorClassName, boolean offHeap);

  void cmnKrfCreate(long oplogId);

  boolean cmnPRCreate(String name, PRPersistentConfig config);

  boolean cmnPRDestroy(String name);

  void cmnAddCanonicalMemberId(int id, Object object);

  void cmnDiskStoreID(DiskStoreID result);

  boolean cmnRevokeDiskStoreId(PersistentMemberPattern id);

  void cmnGemfireVersion(KnownVersion version);
}
