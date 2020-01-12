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

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionMap;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * Used by the disk code to store recovered data into the cache. The primary implementor of this
 * interface is LocalRegion.
 *
 * @since GemFire prPersistSprint3
 */
public interface DiskRecoveryStore {
  DiskRegionView getDiskRegionView();

  DiskEntry getDiskEntry(Object key);

  void foreachRegionEntry(LocalRegion.RegionEntryCallback callback);

  DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re);

  DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re);

  void destroyRecoveredEntry(Object key);

  boolean lruLimitExceeded();

  void copyRecoveredEntries(RegionMap rm);

  void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk);

  int calculateValueSize(Object val);

  RegionMap getRegionMap();

  void handleDiskAccessException(DiskAccessException dae);

  EvictionAttributes getEvictionAttributes();

  void initializeStats(long numEntriesInVM, long numOverflowOnDisk, long numOverflowBytesOnDisk);

  void recordRecoveredGCVersion(VersionSource member, long gcVersion);

  void recordRecoveredVersionHolder(VersionSource member, RegionVersionHolder versionHolder,
      boolean latestOplog);

  void recordRecoveredVersionTag(VersionTag tag);

  long getVersionForMember(VersionSource member);

  void setRVVTrusted(boolean rvvTrusted);

  DiskStoreImpl getDiskStore();
}
