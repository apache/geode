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
package com.gemstone.gemfire.internal.cache.persistence;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.internal.cache.DiskEntry;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionMap;
import com.gemstone.gemfire.internal.cache.DiskEntry.RecoveredEntry;
import com.gemstone.gemfire.internal.cache.LocalRegion.RegionEntryCallback;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * Used by the disk code to store recovered data into the cache.
 * The primary implementor of this interface is LocalRegion.
 *
 *
 * @since GemFire prPersistSprint3
 */
public interface DiskRecoveryStore {
  public DiskRegionView getDiskRegionView();
  public DiskEntry getDiskEntry(Object key);
  public void foreachRegionEntry(LocalRegion.RegionEntryCallback callback);
  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re);
  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re);
  public void destroyRecoveredEntry(Object key);
  public boolean lruLimitExceeded();
  public void copyRecoveredEntries(RegionMap rm);
  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk);
  public int calculateValueSize(Object val);
  public int calculateRegionEntryValueSize(RegionEntry re);
  public RegionMap getRegionMap();
  public void handleDiskAccessException(DiskAccessException dae);
  public EvictionAttributes getEvictionAttributes();
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk);
  public void recordRecoveredGCVersion(VersionSource member, long gcVersion);
  public void recordRecoveredVersonHolder(VersionSource member,
      RegionVersionHolder versionHolder, boolean latestOplog);
  public void recordRecoveredVersionTag(VersionTag tag);
  public long getVersionForMember(VersionSource member);
  public void setRVVTrusted(boolean rvvTrusted);
  public DiskStoreImpl getDiskStore();
}
