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

import java.io.IOException;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;

/**
 * Interface to be used instead of type-casting to LocalRegion.
 *
 * <p>
 * The following interfaces are implemented by LocalRegion and may need to be extended by
 * InternalRegion to completely allow code to move to using InternalRegion:
 * <ul>
 * <li>RegionAttributes
 * <li>AttributesMutator
 * <li>CacheStatistics
 * <li>DataSerializableFixedID
 * <li>RegionEntryContext
 * <li>Extensible
 * </ul>
 */
public interface InternalRegion<K, V>
    extends Region<K, V>, HasCachePerfStats, RegionEntryContext, RegionAttributes, HasDiskRegion {

  CachePerfStats getCachePerfStats();

  DiskRegion getDiskRegion();

  RegionEntry getRegionEntry(K key);

  RegionVersionVector getVersionVector();

  long cacheTimeMillis();

  Object getValueInVM(Object key) throws EntryNotFoundException;

  Object getValueOnDisk(Object key) throws EntryNotFoundException;

  void dispatchListenerEvent(EnumListenerEvent op, InternalCacheEvent event);

  boolean isUsedForPartitionedRegionAdmin();

  ImageState getImageState();

  VersionSource getVersionMember();

  long updateStatsForPut(RegionEntry entry, long lastModified, boolean lruRecentUse);

  FilterProfile getFilterProfile();

  ServerRegionProxy getServerProxy();

  void unscheduleTombstone(RegionEntry entry);

  void scheduleTombstone(RegionEntry entry, VersionTag destroyedVersion);

  boolean isEntryExpiryPossible();

  void addExpiryTaskIfAbsent(RegionEntry entry);

  DistributionManager getDistributionManager();

  void generateAndSetVersionTag(InternalCacheEvent event, RegionEntry entry);

  boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException;

  void recordEvent(InternalCacheEvent event);

  boolean isProxy();

  IndexManager getIndexManager();

  boolean isThisRegionBeingClosedOrDestroyed();

  CancelCriterion getCancelCriterion();

  boolean isIndexCreationThread();

  int updateSizeOnEvict(Object key, int oldSize);

  RegionEntry basicGetEntry(Object key);

  void invokePutCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways);

  void invokeDestroyCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways);

  void invokeInvalidateCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent);

  long getEvictions();

  Region createSubregion(String subregionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException;

  void addCacheServiceProfile(CacheServiceProfile profile);

}
