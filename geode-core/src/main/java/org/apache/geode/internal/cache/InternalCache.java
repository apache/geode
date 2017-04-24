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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.TransactionManager;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache
 * operations that should not be part of the "public" API of the implementing class.
 *
 * @see org.apache.geode.cache.Cache
 * @since GemFire 7.0
 */
public interface InternalCache extends Cache, Extensible<Cache> {

  DistributedMember getMyId();

  Collection<DiskStoreImpl> listDiskStores();

  Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned();

  CqService getCqService();

  <T extends CacheService> T getService(Class<T> clazz);

  SystemTimer getCCPTimer();

  void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client);

  void purgeCCPTimer();

  FilterProfile getFilterProfile(String regionName);

  Region getRegion(String path, boolean returnDestroyedRegion);

  MemoryAllocator getOffHeapStore();

  <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException;

  DistributedLockService getPartitionedRegionLockService();

  PersistentMemberManager getPersistentMemberManager();

  Set<GatewaySender> getAllGatewaySenders();

  CachePerfStats getCachePerfStats();

  DM getDistributionManager();

  void regionReinitialized(Region region);

  void setRegionByPath(String path, LocalRegion r);

  InternalResourceManager getInternalResourceManager();

  ResourceAdvisor getResourceAdvisor();

  boolean isCacheAtShutdownAll();

  boolean requiresNotificationFromPR(PartitionedRegion r);

  <K, V> RegionAttributes<K, V> invokeRegionBefore(LocalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs);

  void invokeRegionAfter(LocalRegion region);

  TXManagerImpl getTXMgr();

  boolean forcedDisconnect();

  InternalResourceManager getInternalResourceManager(boolean checkCancellationInProgress);

  boolean isCopyOnRead();

  TombstoneService getTombstoneService();

  QueryService getLocalQueryService();

  void registerInterestStarted();

  void registerInterestCompleted();

  void regionReinitializing(String fullPath);

  void unregisterReinitializingRegion(String fullPath);

  boolean removeRoot(LocalRegion rootRgn);

  Executor getEventThreadPool();

  LocalRegion getReinitializingRegion(String fullPath);

  boolean keepDurableSubscriptionsAlive();

  CacheClosedException getCacheClosedException(String reason, Throwable cause);

  TypeRegistry getPdxRegistry();

  DiskStoreImpl getOrCreateDefaultDiskStore();

  ExpirationScheduler getExpirationScheduler();

  TransactionManager getJTATransactionManager();

  TXManagerImpl getTxManager();

  void beginDestroy(String path, DistributedRegion region);

  void endDestroy(String path, DistributedRegion region);

  ClientMetadataService getClientMetadataService();

  long cacheTimeMillis();

  void clearBackupManager();

  URL getCacheXmlURL();

  List<File> getBackupFiles();

  LocalRegion getRegionByPath(String path);

  boolean isClient();

  InternalDistributedSystem getInternalDistributedSystem();

  Set<PartitionedRegion> getPartitionedRegions();

  void addRegionListener(RegionListener l);

  void removeRegionListener(RegionListener l);

  Set<RegionListener> getRegionListeners();

  CacheConfig getCacheConfig();
}
