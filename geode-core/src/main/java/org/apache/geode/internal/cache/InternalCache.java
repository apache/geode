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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.TransactionManager;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClockSupplier;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache
 * operations that should not be part of the "public" API of the implementing class.
 *
 * @see org.apache.geode.cache.Cache
 * @since GemFire 7.0
 */
public interface InternalCache extends Cache, Extensible<Cache>, CacheTime, ReconnectableCache,
    StatisticsClockSupplier {

  InternalDistributedMember getMyId();

  Collection<DiskStore> listDiskStores();

  Collection<DiskStore> listDiskStoresIncludingRegionOwned();

  CqService getCqService();

  <T extends CacheService> T getService(Class<T> clazz);

  <T extends CacheService> Optional<T> getOptionalService(Class<T> clazz);

  Collection<CacheService> getServices();

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

  DistributionManager getDistributionManager();

  void regionReinitialized(Region region);

  void setRegionByPath(String path, InternalRegion r);

  InternalResourceManager getInternalResourceManager();

  ResourceAdvisor getResourceAdvisor();

  boolean isCacheAtShutdownAll();

  boolean requiresNotificationFromPR(PartitionedRegion r);

  <K, V> RegionAttributes<K, V> invokeRegionBefore(InternalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs);

  void invokeRegionAfter(InternalRegion region);

  void invokeBeforeDestroyed(InternalRegion region);

  void invokeCleanupFailedInitialization(InternalRegion region);

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

  boolean removeRoot(InternalRegion rootRgn);

  Executor getEventThreadPool();

  InternalRegion getReinitializingRegion(String fullPath);

  boolean keepDurableSubscriptionsAlive();

  CacheClosedException getCacheClosedException(String reason);

  CacheClosedException getCacheClosedException(String reason, Throwable cause);

  TypeRegistry getPdxRegistry();

  DiskStoreImpl getOrCreateDefaultDiskStore();

  ExpirationScheduler getExpirationScheduler();

  TransactionManager getJTATransactionManager();

  TXManagerImpl getTxManager();

  void beginDestroy(String path, DistributedRegion region);

  void endDestroy(String path, DistributedRegion region);

  ClientMetadataService getClientMetadataService();

  @Override
  long cacheTimeMillis();

  URL getCacheXmlURL();

  List<File> getBackupFiles();

  InternalRegion getRegionByPath(String path);

  boolean isClient();

  InternalDistributedSystem getInternalDistributedSystem();

  Set<PartitionedRegion> getPartitionedRegions();

  void addRegionListener(RegionListener regionListener);

  void removeRegionListener(RegionListener regionListener);

  Set<RegionListener> getRegionListeners();

  CacheConfig getCacheConfig();

  boolean getPdxReadSerializedByAnyGemFireServices();

  void setDeclarativeCacheConfig(CacheConfig cacheConfig);

  void initializePdxRegistry();

  void readyDynamicRegionFactory();

  void setBackupFiles(List<File> backups);

  void addDeclarableProperties(final Map<Declarable, Properties> mapOfNewDeclarableProps);

  void setInitializer(Declarable initializer, Properties initializerProps);

  boolean hasPool();

  DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs);

  void determineDefaultPool();

  <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException;

  BackupService getBackupService();

  Throwable getDisconnectCause();

  void addPartitionedRegion(PartitionedRegion region);

  void removePartitionedRegion(PartitionedRegion region);

  void addDiskStore(DiskStoreImpl dsi);

  TXEntryStateFactory getTXEntryStateFactory();

  EventTrackerExpiryTask getEventTrackerTask();

  void removeDiskStore(DiskStoreImpl diskStore);

  void addGatewaySender(GatewaySender sender);

  void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue);

  void removeAsyncEventQueue(AsyncEventQueue asyncQueue);

  QueryMonitor getQueryMonitor();

  void close(String reason, Throwable systemFailureCause, boolean keepAlive, boolean keepDS);

  JmxManagerAdvisor getJmxManagerAdvisor();

  List<Properties> getDeclarableProperties(final String className);

  int getUpTime();

  Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions);

  Set<InternalRegion> getAllRegions();

  DistributedRegion getRegionInDestroy(String path);

  void addRegionOwnedDiskStore(DiskStoreImpl dsi);

  DiskStoreMonitor getDiskStoreMonitor();

  void close(String reason, Throwable optionalCause);

  InternalRegion getRegionByPathForProcessing(String path);

  List<InternalCacheServer> getCacheServersAndGatewayReceiver();

  boolean isGlobalRegionInitializing(String fullPath);

  DistributionAdvisor getDistributionAdvisor();

  void setQueryMonitorRequiredForResourceManager(boolean required);

  boolean isQueryMonitorDisabledForLowMemory();

  boolean isRESTServiceRunning();

  InternalLogWriter getInternalLogWriter();

  InternalLogWriter getSecurityInternalLogWriter();

  Set<InternalRegion> getApplicationRegions();

  void removeGatewaySender(GatewaySender sender);

  DistributedLockService getGatewaySenderLockService();

  RestAgent getRestAgent();

  Properties getDeclarableProperties(final Declarable declarable);

  void setRESTServiceRunning(boolean isRESTServiceRunning);

  void close(String reason, boolean keepAlive, boolean keepDS);

  void addGatewayReceiver(GatewayReceiver receiver);

  void removeGatewayReceiver(GatewayReceiver receiver);

  InternalCacheServer addGatewayReceiverServer(GatewayReceiver receiver);

  boolean removeCacheServer(CacheServer cacheServer);

  boolean removeGatewayReceiverServer(InternalCacheServer receiverServer);

  /**
   * A test-hook allowing you to alter the cache setting established by
   * CacheFactory.setPdxReadSerialized()
   *
   * @deprecated tests using this method should be refactored to not require it
   */
  void setReadSerializedForTest(boolean value);

  /**
   * Enables or disables the reading of PdxInstances from all cache Regions for the thread that
   * invokes this method.
   */
  void setReadSerializedForCurrentThread(boolean value);

  PdxInstanceFactory createPdxInstanceFactory(String className, boolean expectDomainClass);

  void waitForRegisterInterestsInProgress();

  void reLoadClusterConfiguration() throws IOException, ClassNotFoundException;

  SecurityService getSecurityService();

  boolean hasPersistentRegion();

  void shutDownAll();

  void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize);

  @Override
  InternalQueryService getQueryService();

  Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly);

  void closeDiskStores();

  /**
   * If obj is a PdxInstance and pdxReadSerialized is not true
   * then convert obj by calling PdxInstance.getObject.
   *
   * @return either the original obj if no conversion was needed;
   *         or the result of calling PdxInstance.getObject on obj.
   */
  Object convertPdxInstanceIfNeeded(Object obj, boolean preferCD);

  Boolean getPdxReadSerializedOverride();

  void setPdxReadSerializedOverride(boolean pdxReadSerialized);

  /**
   * Returns a version of InternalCache that can only access
   * application visible regions. Any regions created internally
   * by Geode will not be accessible from the returned cache.
   */
  InternalCacheForClientAccess getCacheForProcessingClientRequests();

  void initialize();

  void throwCacheExistsException();

  MeterRegistry getMeterRegistry();

  void saveCacheXmlForReconnect();
}
