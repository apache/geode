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
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
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
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache
 * operations that should not be part of the "public" API of the implementing class.
 *
 * @see Cache
 * @since GemFire 7.0
 */
public interface InternalCache extends Cache, Extensible<Cache>, CacheTime, InternalCacheForTesting,
    StatisticsClockSupplier {

  /**
   * Return the member id of the local distributed system connection.
   *
   * @since GemFire 5.0
   */
  InternalDistributedMember getMyId();

  /**
   * Return list of all disk stores.
   *
   * @since GemFire prPersistSprint2
   */
  Collection<DiskStore> listDiskStores();

  Collection<DiskStore> listDiskStoresIncludingRegionOwned();

  CqService getCqService();

  <T extends CacheService> T getService(Class<T> clazz);

  <T extends CacheService> Optional<T> getOptionalService(Class<T> clazz);

  Collection<CacheService> getServices();

  /**
   * Get the CacheClientProxy SystemTimer for this cache.
   *
   * @return the timer, lazily created
   */
  SystemTimer getCCPTimer();

  /**
   * Called by the {@link CacheClientNotifier} when a client goes away.
   *
   * @since GemFire 5.7
   */
  void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client);

  /**
   * Purge the CCPTimer to prevent a large number of cancelled tasks from building up in it.
   */
  void purgeCCPTimer();

  /**
   * Return the cq/interest information for a named region, creating one if it doesn't exist.
   */
  FilterProfile getFilterProfile(String regionName);

  /**
   * @param returnDestroyedRegion if true, okay to return a destroyed region
   */
  <K, V> Region<K, V> getRegion(String path, boolean returnDestroyedRegion);

  MemoryAllocator getOffHeapStore();

  <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException;

  /**
   * Gets or lazily creates the PartitionedRegion distributed lock service. This call will
   * synchronize on this GemFireCache.
   *
   * @return the PartitionedRegion distributed lock service
   */
  DistributedLockService getPartitionedRegionLockService();

  PersistentMemberManager getPersistentMemberManager();

  /**
   * Return a list of all GatewaySenders (including the senders for internal use).
   */
  Set<GatewaySender> getAllGatewaySenders();

  CachePerfStats getCachePerfStats();

  DistributionManager getDistributionManager();

  /**
   * Set the reinitialized region and unregister it as reinitializing.
   *
   * @throws IllegalStateException if there is no region by that name registered as reinitializing.
   */
  void regionReinitialized(Region<?, ?> region);

  void setRegionByPath(String path, InternalRegion r);

  InternalResourceManager getInternalResourceManager();

  ResourceAdvisor getResourceAdvisor();

  boolean isCacheAtShutdownAll();

  /**
   * Check to see if any cache components require notification from a partitioned region.
   * Notification adds to the messaging a PR must do on each put/destroy/invalidate operation and
   * should be kept to a minimum
   *
   * @param r the partitioned region
   * @return true if the region should deliver all of its events to this cache
   */
  boolean requiresNotificationFromPR(PartitionedRegion r);

  <K, V> RegionAttributes<K, V> invokeRegionBefore(InternalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs);

  void invokeRegionAfter(InternalRegion region);

  void invokeBeforeDestroyed(InternalRegion region);

  void invokeCleanupFailedInitialization(InternalRegion region);

  TXManagerImpl getTXMgr();

  /**
   * True if the cache was closed due to being shunned by other members.
   */
  boolean forcedDisconnect();

  InternalResourceManager getInternalResourceManager(boolean checkCancellationInProgress);

  /**
   * Return true if get operations should return a copy; false if a reference to the value should be
   * returned.
   *
   * @since GemFire 4.0
   */
  boolean isCopyOnRead();

  TombstoneService getTombstoneService();

  QueryService getLocalQueryService();

  void registerInterestStarted();

  /**
   * Update stats for completion of a registerInterest operation.
   */
  void registerInterestCompleted();

  /**
   * Register the specified region name as reinitializing, creating and adding a Future for it to
   * the map.
   *
   * @throws IllegalStateException if there is already a region by that name registered.
   */
  void regionReinitializing(String fullPath);

  /**
   * Clear a reinitializing region, e.g. reinitialization failed.
   *
   * @throws IllegalStateException if cannot find reinitializing region registered by that name.
   */
  void unregisterReinitializingRegion(String fullPath);

  /**
   * Remove the specified root region.
   *
   * @param rootRgn the region to be removed
   *
   * @return true if root region was removed, false if not found
   */
  boolean removeRoot(InternalRegion rootRgn);

  /**
   * Return the {@code Executor} (thread pool) that is used to execute cache event listeners or
   * {@code null} if no pool exists.
   *
   * @since GemFire 3.5
   */
  Executor getEventThreadPool();

  /**
   * Get a reference to a Region that is reinitializing, or null if that Region is not
   * reinitializing or this thread is interrupted. If a reinitializing region is found, then this
   * method blocks until reinitialization is complete and then returns the region.
   */
  InternalRegion getReinitializingRegion(String fullPath);

  /**
   * True if durable subscriptions (registrations and queries) should be preserved.
   *
   * @since GemFire 5.7
   */
  boolean keepDurableSubscriptionsAlive();

  /**
   * Creates a CacheClosedException with the given reason.
   */
  CacheClosedException getCacheClosedException(String reason);

  /**
   * Creates a CacheClosedException with the given reason and cause.
   */
  CacheClosedException getCacheClosedException(String reason, Throwable cause);

  TypeRegistry getPdxRegistry();

  DiskStoreImpl getOrCreateDefaultDiskStore();

  /**
   * Get the ExpirationScheduler for this cache.
   *
   * @return the scheduler, lazily created
   */
  ExpirationScheduler getExpirationScheduler();

  /**
   * @return JTA TransactionManager associated with the Cache.
   * @since GemFire 4.0
   */
  TransactionManager getJTATransactionManager();

  TXManagerImpl getTxManager();

  void beginDestroy(String path, DistributedRegion region);

  void endDestroy(String path, DistributedRegion region);

  ClientMetadataService getClientMetadataService();

  URL getCacheXmlURL();

  List<File> getBackupFiles();

  <K, V> Region<K, V> getRegionByPath(String path);

  InternalRegion getInternalRegionByPath(String path);

  /**
   * @return true if cache is created using a ClientCacheFactory
   * @see #hasPool()
   */
  boolean isClient();

  InternalDistributedSystem getInternalDistributedSystem();

  /**
   * Return a set of all current partitioned regions for test hook.
   */
  Set<PartitionedRegion> getPartitionedRegions();

  void addRegionListener(RegionListener regionListener);

  void removeRegionListener(RegionListener regionListener);

  Set<RegionListener> getRegionListeners();

  CacheConfig getCacheConfig();

  /**
   * Return true if any of the GemFire services prefer PdxInstance and an application has not
   * requested getObject() on the PdxInstance.
   */
  boolean getPdxReadSerializedByAnyGemFireServices();

  void setDeclarativeCacheConfig(CacheConfig cacheConfig);

  void initializePdxRegistry();

  /**
   * Make the dynamic region factory ready. Public so it can be called from CacheCreation during xml
   * processing.
   */
  void readyDynamicRegionFactory();

  void setBackupFiles(List<File> backups);

  /**
   * Add to the map of declarable properties. Any properties that exactly match existing properties
   * for a class in the list will be discarded (no duplicate Properties allowed).
   *
   * @param mapOfNewDeclarableProps Map of the declarable properties to add
   */
  void addDeclarableProperties(Map<Declarable, Properties> mapOfNewDeclarableProps);

  void setInitializer(Declarable initializer, Properties initializerProps);

  /**
   * Method to check for GemFire client. In addition to checking for ClientCacheFactory, this method
   * checks for any defined pools.
   *
   * @return true if the cache has pools declared
   */
  boolean hasPool();

  /**
   * Create disk store factory with predefined attributes.
   *
   * @since GemFire prPersistSprint2
   */
  DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs);

  <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException;

  BackupService getBackupService();

  /**
   * Add the partitioned region to the set of tracked partitioned regions. Used to notify the
   * regions when this cache requires, or does not require notification of all region/entry events.
   */
  void addPartitionedRegion(PartitionedRegion region);

  /**
   * Remove the partitioned region from the set of tracked instances.
   */
  void removePartitionedRegion(PartitionedRegion region);

  void addDiskStore(DiskStoreImpl dsi);

  TXEntryStateFactory getTXEntryStateFactory();

  /**
   * Return the threadId/sequenceId sweeper task for this cache.
   *
   * @return the sweeper task
   */
  EventTrackerExpiryTask getEventTrackerTask();

  void removeDiskStore(DiskStoreImpl diskStore);

  void addGatewaySender(GatewaySender sender);

  void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue);

  void removeAsyncEventQueue(AsyncEventQueue asyncQueue);

  /**
   * Return the QueryMonitor for this cache based on system property MAX_QUERY_EXECUTION_TIME.
   *
   * @since GemFire 6.0
   */
  QueryMonitor getQueryMonitor();

  void close(String reason, Throwable systemFailureCause, boolean keepAlive, boolean keepDS);

  JmxManagerAdvisor getJmxManagerAdvisor();

  /**
   * Return the list of all instances of properties for Declarables with the given class name.
   *
   * @param className Class name of the declarable
   * @return List of all instances of properties found for the given declarable
   */
  List<Properties> getDeclarableProperties(String className);

  /**
   * Return the number of seconds that have elapsed since the Cache was created.
   *
   * @since GemFire 3.5
   */
  long getUpTime();

  Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions);

  /**
   * Return a set of all current regions in the cache, including buckets.
   *
   * @since GemFire 6.0
   */
  Set<InternalRegion> getAllRegions();

  DistributedRegion getRegionInDestroy(String path);

  void addRegionOwnedDiskStore(DiskStoreImpl dsi);

  DiskStoreMonitor getDiskStoreMonitor();

  void close(String reason, Throwable optionalCause);

  InternalRegion getRegionByPathForProcessing(String path);

  List<InternalCacheServer> getCacheServersAndGatewayReceiver();

  /**
   * Return true if the named global region is initializing.
   */
  boolean isGlobalRegionInitializing(String fullPath);

  DistributionAdvisor getDistributionAdvisor();

  void setQueryMonitorRequiredForResourceManager(boolean required);

  boolean isQueryMonitorDisabledForLowMemory();

  boolean isRESTServiceRunning();

  /**
   * @deprecated Please use {@code LogService.getLogger()} instead.
   */
  @Deprecated
  InternalLogWriter getInternalLogWriter();

  @Deprecated
  InternalLogWriter getSecurityInternalLogWriter();

  Set<InternalRegion> getApplicationRegions();

  void removeGatewaySender(GatewaySender sender);

  /**
   * Gets or lazily creates the GatewaySender distributed lock service.
   *
   * @return the GatewaySender distributed lock service
   */
  DistributedLockService getGatewaySenderLockService();

  /**
   * Get the properties for the given declarable.
   *
   * @param declarable The declarable
   * @return Properties found for the given declarable
   */
  Properties getDeclarableProperties(Declarable declarable);

  void setRESTServiceRunning(boolean isRESTServiceRunning);

  /**
   * TODO: is this flavor of close not used?
   */
  void close(String reason, boolean keepAlive, boolean keepDS);

  void addGatewayReceiver(GatewayReceiver receiver);

  void removeGatewayReceiver(GatewayReceiver receiver);

  InternalCacheServer addGatewayReceiverServer(GatewayReceiver receiver);

  boolean removeGatewayReceiverServer(InternalCacheServer receiverServer);

  /**
   * Enables or disables the reading of PdxInstances from all cache Regions for the thread that
   * invokes this method.
   */
  void setReadSerializedForCurrentThread(boolean value);

  PdxInstanceFactory createPdxInstanceFactory(String className, boolean expectDomainClass);

  /**
   * Blocks until no register interests are in progress.
   */
  void waitForRegisterInterestsInProgress();

  void reLoadClusterConfiguration() throws IOException, ClassNotFoundException;

  SecurityService getSecurityService();

  boolean hasPersistentRegion();

  void shutDownAll();

  void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize);

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

  /**
   * Perform initialization, solve the early escaped reference problem by putting publishing
   * references to this instance in this method (vs. the constructor).
   */
  void initialize();

  void throwCacheExistsException();

  MeterRegistry getMeterRegistry();

  /**
   * Generate XML for the cache before shutting down due to forced disconnect.
   */
  void saveCacheXmlForReconnect();

  InternalQueryService getInternalQueryService();

  default <K, V> InternalRegionFactory<K, V> createInternalRegionFactory() {
    return (InternalRegionFactory) createRegionFactory();
  }

  default <K, V> InternalRegionFactory<K, V> createInternalRegionFactory(RegionShortcut shortcut) {
    return (InternalRegionFactory) createRegionFactory(shortcut);
  }
}
