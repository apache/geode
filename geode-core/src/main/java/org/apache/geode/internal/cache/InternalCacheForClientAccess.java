/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.transaction.TransactionManager;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.CancelCriterion;
import org.apache.geode.LogWriter;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.admin.ClientHealthMonitoringRegion;
import org.apache.geode.internal.cache.InitialImageOperation.Entry;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.NotAuthorizedException;

/**
 * This class delegates all methods to the InternalCache instance
 * it wraps. Any regions returned will be checked and if they are
 * internal an exception is thrown if they are.
 *
 * <p>
 * Note: an instance of this class should be used by servers that
 * process requests from clients that contains region names to prevent
 * the client from directly accessing internal regions.
 */
public class InternalCacheForClientAccess implements InternalCache {

  private final InternalCache delegate;

  public InternalCacheForClientAccess(InternalCache delegate) {
    this.delegate = delegate;
  }

  private void checkForInternalRegion(Region<?, ?> r) {
    if (r == null) {
      return;
    }
    InternalRegion ir = (InternalRegion) r;
    if (ir.isInternalRegion()
        && !r.getName().equals(DynamicRegionFactory.DYNAMIC_REGION_LIST_NAME)
        && !r.getName().equals(ClientHealthMonitoringRegion.ADMIN_REGION_NAME)) {
      throw new NotAuthorizedException("The region " + r.getName()
          + " is an internal region that a client is never allowed to access");
    }
  }

  @SuppressWarnings("unchecked")
  private void checkSetOfRegions(Set regions) {
    for (Region r : (Set<Region>) regions) {
      checkForInternalRegion(r);
    }
  }

  @Override
  public <K, V> Region<K, V> getRegion(String path) {
    Region<K, V> result = delegate.getRegion(path);
    checkForInternalRegion(result);
    return result;
  }

  /**
   * This method can be used to locate an internal region.
   * It should not be invoked with a region name obtained
   * from a client.
   */
  public <K, V> Region<K, V> getInternalRegion(String path) {
    return delegate.getRegion(path);
  }

  @Override
  public <K, V> Region<K, V> getRegion(String path, boolean returnDestroyedRegion) {
    Region result = delegate.getRegion(path, returnDestroyedRegion);
    checkForInternalRegion(result);
    return uncheckedCast(result);
  }

  @Override
  public InternalRegion getReinitializingRegion(String fullPath) {
    InternalRegion result = delegate.getReinitializingRegion(fullPath);
    checkForInternalRegion(result);
    return result;
  }

  @Override
  public <K, V> Region<K, V> getRegionByPath(String path) {
    InternalRegion result = delegate.getInternalRegionByPath(path);
    checkForInternalRegion(result);
    return uncheckedCast(result);
  }

  @Override
  public InternalRegion getInternalRegionByPath(String path) {
    InternalRegion result = delegate.getInternalRegionByPath(path);
    checkForInternalRegion(result);
    return result;
  }

  @Override
  public InternalRegion getRegionByPathForProcessing(String path) {
    InternalRegion result = delegate.getRegionByPathForProcessing(path);
    checkForInternalRegion(result);
    return result;
  }

  @Override
  public DistributedRegion getRegionInDestroy(String path) {
    DistributedRegion result = delegate.getRegionInDestroy(path);
    checkForInternalRegion(result);
    return result;
  }

  @Override
  public Set<PartitionedRegion> getPartitionedRegions() {
    Set<PartitionedRegion> result = delegate.getPartitionedRegions();
    checkSetOfRegions(result);
    return result;
  }

  @Override
  public Set<Region<?, ?>> rootRegions() {
    Set<Region<?, ?>> result = delegate.rootRegions();
    checkSetOfRegions(result);
    return result;
  }

  @Override
  public Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions) {
    Set<Region<?, ?>> result = delegate.rootRegions(includePRAdminRegions);
    checkSetOfRegions(result);
    return result;
  }

  @Override
  public Set<InternalRegion> getAllRegions() {
    Set<InternalRegion> result = delegate.getAllRegions();
    checkSetOfRegions(result);
    return result;
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    if (internalRegionArgs != null) {
      if (internalRegionArgs.isInternalRegion()
          || internalRegionArgs.isUsedForPartitionedRegionBucket()
          || internalRegionArgs.isUsedForMetaRegion()
          || internalRegionArgs.isUsedForSerialGatewaySenderQueue()
          || internalRegionArgs.isUsedForParallelGatewaySenderQueue()) {
        throw new NotAuthorizedException("The region " + name
            + " is an internal region that a client is never allowed to create");
      }
    }
    return delegate.createVMRegion(name, p_attrs, internalRegionArgs);
  }

  /**
   * This method allows server-side code to create an internal region. It should
   * not be invoked with a region name obtained from a client.
   */
  public <K, V> Region<K, V> createInternalRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    return delegate.createVMRegion(name, p_attrs, internalRegionArgs);
  }

  @Override
  public Cache getReconnectedCache() {
    Cache reconnectedCache = delegate.getReconnectedCache();
    if (reconnectedCache != null) {
      return new InternalCacheForClientAccess((InternalCache) reconnectedCache);
    }
    return null;
  }

  @Override
  public FilterProfile getFilterProfile(String regionName) {
    InternalRegion r = (InternalRegion) getRegion(regionName, true);
    if (r != null) {
      return r.getFilterProfile();
    }
    return null;
  }

  @Override
  public <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException {
    return delegate.basicCreateRegion(name, attrs);
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return delegate.createVMRegion(name, aRegionAttributes);
  }

  @Override
  public <K, V> Region<K, V> createRegion(String name, RegionAttributes<K, V> aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return delegate.createRegion(name, aRegionAttributes);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory() {
    return delegate.createRegionFactory();
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut shortcut) {
    return delegate.createRegionFactory(shortcut);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(String regionAttributesId) {
    return delegate.createRegionFactory(regionAttributesId);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionAttributes<K, V> regionAttributes) {
    return delegate.createRegionFactory(regionAttributes);
  }

  @Override
  public void close(boolean keepAlive) {
    delegate.close(keepAlive);
  }

  @Override
  public LogWriterI18n getLoggerI18n() {
    return delegate.getLoggerI18n();
  }

  @Override
  public LogWriterI18n getSecurityLoggerI18n() {
    return delegate.getSecurityLoggerI18n();
  }

  @Override
  public int getLockTimeout() {
    return delegate.getLockTimeout();
  }

  @Override
  public void setLockTimeout(int seconds) {
    delegate.setLockTimeout(seconds);
  }

  @Override
  public int getMessageSyncInterval() {
    return delegate.getMessageSyncInterval();
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    delegate.setMessageSyncInterval(seconds);
  }

  @Override
  public int getLockLease() {
    return delegate.getLockLease();
  }

  @Override
  public void setLockLease(int seconds) {
    delegate.setLockLease(seconds);
  }

  @Override
  public int getSearchTimeout() {
    return delegate.getSearchTimeout();
  }

  @Override
  public void setSearchTimeout(int seconds) {
    delegate.setSearchTimeout(seconds);
  }

  @Override
  public CacheServer addCacheServer() {
    return delegate.addCacheServer();
  }

  @Override
  public List<CacheServer> getCacheServers() {
    return delegate.getCacheServers();
  }

  @Override
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    delegate.setGatewayConflictResolver(resolver);
  }

  @Override
  public GatewayConflictResolver getGatewayConflictResolver() {
    return delegate.getGatewayConflictResolver();
  }

  @Override
  public void setIsServer(boolean isServer) {
    delegate.setIsServer(isServer);
  }

  @Override
  public boolean isServer() {
    return delegate.isServer();
  }

  @Override
  public void readyForEvents() {
    delegate.readyForEvents();
  }

  @Override
  public GatewaySenderFactory createGatewaySenderFactory() {
    return delegate.createGatewaySenderFactory();
  }

  @Override
  public AsyncEventQueueFactory createAsyncEventQueueFactory() {
    return delegate.createAsyncEventQueueFactory();
  }

  @Override
  public GatewayReceiverFactory createGatewayReceiverFactory() {
    return delegate.createGatewayReceiverFactory();
  }

  @Override
  public Set<GatewaySender> getGatewaySenders() {
    return delegate.getGatewaySenders();
  }

  @Override
  public GatewaySender getGatewaySender(String id) {
    return delegate.getGatewaySender(id);
  }

  @Override
  public Set<GatewayReceiver> getGatewayReceivers() {
    return delegate.getGatewayReceivers();
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return delegate.getAsyncEventQueues();
  }

  @Override
  public AsyncEventQueue getAsyncEventQueue(String id) {
    return delegate.getAsyncEventQueue(id);
  }

  @Override
  public Set<DistributedMember> getMembers() {
    return delegate.getMembers();
  }

  @Override
  public Set<DistributedMember> getAdminMembers() {
    return delegate.getAdminMembers();
  }

  @Override
  public Set<DistributedMember> getMembers(Region region) {
    return delegate.getMembers(region);
  }

  @Override
  public CacheSnapshotService getSnapshotService() {
    return delegate.getSnapshotService();
  }

  @Override
  public boolean isReconnecting() {
    return delegate.isReconnecting();
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    return delegate.waitUntilReconnected(time, units);
  }

  @Override
  public void stopReconnecting() {
    delegate.stopReconnecting();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    return delegate.getDistributedSystem();
  }

  @Override
  public ResourceManager getResourceManager() {
    return delegate.getResourceManager();
  }

  @Override
  public void setCopyOnRead(boolean copyOnRead) {
    delegate.setCopyOnRead(copyOnRead);
  }

  @Override
  public boolean getCopyOnRead() {
    return delegate.getCopyOnRead();
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes(String id) {
    return delegate.getRegionAttributes(id);
  }

  @Override
  public <K, V> void setRegionAttributes(String id, RegionAttributes<K, V> attrs) {
    delegate.setRegionAttributes(id, attrs);
  }

  @Override
  public <K, V> Map<String, RegionAttributes<K, V>> listRegionAttributes() {
    return delegate.listRegionAttributes();
  }

  @Override
  public void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    delegate.loadCacheXml(is);
  }

  @Override
  public LogWriter getLogger() {
    return delegate.getLogger();
  }

  @Override
  public LogWriter getSecurityLogger() {
    return delegate.getSecurityLogger();
  }

  @Override
  public DiskStore findDiskStore(String name) {
    return delegate.findDiskStore(name);
  }

  @Override
  public DiskStoreFactory createDiskStoreFactory() {
    return delegate.createDiskStoreFactory();
  }

  @Override
  public boolean getPdxReadSerialized() {
    return delegate.getPdxReadSerialized();
  }

  @Override
  public PdxSerializer getPdxSerializer() {
    return delegate.getPdxSerializer();
  }

  @Override
  public String getPdxDiskStore() {
    return delegate.getPdxDiskStore();
  }

  @Override
  public boolean getPdxPersistent() {
    return delegate.getPdxPersistent();
  }

  @Override
  public boolean getPdxIgnoreUnreadFields() {
    return delegate.getPdxIgnoreUnreadFields();
  }

  @Override
  public void registerPdxMetaData(Object instance) {
    delegate.registerPdxMetaData(instance);
  }

  @Override
  public CacheTransactionManager getCacheTransactionManager() {
    return delegate.getCacheTransactionManager();
  }

  @Override
  public Context getJNDIContext() {
    return delegate.getJNDIContext();
  }

  @Override
  public Declarable getInitializer() {
    return delegate.getInitializer();
  }

  @Override
  public Properties getInitializerProps() {
    return delegate.getInitializerProps();
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return delegate.getCancelCriterion();
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    return delegate.createPdxInstanceFactory(className);
  }

  @Override
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    return delegate.createPdxEnum(className, enumName, enumOrdinal);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return delegate.getExtensionPoint();
  }

  @Override
  public InternalDistributedMember getMyId() {
    return delegate.getMyId();
  }

  @Override
  public Collection<DiskStore> listDiskStores() {
    return delegate.listDiskStores();
  }

  @Override
  public Collection<DiskStore> listDiskStoresIncludingRegionOwned() {
    return delegate.listDiskStoresIncludingRegionOwned();
  }

  @Override
  public CqService getCqService() {
    return delegate.getCqService();
  }

  @Override
  public <T extends CacheService> T getService(Class<T> clazz) {
    return delegate.getService(clazz);
  }

  @Override
  public <T extends CacheService> Optional<T> getOptionalService(Class<T> clazz) {
    return Optional.ofNullable(getService(clazz));
  }

  @Override
  public Collection<CacheService> getServices() {
    return delegate.getServices();
  }

  @Override
  public SystemTimer getCCPTimer() {
    return delegate.getCCPTimer();
  }

  @Override
  public void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client) {
    delegate.cleanupForClient(ccn, client);
  }

  @Override
  public void purgeCCPTimer() {
    delegate.purgeCCPTimer();
  }

  @Override
  public MemoryAllocator getOffHeapStore() {
    return delegate.getOffHeapStore();
  }

  @Override
  public DistributedLockService getPartitionedRegionLockService() {
    return delegate.getPartitionedRegionLockService();
  }

  @Override
  public PersistentMemberManager getPersistentMemberManager() {
    return delegate.getPersistentMemberManager();
  }

  @Override
  public Set<GatewaySender> getAllGatewaySenders() {
    return delegate.getAllGatewaySenders();
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    return delegate.getCachePerfStats();
  }

  @Override
  public DistributionManager getDistributionManager() {
    return delegate.getDistributionManager();
  }

  @Override
  public void regionReinitialized(Region region) {
    delegate.regionReinitialized(region);
  }

  @Override
  public void setRegionByPath(String path, InternalRegion r) {
    delegate.setRegionByPath(path, r);
  }

  @Override
  public InternalResourceManager getInternalResourceManager() {
    return delegate.getInternalResourceManager();
  }

  @Override
  public ResourceAdvisor getResourceAdvisor() {
    return delegate.getResourceAdvisor();
  }

  @Override
  public boolean isCacheAtShutdownAll() {
    return delegate.isCacheAtShutdownAll();
  }

  @Override
  public boolean requiresNotificationFromPR(PartitionedRegion r) {
    return delegate.requiresNotificationFromPR(r);
  }

  @Override
  public <K, V> RegionAttributes<K, V> invokeRegionBefore(InternalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs) {
    return delegate.invokeRegionBefore(parent, name, attrs, internalRegionArgs);
  }

  @Override
  public void invokeRegionAfter(InternalRegion region) {
    delegate.invokeRegionAfter(region);
  }

  @Override
  public void invokeBeforeDestroyed(InternalRegion region) {
    delegate.invokeBeforeDestroyed(region);
  }

  @Override
  public void invokeCleanupFailedInitialization(InternalRegion region) {
    delegate.invokeCleanupFailedInitialization(region);
  }

  @Override
  public TXManagerImpl getTXMgr() {
    return delegate.getTXMgr();
  }

  @Override
  public boolean forcedDisconnect() {
    return delegate.forcedDisconnect();
  }

  @Override
  public InternalResourceManager getInternalResourceManager(boolean checkCancellationInProgress) {
    return delegate.getInternalResourceManager();
  }

  @Override
  public boolean isCopyOnRead() {
    return delegate.isCopyOnRead();
  }

  @Override
  public TombstoneService getTombstoneService() {
    return delegate.getTombstoneService();
  }

  @Override
  public QueryService getLocalQueryService() {
    return delegate.getLocalQueryService();
  }

  @Override
  public void registerInterestStarted() {
    delegate.registerInterestStarted();
  }

  @Override
  public void registerInterestCompleted() {
    delegate.registerInterestCompleted();
  }

  @Override
  public void regionReinitializing(String fullPath) {
    delegate.regionReinitializing(fullPath);
  }

  @Override
  public void unregisterReinitializingRegion(String fullPath) {
    delegate.unregisterReinitializingRegion(fullPath);
  }

  @Override
  public boolean removeRoot(InternalRegion rootRgn) {
    return delegate.removeRoot(rootRgn);
  }

  @Override
  public Executor getEventThreadPool() {
    return delegate.getEventThreadPool();
  }

  @Override
  public boolean keepDurableSubscriptionsAlive() {
    return delegate.keepDurableSubscriptionsAlive();
  }

  @Override
  public CacheClosedException getCacheClosedException(String reason) {
    return delegate.getCacheClosedException(reason);
  }

  @Override
  public CacheClosedException getCacheClosedException(String reason, Throwable cause) {
    return delegate.getCacheClosedException(reason, cause);
  }

  @Override
  public TypeRegistry getPdxRegistry() {
    return delegate.getPdxRegistry();
  }

  @Override
  public DiskStoreImpl getOrCreateDefaultDiskStore() {
    return delegate.getOrCreateDefaultDiskStore();
  }

  @Override
  public ExpirationScheduler getExpirationScheduler() {
    return delegate.getExpirationScheduler();
  }

  @Override
  public TransactionManager getJTATransactionManager() {
    return delegate.getJTATransactionManager();
  }

  @Override
  public TXManagerImpl getTxManager() {
    return delegate.getTxManager();
  }

  @Override
  public void beginDestroy(String path, DistributedRegion region) {
    delegate.beginDestroy(path, region);
  }

  @Override
  public void endDestroy(String path, DistributedRegion region) {
    delegate.endDestroy(path, region);
  }

  @Override
  public ClientMetadataService getClientMetadataService() {
    return delegate.getClientMetadataService();
  }

  @Override
  public long cacheTimeMillis() {
    return delegate.cacheTimeMillis();
  }

  @Override
  public URL getCacheXmlURL() {
    return delegate.getCacheXmlURL();
  }

  @Override
  public List<File> getBackupFiles() {
    return delegate.getBackupFiles();
  }

  @Override
  public boolean isClient() {
    return delegate.isClient();
  }

  @Override
  public InternalDistributedSystem getInternalDistributedSystem() {
    return delegate.getInternalDistributedSystem();
  }

  @Override
  public void addRegionListener(RegionListener regionListener) {
    delegate.addRegionListener(regionListener);
  }

  @Override
  public void removeRegionListener(RegionListener regionListener) {
    delegate.removeRegionListener(regionListener);
  }

  @Override
  public Set<RegionListener> getRegionListeners() {
    return delegate.getRegionListeners();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return delegate.getCacheConfig();
  }

  @Override
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    return delegate.getPdxReadSerializedByAnyGemFireServices();
  }

  @Override
  public void setDeclarativeCacheConfig(CacheConfig cacheConfig) {
    delegate.setDeclarativeCacheConfig(cacheConfig);
  }

  @Override
  public void initializePdxRegistry() {
    delegate.initializePdxRegistry();
  }

  @Override
  public void readyDynamicRegionFactory() {
    delegate.readyDynamicRegionFactory();
  }

  @Override
  public void setBackupFiles(List<File> backups) {
    delegate.setBackupFiles(backups);
  }

  @Override
  public void addDeclarableProperties(Map<Declarable, Properties> mapOfNewDeclarableProps) {
    delegate.addDeclarableProperties(mapOfNewDeclarableProps);
  }

  @Override
  public void setInitializer(Declarable initializer, Properties initializerProps) {
    delegate.setInitializer(initializer, initializerProps);
  }

  @Override
  public boolean hasPool() {
    return delegate.hasPool();
  }

  @Override
  public DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs) {
    return delegate.createDiskStoreFactory(attrs);
  }

  @Override
  public BackupService getBackupService() {
    return delegate.getBackupService();
  }

  @Override
  @VisibleForTesting
  public Throwable getDisconnectCause() {
    return delegate.getDisconnectCause();
  }

  @Override
  public void addPartitionedRegion(PartitionedRegion region) {
    delegate.addPartitionedRegion(region);
  }

  @Override
  public void removePartitionedRegion(PartitionedRegion region) {
    delegate.removePartitionedRegion(region);
  }

  @Override
  public void addDiskStore(DiskStoreImpl dsi) {
    delegate.addDiskStore(dsi);
  }

  @Override
  public TXEntryStateFactory getTXEntryStateFactory() {
    return delegate.getTXEntryStateFactory();
  }

  @Override
  public EventTrackerExpiryTask getEventTrackerTask() {
    return delegate.getEventTrackerTask();
  }

  @Override
  public void removeDiskStore(DiskStoreImpl diskStore) {
    delegate.removeDiskStore(diskStore);
  }

  @Override
  public void addGatewaySender(GatewaySender sender) {
    delegate.addGatewaySender(sender);
  }

  @Override
  public void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue) {
    delegate.addAsyncEventQueue(asyncQueue);
  }

  @Override
  public void removeAsyncEventQueue(AsyncEventQueue asyncQueue) {
    delegate.removeAsyncEventQueue(asyncQueue);
  }

  @Override
  public QueryMonitor getQueryMonitor() {
    return delegate.getQueryMonitor();
  }

  @Override
  public void close(String reason, Throwable systemFailureCause, boolean keepAlive, boolean keepDS,
      boolean skipAwait) {
    delegate.close(reason, systemFailureCause, keepAlive, keepDS, skipAwait);
  }

  @Override
  public JmxManagerAdvisor getJmxManagerAdvisor() {
    return delegate.getJmxManagerAdvisor();
  }

  @Override
  public List<Properties> getDeclarableProperties(String className) {
    return delegate.getDeclarableProperties(className);
  }

  @Override
  public long getUpTime() {
    return delegate.getUpTime();
  }

  @Override
  public void addRegionOwnedDiskStore(DiskStoreImpl dsi) {
    delegate.addRegionOwnedDiskStore(dsi);
  }

  @Override
  public DiskStoreMonitor getDiskStoreMonitor() {
    return delegate.getDiskStoreMonitor();
  }

  @Override
  public void close(String reason, Throwable optionalCause) {
    delegate.close(reason, optionalCause);
  }

  @Override
  public List<InternalCacheServer> getCacheServersAndGatewayReceiver() {
    return delegate.getCacheServersAndGatewayReceiver();
  }

  @Override
  public boolean isGlobalRegionInitializing(String fullPath) {
    return delegate.isGlobalRegionInitializing(fullPath);
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return delegate.getDistributionAdvisor();
  }

  @Override
  public void setQueryMonitorRequiredForResourceManager(boolean required) {
    delegate.setQueryMonitorRequiredForResourceManager(required);
  }

  @Override
  public boolean isQueryMonitorDisabledForLowMemory() {
    return delegate.isQueryMonitorDisabledForLowMemory();
  }

  @Override
  public boolean isRESTServiceRunning() {
    return delegate.isRESTServiceRunning();
  }

  @Override
  public InternalLogWriter getInternalLogWriter() {
    return delegate.getInternalLogWriter();
  }

  @Override
  public InternalLogWriter getSecurityInternalLogWriter() {
    return delegate.getSecurityInternalLogWriter();
  }

  @Override
  public Set<InternalRegion> getApplicationRegions() {
    return delegate.getApplicationRegions();
  }

  @Override
  public void removeGatewaySender(GatewaySender sender) {
    delegate.removeGatewaySender(sender);
  }

  @Override
  public DistributedLockService getGatewaySenderLockService() {
    return delegate.getGatewaySenderLockService();
  }

  @Override
  @VisibleForTesting
  public RestAgent getRestAgent() {
    return delegate.getRestAgent();
  }

  @Override
  public Properties getDeclarableProperties(Declarable declarable) {
    return delegate.getDeclarableProperties(declarable);
  }

  @Override
  public void setRESTServiceRunning(boolean isRESTServiceRunning) {
    delegate.setRESTServiceRunning(isRESTServiceRunning);
  }

  @Override
  public void close(String reason, boolean keepAlive, boolean keepDS) {
    delegate.close(reason, keepAlive, keepDS);
  }

  @Override
  public void addGatewayReceiver(GatewayReceiver receiver) {
    delegate.addGatewayReceiver(receiver);
  }

  @Override
  public void removeGatewayReceiver(GatewayReceiver receiver) {
    delegate.removeGatewayReceiver(receiver);
  }

  @Override
  public InternalCacheServer addGatewayReceiverServer(GatewayReceiver receiver) {
    return delegate.addGatewayReceiverServer(receiver);
  }

  @Override
  @VisibleForTesting
  public boolean removeCacheServer(CacheServer cacheServer) {
    return delegate.removeCacheServer(cacheServer);
  }

  @Override
  public boolean removeGatewayReceiverServer(InternalCacheServer receiverServer) {
    return delegate.removeGatewayReceiverServer(receiverServer);
  }

  @Override
  @VisibleForTesting
  public void setReadSerializedForTest(boolean value) {
    delegate.setReadSerializedForTest(value);
  }

  @Override
  public void setReadSerializedForCurrentThread(boolean value) {
    delegate.setReadSerializedForCurrentThread(value);
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean expectDomainClass) {
    return delegate.createPdxInstanceFactory(className, expectDomainClass);
  }

  @Override
  public void waitForRegisterInterestsInProgress() {
    delegate.waitForRegisterInterestsInProgress();
  }

  @Override
  public void reLoadClusterConfiguration() throws IOException, ClassNotFoundException {
    delegate.reLoadClusterConfiguration();
  }

  @Override
  public SecurityService getSecurityService() {
    return delegate.getSecurityService();
  }

  @Override
  public boolean hasPersistentRegion() {
    return delegate.hasPersistentRegion();
  }

  @Override
  public void shutDownAll() {
    delegate.shutDownAll();
  }

  @Override
  public void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region, List<Entry> entriesToSynchronize) {
    delegate.invokeRegionEntrySynchronizationListenersAfterSynchronization(sender, region,
        entriesToSynchronize);
  }

  @Override
  public QueryService getQueryService() {
    return delegate.getQueryService();
  }

  @Override
  public InternalQueryService getInternalQueryService() {
    return delegate.getInternalQueryService();
  }

  @Override
  public void lockDiskStore(String diskStoreName) {

  }

  @Override
  public void unlockDiskStore(String diskStoreName) {

  }

  @Override
  public JSONFormatter getJsonFormatter() {
    return delegate.getJsonFormatter();
  }

  @Override
  @VisibleForTesting
  public Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly) {
    return delegate.getAsyncEventQueues(visibleOnly);
  }

  @Override
  @VisibleForTesting
  public void closeDiskStores() {
    delegate.closeDiskStores();
  }

  @Override
  public Object convertPdxInstanceIfNeeded(Object obj, boolean preferCD) {
    return delegate.convertPdxInstanceIfNeeded(obj, preferCD);
  }

  @Override
  public Boolean getPdxReadSerializedOverride() {
    return delegate.getPdxReadSerializedOverride();
  }

  @Override
  public void setPdxReadSerializedOverride(boolean pdxReadSerialized) {
    delegate.setPdxReadSerializedOverride(pdxReadSerialized);
  }

  @Override
  public InternalCacheForClientAccess getCacheForProcessingClientRequests() {
    return this;
  }

  @Override
  public void initialize() {
    // do nothing
  }

  @Override
  public void throwCacheExistsException() {
    delegate.throwCacheExistsException();
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    return delegate.getMeterRegistry();
  }

  @Override
  public void saveCacheXmlForReconnect() {
    delegate.saveCacheXmlForReconnect();
  }

  @Override
  @VisibleForTesting
  public HeapEvictor getHeapEvictor() {
    return delegate.getHeapEvictor();
  }

  @Override
  @VisibleForTesting
  public OffHeapEvictor getOffHeapEvictor() {
    return delegate.getOffHeapEvictor();
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    return delegate.getStatisticsClock();
  }
}
