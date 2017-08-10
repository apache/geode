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
package org.apache.geode.internal.cache.xmlcache;

import org.apache.geode.CancelCriterion;
import org.apache.geode.GemFireIOException;
import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
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
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CacheServerLauncher;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreMonitor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.ExpirationScheduler;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.TXEntryStateFactory;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.persistence.BackupManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.internal.TypeRegistry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.transaction.TransactionManager;

/**
 * Represents a {@link Cache} that is created declaratively. Notice that it implements the
 * {@link Cache} interface so that this class must be updated when {@link Cache} is modified. This
 * class is public for testing purposes.
 *
 * @since GemFire 3.0
 */
public class CacheCreation implements InternalCache {

  /** The amount of time to wait for a distributed lock */
  private int lockTimeout = GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT;
  private boolean hasLockTimeout = false;

  /** The duration of a lease on a distributed lock */
  private int lockLease = GemFireCacheImpl.DEFAULT_LOCK_LEASE;
  private boolean hasLockLease = false;

  /** The amount of time to wait for a {@code netSearch} */
  private int searchTimeout = GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT;
  private boolean hasSearchTimeout = false;

  private boolean hasMessageSyncInterval = false;

  /** This cache's roots keyed on name */
  protected final Map<String, Region<?, ?>> roots = new LinkedHashMap<>();

  /** Are dynamic regions enabled in this cache? */
  private DynamicRegionFactory.Config dynamicRegionFactoryConfig = null;
  private boolean hasDynamicRegionFactory = false;

  /** Is this a cache server? */
  private boolean isServer = false;
  private boolean hasServer = false;

  /** The bridge servers configured for this cache */
  private final List<CacheServer> bridgeServers = new ArrayList<>();

  // Stores the properties used to initialize declarables.
  private final Map<Declarable, Properties> declarablePropertiesMap = new HashMap<>();

  private final Set<GatewaySender> gatewaySenders = new HashSet<>();

  private final Set<GatewayReceiver> gatewayReceivers = new HashSet<>();

  private final Set<AsyncEventQueue> asyncEventQueues = new HashSet<>();

  private GatewayConflictResolver gatewayConflictResolver;

  /** The copyOnRead attribute */
  private boolean copyOnRead = GemFireCacheImpl.DEFAULT_COPY_ON_READ;
  private boolean hasCopyOnRead = false;

  /** The CacheTransactionManager representative for this Cache */
  CacheTransactionManagerCreation txMgrCreation = null;

  /** The named region attributes associated with this cache */
  private final Map<String, RegionAttributes<?, ?>> namedRegionAttributes = new HashMap<>();

  /**
   * The names of the region attributes in the order in which they were added. Keeping track of this
   * ensures that named region attributes are processed in the correct order. That is, "parent"
   * named region attributes will be processed before "children" named region attributes.
   */
  final List<String> regionAttributesNames = new ArrayList<>();

  /**
   * The named disk store attributes associated with this cache. Made this linked so its iteration
   * would be in insert order. This is important for unit testing 44914.
   */
  protected final Map<String, DiskStore> diskStores = new LinkedHashMap<>();

  private final List<File> backups = new ArrayList<>();

  private final CacheConfig cacheConfig = new CacheConfig();

  /** A logger that is used in debugging */
  private final InternalLogWriter logWriter =
      new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);

  private final InternalLogWriter securityLogWriter =
      LogWriterFactory.toSecurityLogWriter(this.logWriter);

  /**
   * {@link ExtensionPoint} support.
   * 
   * @since GemFire 8.1
   */
  private final SimpleExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<>(this, this);

  /**
   * Creates a new {@code CacheCreation} with no root regions
   */
  public CacheCreation() {
    this(false);
  }

  /** clear thread locals that may have been set by previous uses of CacheCreation */
  public static void clearThreadLocals() {
    createInProgress.remove();
  }

  /**
   * @param forParsing if true then this creation is used for parsing xml; if false then it is used
   *        for generating xml.
   * @since GemFire 5.7
   */
  public CacheCreation(boolean forParsing) {
    initializeRegionShortcuts();
    if (!forParsing) {
      createInProgress.set(this.poolManager);
    }
  }

  /**
   * @since GemFire 5.7
   */
  void startingGenerate() {
    createInProgress.set(null);
  }

  private static final RegionAttributes defaults = new AttributesFactory().create();

  RegionAttributes getDefaultAttributes() {
    return defaults;
  }

  protected void initializeRegionShortcuts() {
    GemFireCacheImpl.initializeRegionShortcuts(this);
  }

  /**
   * Sets the attributes of the root region
   *
   * @throws RegionExistsException If this cache already contains a region with the same name as
   *         {@code root}.
   */
  void addRootRegion(RegionCreation root) throws RegionExistsException {

    String name = root.getName();
    RegionCreation existing = (RegionCreation) this.roots.get(name);
    if (existing != null) {
      throw new RegionExistsException(existing);

    } else {
      this.roots.put(root.getName(), root);
    }
  }

  @Override
  public int getLockTimeout() {
    return this.lockTimeout;
  }

  @Override
  public void setLockTimeout(int seconds) {
    this.lockTimeout = seconds;
    this.hasLockTimeout = true;
  }

  boolean hasLockTimeout() {
    return this.hasLockTimeout;
  }

  @Override
  public int getLockLease() {
    return this.lockLease;
  }

  @Override
  public void setLockLease(int seconds) {
    this.lockLease = seconds;
    this.hasLockLease = true;
  }

  boolean hasLockLease() {
    return this.hasLockLease;
  }

  @Override
  public int getSearchTimeout() {
    return this.searchTimeout;
  }

  @Override
  public void setSearchTimeout(int seconds) {
    this.searchTimeout = seconds;
    this.hasSearchTimeout = true;
  }

  boolean hasSearchTimeout() {
    return this.hasSearchTimeout;
  }

  @Override
  public int getMessageSyncInterval() {
    return HARegionQueue.getMessageSyncInterval();
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    if (seconds < 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.CacheCreation_THE_MESSAGESYNCINTERVAL_PROPERTY_FOR_CACHE_CANNOT_BE_NEGATIVE
              .toLocalizedString());
    }
    HARegionQueue.setMessageSyncInterval(seconds);
    this.hasMessageSyncInterval = true;
  }

  boolean hasMessageSyncInterval() {
    return this.hasMessageSyncInterval;
  }

  @Override
  public Set<Region<?, ?>> rootRegions() {
    Set<Region<?, ?>> regions = new LinkedHashSet<>(this.roots.values());
    return Collections.unmodifiableSet(regions);
  }

  /**
   * create diskstore factory
   * 
   * @since GemFire prPersistSprint2
   */
  @Override
  public DiskStoreFactory createDiskStoreFactory() {
    return new DiskStoreFactoryImpl(this);
  }

  /**
   * Store the current CacheCreation that is doing a create. Used from PoolManager to defer to
   * CacheCreation as a manager of pools.
   * 
   * @since GemFire 5.7
   */
  private static final ThreadLocal<PoolManagerImpl> createInProgress = new ThreadLocal<>();

  /**
   * Returns null if the current thread is not doing a CacheCreation create. Otherwise returns the
   * PoolManagerImpl of the CacheCreation of the create being invoked.
   * 
   * @since GemFire 5.7
   */
  public static PoolManagerImpl getCurrentPoolManager() {
    return createInProgress.get();
  }

  /**
   * Fills in the contents of a {@link Cache} based on this creation object's state.
   */
  void create(InternalCache cache)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    this.extensionPoint.beforeCreate(cache);

    cache.setDeclarativeCacheConfig(this.cacheConfig);

    if (cache.isClient()) {
      throw new IllegalStateException(
          "You must use client-cache in the cache.xml when ClientCacheFactory is used.");
    }
    if (this.hasLockLease()) {
      cache.setLockLease(this.lockLease);
    }
    if (this.hasLockTimeout()) {
      cache.setLockTimeout(this.lockTimeout);
    }
    if (this.hasSearchTimeout()) {
      cache.setSearchTimeout(this.searchTimeout);
    }
    if (this.hasMessageSyncInterval()) {
      cache.setMessageSyncInterval(this.getMessageSyncInterval());
    }
    if (this.gatewayConflictResolver != null) {
      cache.setGatewayConflictResolver(this.gatewayConflictResolver);
    }
    // create connection pools
    Map<String, Pool> pools = getPools();
    if (!pools.isEmpty()) {
      for (Pool pool : pools.values()) {
        PoolFactoryImpl poolFactory = (PoolFactoryImpl) PoolManager.createFactory();
        poolFactory.init(pool);
        poolFactory.create(pool.getName());
      }
    }

    if (hasResourceManager()) {
      // moved this up to fix bug 42128
      getResourceManager().configure(cache.getResourceManager());
    }

    DiskStoreAttributesCreation pdxRegDSC = initializePdxDiskStore(cache);

    cache.initializePdxRegistry();

    for (DiskStore diskStore : this.diskStores.values()) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) diskStore;
      if (creation != pdxRegDSC) {
        createDiskStore(creation, cache);
      }
    }

    if (this.hasDynamicRegionFactory()) {
      DynamicRegionFactory.get().open(this.getDynamicRegionFactoryConfig());
    }
    if (this.hasServer()) {
      cache.setIsServer(this.isServer);
    }
    if (this.hasCopyOnRead()) {
      cache.setCopyOnRead(this.copyOnRead);
    }

    if (this.txMgrCreation != null && this.txMgrCreation.getListeners().length > 0
        && cache.getCacheTransactionManager() != null) {
      cache.getCacheTransactionManager().initListeners(this.txMgrCreation.getListeners());
    }

    if (this.txMgrCreation != null && cache.getCacheTransactionManager() != null) {
      cache.getCacheTransactionManager().setWriter(this.txMgrCreation.getWriter());
    }

    for (GatewaySender senderCreation : this.getGatewaySenders()) {
      GatewaySenderFactory factory = cache.createGatewaySenderFactory();
      ((InternalGatewaySenderFactory) factory).configureGatewaySender(senderCreation);
      GatewaySender gatewaySender =
          factory.create(senderCreation.getId(), senderCreation.getRemoteDSId());
      // Start the sender if it is not set to manually start
      if (gatewaySender.isManualStart()) {
        cache.getLoggerI18n().info(
            LocalizedStrings.CacheCreation_0_IS_NOT_BEING_STARTED_SINCE_IT_IS_CONFIGURED_FOR_MANUAL_START,
            gatewaySender);
      }
    }

    for (AsyncEventQueue asyncEventQueueCreation : this.getAsyncEventQueues()) {
      AsyncEventQueueFactoryImpl asyncQueueFactory =
          (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
      asyncQueueFactory.configureAsyncEventQueue(asyncEventQueueCreation);

      AsyncEventQueue asyncEventQueue = cache.getAsyncEventQueue(asyncEventQueueCreation.getId());
      if (asyncEventQueue == null) {
        asyncQueueFactory.create(asyncEventQueueCreation.getId(),
            asyncEventQueueCreation.getAsyncEventListener());
      }
    }

    cache.initializePdxRegistry();

    for (String id : this.regionAttributesNames) {
      RegionAttributesCreation creation = (RegionAttributesCreation) getRegionAttributes(id);
      creation.inheritAttributes(cache, false);

      // Don't let the RegionAttributesCreation escape to the user
      AttributesFactory<?, ?> factory = new AttributesFactory<>(creation);
      RegionAttributes<?, ?> attrs = factory.create();

      cache.setRegionAttributes(id, attrs);
    }

    initializeRegions(this.roots, cache);

    cache.readyDynamicRegionFactory();

    // Create and start the BridgeServers. This code was moved from
    // before region initialization to after it to fix bug 33587.
    // Create and start the CacheServers after the gateways have been initialized
    // to fix bug 39736.

    Integer serverPort = CacheServerLauncher.getServerPort();
    String serverBindAdd = CacheServerLauncher.getServerBindAddress();
    Boolean disableDefaultServer = CacheServerLauncher.getDisableDefaultServer();
    startCacheServers(getCacheServers(), cache, serverPort, serverBindAdd, disableDefaultServer);

    for (GatewayReceiver receiverCreation : this.getGatewayReceivers()) {
      GatewayReceiverFactory factory = cache.createGatewayReceiverFactory();
      factory.setBindAddress(receiverCreation.getBindAddress());
      factory.setMaximumTimeBetweenPings(receiverCreation.getMaximumTimeBetweenPings());
      factory.setStartPort(receiverCreation.getStartPort());
      factory.setEndPort(receiverCreation.getEndPort());
      factory.setSocketBufferSize(receiverCreation.getSocketBufferSize());
      factory.setManualStart(receiverCreation.isManualStart());
      for (GatewayTransportFilter filter : receiverCreation.getGatewayTransportFilters()) {
        factory.addGatewayTransportFilter(filter);
      }
      factory.setHostnameForSenders(receiverCreation.getHost());
      GatewayReceiver receiver = factory.create();
      if (receiver.isManualStart()) {
        cache.getLoggerI18n().info(
            LocalizedStrings.CacheCreation_0_IS_NOT_BEING_STARTED_SINCE_IT_IS_CONFIGURED_FOR_MANUAL_START,
            receiver);
      }
    }

    cache.setBackupFiles(this.backups);
    cache.addDeclarableProperties(this.declarablePropertiesMap);
    runInitializer();
    cache.setInitializer(getInitializer(), getInitializerProps());

    // Create all extensions
    this.extensionPoint.fireCreate(cache);
  }

  void initializeRegions(Map<String, Region<?, ?>> declarativeRegions, Cache cache) {
    for (Region region : declarativeRegions.values()) {
      RegionCreation regionCreation = (RegionCreation) region;
      regionCreation.createRoot(cache);
    }
  }

  /**
   * starts declarative cache servers if a server is not running on the port already. Also adds a
   * default server to the param declarativeCacheServers if a serverPort is specified.
   */
  void startCacheServers(List<CacheServer> declarativeCacheServers, Cache cache, Integer serverPort,
      String serverBindAdd, Boolean disableDefaultServer) {

    if (declarativeCacheServers.size() > 1 && (serverPort != null || serverBindAdd != null)) {
      throw new RuntimeException(
          LocalizedStrings.CacheServerLauncher_SERVER_PORT_MORE_THAN_ONE_CACHE_SERVER
              .toLocalizedString());
    }

    CacheServerCreation defaultServer = null;
    boolean hasServerPortOrBindAddress = serverPort != null || serverBindAdd != null;
    boolean isDefaultServerDisabled = disableDefaultServer == null || !disableDefaultServer;
    if (declarativeCacheServers.isEmpty() && hasServerPortOrBindAddress
        && isDefaultServerDisabled) {
      boolean existingCacheServer = false;

      List<CacheServer> cacheServers = cache.getCacheServers();
      if (cacheServers != null) {
        for (CacheServer cacheServer : cacheServers) {
          if (serverPort == cacheServer.getPort()) {
            existingCacheServer = true;
          }
        }
      }

      if (!existingCacheServer) {
        defaultServer = new CacheServerCreation((InternalCache) cache, false);
        declarativeCacheServers.add(defaultServer);
      }
    }

    for (CacheServer declarativeCacheServer : declarativeCacheServers) {
      CacheServerCreation declaredCacheServer = (CacheServerCreation) declarativeCacheServer;

      boolean startServer = true;
      List<CacheServer> cacheServers = cache.getCacheServers();
      if (cacheServers != null) {
        for (CacheServer cacheServer : cacheServers) {
          if (declaredCacheServer.getPort() == cacheServer.getPort()) {
            startServer = false;
          }
        }
      }

      if (!startServer) {
        continue;
      }

      CacheServerImpl impl = (CacheServerImpl) cache.addCacheServer();
      impl.configureFrom(declaredCacheServer);
      if (declaredCacheServer == defaultServer) {
        impl.setIsDefaultServer();
      }

      if (serverPort != null && serverPort != CacheServer.DEFAULT_PORT) {
        impl.setPort(serverPort);
      }
      if (serverBindAdd != null) {
        impl.setBindAddress(serverBindAdd.trim());
      }

      try {
        if (!impl.isRunning()) {
          impl.start();
        }

      } catch (IOException ex) {
        throw new GemFireIOException(
            LocalizedStrings.CacheCreation_WHILE_STARTING_CACHE_SERVER_0.toLocalizedString(impl),
            ex);
      }
    }
  }

  /**
   * Returns a description of the disk store used by the pdx registry.
   */
  DiskStoreAttributesCreation initializePdxDiskStore(InternalCache cache) {
    // to fix bug 44271 create the disk store used by the pdx registry first.
    // If it is using the default disk store we need to create it now.
    // If the cache has a pool then no need to create disk store.
    DiskStoreAttributesCreation pdxRegDSC = null;
    if (TypeRegistry.mayNeedDiskStore(cache)) {
      String pdxRegDsName = cache.getPdxDiskStore();
      if (pdxRegDsName == null) {
        pdxRegDsName = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;
      }
      // make sure pdxRegDSC gets set to fix for bug 44914
      pdxRegDSC = (DiskStoreAttributesCreation) this.diskStores.get(pdxRegDsName);
      if (pdxRegDSC == null) {
        if (pdxRegDsName.equals(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)) {
          // need to create default disk store
          cache.getOrCreateDefaultDiskStore();
        }
      } else {
        createDiskStore(pdxRegDSC, cache);
      }
    }
    return pdxRegDSC;
  }

  protected void createDiskStore(DiskStoreAttributesCreation creation, InternalCache cache) {
    // Don't let the DiskStoreAttributesCreation escape to the user
    DiskStoreFactory factory = cache.createDiskStoreFactory(creation);
    factory.create(creation.getName());
  }

  /**
   * Returns whether or not this {@code CacheCreation} is equivalent to another {@code Cache}.
   */
  public boolean sameAs(Cache other) {
    boolean sameConfig = other.getLockLease() == this.getLockLease()
        && other.getLockTimeout() == this.getLockTimeout()
        && other.getSearchTimeout() == this.getSearchTimeout()
        && other.getMessageSyncInterval() == this.getMessageSyncInterval()
        && other.getCopyOnRead() == this.getCopyOnRead() && other.isServer() == this.isServer();

    if (!sameConfig) {
      throw new RuntimeException(LocalizedStrings.CacheCreation_SAMECONFIG.toLocalizedString());

    } else {
      DynamicRegionFactory.Config drc1 = this.getDynamicRegionFactoryConfig();
      if (drc1 != null) {
        // we have a dynamic region factory
        DynamicRegionFactory.Config drc2 = null;
        if (other instanceof CacheCreation) {
          drc2 = ((CacheCreation) other).getDynamicRegionFactoryConfig();
        } else {
          drc2 = DynamicRegionFactory.get().getConfig();
        }
        if (drc2 == null) {
          return false;
        }
        if (!drc1.equals(drc2)) {
          return false;
        }
      } else {
        // we have no dynamic region factory; how about other?
        if (other instanceof CacheCreation) {
          if (((CacheCreation) other).getDynamicRegionFactoryConfig() != null) {
            return false;
          }
        } else {
          // other must be real cache in which case we compare to DynamicRegionFactory
          if (DynamicRegionFactory.get().isOpen()) {
            return false;
          }
        }
      }

      Collection<CacheServer> myBridges = this.getCacheServers();
      Collection<CacheServer> otherBridges = other.getCacheServers();
      if (myBridges.size() != otherBridges.size()) {
        throw new RuntimeException(
            LocalizedStrings.CacheCreation_CACHESERVERS_SIZE.toLocalizedString());
      }

      for (CacheServer myBridge1 : myBridges) {
        CacheServerCreation myBridge = (CacheServerCreation) myBridge1;
        boolean found = false;
        for (CacheServer otherBridge : otherBridges) {
          if (myBridge.sameAs(otherBridge)) {
            found = true;
            break;
          }
        }

        if (!found) {
          throw new RuntimeException(
              LocalizedStrings.CacheCreation_CACHE_SERVER_0_NOT_FOUND.toLocalizedString(myBridge));
        }
      }

      // compare connection pools
      Map<String, Pool> m1 = getPools();
      Map<String, Pool> m2 = other instanceof CacheCreation ? ((CacheCreation) other).getPools()
          : PoolManager.getAll();
      int m1Size = m1.size();

      // ignore any gateway instances
      for (Pool cp : m1.values()) {
        if (((PoolImpl) cp).isUsedByGateway()) {
          m1Size--;
        }
      }

      int m2Size = m2.size();

      // ignore any gateway instances
      for (Pool cp : m2.values()) {
        if (((PoolImpl) cp).isUsedByGateway()) {
          m2Size--;
        }
      }

      if (m2Size == 1) {
        // if it is just the DEFAULT pool then ignore it
        Pool p = (Pool) m2.values().iterator().next();
        if (p.getName().equals("DEFAULT")) {
          m2Size = 0;
        }
      }

      if (m1Size != m2Size) {
        throw new RuntimeException("pool sizes differ m1Size=" + m1Size + " m2Size=" + m2Size
            + " m1=" + m1.values() + " m2=" + m2.values());
      }

      if (m1Size > 0) {
        for (Pool pool : m1.values()) {
          PoolImpl poolImpl = (PoolImpl) pool;
          // ignore any gateway instances
          if (!poolImpl.isUsedByGateway()) {
            poolImpl.sameAs(m2.get(poolImpl.getName()));
          }
        }
      }

      // compare disk stores
      for (DiskStore diskStore : this.diskStores.values()) {
        DiskStoreAttributesCreation dsac = (DiskStoreAttributesCreation) diskStore;
        String name = dsac.getName();
        DiskStore ds = other.findDiskStore(name);
        if (ds == null) {
          getLogger().fine("Disk store " + name + " not found.");
          throw new RuntimeException(
              LocalizedStrings.CacheCreation_DISKSTORE_NOTFOUND_0.toLocalizedString(name));
        } else {
          if (!dsac.sameAs(ds)) {
            getLogger().fine("Attributes for disk store " + name + " do not match");
            throw new RuntimeException(
                LocalizedStrings.CacheCreation_ATTRIBUTES_FOR_DISKSTORE_0_DO_NOT_MATCH
                    .toLocalizedString(name));
          }
        }
      }

      Map<String, RegionAttributes<?, ?>> myNamedAttributes = this.listRegionAttributes();
      Map<String, RegionAttributes<Object, Object>> otherNamedAttributes =
          other.listRegionAttributes();
      if (myNamedAttributes.size() != otherNamedAttributes.size()) {
        throw new RuntimeException(
            LocalizedStrings.CacheCreation_NAMEDATTRIBUTES_SIZE.toLocalizedString());
      }

      for (Object object : myNamedAttributes.entrySet()) {
        Entry myEntry = (Entry) object;
        String myId = (String) myEntry.getKey();
        Assert.assertTrue(myEntry.getValue() instanceof RegionAttributesCreation,
            "Entry value is a " + myEntry.getValue().getClass().getName());
        RegionAttributesCreation myAttrs = (RegionAttributesCreation) myEntry.getValue();
        RegionAttributes<Object, Object> otherAttrs = other.getRegionAttributes(myId);
        if (otherAttrs == null) {
          getLogger().fine("No attributes for " + myId);
          throw new RuntimeException(
              LocalizedStrings.CacheCreation_NO_ATTRIBUTES_FOR_0.toLocalizedString(myId));

        } else {
          if (!myAttrs.sameAs(otherAttrs)) {
            getLogger().fine("Attributes for " + myId + " do not match");
            throw new RuntimeException(LocalizedStrings.CacheCreation_ATTRIBUTES_FOR_0_DO_NOT_MATCH
                .toLocalizedString(myId));
          }
        }
      }

      Collection<Region<?, ?>> myRoots = this.roots.values();
      Collection<Region<?, ?>> otherRoots = other.rootRegions();
      if (myRoots.size() != otherRoots.size()) {
        throw new RuntimeException(LocalizedStrings.CacheCreation_ROOTS_SIZE.toLocalizedString());
      }

      for (final Region<?, ?> myRoot : myRoots) {
        RegionCreation rootRegion = (RegionCreation) myRoot;
        Region<Object, Object> otherRegion = other.getRegion(rootRegion.getName());
        if (otherRegion == null) {
          throw new RuntimeException(
              LocalizedStrings.CacheCreation_NO_ROOT_0.toLocalizedString(rootRegion.getName()));
        } else if (!rootRegion.sameAs(otherRegion)) {
          throw new RuntimeException(
              LocalizedStrings.CacheCreation_REGIONS_DIFFER.toLocalizedString());
        }
      }

      // If both have a listener, make sure they are equal.
      if (getCacheTransactionManager() != null) {
        // Currently the GemFireCache always has a CacheTransactionManager,
        // whereas that is not true for CacheTransactionManagerCreation.

        List<TransactionListener> otherTxListeners =
            Arrays.asList(other.getCacheTransactionManager().getListeners());
        List<TransactionListener> thisTxListeners =
            Arrays.asList(getCacheTransactionManager().getListeners());

        if (!thisTxListeners.equals(otherTxListeners)) {
          throw new RuntimeException(LocalizedStrings.CacheCreation_TXLISTENER.toLocalizedString());
        }
      }
    }

    if (hasResourceManager()) {
      getResourceManager().sameAs(other.getResourceManager());
    }

    return true;
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void close(boolean keepAlive) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isReconnecting() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void stopReconnecting() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Cache getReconnectedCache() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public LogWriter getLogger() {
    return this.logWriter;
  }

  @Override
  public LogWriter getSecurityLogger() {
    return this.securityLogWriter;
  }

  @Override
  public LogWriterI18n getLoggerI18n() {
    return this.logWriter.convertToLogWriterI18n();
  }

  @Override
  public LogWriterI18n getSecurityLoggerI18n() {
    return this.securityLogWriter.convertToLogWriterI18n();
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public QueryService getQueryService() {
    return this.queryService;
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut shortcut) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(String regionAttributesId) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionAttributes<K, V> regionAttributes) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Region createVMRegion(String name, RegionAttributes aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return createRegion(name, aRegionAttributes);
  }

  @Override
  public Region createRegion(String name, RegionAttributes aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    if (aRegionAttributes instanceof RegionAttributesCreation) {
      ((RegionAttributesCreation) aRegionAttributes).inheritAttributes(this);
      ((RegionAttributesCreation) aRegionAttributes).prepareForValidation();
    }
    AttributesFactory.validateAttributes(aRegionAttributes);
    RegionCreation region = new RegionCreation(this, name, null);
    region.setAttributes(aRegionAttributes);
    this.addRootRegion(region);
    return region;
  }

  public Region createRegion(String name, String refid)
      throws RegionExistsException, TimeoutException {
    RegionCreation region = new RegionCreation(this, name, refid);
    this.addRootRegion(region);
    return region;
  }

  @Override
  public Region getRegion(String path) {
    if (path.contains("/")) {
      throw new UnsupportedOperationException("Region path '" + path + "' contains '/'");
    }
    return this.roots.get(path);
  }

  @Override
  public CacheServer addCacheServer() {
    return addCacheServer(false);
  }

  public CacheServer addCacheServer(boolean isGatewayReceiver) {
    CacheServer bridge = new CacheServerCreation(this, false);
    this.bridgeServers.add(bridge);
    return bridge;
  }

  @Override
  public void setReadSerialized(final boolean value) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(final String className,
      final boolean expectDomainClass) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void waitForRegisterInterestsInProgress() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public SecurityService getSecurityService() {
    return SecurityServiceFactory.create();
  }

  void addDeclarableProperties(final Declarable declarable, final Properties properties) {
    this.declarablePropertiesMap.put(declarable, properties);
  }

  @Override
  public List<CacheServer> getCacheServers() {
    return this.bridgeServers;
  }

  @Override
  public void addGatewaySender(GatewaySender sender) {
    this.gatewaySenders.add(sender);
  }

  @Override
  public void addAsyncEventQueue(final AsyncEventQueueImpl asyncQueue) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void removeAsyncEventQueue(final AsyncEventQueue asyncQueue) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public QueryMonitor getQueryMonitor() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void close(final String reason, final Throwable systemFailureCause,
      final boolean keepAlive, final boolean keepDS) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public JmxManagerAdvisor getJmxManagerAdvisor() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public List<Properties> getDeclarableProperties(final String className) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public int getUpTime() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<Region<?, ?>> rootRegions(final boolean includePRAdminRegions) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<LocalRegion> getAllRegions() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DistributedRegion getRegionInDestroy(final String path) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void addRegionOwnedDiskStore(final DiskStoreImpl dsi) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DiskStoreMonitor getDiskStoreMonitor() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void close(final String reason, final Throwable optionalCause) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public LocalRegion getRegionByPathForProcessing(final String path) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public List getCacheServersAndGatewayReceiver() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isGlobalRegionInitializing(final String fullPath) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setQueryMonitorRequiredForResourceManager(final boolean required) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isQueryMonitorDisabledForLowMemory() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isRESTServiceRunning() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public InternalLogWriter getInternalLogWriter() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public InternalLogWriter getSecurityInternalLogWriter() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<LocalRegion> getApplicationRegions() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void removeGatewaySender(final GatewaySender sender) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DistributedLockService getGatewaySenderLockService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public RestAgent getRestAgent() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Properties getDeclarableProperties(final Declarable declarable) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setRESTServiceRunning(final boolean isRESTServiceRunning) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void close(final String reason, final boolean keepAlive, final boolean keepDS) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void addGatewayReceiver(GatewayReceiver receiver) {
    this.gatewayReceivers.add(receiver);
  }

  public void addAsyncEventQueue(AsyncEventQueue asyncEventQueue) {
    this.asyncEventQueues.add(asyncEventQueue);
  }

  @Override
  public Set<GatewaySender> getGatewaySenders() {
    Set<GatewaySender> tempSet = new HashSet<>();
    for (GatewaySender sender : this.gatewaySenders) {
      if (!((AbstractGatewaySender) sender).isForInternalUse()) {
        tempSet.add(sender);
      }
    }
    return tempSet;
  }

  @Override
  public GatewaySender getGatewaySender(String id) {
    for (GatewaySender sender : this.gatewaySenders) {
      if (sender.getId().equals(id)) {
        return sender;
      }
    }
    return null;
  }

  @Override
  public Set<GatewayReceiver> getGatewayReceivers() {
    return this.gatewayReceivers;
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return this.asyncEventQueues;
  }

  @Override
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : this.asyncEventQueues) {
      if (asyncEventQueue.getId().equals(id)) {
        return asyncEventQueue;
      }
    }
    return null;
  }

  @Override
  public void setIsServer(boolean isServer) {
    this.isServer = isServer;
    this.hasServer = true;
  }

  @Override
  public boolean isServer() {
    return this.isServer || !this.bridgeServers.isEmpty();
  }

  boolean hasServer() {
    return this.hasServer;
  }

  public void setDynamicRegionFactoryConfig(DynamicRegionFactory.Config v) {
    this.dynamicRegionFactoryConfig = v;
    this.hasDynamicRegionFactory = true;
  }

  boolean hasDynamicRegionFactory() {
    return this.hasDynamicRegionFactory;
  }

  DynamicRegionFactory.Config getDynamicRegionFactoryConfig() {
    return this.dynamicRegionFactoryConfig;
  }

  @Override
  public CacheTransactionManager getCacheTransactionManager() {
    return this.txMgrCreation;
  }

  /**
   * Implementation of {@link Cache#setCopyOnRead}
   * 
   * @since GemFire 4.0
   */
  @Override
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
    this.hasCopyOnRead = true;
  }

  /**
   * Implementation of {@link Cache#getCopyOnRead}
   * 
   * @since GemFire 4.0
   */
  @Override
  public boolean getCopyOnRead() {
    return this.copyOnRead;
  }

  boolean hasCopyOnRead() {
    return this.hasCopyOnRead;
  }

  /**
   * Adds a CacheTransactionManagerCreation for this Cache (really just a placeholder since a
   * CacheTransactionManager is really a Cache singleton)
   * 
   * @since GemFire 4.0
   * @see GemFireCacheImpl
   */
  public void addCacheTransactionManagerCreation(CacheTransactionManagerCreation txm) {
    this.txMgrCreation = txm;
  }

  /**
   * @return Context jndi context associated with the Cache.
   */
  @Override
  public Context getJNDIContext() {
    return JNDIInvoker.getJNDIContext();
  }

  @Override
  public DiskStore findDiskStore(String name) {
    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }
    return this.diskStores.get(name);
  }

  public void addDiskStore(DiskStore ds) {
    this.diskStores.put(ds.getName(), ds);
  }

  /**
   * Returns the DiskStore list
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public Collection<DiskStore> listDiskStores() {
    return this.diskStores.values();
  }

  void setDiskStore(String name, DiskStoreAttributesCreation dsac) {
    this.diskStores.put(name, dsac);
  }

  @Override
  public RegionAttributes getRegionAttributes(String id) {
    return this.namedRegionAttributes.get(id);
  }

  @Override
  public void setRegionAttributes(String id, RegionAttributes attrs) {
    RegionAttributes a = attrs;
    if (!(a instanceof RegionAttributesCreation)) {
      a = new RegionAttributesCreation(this, a, false);
    }
    this.namedRegionAttributes.put(id, a);
    this.regionAttributesNames.add(id);
  }

  @Override
  public Map<String, RegionAttributes<?, ?>> listRegionAttributes() {
    return Collections.unmodifiableMap(this.namedRegionAttributes);
  }

  @Override
  public void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, RegionExistsException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void readyForEvents() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  private final PoolManagerImpl poolManager = new PoolManagerImpl(false);

  private volatile FunctionServiceCreation functionServiceCreation;

  public Map<String, Pool> getPools() {
    return this.poolManager.getMap();
  }

  public PoolFactory createPoolFactory() {
    return new PoolFactoryImpl(this.poolManager).setStartDisabled(true);
  }

  public void setFunctionServiceCreation(FunctionServiceCreation functionServiceCreation) {
    this.functionServiceCreation = functionServiceCreation;
  }

  private volatile boolean hasResourceManager = false;

  private volatile ResourceManagerCreation resourceManagerCreation;

  public void setResourceManagerCreation(ResourceManagerCreation resourceManagerCreation) {
    this.hasResourceManager = true;
    this.resourceManagerCreation = resourceManagerCreation;
  }

  @Override
  public ResourceManagerCreation getResourceManager() {
    return this.resourceManagerCreation;
  }

  boolean hasResourceManager() {
    return this.hasResourceManager;
  }

  private volatile SerializerCreation serializerCreation;

  public void setSerializerCreation(SerializerCreation serializerCreation) {
    this.serializerCreation = serializerCreation;
  }

  SerializerCreation getSerializerCreation() {
    return this.serializerCreation;
  }

  public void addBackup(File backup) {
    this.backups.add(backup);
  }

  @Override
  public List<File> getBackupFiles() {
    return Collections.unmodifiableList(this.backups);
  }

  @Override
  public LocalRegion getRegionByPath(final String path) {
    return null;
  }

  @Override
  public boolean isClient() {
    return false;
  }

  @Override
  public InternalDistributedSystem getInternalDistributedSystem() {
    return InternalDistributedSystem.getAnyInstance();
  }

  @Override
  public Set<PartitionedRegion> getPartitionedRegions() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void addRegionListener(final RegionListener regionListener) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void removeRegionListener(final RegionListener regionListener) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<RegionListener> getRegionListeners() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public GatewaySenderFactory createGatewaySenderFactory() {
    return WANServiceProvider.createGatewaySenderFactory(this);
  }

  @Override
  public GatewayReceiverFactory createGatewayReceiverFactory() {
    return WANServiceProvider.createGatewayReceiverFactory(this);
  }

  @Override
  public AsyncEventQueueFactory createAsyncEventQueueFactory() {
    return new AsyncEventQueueFactoryImpl(this);
  }

  public void setPdxReadSerialized(boolean readSerialized) {
    this.cacheConfig.setPdxReadSerialized(readSerialized);
  }

  public void setPdxIgnoreUnreadFields(boolean ignore) {
    this.cacheConfig.setPdxIgnoreUnreadFields(ignore);
  }

  public void setPdxSerializer(PdxSerializer serializer) {
    this.cacheConfig.setPdxSerializer(serializer);
  }

  public void setPdxDiskStore(String diskStore) {
    this.cacheConfig.setPdxDiskStore(diskStore);
  }

  public void setPdxPersistent(boolean persistent) {
    this.cacheConfig.setPdxPersistent(persistent);
  }

  /**
   * Returns whether PdxInstance is preferred for PDX types instead of Java object.
   * 
   * @see org.apache.geode.cache.CacheFactory#setPdxReadSerialized(boolean)
   *
   * @since GemFire 6.6
   */
  @Override
  public boolean getPdxReadSerialized() {
    return this.cacheConfig.isPdxReadSerialized();
  }

  @Override
  public PdxSerializer getPdxSerializer() {
    return this.cacheConfig.getPdxSerializer();
  }

  @Override
  public String getPdxDiskStore() {
    return this.cacheConfig.getPdxDiskStore();
  }

  @Override
  public boolean getPdxPersistent() {
    return this.cacheConfig.isPdxPersistent();
  }

  @Override
  public boolean getPdxIgnoreUnreadFields() {
    return this.cacheConfig.getPdxIgnoreUnreadFields();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return this.cacheConfig;
  }

  @Override
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public BackupManager getBackupManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setDeclarativeCacheConfig(final CacheConfig cacheConfig) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void initializePdxRegistry() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void readyDynamicRegionFactory() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setBackupFiles(final List<File> backups) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void addDeclarableProperties(final Map<Declarable, Properties> mapOfNewDeclarableProps) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<DistributedMember> getMembers() {
    return Collections.emptySet();
  }

  @Override
  public Set<DistributedMember> getAdminMembers() {
    return Collections.emptySet();
  }

  @Override
  public Set<DistributedMember> getMembers(Region region) {
    return Collections.emptySet();
  }

  private Declarable initializer = null;

  private Properties initializerProps = null;

  @Override
  public Declarable getInitializer() {
    return this.initializer;
  }

  @Override
  public Properties getInitializerProps() {
    return this.initializerProps;
  }

  @Override
  public void setInitializer(Declarable declarable, Properties props) {
    this.initializer = declarable;
    this.initializerProps = props;
  }

  @Override
  public boolean hasPool() {
    return false;
  }

  @Override
  public DiskStoreFactory createDiskStoreFactory(final DiskStoreAttributes attrs) {
    return null;
  }

  @Override
  public void determineDefaultPool() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public <K, V> Region<K, V> basicCreateRegion(final String name,
      final RegionAttributes<K, V> attrs) throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public BackupManager startBackup(final InternalDistributedMember sender) throws IOException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Throwable getDisconnectCause() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void addPartitionedRegion(final PartitionedRegion region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void removePartitionedRegion(final PartitionedRegion region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void addDiskStore(final DiskStoreImpl dsi) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TXEntryStateFactory getTXEntryStateFactory() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public EventTrackerExpiryTask getEventTrackerTask() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void removeDiskStore(final DiskStoreImpl diskStore) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  void runInitializer() {
    if (getInitializer() != null) {
      getInitializer().init(getInitializerProps());
    }
  }

  @Override
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    this.gatewayConflictResolver = resolver;
  }

  @Override
  public GatewayConflictResolver getGatewayConflictResolver() {
    return this.gatewayConflictResolver;
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public CacheSnapshotService getSnapshotService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @see Extensible#getExtensionPoint()
   * @since GemFire 8.1
   */
  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return this.extensionPoint;
  }

  @Override
  public InternalDistributedMember getMyId() {
    return null;
  }

  @Override
  public Collection<DiskStore> listDiskStoresIncludingRegionOwned() {
    return null;
  }

  @Override
  public CqService getCqService() {
    return null;
  }

  private final QueryService queryService = new QueryService() {

    private final Map<String, List<Index>> indexes = new HashMap<>();

    @Override
    public Query newQuery(String queryString) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public Index createHashIndex(String indexName, String indexedExpression, String regionPath)
        throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createHashIndex(indexName, indexedExpression, regionPath, "");
    }

    @Override
    public Index createHashIndex(String indexName, String indexedExpression, String regionPath,
        String imports) throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, IndexType.HASH, indexedExpression, regionPath, imports);
    }

    @Override
    public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
        String fromClause) throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, indexType, indexedExpression, fromClause, "");
    }

    /**
     * Due to not having the full implementation to determine region names etc this implementation
     * will only match a single region with no alias at this time
     */
    @Override
    public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
        String fromClause, String imports) throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
      IndexCreationData indexData = new IndexCreationData(indexName);
      indexData.setFunctionalIndexData(fromClause, indexedExpression, imports);
      indexData.setIndexType(indexType.toString());
      List<Index> indexesForRegion = this.indexes.get(fromClause);
      if (indexesForRegion == null) {
        indexesForRegion = new ArrayList<>();
        this.indexes.put(fromClause, indexesForRegion);
      }
      indexesForRegion.add(indexData);
      return indexData;
    }

    @Override
    public Index createIndex(String indexName, String indexedExpression, String regionPath)
        throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, indexedExpression, regionPath, "");
    }

    @Override
    public Index createIndex(String indexName, String indexedExpression, String regionPath,
        String imports) throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, regionPath, imports);

    }

    @Override
    public Index createKeyIndex(String indexName, String indexedExpression, String regionPath)
        throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, IndexType.PRIMARY_KEY, indexedExpression, regionPath, "");

    }

    @Override
    public Index getIndex(Region<?, ?> region, String indexName) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public Collection<Index> getIndexes() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public Collection<Index> getIndexes(Region<?, ?> region) {
      return this.indexes.get(region.getFullPath());
    }

    @Override
    public Collection<Index> getIndexes(Region<?, ?> region, IndexType indexType) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void removeIndex(Index index) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void removeIndexes() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void removeIndexes(Region<?, ?> region) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String queryString, CqAttributes cqAttr)
        throws QueryInvalidException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String queryString, CqAttributes cqAttr, boolean isDurable)
        throws QueryInvalidException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String name, String queryString, CqAttributes cqAttr)
        throws QueryInvalidException, CqExistsException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String name, String queryString, CqAttributes cqAttr, boolean isDurable)
        throws QueryInvalidException, CqExistsException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void closeCqs() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery[] getCqs() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery[] getCqs(String regionName) throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery getCq(String cqName) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void executeCqs() throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void stopCqs() throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void executeCqs(String regionName) throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void stopCqs(String regionName) throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public List<String> getAllDurableCqsFromServer() throws CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqServiceStatistics getCqStatistics() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }


    @Override
    public void defineKeyIndex(String indexName, String indexedExpression, String regionPath)
        throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineHashIndex(String indexName, String indexedExpression, String regionPath)
        throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineHashIndex(String indexName, String indexedExpression, String regionPath,
        String imports) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineIndex(String indexName, String indexedExpression, String regionPath)
        throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineIndex(String indexName, String indexedExpression, String regionPath,
        String imports) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public List<Index> createDefinedIndexes() throws MultiIndexCreationException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public boolean clearDefinedIndexes() {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

  };

  @Override
  public <T extends CacheService> T getService(Class<T> clazz) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Collection<CacheService> getServices() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public SystemTimer getCCPTimer() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void cleanupForClient(final CacheClientNotifier ccn,
      final ClientProxyMembershipID client) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void purgeCCPTimer() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public FilterProfile getFilterProfile(final String regionName) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Region getRegion(final String path, final boolean returnDestroyedRegion) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public MemoryAllocator getOffHeapStore() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(final String name, final RegionAttributes<K, V> p_attrs,
      final InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DistributedLockService getPartitionedRegionLockService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public PersistentMemberManager getPersistentMemberManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Set<GatewaySender> getAllGatewaySenders() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DM getDistributionManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void regionReinitialized(final Region region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setRegionByPath(final String path, final LocalRegion r) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public InternalResourceManager getInternalResourceManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public ResourceAdvisor getResourceAdvisor() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isCacheAtShutdownAll() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean requiresNotificationFromPR(final PartitionedRegion r) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public <K, V> RegionAttributes<K, V> invokeRegionBefore(final LocalRegion parent,
      final String name, final RegionAttributes<K, V> attrs,
      final InternalRegionArguments internalRegionArgs) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void invokeRegionAfter(final LocalRegion region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TXManagerImpl getTXMgr() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean forcedDisconnect() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public InternalResourceManager getInternalResourceManager(
      final boolean checkCancellationInProgress) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean isCopyOnRead() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TombstoneService getTombstoneService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public QueryService getLocalQueryService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void registerInterestStarted() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void registerInterestCompleted() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void regionReinitializing(final String fullPath) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void unregisterReinitializingRegion(final String fullPath) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean removeRoot(final LocalRegion rootRgn) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Executor getEventThreadPool() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public LocalRegion getReinitializingRegion(final String fullPath) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean keepDurableSubscriptionsAlive() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public CacheClosedException getCacheClosedException(final String reason, final Throwable cause) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TypeRegistry getPdxRegistry() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public DiskStoreImpl getOrCreateDefaultDiskStore() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public ExpirationScheduler getExpirationScheduler() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TransactionManager getJTATransactionManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public TXManagerImpl getTxManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void beginDestroy(final String path, final DistributedRegion region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void endDestroy(final String path, final DistributedRegion region) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public ClientMetadataService getClientMetadataService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public long cacheTimeMillis() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void clearBackupManager() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public URL getCacheXmlURL() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public boolean hasPersistentRegion() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
}
