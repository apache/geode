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

import static java.lang.String.format;
import static org.apache.geode.internal.logging.LogWriterFactory.toSecurityLogWriter;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ALL;

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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;
import javax.transaction.TransactionManager;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.GemFireIOException;
import org.apache.geode.LogWriter;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
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
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceException;
import org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.xml.QueryConfigurationServiceCreation;
import org.apache.geode.cache.query.internal.xml.QueryMethodAuthorizerCreation;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
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
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreMonitor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.ExpirationScheduler;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.TXEntryStateFactory;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Represents a {@link Cache} that is created declaratively. Notice that it implements the
 * {@link Cache} interface so that this class must be updated when {@link Cache} is modified. This
 * class is public for testing purposes.
 *
 * @since GemFire 3.0
 */
public class CacheCreation implements InternalCache {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final RegionAttributes defaults = new AttributesFactory().create();

  /**
   * Store the current CacheCreation that is doing a create. Used from PoolManager to defer to
   * CacheCreation as a manager of pools.
   *
   * @since GemFire 5.7
   */
  private static final ThreadLocal<PoolManagerImpl> createInProgress = new ThreadLocal<>();

  /**
   * The amount of time to wait for a distributed lock
   */
  private int lockTimeout = GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT;
  private boolean hasLockTimeout;

  /**
   * The duration of a lease on a distributed lock
   */
  private int lockLease = GemFireCacheImpl.DEFAULT_LOCK_LEASE;
  private boolean hasLockLease;

  /**
   * The amount of time to wait for a {@code netSearch}
   */
  private int searchTimeout = GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT;
  private boolean hasSearchTimeout;

  private boolean hasMessageSyncInterval;

  /**
   * This cache's roots keyed on name
   */
  protected final Map<String, Region<?, ?>> roots = new LinkedHashMap<>();

  /**
   * Are dynamic regions enabled in this cache?
   */
  private DynamicRegionFactory.Config dynamicRegionFactoryConfig;
  private boolean hasDynamicRegionFactory;

  /**
   * Is this a cache server?
   */
  private boolean isServer;
  private boolean hasServer;

  /**
   * The cache servers configured for this cache
   */
  private final List<CacheServer> bridgeServers = new ArrayList<>();

  // Stores the properties used to initialize declarables.
  private final Map<Declarable, Properties> declarablePropertiesMap = new HashMap<>();
  private final List<DeclarableAndProperties> declarablePropertiesList = new ArrayList<>();

  private final Set<GatewaySender> gatewaySenders = new HashSet<>();

  private final Set<GatewayReceiver> gatewayReceivers = new HashSet<>();

  private final Set<AsyncEventQueue> asyncEventQueues = new HashSet<>();

  private GatewayConflictResolver gatewayConflictResolver;

  /**
   * The copyOnRead attribute
   */
  private boolean copyOnRead = GemFireCacheImpl.DEFAULT_COPY_ON_READ;
  private boolean hasCopyOnRead;

  /**
   * The CacheTransactionManager representative for this Cache
   */
  private CacheTransactionManagerCreation cacheTransactionManagerCreation;

  /**
   * The named region attributes associated with this cache
   */
  private final Map<String, RegionAttributes<?, ?>> namedRegionAttributes = new HashMap<>();

  /**
   * The names of the region attributes in the order in which they were added. Keeping track of this
   * ensures that named region attributes are processed in the correct order. That is, "parent"
   * named region attributes will be processed before "children" named region attributes.
   */
  private final List<String> regionAttributesNames = new ArrayList<>();

  /**
   * The named disk store attributes associated with this cache. Made this linked so its iteration
   * would be in insert order. This is important for unit testing 44914.
   */
  private final Map<String, DiskStore> diskStores = new LinkedHashMap<>();

  private final List<File> backups = new ArrayList<>();

  private final CacheConfig cacheConfig = new CacheConfig();

  /**
   * A logger that is used in debugging
   */
  private final InternalLogWriter logWriter = new LocalLogWriter(ALL.intLevel(), System.out);

  private final InternalLogWriter securityLogWriter = toSecurityLogWriter(logWriter);

  /**
   * {@link ExtensionPoint} support.
   *
   * @since GemFire 8.1
   */
  private final SimpleExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<>(this, this);

  private final PoolManagerImpl poolManager = new PoolManagerImpl(false);

  private volatile FunctionServiceCreation functionServiceCreation;

  private volatile boolean hasFunctionService;

  private volatile boolean hasResourceManager;

  private volatile ResourceManagerCreation resourceManagerCreation;

  private volatile SerializerCreation serializerCreation;

  private Declarable initializer;

  private Properties initializerProps;

  private final InternalQueryService queryService = createInternalQueryService();

  private QueryConfigurationServiceCreation queryConfigurationServiceCreation;

  /**
   * Creates a new {@code CacheCreation} with no root regions
   */
  public CacheCreation() {
    this(false);
  }

  /**
   * @param forParsing if true then this creation is used for parsing xml; if false then it is used
   *        for generating xml.
   *
   * @since GemFire 5.7
   */
  public CacheCreation(boolean forParsing) {
    initializeRegionShortcuts();
    if (!forParsing) {
      createInProgress.set(poolManager);
    }
  }

  /**
   * clear thread locals that may have been set by previous uses of CacheCreation
   */
  public static void clearThreadLocals() {
    createInProgress.remove();
  }

  /**
   * @since GemFire 5.7
   */
  void startingGenerate() {
    createInProgress.set(null);
  }

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
    RegionCreation existing = (RegionCreation) roots.get(name);

    if (existing != null) {
      throw new RegionExistsException(existing);
    }

    roots.put(root.getName(), root);
  }

  @Override
  public int getLockTimeout() {
    return lockTimeout;
  }

  @Override
  public void setLockTimeout(int seconds) {
    lockTimeout = seconds;
    hasLockTimeout = true;
  }

  boolean hasLockTimeout() {
    return hasLockTimeout;
  }

  @Override
  public int getLockLease() {
    return lockLease;
  }

  @Override
  public void setLockLease(int seconds) {
    lockLease = seconds;
    hasLockLease = true;
  }

  boolean hasLockLease() {
    return hasLockLease;
  }

  @Override
  public int getSearchTimeout() {
    return searchTimeout;
  }

  @Override
  public void setSearchTimeout(int seconds) {
    searchTimeout = seconds;
    hasSearchTimeout = true;
  }

  boolean hasSearchTimeout() {
    return hasSearchTimeout;
  }

  @Override
  public int getMessageSyncInterval() {
    return HARegionQueue.getMessageSyncInterval();
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    if (seconds < 0) {
      throw new IllegalArgumentException(
          "The 'message-sync-interval' property for cache cannot be negative");
    }
    HARegionQueue.setMessageSyncInterval(seconds);
    hasMessageSyncInterval = true;
  }

  boolean hasMessageSyncInterval() {
    return hasMessageSyncInterval;
  }

  @Override
  public Set<Region<?, ?>> rootRegions() {
    return Collections.unmodifiableSet(new LinkedHashSet<>(roots.values()));
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
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException,
      QueryConfigurationServiceException {
    extensionPoint.beforeCreate(cache);

    cache.setDeclarativeCacheConfig(cacheConfig);

    if (cache.isClient()) {
      throw new IllegalStateException(
          "You must use client-cache in the cache.xml when ClientCacheFactory is used.");
    }

    initializeDeclarablesMap(cache);

    if (hasFunctionService()) {
      getFunctionServiceCreation().create();
    }

    if (hasLockLease()) {
      cache.setLockLease(lockLease);
    }
    if (hasLockTimeout()) {
      cache.setLockTimeout(lockTimeout);
    }
    if (hasSearchTimeout()) {
      cache.setSearchTimeout(searchTimeout);
    }
    if (hasMessageSyncInterval()) {
      cache.setMessageSyncInterval(getMessageSyncInterval());
    }
    if (gatewayConflictResolver != null) {
      cache.setGatewayConflictResolver(gatewayConflictResolver);
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
      getResourceManager().configure(cache.getResourceManager());
    }

    DiskStoreAttributesCreation pdxRegDSC = initializePdxDiskStore(cache);

    cache.initializePdxRegistry();

    for (DiskStore diskStore : diskStores.values()) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) diskStore;
      if (creation != pdxRegDSC) {
        createDiskStore(creation, cache);
      }
    }

    if (hasDynamicRegionFactory()) {
      DynamicRegionFactory.get().open(getDynamicRegionFactoryConfig());
    }
    if (hasServer()) {
      cache.setIsServer(isServer);
    }
    if (hasCopyOnRead()) {
      cache.setCopyOnRead(copyOnRead);
    }

    if (cacheTransactionManagerCreation != null
        && cacheTransactionManagerCreation.getListeners().length > 0
        && cache.getCacheTransactionManager() != null) {
      cache.getCacheTransactionManager()
          .initListeners(cacheTransactionManagerCreation.getListeners());
    }

    if (cacheTransactionManagerCreation != null && cache.getCacheTransactionManager() != null) {
      cache.getCacheTransactionManager().setWriter(cacheTransactionManagerCreation.getWriter());
    }

    for (GatewaySender senderCreation : getGatewaySenders()) {
      GatewaySenderFactory factory = cache.createGatewaySenderFactory();
      ((InternalGatewaySenderFactory) factory).configureGatewaySender(senderCreation);
      GatewaySender gatewaySender =
          factory.create(senderCreation.getId(), senderCreation.getRemoteDSId());
      // Start the sender if it is not set to manually start
      if (gatewaySender.isManualStart()) {
        logger.info("{} is not being started since it is configured for manual start",
            gatewaySender);
      }
    }

    for (AsyncEventQueue asyncEventQueueCreation : getAsyncEventQueues()) {
      AsyncEventQueueFactoryImpl asyncQueueFactory =
          (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
      asyncQueueFactory.configureAsyncEventQueue(asyncEventQueueCreation);
      if (asyncEventQueueCreation.isDispatchingPaused()) {
        asyncQueueFactory.pauseEventDispatching();
      }

      AsyncEventQueue asyncEventQueue = cache.getAsyncEventQueue(asyncEventQueueCreation.getId());
      if (asyncEventQueue == null) {
        asyncQueueFactory.create(asyncEventQueueCreation.getId(),
            asyncEventQueueCreation.getAsyncEventListener());
      }
    }

    cache.initializePdxRegistry();

    for (String id : regionAttributesNames) {
      RegionAttributesCreation creation = (RegionAttributesCreation) getRegionAttributes(id);
      creation.inheritAttributes(cache, false);

      // Don't let the RegionAttributesCreation escape to the user
      AttributesFactory<?, ?> factory = new AttributesFactory<>(creation);
      RegionAttributes<?, ?> attrs = factory.create();

      cache.setRegionAttributes(id, attrs);
    }

    initializeRegions(roots, cache);

    cache.readyDynamicRegionFactory();

    // Create and start the CacheServers after the gateways have been initialized
    startCacheServers(getCacheServers(), cache, ServerLauncherParameters.INSTANCE);

    for (GatewayReceiver receiverCreation : getGatewayReceivers()) {
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
      factory.setHostnameForSenders(receiverCreation.getHostnameForSenders());
      GatewayReceiver receiver = factory.create();
      if (receiver.isManualStart()) {
        logger.info("{} is not being started since it is configured for manual start", receiver);
      }
    }

    if (queryConfigurationServiceCreation != null) {
      QueryConfigurationServiceImpl queryConfigService =
          (QueryConfigurationServiceImpl) cache.getService(QueryConfigurationService.class);
      if (queryConfigService != null) {
        QueryMethodAuthorizerCreation authorizerCreation =
            queryConfigurationServiceCreation.getMethodAuthorizerCreation();
        if (authorizerCreation != null) {
          queryConfigService.updateMethodAuthorizer(cache, true, authorizerCreation);
        }
      }
    }

    cache.setBackupFiles(backups);

    runInitializer(cache);
    cache.setInitializer(getInitializer(), getInitializerProps());

    // Create all extensions
    extensionPoint.fireCreate(cache);
  }

  public void initializeDeclarablesMap(InternalCache cache) {
    for (DeclarableAndProperties struct : declarablePropertiesList) {
      Declarable declarable = struct.getDeclarable();
      Properties properties = struct.getProperties();
      try {
        declarable.initialize(cache, properties);
        // for backwards compatibility
        declarable.init(properties);
      } catch (Exception ex) {
        throw new CacheXmlException(
            "Exception while initializing an instance of " + declarable.getClass().getName(), ex);
      }
      declarablePropertiesMap.put(declarable, properties);
    }
    cache.addDeclarableProperties(declarablePropertiesMap);
  }

  void initializeRegions(Map<String, Region<?, ?>> declarativeRegions, Cache cache) {
    for (Region region : declarativeRegions.values()) {
      RegionCreation regionCreation = (RegionCreation) region;
      regionCreation.createRoot(cache);
    }
  }

  /**
   * Reconfigures the server using the specified parameters.
   *
   * @param serverImpl CacheServer implementation.
   * @param parameters Parameters to reconfigure the server.
   */
  private void reconfigureServer(CacheServerImpl serverImpl, ServerLauncherParameters parameters) {
    if (parameters == null) {
      return;
    }

    if (parameters.getPort() != null
        && parameters.getPort() != CacheServer.DEFAULT_PORT) {
      serverImpl.setPort(parameters.getPort());
    }
    if (parameters.getMaxThreads() != null
        && parameters.getMaxThreads() != CacheServer.DEFAULT_MAX_THREADS) {
      serverImpl.setMaxThreads(parameters.getMaxThreads());
    }
    if (parameters.getMaxConnections() != null
        && parameters.getMaxConnections() != CacheServer.DEFAULT_MAX_CONNECTIONS) {
      serverImpl.setMaxConnections(parameters.getMaxConnections());
    }
    if (parameters.getMaxMessageCount() != null
        && parameters.getMaxMessageCount() != CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT) {
      serverImpl.setMaximumMessageCount(parameters.getMaxMessageCount());
    }
    if (parameters.getSocketBufferSize() != null
        && parameters.getSocketBufferSize() != CacheServer.DEFAULT_SOCKET_BUFFER_SIZE) {
      serverImpl.setSocketBufferSize(parameters.getSocketBufferSize());
    }
    if (parameters.getBindAddress() != null
        && parameters.getBindAddress() != CacheServer.DEFAULT_BIND_ADDRESS) {
      serverImpl.setBindAddress(parameters.getBindAddress().trim());
    }
    if (parameters.getMessageTimeToLive() != null
        && parameters.getMessageTimeToLive() != CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE) {
      serverImpl.setMessageTimeToLive(parameters.getMessageTimeToLive());
    }
    if (parameters.getHostnameForClients() != null
        && parameters.getHostnameForClients() != CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS) {
      serverImpl.setHostnameForClients(parameters.getHostnameForClients());
    }
  }

  /**
   * starts declarative cache servers if a server is not running on the port already. Also adds a
   * default server to the param declarativeCacheServers if a serverPort is specified.
   */
  void startCacheServers(List<CacheServer> declarativeCacheServers, Cache cache,
      ServerLauncherParameters parameters) {
    Integer serverPort = null;
    String serverBindAdd = null;
    Boolean disableDefaultServer = null;

    if (parameters != null) {
      serverPort = parameters.getPort();
      serverBindAdd = parameters.getBindAddress();
      disableDefaultServer = parameters.isDisableDefaultServer();
    }

    if (declarativeCacheServers.size() > 1 && (serverPort != null || serverBindAdd != null)) {
      throw new RuntimeException(
          "When using -server-port or -server-bind-address arguments, the cache-xml-file can not have more than one cache-server defined.");
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

      reconfigureServer(impl, parameters);

      try {
        if (!impl.isRunning()) {
          impl.start();
        }

      } catch (IOException ex) {
        throw new GemFireIOException(format("While starting cache server %s", impl), ex);
      }
    }
  }

  /**
   * Returns a description of the disk store used by the pdx registry.
   */
  DiskStoreAttributesCreation initializePdxDiskStore(InternalCache cache) {
    // Create the disk store used by the pdx registry first.
    // If it is using the default disk store we need to create it now.
    // If the cache has a pool then no need to create disk store.
    DiskStoreAttributesCreation pdxRegDSC = null;
    if (TypeRegistry.mayNeedDiskStore(cache)) {
      String pdxRegDsName = cache.getPdxDiskStore();
      if (pdxRegDsName == null) {
        pdxRegDsName = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;
      }
      pdxRegDSC = (DiskStoreAttributesCreation) diskStores.get(pdxRegDsName);
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
    boolean sameConfig = other.getLockLease() == getLockLease()
        && other.getLockTimeout() == getLockTimeout()
        && other.getSearchTimeout() == getSearchTimeout()
        && other.getMessageSyncInterval() == getMessageSyncInterval()
        && other.getCopyOnRead() == getCopyOnRead() && other.isServer() == isServer();

    if (!sameConfig) {
      throw new RuntimeException("!sameConfig");
    }

    DynamicRegionFactory.Config drc1 = getDynamicRegionFactoryConfig();
    if (drc1 != null) {
      // we have a dynamic region factory
      DynamicRegionFactory.Config drc2;
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

    Collection<CacheServer> myBridges = getCacheServers();
    Collection<CacheServer> otherBridges = other.getCacheServers();
    if (myBridges.size() != otherBridges.size()) {
      throw new RuntimeException("cacheServers size");
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
        throw new RuntimeException(format("cache server %s not found", myBridge));
      }
    }

    // compare connection pools
    Map<String, Pool> connectionPools1 = getPools();
    Map<String, Pool> connectionPools2 =
        other instanceof CacheCreation ? ((CacheCreation) other).getPools()
            : PoolManager.getAll();
    int connectionPools1Size = connectionPools1.size();

    // ignore any gateway instances
    for (Pool connectionPool : connectionPools1.values()) {
      if (((PoolImpl) connectionPool).isUsedByGateway()) {
        connectionPools1Size--;
      }
    }

    int connectionPools2Size = connectionPools2.size();

    // ignore any gateway instances
    for (Pool connectionPool : connectionPools2.values()) {
      if (((PoolImpl) connectionPool).isUsedByGateway()) {
        connectionPools2Size--;
      }
    }

    if (connectionPools2Size == 1) {
      // if it is just the DEFAULT pool then ignore it
      Pool connectionPool = connectionPools2.values().iterator().next();
      if (connectionPool.getName().equals("DEFAULT")) {
        connectionPools2Size = 0;
      }
    }

    if (connectionPools1Size != connectionPools2Size) {
      throw new RuntimeException("pool sizes differ connectionPools1Size=" + connectionPools1Size
          + " connectionPools2Size=" + connectionPools2Size
          + " connectionPools1=" + connectionPools1.values() + " connectionPools2="
          + connectionPools2.values());
    }

    if (connectionPools1Size > 0) {
      for (Pool pool : connectionPools1.values()) {
        PoolImpl poolImpl = (PoolImpl) pool;
        // ignore any gateway instances
        if (!poolImpl.isUsedByGateway()) {
          poolImpl.sameAs(connectionPools2.get(poolImpl.getName()));
        }
      }
    }

    // compare disk stores
    for (DiskStore diskStore : diskStores.values()) {
      DiskStoreAttributesCreation dsac = (DiskStoreAttributesCreation) diskStore;
      String name = dsac.getName();
      DiskStore otherDiskStore = other.findDiskStore(name);
      if (otherDiskStore == null) {
        logger.debug("Disk store {} not found.", name);
        throw new RuntimeException(format("Disk store %s not found", name));
      }
      if (!dsac.sameAs(otherDiskStore)) {
        logger.debug("Attributes for disk store {} do not match", name);
        throw new RuntimeException(format("Attributes for disk store %s do not match", name));
      }
    }

    Map<String, RegionAttributes<?, ?>> myNamedAttributes = listRegionAttributes();
    Map<String, RegionAttributes<Object, Object>> otherNamedAttributes =
        other.listRegionAttributes();
    if (myNamedAttributes.size() != otherNamedAttributes.size()) {
      throw new RuntimeException("namedAttributes size");
    }

    for (Object object : myNamedAttributes.entrySet()) {
      Entry myEntry = (Entry) object;
      String myId = (String) myEntry.getKey();
      Assert.assertTrue(myEntry.getValue() instanceof RegionAttributesCreation,
          "Entry value is a " + myEntry.getValue().getClass().getName());
      RegionAttributesCreation myAttrs = (RegionAttributesCreation) myEntry.getValue();
      RegionAttributes<Object, Object> otherAttrs = other.getRegionAttributes(myId);
      if (otherAttrs == null) {
        logger.debug("No attributes for {}", myId);
        throw new RuntimeException(format("No attributes for %s", myId));
      }
      if (!myAttrs.sameAs(otherAttrs)) {
        logger.debug("Attributes for " + myId + " do not match");
        throw new RuntimeException(format("Attributes for %s do not match", myId));
      }
    }

    Collection<Region<?, ?>> myRoots = roots.values();
    Collection<Region<?, ?>> otherRoots = other.rootRegions();
    if (myRoots.size() != otherRoots.size()) {
      throw new RuntimeException("roots size");
    }

    for (final Region<?, ?> myRoot : myRoots) {
      RegionCreation rootRegion = (RegionCreation) myRoot;
      Region<Object, Object> otherRegion = other.getRegion(rootRegion.getName());
      if (otherRegion == null) {
        throw new RuntimeException(format("no root %s", rootRegion.getName()));
      }
      if (!rootRegion.sameAs(otherRegion)) {
        throw new RuntimeException("regions differ");
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
        throw new RuntimeException("txListener");
      }
    }

    if (hasResourceManager()) {
      getResourceManager().sameAs(other.getResourceManager());
    }

    return true;
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void close(boolean keepAlive) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isReconnecting() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void stopReconnecting() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Cache getReconnectedCache() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public LogWriter getLogger() {
    return logWriter;
  }

  @Override
  public LogWriter getSecurityLogger() {
    return securityLogWriter;
  }

  @Override
  public LogWriterI18n getLoggerI18n() {
    return logWriter.convertToLogWriterI18n();
  }

  @Override
  public LogWriterI18n getSecurityLoggerI18n() {
    return securityLogWriter.convertToLogWriterI18n();
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public String getName() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public QueryService getQueryService() {
    return queryService;
  }

  @Override
  public InternalQueryService getInternalQueryService() {
    return queryService;
  }

  public QueryConfigurationServiceCreation getQueryConfigurationServiceCreation() {
    return queryConfigurationServiceCreation;
  }

  public void setQueryConfigurationServiceCreation(
      QueryConfigurationServiceCreation queryConfigurationServiceCreation) {
    this.queryConfigurationServiceCreation = queryConfigurationServiceCreation;
  }

  @Override
  public JSONFormatter getJsonFormatter() {
    return new JSONFormatter();
  }

  private void throwIfClient() {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut shortcut) {
    throwIfClient();
    return new InternalRegionFactory<>(this, shortcut);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory() {
    throwIfClient();
    return new InternalRegionFactory<>(this);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(String regionAttributesId) {
    throwIfClient();
    return new InternalRegionFactory<>(this, regionAttributesId);
  }

  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionAttributes<K, V> regionAttributes) {
    throwIfClient();
    return new InternalRegionFactory<>(this, regionAttributes);
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
    addRootRegion(region);
    return region;
  }

  public Region createRegion(String name, String refid)
      throws RegionExistsException, TimeoutException {
    RegionCreation region = new RegionCreation(this, name, refid);
    addRootRegion(region);
    return region;
  }

  @Override
  public Region getRegion(String path) {
    if (path.contains("/")) {
      throw new UnsupportedOperationException("Region path '" + path + "' contains '/'");
    }
    return roots.get(path);
  }

  @Override
  public CacheServer addCacheServer() {
    CacheServer bridge = new CacheServerCreation(this, false);
    bridgeServers.add(bridge);
    return bridge;
  }

  @Override
  public InternalCacheServer addGatewayReceiverServer(GatewayReceiver gatewayReceiver) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public boolean removeCacheServer(final CacheServer cacheServer) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean removeGatewayReceiverServer(InternalCacheServer receiverServer) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setReadSerializedForCurrentThread(final boolean value) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public void setReadSerializedForTest(final boolean value) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(final String className,
      final boolean expectDomainClass) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void waitForRegisterInterestsInProgress() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void reLoadClusterConfiguration() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public SecurityService getSecurityService() {
    return SecurityServiceFactory.create();
  }

  void addDeclarableProperties(final Declarable declarable, final Properties properties) {
    declarablePropertiesList.add(new DeclarableAndProperties(declarable, properties));
  }

  @Override
  public List<CacheServer> getCacheServers() {
    return bridgeServers;
  }

  @Override
  public void addGatewaySender(GatewaySender sender) {
    gatewaySenders.add(sender);
  }

  @Override
  public void addAsyncEventQueue(final AsyncEventQueueImpl asyncQueue) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void removeAsyncEventQueue(final AsyncEventQueue asyncQueue) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public QueryMonitor getQueryMonitor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void close(final String reason, final Throwable systemFailureCause,
      final boolean keepAlive, final boolean keepDS) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public JmxManagerAdvisor getJmxManagerAdvisor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public List<Properties> getDeclarableProperties(final String className) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public long getUpTime() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<Region<?, ?>> rootRegions(final boolean includePRAdminRegions) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<InternalRegion> getAllRegions() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DistributedRegion getRegionInDestroy(final String path) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addRegionOwnedDiskStore(final DiskStoreImpl dsi) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DiskStoreMonitor getDiskStoreMonitor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void close(final String reason, final Throwable optionalCause) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public LocalRegion getRegionByPathForProcessing(final String path) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public List<InternalCacheServer> getCacheServersAndGatewayReceiver() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isGlobalRegionInitializing(final String fullPath) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setQueryMonitorRequiredForResourceManager(final boolean required) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isQueryMonitorDisabledForLowMemory() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isRESTServiceRunning() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public InternalLogWriter getInternalLogWriter() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public InternalLogWriter getSecurityInternalLogWriter() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<InternalRegion> getApplicationRegions() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void removeGatewaySender(final GatewaySender sender) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DistributedLockService getGatewaySenderLockService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public RestAgent getRestAgent() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Properties getDeclarableProperties(final Declarable declarable) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setRESTServiceRunning(final boolean isRESTServiceRunning) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void close(final String reason, final boolean keepAlive, final boolean keepDS) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addGatewayReceiver(GatewayReceiver receiver) {
    gatewayReceivers.add(receiver);
  }

  @Override
  public void removeGatewayReceiver(GatewayReceiver receiver) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  public void addAsyncEventQueue(AsyncEventQueue asyncEventQueue) {
    asyncEventQueues.add(asyncEventQueue);
  }

  @Override
  public Set<GatewaySender> getGatewaySenders() {
    Set<GatewaySender> tempSet = new HashSet<>();
    for (GatewaySender sender : gatewaySenders) {
      if (!((AbstractGatewaySender) sender).isForInternalUse()) {
        tempSet.add(sender);
      }
    }
    return tempSet;
  }

  @Override
  public GatewaySender getGatewaySender(String id) {
    for (GatewaySender sender : gatewaySenders) {
      if (sender.getId().equals(id)) {
        return sender;
      }
    }
    return null;
  }

  @Override
  public Set<GatewayReceiver> getGatewayReceivers() {
    return gatewayReceivers;
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return asyncEventQueues;
  }

  @Override
  @VisibleForTesting
  public Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly) {
    return asyncEventQueues;
  }

  @Override
  @VisibleForTesting
  public void closeDiskStores() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : asyncEventQueues) {
      if (asyncEventQueue.getId().equals(id)) {
        return asyncEventQueue;
      }
    }
    return null;
  }

  @Override
  public void setIsServer(boolean isServer) {
    this.isServer = isServer;
    hasServer = true;
  }

  @Override
  public boolean isServer() {
    return isServer || !bridgeServers.isEmpty();
  }

  boolean hasServer() {
    return hasServer;
  }

  public void setDynamicRegionFactoryConfig(DynamicRegionFactory.Config v) {
    dynamicRegionFactoryConfig = v;
    hasDynamicRegionFactory = true;
  }

  boolean hasDynamicRegionFactory() {
    return hasDynamicRegionFactory;
  }

  DynamicRegionFactory.Config getDynamicRegionFactoryConfig() {
    return dynamicRegionFactoryConfig;
  }

  @Override
  public CacheTransactionManager getCacheTransactionManager() {
    return cacheTransactionManagerCreation;
  }

  /**
   * Implementation of {@link Cache#setCopyOnRead}
   *
   * @since GemFire 4.0
   */
  @Override
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
    hasCopyOnRead = true;
  }

  /**
   * Implementation of {@link Cache#getCopyOnRead}
   *
   * @since GemFire 4.0
   */
  @Override
  public boolean getCopyOnRead() {
    return copyOnRead;
  }

  boolean hasCopyOnRead() {
    return hasCopyOnRead;
  }

  /**
   * Adds a CacheTransactionManagerCreation for this Cache (really just a placeholder since a
   * CacheTransactionManager is really a Cache singleton)
   *
   * @see GemFireCacheImpl
   * @since GemFire 4.0
   */
  public void addCacheTransactionManagerCreation(
      CacheTransactionManagerCreation cacheTransactionManagerCreation) {
    this.cacheTransactionManagerCreation = cacheTransactionManagerCreation;
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
    return diskStores.get(name);
  }

  public void addDiskStore(DiskStore ds) {
    diskStores.put(ds.getName(), ds);
  }

  /**
   * Returns the DiskStore list
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public Collection<DiskStore> listDiskStores() {
    return diskStores.values();
  }

  void setDiskStore(String name, DiskStoreAttributesCreation diskStoreAttributesCreation) {
    diskStores.put(name, diskStoreAttributesCreation);
  }

  @Override
  public RegionAttributes getRegionAttributes(String id) {
    return namedRegionAttributes.get(id);
  }

  @Override
  public void setRegionAttributes(String id, RegionAttributes attrs) {
    RegionAttributes regionAttributes = attrs;
    if (!(regionAttributes instanceof RegionAttributesCreation)) {
      regionAttributes = new RegionAttributesCreation(this, regionAttributes, false);
    }
    namedRegionAttributes.put(id, regionAttributes);
    regionAttributesNames.add(id);
  }

  @Override
  public Map<String, RegionAttributes<?, ?>> listRegionAttributes() {
    return Collections.unmodifiableMap(namedRegionAttributes);
  }

  @Override
  public void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, RegionExistsException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void readyForEvents() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  public Map<String, Pool> getPools() {
    return poolManager.getMap();
  }

  public PoolFactory createPoolFactory() {
    return new PoolFactoryImpl(poolManager).setStartDisabled(true);
  }

  boolean hasFunctionService() {
    return hasFunctionService;
  }

  public void setFunctionServiceCreation(FunctionServiceCreation functionServiceCreation) {
    hasFunctionService = true;
    this.functionServiceCreation = functionServiceCreation;
  }

  FunctionServiceCreation getFunctionServiceCreation() {
    return functionServiceCreation;
  }

  public void setResourceManagerCreation(ResourceManagerCreation resourceManagerCreation) {
    hasResourceManager = true;
    this.resourceManagerCreation = resourceManagerCreation;
  }

  @Override
  public ResourceManagerCreation getResourceManager() {
    return resourceManagerCreation;
  }

  boolean hasResourceManager() {
    return hasResourceManager;
  }

  public void setSerializerCreation(SerializerCreation serializerCreation) {
    this.serializerCreation = serializerCreation;
  }

  SerializerCreation getSerializerCreation() {
    return serializerCreation;
  }

  public void addBackup(File backup) {
    backups.add(backup);
  }

  @Override
  public List<File> getBackupFiles() {
    return Collections.unmodifiableList(backups);
  }

  @Override
  public <K, V> Region<K, V> getRegionByPath(String path) {
    return null;
  }

  @Override
  public InternalRegion getInternalRegionByPath(String path) {
    return null;
  }

  @Override
  public boolean isClient() {
    return false;
  }

  @Override
  public InternalDistributedSystem getInternalDistributedSystem() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<PartitionedRegion> getPartitionedRegions() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addRegionListener(final RegionListener regionListener) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void removeRegionListener(final RegionListener regionListener) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<RegionListener> getRegionListeners() {
    throw new UnsupportedOperationException("Should not be invoked");
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
    cacheConfig.setPdxReadSerialized(readSerialized);
  }

  public void setPdxIgnoreUnreadFields(boolean ignore) {
    cacheConfig.setPdxIgnoreUnreadFields(ignore);
  }

  public void setPdxSerializer(PdxSerializer serializer) {
    cacheConfig.setPdxSerializer(serializer);
  }

  public void setPdxDiskStore(String diskStore) {
    cacheConfig.setPdxDiskStore(diskStore);
  }

  public void setPdxPersistent(boolean persistent) {
    cacheConfig.setPdxPersistent(persistent);
  }

  /**
   * Returns whether PdxInstance is preferred for PDX types instead of Java object.
   *
   * @see CacheFactory#setPdxReadSerialized(boolean)
   * @since GemFire 6.6
   */
  @Override
  public boolean getPdxReadSerialized() {
    return cacheConfig.isPdxReadSerialized();
  }

  @Override
  public PdxSerializer getPdxSerializer() {
    return cacheConfig.getPdxSerializer();
  }

  @Override
  public String getPdxDiskStore() {
    return cacheConfig.getPdxDiskStore();
  }

  @Override
  public boolean getPdxPersistent() {
    return cacheConfig.isPdxPersistent();
  }

  @Override
  public boolean getPdxIgnoreUnreadFields() {
    return cacheConfig.getPdxIgnoreUnreadFields();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  @Override
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setDeclarativeCacheConfig(final CacheConfig cacheConfig) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void initializePdxRegistry() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void readyDynamicRegionFactory() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setBackupFiles(final List<File> backups) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addDeclarableProperties(final Map<Declarable, Properties> mapOfNewDeclarableProps) {
    throw new UnsupportedOperationException("Should not be invoked");
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

  @Override
  public Declarable getInitializer() {
    return initializer;
  }

  @Override
  public Properties getInitializerProps() {
    return initializerProps;
  }

  @Override
  public void setInitializer(Declarable declarable, Properties props) {
    initializer = declarable;
    initializerProps = props;
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
  public <K, V> Region<K, V> basicCreateRegion(final String name,
      final RegionAttributes<K, V> attrs) throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public BackupService getBackupService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public Throwable getDisconnectCause() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addPartitionedRegion(final PartitionedRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void removePartitionedRegion(final PartitionedRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addDiskStore(final DiskStoreImpl dsi) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TXEntryStateFactory getTXEntryStateFactory() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public EventTrackerExpiryTask getEventTrackerTask() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void removeDiskStore(final DiskStoreImpl diskStore) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  void runInitializer(InternalCache cache) {
    Declarable initializer = getInitializer();
    if (initializer != null) {
      initializer.initialize(cache, getInitializerProps());
      // for backwards compatibility
      initializer.init(getInitializerProps());
    }
  }

  @Override
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    gatewayConflictResolver = resolver;
  }

  @Override
  public GatewayConflictResolver getGatewayConflictResolver() {
    return gatewayConflictResolver;
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CacheSnapshotService getSnapshotService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * @see Extensible#getExtensionPoint()
   * @since GemFire 8.1
   */
  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return extensionPoint;
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

  private InternalQueryService createInternalQueryService() {
    return new InternalQueryService() {

      private final Map<String, List<Index>> indexes = new HashMap<>();

      @Override
      public Query newQuery(String queryString) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public Index createHashIndex(String indexName, String indexedExpression, String regionPath)
          throws IndexInvalidException, UnsupportedOperationException {
        return createHashIndex(indexName, indexedExpression, regionPath, "");
      }

      @Override
      public Index createHashIndex(String indexName, String indexedExpression, String regionPath,
          String imports) throws IndexInvalidException, UnsupportedOperationException {
        return createIndex(indexName, IndexType.HASH, indexedExpression, regionPath, imports);
      }

      @Override
      public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
          String fromClause) throws IndexInvalidException, UnsupportedOperationException {
        return createIndex(indexName, indexType, indexedExpression, fromClause, "");
      }

      /**
       * Due to not having the full implementation to determine region names etc this implementation
       * will only match a single region with no alias at this time
       */
      @Override
      public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
          String fromClause, String imports)
          throws IndexInvalidException, UnsupportedOperationException {
        IndexCreationData indexData = new IndexCreationData(indexName);
        indexData.setFunctionalIndexData(fromClause, indexedExpression, imports);
        indexData.setIndexType(indexType.toString());
        List<Index> indexesForRegion = indexes.get(fromClause);
        if (indexesForRegion == null) {
          indexesForRegion = new ArrayList<>();
          indexes.put(fromClause, indexesForRegion);
        }
        indexesForRegion.add(indexData);
        return indexData;
      }

      @Override
      public Index createIndex(String indexName, String indexedExpression, String regionPath)
          throws IndexInvalidException, UnsupportedOperationException {
        return createIndex(indexName, indexedExpression, regionPath, "");
      }

      @Override
      public Index createIndex(String indexName, String indexedExpression, String regionPath,
          String imports) throws IndexInvalidException, UnsupportedOperationException {
        return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, regionPath, imports);
      }

      @Override
      public Index createKeyIndex(String indexName, String indexedExpression, String regionPath)
          throws IndexInvalidException, UnsupportedOperationException {
        return createIndex(indexName, IndexType.PRIMARY_KEY, indexedExpression, regionPath, "");
      }

      @Override
      public Index getIndex(Region<?, ?> region, String indexName) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public Collection<Index> getIndexes() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public Collection<Index> getIndexes(Region<?, ?> region) {
        Collection<Index> indexes = this.indexes.get(region.getFullPath());
        return indexes != null ? indexes : Collections.emptyList();
      }

      @Override
      public Collection<Index> getIndexes(Region<?, ?> region, IndexType indexType) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void removeIndex(Index index) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void removeIndexes() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void removeIndexes(Region<?, ?> region) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery newCq(String queryString, CqAttributes cqAttr) throws QueryInvalidException {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery newCq(String queryString, CqAttributes cqAttr, boolean isDurable)
          throws QueryInvalidException {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery newCq(String name, String queryString, CqAttributes cqAttr)
          throws QueryInvalidException {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery newCq(String name, String queryString, CqAttributes cqAttr, boolean isDurable)
          throws QueryInvalidException {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void closeCqs() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery[] getCqs() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery[] getCqs(String regionName) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqQuery getCq(String cqName) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void executeCqs() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void stopCqs() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void executeCqs(String regionName) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void stopCqs(String regionName) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public List<String> getAllDurableCqsFromServer() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public CqServiceStatistics getCqStatistics() {
        throw new UnsupportedOperationException("Should not be invoked");
      }


      @Override
      public void defineKeyIndex(String indexName, String indexedExpression, String regionPath) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void defineHashIndex(String indexName, String indexedExpression, String regionPath) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void defineHashIndex(String indexName, String indexedExpression, String regionPath,
          String imports) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void defineIndex(String indexName, String indexedExpression, String regionPath) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public void defineIndex(String indexName, String indexedExpression, String regionPath,
          String imports) {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public List<Index> createDefinedIndexes() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public boolean clearDefinedIndexes() {
        throw new UnsupportedOperationException("Should not be invoked");
      }

      @Override
      public MethodInvocationAuthorizer getMethodInvocationAuthorizer() {
        throw new UnsupportedOperationException("Should not be invoked");
      }
    };
  }

  @Override
  public <T extends CacheService> T getService(Class<T> clazz) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public <T extends CacheService> Optional<T> getOptionalService(Class<T> clazz) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Collection<CacheService> getServices() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public SystemTimer getCCPTimer() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void cleanupForClient(final CacheClientNotifier cacheClientNotifier,
      final ClientProxyMembershipID client) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void purgeCCPTimer() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public FilterProfile getFilterProfile(final String regionName) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Region getRegion(final String path, final boolean returnDestroyedRegion) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public MemoryAllocator getOffHeapStore() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(final String name, final RegionAttributes<K, V> p_attrs,
      final InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DistributedLockService getPartitionedRegionLockService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public PersistentMemberManager getPersistentMemberManager() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Set<GatewaySender> getAllGatewaySenders() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DistributionManager getDistributionManager() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void regionReinitialized(final Region region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setRegionByPath(final String path, final InternalRegion r) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public InternalResourceManager getInternalResourceManager() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public ResourceAdvisor getResourceAdvisor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isCacheAtShutdownAll() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean requiresNotificationFromPR(final PartitionedRegion r) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public <K, V> RegionAttributes<K, V> invokeRegionBefore(final InternalRegion parent,
      final String name, final RegionAttributes<K, V> attrs,
      final InternalRegionArguments internalRegionArgs) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void invokeRegionAfter(final InternalRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void invokeBeforeDestroyed(final InternalRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void invokeCleanupFailedInitialization(final InternalRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TXManagerImpl getTXMgr() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean forcedDisconnect() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public InternalResourceManager getInternalResourceManager(
      final boolean checkCancellationInProgress) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean isCopyOnRead() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TombstoneService getTombstoneService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public QueryService getLocalQueryService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void registerInterestStarted() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void registerInterestCompleted() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void regionReinitializing(final String fullPath) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void unregisterReinitializingRegion(final String fullPath) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean removeRoot(final InternalRegion rootRgn) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Executor getEventThreadPool() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public LocalRegion getReinitializingRegion(final String fullPath) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean keepDurableSubscriptionsAlive() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CacheClosedException getCacheClosedException(final String reason) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CacheClosedException getCacheClosedException(final String reason, final Throwable cause) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TypeRegistry getPdxRegistry() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public DiskStoreImpl getOrCreateDefaultDiskStore() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public ExpirationScheduler getExpirationScheduler() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TransactionManager getJTATransactionManager() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public TXManagerImpl getTxManager() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void beginDestroy(final String path, final DistributedRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void endDestroy(final String path, final DistributedRegion region) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public ClientMetadataService getClientMetadataService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public long cacheTimeMillis() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void initialize() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public URL getCacheXmlURL() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public boolean hasPersistentRegion() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void shutDownAll() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Object convertPdxInstanceIfNeeded(Object obj, boolean preferCD) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Boolean getPdxReadSerializedOverride() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setPdxReadSerializedOverride(boolean pdxReadSerialized) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void registerPdxMetaData(Object instance) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public InternalCacheForClientAccess getCacheForProcessingClientRequests() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void throwCacheExistsException() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void saveCacheXmlForReconnect() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public HeapEvictor getHeapEvictor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  @VisibleForTesting
  public OffHeapEvictor getOffHeapEvictor() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    return disabledClock();
  }

  CacheTransactionManagerCreation getCacheTransactionManagerCreation() {
    return cacheTransactionManagerCreation;
  }

  List<String> getRegionAttributesNames() {
    return regionAttributesNames;
  }

  private static class DeclarableAndProperties {
    private final Declarable declarable;
    private final Properties properties;

    private DeclarableAndProperties(Declarable d, Properties p) {
      declarable = d;
      properties = p;
    }

    public Declarable getDeclarable() {
      return declarable;
    }

    public Properties getProperties() {
      return properties;
    }
  }
}
