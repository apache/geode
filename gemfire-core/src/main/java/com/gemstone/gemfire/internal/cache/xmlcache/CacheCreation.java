/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.naming.Context;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqServiceStatistics;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.MultiIndexCreationException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.snapshot.CacheSnapshotService;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSIntegrationUtil;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreCreation;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.cache.DiskStoreFactoryImpl;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.ExtensionPoint;
import com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPoint;
import com.gemstone.gemfire.internal.cache.ha.HARegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANServiceProvider;
import com.gemstone.gemfire.internal.cache.wan.InternalGatewaySenderFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterFactory;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * Represents a {@link Cache} that is created declaratively.  Notice
 * that it implements the {@link Cache} interface so that this class
 * must be updated when {@link Cache} is modified.  This class is
 * public for testing purposes.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class CacheCreation implements InternalCache, Extensible<Cache> {

  /** The amount of time to wait for a distributed lock */
  private int lockTimeout = GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT;
  private boolean hasLockTimeout = false;

  /** The duration of a lease on a distributed lock */
  private int lockLease = GemFireCacheImpl.DEFAULT_LOCK_LEASE;
  private boolean hasLockLease = false;

  /** The amount of time to wait for a <code>netSearch</code> */
  private int searchTimeout = GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT;
  private boolean hasSearchTimeout = false;

  private boolean hasMessageSyncInterval = false;

  /** This cache's roots keyed on name */
  protected final Map roots = new LinkedHashMap();
  
  /** Are dynamic regions enabled in this cache? */
  private DynamicRegionFactory.Config dynamicRegionFactoryConfig = null;
  private boolean hasDynamicRegionFactory = false;

  /** Is this a cache server? */
  private boolean isServer = false;
  private boolean hasServer = false;

  /** The bridge servers configured for this cache */
  private final List bridgeServers = new ArrayList();
  
  // Stores the properties used to initialize declarables.
  private final Map<Declarable, Properties> declarablePropertiesMap = new HashMap<Declarable, Properties>();
  
  private Set<GatewaySender> gatewaySenders = new HashSet<GatewaySender>(); 
  
  private Set<GatewayReceiver> gatewayReceivers = new HashSet<GatewayReceiver>();
  
  private Set<AsyncEventQueue> asyncEventQueues = new HashSet<AsyncEventQueue>();
  
  private GatewayConflictResolver gatewayConflictResolver;
  
  /** The copyOnRead attribute */
  private boolean copyOnRead = GemFireCacheImpl.DEFAULT_COPY_ON_READ;
  private boolean hasCopyOnRead = false;

  /** The CacheTransactionManager representative for this Cache */
  protected CacheTransactionManagerCreation txMgrCreation = null;

  /** JNDI Context associated with the Gemfire */
//  private static Context ctx;

  /** The named region attributes associated with this cache */
  private final Map namedRegionAttributes = new HashMap();

  /** The names of the region attributes in the order in which they
   * were added.  Keeping track of this ensures that named region
   * attributes are processed in the correct order.  That is, "parent"
   * named region attributes will be processed before "children" named
   * region attributes. */
  protected final List regionAttributesNames = new ArrayList();

  /** The named disk store attributes associated with this cache.
   * Made this linked so its iteration would be in insert order.
   * This is important for unit testing 44914.
   */
  protected final Map diskStores = new LinkedHashMap();
  protected final Map hdfsStores = new LinkedHashMap();
  
  private final List<File> backups = new ArrayList<File>();

  private CacheConfig cacheConfig = new CacheConfig();

  /** A logger that is used in debugging */
  private InternalLogWriter logWriter =
    new LocalLogWriter(InternalLogWriter.ALL_LEVEL, System.out);

  private InternalLogWriter securityLogWriter =
      LogWriterFactory.toSecurityLogWriter(logWriter);
  
  /**
   * {@link ExtensionPoint} support.
   * @since 8.1
   */
  private SimpleExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<Cache>(this, this);

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>CacheCreation</code> with no root regions
   */
  public CacheCreation() {
    this(false);
  }

  /**
   * @param forParsing if true then this creation is used for parsing xml;
   *   if false then it is used for generating xml.
   * @since 5.7
   */
  public CacheCreation(boolean forParsing) {
    initializeRegionShortcuts();
    if (!forParsing) {
      createInProgress.set(this.pm);
    }
  }
  /**
   * @since 5.7
   */
  public void startingGenerate() {
    createInProgress.set(null);
  }
  
  //////////////////////  Instance Methods  //////////////////////

  static final private RegionAttributes defaults = new AttributesFactory().create();
    
  RegionAttributes getDefaultAttributes() {
    return defaults;
  }

  protected void initializeRegionShortcuts() {
    GemFireCacheImpl.initializeRegionShortcuts(this);
  }

  /**
   * Sets the attributes of the root region
   *
   * @throws RegionExistsException
   *         If this cache already contains a region with the same
   *         name as <code>root</code>.
   */
  void addRootRegion(RegionCreation root)
    throws RegionExistsException {

    String name = root.getName();
    RegionCreation existing = (RegionCreation) this.roots.get(name);
    if (existing != null) {
      throw new RegionExistsException(existing);

    } else {
      this.roots.put(root.getName(), root);
    }
  }

  public int getLockTimeout() {
    return this.lockTimeout;
  }

  public void setLockTimeout(int seconds) {
    this.lockTimeout = seconds;
    this.hasLockTimeout = true;
  }

  boolean hasLockTimeout() {
    return this.hasLockTimeout;
  }

  public int getLockLease() {
    return this.lockLease;
  }

  public void setLockLease(int seconds) {
    this.lockLease = seconds;
    this.hasLockLease = true;
  }

  boolean hasLockLease() {
    return this.hasLockLease;
  }

  public int getSearchTimeout() {
    return this.searchTimeout;
  }

  public void setSearchTimeout(int seconds) {
    this.searchTimeout = seconds;
    this.hasSearchTimeout = true;
  }

  boolean hasSearchTimeout() {
    return this.hasSearchTimeout;
  }

  public int getMessageSyncInterval()
  {
    return HARegionQueue.getMessageSyncInterval();
  }

  public void setMessageSyncInterval(int seconds)
  {
    if (seconds < 0) {
      throw new IllegalArgumentException(LocalizedStrings.CacheCreation_THE_MESSAGESYNCINTERVAL_PROPERTY_FOR_CACHE_CANNOT_BE_NEGATIVE.toLocalizedString());
    }
    HARegionQueue.setMessageSyncInterval(seconds);
    this.hasMessageSyncInterval = true;
  }

  boolean hasMessageSyncInterval()
  {
    return this.hasMessageSyncInterval;
  }

  public Set rootRegions() {
    Set regions = new LinkedHashSet();
    for (Iterator itr = this.roots.values().iterator(); itr.hasNext();) {
      regions.add(itr.next());
    }
    return Collections.unmodifiableSet(regions);
  }

  /**
   * create diskstore factory
   * 
   * @since prPersistSprint2
   */
  public DiskStoreFactory createDiskStoreFactory() {
    return new DiskStoreFactoryImpl(this);
  }
  
  /**
   * Store the current CacheCreation that is doing a create.
   * Used from PoolManager to defer to CacheCreation as a manager of pools.
   * @since 5.7
   */
  private static final ThreadLocal createInProgress = new ThreadLocal();

  /**
   * Returns null if the current thread is not doing a CacheCreation create.
   * Otherwise returns the PoolManagerImpl of the CacheCreation of the
   * create being invoked.
   * @since 5.7
   */
  public static final PoolManagerImpl getCurrentPoolManager() {
    return (PoolManagerImpl)createInProgress.get();
  }
  
  /**
   * Fills in the contents of a {@link Cache} based on this creation
   * object's state.
   *
   * @throws TimeoutException
   * @throws CacheWriterException
   * @throws RegionExistsException
   * @throws GatewayException
   */
  void create(GemFireCacheImpl cache)
    throws TimeoutException, CacheWriterException,
           GatewayException,
           RegionExistsException {
    cache.setDeclarativeCacheConfig(cacheConfig);
    
    if (cache.isClient()) {
      throw new IllegalStateException("You must use client-cache in the cache.xml when ClientCacheFactory is used.");
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
//    if (this.hasCopyOnRead()) {
//      cache.setCopyOnRead(this.copyOnRead);
//    }
    { // create connection pools
      Map m = getPools();
      if (!m.isEmpty()) {
        Iterator it = m.values().iterator();
        while (it.hasNext()) {
          Pool cp = (Pool)it.next();
          PoolFactoryImpl f;
          f = (PoolFactoryImpl)PoolManager.createFactory();
          f.init(cp);
          PoolImpl p = (PoolImpl)f.create(cp.getName());
        }
      }
    }

    if (hasResourceManager()) {
      // moved this up to fix bug 42128
      getResourceManager().configure(cache.getResourceManager());
    }
    
    DiskStoreAttributesCreation pdxRegDSC = initializePdxDiskStore(cache);
    
    cache.initializePdxRegistry();

    for (Iterator iter = this.diskStores.values().iterator(); iter.hasNext();) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) iter.next();
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

    if (this.txMgrCreation != null &&
        this.txMgrCreation.getListeners().length > 0 &&
        cache.getCacheTransactionManager()!=null) {
      cache.getCacheTransactionManager().initListeners(this.txMgrCreation.getListeners());
    }

    if (this.txMgrCreation != null &&
        cache.getCacheTransactionManager()!=null) {
      cache.getCacheTransactionManager().setWriter(this.txMgrCreation.getWriter());
    }
    
    for (GatewaySender senderCreation : this.getGatewaySenders()) {
      GatewaySenderFactory factory = (GatewaySenderFactory)cache
          .createGatewaySenderFactory();
      ((InternalGatewaySenderFactory)factory).configureGatewaySender(senderCreation);
      GatewaySender gatewaySender = factory.create(senderCreation.getId(),
          senderCreation.getRemoteDSId());
      // Start the sender if it is not set to manually start
      if (gatewaySender.isManualStart()) {
        cache
            .getLoggerI18n()
            .info(
                LocalizedStrings.CacheCreation_0_IS_NOT_BEING_STARTED_SINCE_IT_IS_CONFIGURED_FOR_MANUAL_START,
                gatewaySender);
      }
    }
    
    for (AsyncEventQueue asyncEventQueueCreation : this.getAsyncEventQueues()) {
      AsyncEventQueueFactoryImpl asyncQueueFactory = 
        (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
      asyncQueueFactory.configureAsyncEventQueue(asyncEventQueueCreation);
      
      AsyncEventQueue asyncEventQueue = cache.getAsyncEventQueue(asyncEventQueueCreation.getId());
//      AsyncEventQueue asyncEventQueue = 
//        asyncQueueFactory.create(asyncEventQueueCreation.getId(), asyncEventQueueCreation.getAsyncEventListener());
      if (asyncEventQueue == null) {
        asyncQueueFactory.create(asyncEventQueueCreation.getId(), asyncEventQueueCreation.getAsyncEventListener());
      }
    }
    
    for (GatewayReceiver receiverCreation : this.getGatewayReceivers()) {
      GatewayReceiverFactory factory = cache.createGatewayReceiverFactory();
      factory.setBindAddress(receiverCreation.getBindAddress());
      factory.setMaximumTimeBetweenPings(receiverCreation
          .getMaximumTimeBetweenPings());
      factory.setStartPort(receiverCreation.getStartPort());
      factory.setEndPort(receiverCreation.getEndPort());
      factory.setSocketBufferSize(receiverCreation.getSocketBufferSize());
      factory.setManualStart(receiverCreation.isManualStart());
      for (GatewayTransportFilter filter : receiverCreation
          .getGatewayTransportFilters()) {
        factory.addGatewayTransportFilter(filter);
      }
      factory.setHostnameForSenders(receiverCreation.getHost()); 
      GatewayReceiver receiver = factory.create();
       if (receiver.isManualStart()) {
        cache
            .getLoggerI18n()
            .info(
                LocalizedStrings.CacheCreation_0_IS_NOT_BEING_STARTED_SINCE_IT_IS_CONFIGURED_FOR_MANUAL_START,
                receiver);
      }
    }

    for(Iterator iter = this.hdfsStores.entrySet().iterator(); iter.hasNext(); ) {
      Entry entry = (Entry) iter.next();
      HDFSStoreCreation hdfsStoreCreation = (HDFSStoreCreation) entry.getValue();
      HDFSStoreFactory storefactory = cache.createHDFSStoreFactory(hdfsStoreCreation);
      storefactory.create((String) entry.getKey());
    }

    cache.initializePdxRegistry();

    
    for (Iterator iter = this.regionAttributesNames.iterator();
         iter.hasNext(); ) {
      String id = (String) iter.next();
      RegionAttributesCreation creation =
        (RegionAttributesCreation) getRegionAttributes(id);
      creation.inheritAttributes(cache, false);

      // TODO: HDFS: HDFS store/queue will be mapped against region path and not
      // the attribute id; don't really understand what this is trying to do
      if (creation.getHDFSStoreName() != null)
      {
        HDFSStoreImpl store = cache.findHDFSStore(creation.getHDFSStoreName());
        if(store == null) {
          HDFSIntegrationUtil.createDefaultAsyncQueueForHDFS((Cache)cache, creation.getHDFSWriteOnly(), id);
        }
      }
      if (creation.getHDFSStoreName() != null && creation.getPartitionAttributes().getColocatedWith() == null) {
        creation.addAsyncEventQueueId(HDFSStoreFactoryImpl.getEventQueueName(id));
      }
      
      RegionAttributes attrs;
      // Don't let the RegionAttributesCreation escape to the user
      AttributesFactory factory = new AttributesFactory(creation);
      attrs = factory.create();

      cache.setRegionAttributes(id, attrs);
    }

    Iterator it = this.roots.values().iterator();
    while (it.hasNext()) {
      RegionCreation r = (RegionCreation)it.next();
      r.createRoot(cache);
    }

    cache.readyDynamicRegionFactory();

    // Create and start the BridgeServers. This code was moved from
    // before region initialization to after it to fix bug 33587.
    // Create and start the CacheServers after the gateways have been intialized
    // to fix bug 39736.

    Integer serverPort = CacheServerLauncher.getServerPort();
    String serverBindAdd = CacheServerLauncher.getServerBindAddress();
    Boolean disableDefaultServer = CacheServerLauncher.disableDefaultServer.get();
    
    if (this.getCacheServers().size() > 1
        && (serverPort != null || serverBindAdd != null)) {
      throw new RuntimeException(
          LocalizedStrings.CacheServerLauncher_SERVER_PORT_MORE_THAN_ONE_CACHE_SERVER
              .toLocalizedString());
    }
    
    if (this.getCacheServers().isEmpty()
        && (serverPort != null || serverBindAdd != null)
        && (disableDefaultServer == null || !disableDefaultServer)) {
      boolean existingCacheServer = false;
      
      List<CacheServer> cacheServers = cache.getCacheServers();
      if (cacheServers != null) {
        for(CacheServer cacheServer : cacheServers) {
          if (serverPort == cacheServer.getPort() && cacheServer.getBindAddress().equals(serverBindAdd)) {
            existingCacheServer = true;
          }
        }
      }
      
      if (!existingCacheServer) {
        this.getCacheServers().add(new CacheServerCreation(cache, false));
      }
    }
    
    for (Iterator iter = this.getCacheServers().iterator(); iter.hasNext();) {
      CacheServerCreation bridge = (CacheServerCreation)iter.next();
      
      CacheServerImpl impl = (CacheServerImpl)cache.addCacheServer();
      impl.configureFrom(bridge);

      if (serverPort != null && serverPort != CacheServer.DEFAULT_PORT) {
        impl.setPort(serverPort);
      }
      if (serverBindAdd != null) {
        impl.setBindAddress(serverBindAdd.trim());
      }
      
      try {
        if (!impl.isRunning())
          impl.start();

      }
      catch (IOException ex) {
        throw new GemFireIOException(
            LocalizedStrings.CacheCreation_WHILE_STARTING_CACHE_SERVER_0
                .toLocalizedString(impl), ex);
      }
    }
    cache.setBackupFiles(this.backups);
    cache.addDeclarableProperties(this.declarablePropertiesMap);
    runInitializer();
    cache.setInitializer(getInitializer(), getInitializerProps());
    
    // UnitTest CacheXml81Test.testCacheExtension
    // Create all extensions
    extensionPoint.fireCreate(cache);
  }

  /**
   * Returns a description of the disk store used by the pdx registry.
   */
  protected DiskStoreAttributesCreation initializePdxDiskStore(GemFireCacheImpl cache) {
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
      pdxRegDSC = (DiskStoreAttributesCreation)this.diskStores.get(pdxRegDsName);
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
  
  protected void createDiskStore(DiskStoreAttributesCreation creation, GemFireCacheImpl cache) {
    // Don't let the DiskStoreAttributesCreation escape to the user
    DiskStoreFactory factory = cache.createDiskStoreFactory(creation);
    factory.create(creation.getName());
  }

  /**
   * Returns whether or not this <code>CacheCreation</code> is
   * equivalent to another <code>Cache</code>.
   */
  public boolean sameAs(Cache other) {
    boolean sameConfig =
      other.getLockLease() == this.getLockLease() &&
      other.getLockTimeout() == this.getLockTimeout() &&
      other.getSearchTimeout() == this.getSearchTimeout() &&
      other.getMessageSyncInterval() == this.getMessageSyncInterval() &&
      other.getCopyOnRead() == this.getCopyOnRead() &&
      other.isServer() == this.isServer();

    if (!sameConfig) {
      throw new RuntimeException(LocalizedStrings.CacheCreation_SAMECONFIG.toLocalizedString());

    } else {
      DynamicRegionFactory.Config drc1 = this.getDynamicRegionFactoryConfig();
      if (drc1 != null) {
        // we have a dynamic region factory
        DynamicRegionFactory.Config drc2 = null;
        if (other instanceof CacheCreation) {
          drc2 = ((CacheCreation)other).getDynamicRegionFactoryConfig();
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
          if (((CacheCreation)other).getDynamicRegionFactoryConfig() != null) {
            return false;
          }
        } else {
          // other must be real cache in which case we compare to DynamicRegionFactory
          if (DynamicRegionFactory.get().isOpen()) {
            return false;
          }
        }
      }

      Collection myBridges = this.getCacheServers();
      Collection otherBridges = other.getCacheServers();
      if (myBridges.size() != otherBridges.size()) {
        throw new RuntimeException(LocalizedStrings.CacheCreation_CACHESERVERS_SIZE.toLocalizedString());
      }

      for (Iterator myIter = myBridges.iterator(); myIter.hasNext(); ) {
        CacheServerCreation myBridge =
          (CacheServerCreation) myIter.next();
        boolean found = false;
        for (Iterator otherIter = otherBridges.iterator();
             otherIter.hasNext(); ) {
          CacheServer otherBridge = (CacheServer) otherIter.next();
          if (myBridge.sameAs(otherBridge)) {
            found = true;
            break;
          }
        }

        if (!found) {
          throw new RuntimeException(LocalizedStrings.CacheCreation_CACHE_SERVER_0_NOT_FOUND.toLocalizedString(myBridge));
        }
      }

      { // compare connection pools
        Map m1 = getPools();
        Map m2 = (other instanceof CacheCreation)
          ? ((CacheCreation)other).getPools()
          : PoolManager.getAll();
        int m1Size = m1.size();
        {
          // ignore any gateway instances
          Iterator it1 = m1.values().iterator();
          while (it1.hasNext()) {
            Pool cp = (Pool)it1.next();
            if (((PoolImpl)cp).isUsedByGateway()) {
              m1Size--;
            }
          }
        }
        int m2Size = m2.size();
        {
          // ignore any gateway instances
          Iterator it2 = m2.values().iterator();
          while (it2.hasNext()) {
            Pool cp = (Pool)it2.next();
            if (((PoolImpl)cp).isUsedByGateway()) {
              m2Size--;
            }
          }
        }
        if (m2Size == 1) {
          // if it is just the DEFAULT pool then ignore it
          Pool p = (Pool)m2.values().iterator().next();
          if (p.getName().equals("DEFAULT")) {
            m2Size = 0;
          }
        }
        
        if (m1Size != m2Size) {
          throw new RuntimeException("pool sizes differ m1Size=" + m1Size
                                     + " m2Size=" + m2Size
                                     + " m1=" + m1.values()
                                     + " m2=" + m2.values());
        }
        
        if (m1Size > 0) {
          Iterator it1 = m1.values().iterator();
          while (it1.hasNext()) {
            PoolImpl cp = (PoolImpl)it1.next();
            // ignore any gateway instances
            if (!(cp).isUsedByGateway()) {
              cp.sameAs(m2.get(cp.getName()));
            }
          }
        }
      }

      // compare disk stores
      for (Iterator myIter = diskStores.values().iterator(); myIter.hasNext(); ) {
        DiskStoreAttributesCreation dsac = (DiskStoreAttributesCreation)myIter.next();
        String name = dsac.getName();
        DiskStore ds = other.findDiskStore(name);
        if (ds == null) {
          getLogger().fine("Disk store " + name+" not found.");
          throw new RuntimeException(LocalizedStrings.CacheCreation_DISKSTORE_NOTFOUND_0.toLocalizedString(name));
        } else {
          if (!dsac.sameAs(ds)) {
            getLogger().fine("Attributes for disk store " + name + " do not match");
            throw new RuntimeException(LocalizedStrings.CacheCreation_ATTRIBUTES_FOR_DISKSTORE_0_DO_NOT_MATCH.toLocalizedString(name));
          }
        }
      }

      Map myNamedAttributes = this.listRegionAttributes();
      Map otherNamedAttributes = other.listRegionAttributes();
      if (myNamedAttributes.size() != otherNamedAttributes.size()) {
        throw new RuntimeException(LocalizedStrings.CacheCreation_NAMEDATTRIBUTES_SIZE.toLocalizedString());
      }

      for (Iterator myIter = myNamedAttributes.entrySet().iterator();
           myIter.hasNext(); ) {
        Map.Entry myEntry = (Map.Entry) myIter.next();
        String myId = (String) myEntry.getKey();
        Assert.assertTrue(myEntry.getValue() instanceof RegionAttributesCreation,
                          "Entry value is a " + myEntry.getValue().getClass().getName());
        RegionAttributesCreation myAttrs =
          (RegionAttributesCreation) myEntry.getValue();
        RegionAttributes otherAttrs = other.getRegionAttributes(myId);
        if (otherAttrs == null) {
          getLogger().fine("No attributes for " + myId);
          throw new RuntimeException(LocalizedStrings.CacheCreation_NO_ATTRIBUTES_FOR_0.toLocalizedString(myId));

        } else {
          if (!myAttrs.sameAs(otherAttrs)) {
            getLogger().fine("Attributes for " + myId +
                             " do not match");
            throw new RuntimeException(LocalizedStrings.CacheCreation_ATTRIBUTES_FOR_0_DO_NOT_MATCH.toLocalizedString(myId));
          }
        }
      }

      Collection myRoots = this.roots.values();
      Collection otherRoots = other.rootRegions();
      if (myRoots.size() != otherRoots.size()) {
        throw new RuntimeException(LocalizedStrings.CacheCreation_ROOTS_SIZE.toLocalizedString());
      }
      Iterator it = myRoots.iterator();
      while (it.hasNext()) {
        RegionCreation r = (RegionCreation)it.next();
        Region r2 = other.getRegion(r.getName());
        if (r2 == null) {
          throw new RuntimeException(LocalizedStrings.CacheCreation_NO_ROOT_0.toLocalizedString(r.getName()));
        } else if (!r.sameAs(r2)) {
          throw new RuntimeException(LocalizedStrings.CacheCreation_REGIONS_DIFFER.toLocalizedString());
        }
      }

      // If both have a listener, make sure they are equal.
      if (getCacheTransactionManager() != null) {
        // Currently the GemFireCache always has a CacheTransactionManager,
        // whereas that is not true for CacheTransactionManagerCreation.

        List otherTxListeners =
          Arrays.asList(other.getCacheTransactionManager().getListeners());
        List thisTxListeners =
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

  //////////  Inherited methods that don't do anything  //////////

  public void close() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void close(boolean keepalive) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

//  public Region createRootRegion(RegionAttributes aRegionAttributes)
//    throws RegionExistsException, TimeoutException  {
//
//    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
//  }

  // see Cache.isReconnecting()
  public boolean isReconnecting() {
    throw new UnsupportedOperationException();
  }

  // see Cache.waitUntilReconnected(long, TimeUnit)
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    throw new UnsupportedOperationException();
  }
  
  // see Cache.stopReconnecting()
  public void stopReconnecting() {
    throw new UnsupportedOperationException();
  }
  
  // see Cache.getReconnectedCache()
  public Cache getReconnectedCache() {
    throw new UnsupportedOperationException();
  }
  
  public LogWriter getLogger() {
    return this.logWriter;
  }

  public LogWriter getSecurityLogger() {
    return this.securityLogWriter;
  }

  public LogWriterI18n getLoggerI18n() {
    return this.logWriter.convertToLogWriterI18n();
  }
  
  public LogWriterI18n getSecurityLoggerI18n() {
    return this.securityLogWriter.convertToLogWriterI18n();
  }
  
  public DistributedSystem getDistributedSystem() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public boolean isClosed(){
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public String getName() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  
  public CancelCriterion getCancelCriterion() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public com.gemstone.gemfire.cache.query.QueryService getQueryService() {
    return queryService;
  }
    
  /**
   * @since 6.5
   */
  public <K,V> RegionFactory<K,V> createRegionFactory(RegionShortcut atts) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  /**
   * @since 6.5
   */
  public <K,V> RegionFactory<K,V> createRegionFactory() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  /**
   * @since 6.5
   */
  public <K,V> RegionFactory<K,V> createRegionFactory(String regionAttributesId) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  /**
   * @since 6.5
   */
  public <K,V> RegionFactory<K,V> createRegionFactory(RegionAttributes<K,V> regionAttributes) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public Region createVMRegion(String name, RegionAttributes attrs) throws RegionExistsException, TimeoutException {
    return createRegion(name, attrs);
  }
  public Region createRegion(String name, RegionAttributes attrs) throws RegionExistsException, TimeoutException {
    if (attrs instanceof RegionAttributesCreation) {
      ((RegionAttributesCreation) attrs).inheritAttributes(this);
      ((RegionAttributesCreation) attrs).prepareForValidation();
    }
    AttributesFactory.validateAttributes(attrs);
    RegionCreation region = new RegionCreation(this, name, null);
    region.setAttributes(attrs);
    this.addRootRegion(region);
    return region;
  }
  public Region createRegion(String name, String refid) throws RegionExistsException, TimeoutException {
    RegionCreation region = new RegionCreation(this, name, refid);
    this.addRootRegion(region);
    return region;
  }

  public Region getRegion(String path) {
    if (path.indexOf('/') != -1) {
      throw new UnsupportedOperationException();
    }
    return (Region)this.roots.get(path);
  }

  public CacheServer addCacheServer() {
    return addCacheServer(false);
  }
  
  public CacheServer addCacheServer(boolean isGatewayReceiver) {
    CacheServer bridge = new CacheServerCreation(this, false);
    this.bridgeServers.add(bridge);
    return bridge;
  }

  public void addDeclarableProperties(final Declarable declarable, final Properties properties) {
    this.declarablePropertiesMap.put(declarable, properties);
  }
  
  public List getCacheServers() {
    return this.bridgeServers;
  }
  
  public GatewaySender addGatewaySender(GatewaySender sender){
    this.gatewaySenders.add(sender);
    return sender;
  }
  
  public GatewayReceiver addGatewayReceiver(GatewayReceiver receiver){
    this.gatewayReceivers.add(receiver);
    return receiver;
  }
  
  public AsyncEventQueue addAsyncEventQueue(AsyncEventQueue asyncEventQueue) {
    this.asyncEventQueues.add(asyncEventQueue);
    return asyncEventQueue;
  }
  
  public Set<GatewaySender> getGatewaySenders(){
    Set<GatewaySender> tempSet = new HashSet<GatewaySender>();
    for (GatewaySender sender : this.gatewaySenders) {
      if (!((AbstractGatewaySender)sender).isForInternalUse()) {
        tempSet.add(sender);
      }
    }
    return tempSet;
  }
  
  public GatewaySender getGatewaySender(String Id) {
    for (GatewaySender sender : this.gatewaySenders) {
      if (sender.getId().equals(Id)) {
        return sender;
      }
    }
    return null;
  }
  
//  public GatewayReceiver addGatewayReceiver(){
//    GatewayReceiverCreation receiver = new GatewayReceiverCreation();
//    this.gatewayReceivers.add(receiver);
//    return receiver;
//  }
//  
  public Set<GatewayReceiver>  getGatewayReceivers(){
    return this.gatewayReceivers;
  }
  
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return this.asyncEventQueues;
  }
  
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : this.asyncEventQueues) {
      if (asyncEventQueue.getId().equals(id)) {
        return asyncEventQueue;
      }
    }
    return null;
  }

  public void setIsServer(boolean isServer) {
    this.isServer = isServer;
    this.hasServer = true;
  }

  public boolean isServer() {
	if (!this.isServer) {  
	  return (this.bridgeServers.size() > 0);
	}
	else {
	  return true;	
	}
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

  public DynamicRegionFactory.Config getDynamicRegionFactoryConfig() {
    return this.dynamicRegionFactoryConfig;
  }
  
  public CacheTransactionManager getCacheTransactionManager() {
    return this.txMgrCreation;
  }

  /**
   * Implementation of {@link Cache#setCopyOnRead}
   * @since 4.0
   */
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
    this.hasCopyOnRead = true;
  }

  /**
   * Implementation of {@link Cache#getCopyOnRead}
   * @since 4.0
   */
  public boolean getCopyOnRead() {
    return this.copyOnRead;
  }

  boolean hasCopyOnRead() {
    return this.hasCopyOnRead;
  }

  /**
   * Adds a CacheTransactionManagerCreation for this Cache (really just a
   * placeholder since a CacheTransactionManager is really a Cache singleton)
   * @since 4.0
   * @see GemFireCacheImpl
   */
  public void
    addCacheTransactionManagerCreation(CacheTransactionManagerCreation txm) {
    this.txMgrCreation = txm;
  }

/**
 * @return Context jndi context associated with the Cache.
 */
  public Context getJNDIContext() {
    return JNDIInvoker.getJNDIContext();
  }
  
  // It's not used
  public DiskStore findDiskStore(String storeName) {
    String s = storeName;
    if (s == null) {
      s = GemFireCacheImpl.getDefaultDiskStoreName();
    }
    return (DiskStore)this.diskStores.get(s);
  }
  public void addDiskStore(DiskStore ds) {
    this.diskStores.put(ds.getName(), ds);
  }

  /**
   * Returns the DiskStore list
   *
   * @since prPersistSprint2
   */
  public Collection<DiskStoreImpl> listDiskStores() {
    return this.diskStores.values();
  }
  
  public void setDiskStore(String name, DiskStoreAttributesCreation dsac) {
//    Assert.assertTrue(ds instanceof DiskStoreAttributesCreation,
//                      "Attributes are a " + ds.getClass().getName());
    this.diskStores.put(name, dsac);
  }

  public RegionAttributes getRegionAttributes(String id) {
    return (RegionAttributes) this.namedRegionAttributes.get(id);
  }

  public void setRegionAttributes(String id, RegionAttributes attrs) {
    RegionAttributes a = attrs;
    if (!(a instanceof RegionAttributesCreation)) {
      a = new RegionAttributesCreation(this, a, false);
    }
    this.namedRegionAttributes.put(id, a);
    this.regionAttributesNames.add(id);
  }

  public Map listRegionAttributes() {
    return Collections.unmodifiableMap(this.namedRegionAttributes);
  }

  public void loadCacheXml(InputStream is)
    throws TimeoutException, CacheWriterException,
           RegionExistsException {

    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void readyForEvents() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  private final PoolManagerImpl pm = new PoolManagerImpl(false);
  private volatile FunctionServiceCreation functionServiceCreation;

  public Map getPools() {
    return this.pm.getMap();
  }

  public PoolFactory createPoolFactory() {
    return (new PoolFactoryImpl(this.pm)).setStartDisabled(true);
  }

  public Pool findPool(String name) {
    return this.pm.find(name);
  }
  
  public void setFunctionServiceCreation(FunctionServiceCreation f) {
    this.functionServiceCreation = f;
  }
  public FunctionServiceCreation getFunctionServiceCreation() {
    return this.functionServiceCreation;
  }

  private volatile boolean hasResourceManager = false;
  private volatile ResourceManagerCreation resourceManagerCreation;
  public void setResourceManagerCreation(ResourceManagerCreation rmc) {
    this.hasResourceManager = true;
    this.resourceManagerCreation = rmc;
  }
  public ResourceManagerCreation getResourceManager() {
    return this.resourceManagerCreation;
  }
  public boolean hasResourceManager() {
    return this.hasResourceManager;
  }
  
  private volatile boolean hasSerializerRegistration = false;
  private volatile SerializerCreation serializerCreation;
  public void setSerializerCreation(SerializerCreation sc) {
    this.hasSerializerRegistration = true;
    this.serializerCreation = sc;
  }
  public SerializerCreation getSerializerCreation() {
    return this.serializerCreation;
  }
  public boolean hasSerializerCreation() {
    return this.hasSerializerRegistration;
  }
  public FunctionService getFunctionService(){
    throw new UnsupportedOperationException();
  }

  public void addBackup(File backup) {
    this.backups.add(backup);
  }
  
  public List<File> getBackupFiles() {
    return Collections.unmodifiableList(this.backups);
  }
  
  public GatewaySenderFactory createGatewaySenderFactory(){
    return WANServiceProvider.createGatewaySenderFactory(this);
  }
  
  public GatewayReceiverFactory createGatewayReceiverFactory() {
    return WANServiceProvider.createGatewayReceiverFactory(this);
  }
  
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
   * @see com.gemstone.gemfire.cache.CacheFactory#setPdxReadSerialized(boolean)
   *
   * @since 6.6
   */
  public boolean getPdxReadSerialized() {
     return cacheConfig.isPdxReadSerialized();
  }

  public PdxSerializer getPdxSerializer() {
    return cacheConfig.getPdxSerializer();
  }

  public String getPdxDiskStore() {
    return cacheConfig.getPdxDiskStore();
  }

  public boolean getPdxPersistent() {
    return cacheConfig.isPdxPersistent();
  }
  public boolean getPdxIgnoreUnreadFields() {
    return cacheConfig.getPdxIgnoreUnreadFields();
  }


  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Cache#getMembers()
   */
  public Set<DistributedMember> getMembers() {
    return Collections.EMPTY_SET;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Cache#getAdminMembers()
   */
  public Set<DistributedMember> getAdminMembers() {
    return Collections.EMPTY_SET;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Cache#getMembers(com.gemstone.gemfire.cache.Region)
   */
  public Set<DistributedMember> getMembers(Region r) {
    return Collections.EMPTY_SET;
  }

  private Declarable initializer = null;
  private Properties initializerProps = null;
  
  public Declarable getInitializer() {
    return this.initializer;
  }
  public Properties getInitializerProps() {
    return this.initializerProps;
  }
  
  public void setInitializer(Declarable d, Properties props) {
    this.initializer = d;
    this.initializerProps = props;
  }
  
  protected void runInitializer() {
    if (getInitializer() != null) {
      getInitializer().init(getInitializerProps());
    }
  }

  public void setGatewayConflictResolver(GatewayConflictResolver g) {
    this.gatewayConflictResolver = g;
  }

  public GatewayConflictResolver getGatewayConflictResolver() {
    return this.gatewayConflictResolver;
  }
    
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    throw new UnsupportedOperationException();
  }
  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean b) {
    throw new UnsupportedOperationException();
  }
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    throw new UnsupportedOperationException();
  }
  
  public CacheSnapshotService getSnapshotService() {
    throw new UnsupportedOperationException();
  }

  /**
   * @see Extensible#getExtensionPoint()
   * @since 8.1
   */
  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return extensionPoint;
  }
  
  @Override
  public HDFSStoreFactory createHDFSStoreFactory() {
    // TODO Auto-generated method stub
    return new HDFSStoreFactoryImpl(this);
  }
  @Override
  public HDFSStore findHDFSStore(String storeName) {
    return (HDFSStore)this.hdfsStores.get(storeName);
  }

  @Override
  public Collection<HDFSStoreImpl> getHDFSStores() {
    return this.hdfsStores.values();
  }

  public void addHDFSStore(String name, HDFSStoreCreation hs) {
    this.hdfsStores.put(name, hs);
  }

  

  @Override
  public DistributedMember getMyId() {
    return null;
  }

  @Override
  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault() {
    return null;
  }

  @Override
  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned() {
    return null;
  }

  @Override
  public CqService getCqService() {
    return null;
  }

  @Override
  public LuceneService getLuceneService() {
    return null;
  }

  public QueryService queryService = new com.gemstone.gemfire.cache.query.QueryService() {

    private Map<String, List> indexes = new HashMap<String, List>();
    
    @Override
    public Query newQuery(String queryString) {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public Index createHashIndex(String indexName, String indexedExpression,
        String regionPath) throws IndexInvalidException,
        IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createHashIndex(indexName, indexedExpression, regionPath, "");
    }

    @Override
    public Index createHashIndex(String indexName, String indexedExpression,
        String regionPath, String imports) throws IndexInvalidException,
        IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, IndexType.HASH, indexedExpression, regionPath, imports);
    }

    @Override
    public Index createIndex(String indexName, IndexType indexType,
        String indexedExpression, String fromClause)
        throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException,
        UnsupportedOperationException {
      return createIndex(indexName, indexType, indexedExpression, fromClause, "");
    }

    @Override
    /** 
     * Due to not having the full implementation to determine region names etc
     * this implementation will only match a single region with no alias at this time
     */
    public Index createIndex(String indexName, IndexType indexType,
        String indexedExpression, String fromClause, String imports)
        throws IndexInvalidException, IndexNameConflictException,
        IndexExistsException, RegionNotFoundException,
        UnsupportedOperationException {
      IndexCreationData indexData = new IndexCreationData(indexName);
      indexData.setFunctionalIndexData(fromClause, indexedExpression, imports);
      indexData.setIndexType(indexType.toString());
      List indexesForRegion = indexes.get(fromClause);
      if (indexesForRegion == null) {
        indexesForRegion = new ArrayList();
        indexes.put(fromClause, indexesForRegion);
      }
      indexesForRegion.add(indexData);
      return indexData;
    }

    @Override
    public Index createIndex(String indexName, String indexedExpression,
        String regionPath) throws IndexInvalidException,
        IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
      return createIndex(indexName, indexedExpression, regionPath, "");
    }

    @Override
    public Index createIndex(String indexName, String indexedExpression,
        String regionPath, String imports) throws IndexInvalidException,
        IndexNameConflictException, IndexExistsException,
        RegionNotFoundException, UnsupportedOperationException {
       return createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, regionPath, imports);

    }

    @Override
    public Index createKeyIndex(String indexName, String indexedExpression,
        String regionPath) throws IndexInvalidException,
        IndexNameConflictException, IndexExistsException,
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
      return indexes.get(region.getFullPath());
    }

    @Override
    public Collection<Index> getIndexes(Region<?, ?> region,
        IndexType indexType) {
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
    public CqQuery newCq(String queryString, CqAttributes cqAttr,
        boolean isDurable) throws QueryInvalidException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String name, String queryString, CqAttributes cqAttr)
        throws QueryInvalidException, CqExistsException, CqException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public CqQuery newCq(String name, String queryString,
        CqAttributes cqAttr, boolean isDurable) throws QueryInvalidException,
        CqExistsException, CqException {
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
    public void defineKeyIndex(String indexName, String indexedExpression,
        String fromClause) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineHashIndex(String indexName, String indexedExpression,
        String regionPath) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineHashIndex(String indexName, String indexedExpression,
        String regionPath, String imports) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineIndex(String indexName, String indexedExpression,
        String regionPath) throws RegionNotFoundException {
      throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }

    @Override
    public void defineIndex(String indexName, String indexedExpression,
        String regionPath, String imports) throws RegionNotFoundException {
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
	  

}
