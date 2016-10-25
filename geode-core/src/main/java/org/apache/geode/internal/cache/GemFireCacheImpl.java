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

package org.apache.geode.internal.cache;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.naming.Context;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireCacheException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientRegionFactoryImpl;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.CqServiceProvider;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PooledExecutorWithDMStats;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.execute.util.FindRestEnabledServersFunction;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.lru.HeapEvictor;
import org.apache.geode.internal.cache.lru.OffHeapEvictor;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.persistence.BackupManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.query.TemporaryResultSetFactory;
import org.apache.geode.internal.cache.snapshot.CacheSnapshotServiceImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheXmlPropertyResolver;
import org.apache.geode.internal.cache.xmlcache.PropertyResolver;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.jta.TransactionManagerImpl;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.process.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.sequencelog.SequenceLoggerImpl;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.management.internal.JmxManagerAdvisee;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.management.internal.beans.ManagementListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.memcached.GemFireMemcachedServer;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.redis.GeodeRedisServer;

// @todo somebody Come up with more reasonable values for {@link #DEFAULT_LOCK_TIMEOUT}, etc.
/**
 * GemFire's implementation of a distributed {@link org.apache.geode.cache.Cache}.
 *
 */
@SuppressWarnings("deprecation")
public class GemFireCacheImpl implements InternalCache, ClientCache, HasCachePerfStats, DistributionAdvisee, CacheTime {
  private static final Logger logger = LogService.getLogger();
  
  // moved *SERIAL_NUMBER stuff to DistributionAdvisor

  /** The default number of seconds to wait for a distributed lock */
  public static final int DEFAULT_LOCK_TIMEOUT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultLockTimeout", 60).intValue();

  /**
   * The default duration (in seconds) of a lease on a distributed lock
   */
  public static final int DEFAULT_LOCK_LEASE = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultLockLease", 120).intValue();

  /** The default "copy on read" attribute value */
  public static final boolean DEFAULT_COPY_ON_READ = false;

  /** the last instance of GemFireCache created */
  private static volatile GemFireCacheImpl instance = null;
  /**
   * Just like instance but is valid for a bit longer so that pdx can still find the cache during a close.
   */
  private static volatile GemFireCacheImpl pdxInstance = null;

  /**
   * The default amount of time to wait for a <code>netSearch</code> to complete
   */
  public static final int DEFAULT_SEARCH_TIMEOUT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultSearchTimeout", 300).intValue();

  /**
   * The <code>CacheLifecycleListener</code> s that have been registered in this VM
   */
  private static final Set<CacheLifecycleListener> cacheLifecycleListeners = new HashSet<CacheLifecycleListener>();

  /**
   * Define gemfire.Cache.ASYNC_EVENT_LISTENERS=true to invoke event listeners in the background
   */
  private static final boolean ASYNC_EVENT_LISTENERS = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Cache.ASYNC_EVENT_LISTENERS");

  /**
   * If true then when a delta is applied the size of the entry value will be recalculated. If false (the default) then
   * the size of the entry value is unchanged by a delta application. Not a final so that tests can change this value.
   *
   * @since GemFire hitachi 6.1.2.9
   */
  public static boolean DELTAS_RECALCULATE_SIZE = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DELTAS_RECALCULATE_SIZE");

  public static final int EVENT_QUEUE_LIMIT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.EVENT_QUEUE_LIMIT", 4096).intValue();
  public static final int EVENT_THREAD_LIMIT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.EVENT_THREAD_LIMIT", 16).intValue();

  /**
   * System property to limit the max query-execution time. By default its turned off (-1), the time is set in MiliSecs.
   */
  public static final int MAX_QUERY_EXECUTION_TIME = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.MAX_QUERY_EXECUTION_TIME", -1).intValue();

  /**
   * System property to disable query monitor even if resource manager is in use
   */
  public final boolean QUERY_MONITOR_DISABLED_FOR_LOW_MEM = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");
  
  /**
   * Property set to true if resource manager heap percentage is set and query monitor is required
   */
  public static Boolean QUERY_MONITOR_REQUIRED_FOR_RESOURCE_MANAGER = Boolean.FALSE;

  /**
   * This property defines internal function that will get executed on each node to fetch active REST service endpoints (servers).
   */
  public static final String FIND_REST_ENABLED_SERVERS_FUNCTION_ID = FindRestEnabledServersFunction.class.getName();

  /**
   * True if the user is allowed lock when memory resources appear to be overcommitted. 
   */
  public static final boolean ALLOW_MEMORY_LOCK_WHEN_OVERCOMMITTED = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Cache.ALLOW_MEMORY_OVERCOMMIT");

  //time in ms
  private static final int FIVE_HOURS = 5 * 60 * 60 * 1000;
  /** To test MAX_QUERY_EXECUTION_TIME option. */
  public int TEST_MAX_QUERY_EXECUTION_TIME = -1;
  public boolean TEST_MAX_QUERY_EXECUTION_TIME_OVERRIDE_EXCEPTION = false;

  // ///////////////////// Instance Fields ///////////////////////

  private final InternalDistributedSystem system;

  private final DM dm;

  // This is a HashMap because I know that clear() on it does
  // not allocate objects.
  private final HashMap rootRegions;

  /**
   * True if this cache is being created by a ClientCacheFactory.
   */
  private final boolean isClient;
  private PoolFactory clientpf;
  /**
   * It is not final to allow cache.xml parsing to set it.
   */
  private Pool defaultPool;

  private final ConcurrentMap pathToRegion = new ConcurrentHashMap();

  protected volatile boolean isInitialized = false;
  protected volatile boolean isClosing = false;
  protected volatile boolean closingGatewaySendersByShutdownAll = false;
  protected volatile boolean closingGatewayReceiversByShutdownAll = false;

  /** Amount of time (in seconds) to wait for a distributed lock */
  private int lockTimeout = DEFAULT_LOCK_TIMEOUT;

  /** Amount of time a lease of a distributed lock lasts */
  private int lockLease = DEFAULT_LOCK_LEASE;

  /** Amount of time to wait for a <code>netSearch</code> to complete */
  private int searchTimeout = DEFAULT_SEARCH_TIMEOUT;

  private final CachePerfStats cachePerfStats;

  /** Date on which this instances was created */
  private final Date creationDate;

  /** thread pool for event dispatching */
  private final ThreadPoolExecutor eventThreadPool;

  /**
   * the list of all cache servers. CopyOnWriteArrayList is used to allow concurrent add, remove and retrieval
   * operations. It is assumed that the traversal operations on cache servers list vastly outnumber the mutative
   * operations such as add, remove.
   */
  private volatile List allCacheServers = new CopyOnWriteArrayList();

  /**
   * Controls updates to the list of all gateway senders
   *
   * @see #allGatewaySenders
   */
  public final Object allGatewaySendersLock = new Object();

  /**
   * the set of all gateway senders. It may be fetched safely (for enumeration), but updates must by synchronized via
   * {@link #allGatewaySendersLock}
   */
  private volatile Set<GatewaySender> allGatewaySenders = Collections.emptySet();
  
  /**
   * The list of all async event queues added to the cache. 
   * CopyOnWriteArrayList is used to allow concurrent add, remove and retrieval operations.
   */
  private volatile Set<AsyncEventQueue> allVisibleAsyncEventQueues = new CopyOnWriteArraySet<AsyncEventQueue>();

  /**
   * The list of all async event queues added to the cache. 
   * CopyOnWriteArrayList is used to allow concurrent add, remove and retrieval operations.
   */
  private volatile Set<AsyncEventQueue> allAsyncEventQueues = new CopyOnWriteArraySet<AsyncEventQueue>();
  
  /**
   * Controls updates to the list of all gateway receivers
   *
   * @see #allGatewayReceivers
   */
  public final Object allGatewayReceiversLock = new Object();

  /**
   * the list of all gateway Receivers. It may be fetched safely (for enumeration), but updates must by synchronized via
   * {@link #allGatewayReceiversLock}
   */
  private volatile Set<GatewayReceiver> allGatewayReceivers = Collections.emptySet();

  /** PartitionedRegion instances (for required-events notification */
  // This is a HashSet because I know that clear() on it does not
  // allocate any objects.
  private final HashSet<PartitionedRegion> partitionedRegions = new HashSet<PartitionedRegion>();

  /**
   * Fix for 42051 This is a map of regions that are in the process of being destroyed. We could potentially leave the
   * regions in the pathToRegion map, but that would entail too many changes at this point in the release. We need to
   * know which regions are being destroyed so that a profile exchange can get the persistent id of the destroying
   * region and know not to persist that ID if it receives it as part of the persistent view.
   */
  private final ConcurrentMap<String, DistributedRegion> regionsInDestroy = new ConcurrentHashMap<String, DistributedRegion>();

  public final Object allGatewayHubsLock = new Object();
  
  /**
   * conflict resolver for WAN, if any
   * @guarded.By {@link #allGatewayHubsLock}
   */
  private GatewayConflictResolver gatewayConflictResolver;

  /** Is this is "server" cache? */
  private boolean isServer = false;

  /** transaction manager for this cache */
  private final TXManagerImpl txMgr;

  private RestAgent restAgent;
  
  private boolean isRESTServiceRunning = false;
  
  /** Copy on Read feature for all read operations e.g. get */
  private volatile boolean copyOnRead = DEFAULT_COPY_ON_READ;
  
  /** The named region attributes registered with this cache. */
  private final Map namedRegionAttributes = Collections.synchronizedMap(new HashMap());

  /**
   * if this cache was forced to close due to a forced-disconnect, we retain a ForcedDisconnectException that can be
   * used as the cause
   */
  private boolean forcedDisconnect;

  /**
   * if this cache was forced to close due to a forced-disconnect or system failure, this keeps track of the reason
   */
  protected volatile Throwable disconnectCause = null;

  /** context where this cache was created -- for debugging, really... */
  public Exception creationStack = null;

  /**
   * a system timer task for cleaning up old bridge thread event entries
   */
  private EventTracker.ExpiryTask recordedEventSweeper;

  private TombstoneService tombstoneService;

  /**
   * DistributedLockService for PartitionedRegions. Remains null until the first PartitionedRegion is created. Destroyed
   * by GemFireCache when closing the cache. Protected by synchronization on this GemFireCache.
   *
   * @guarded.By prLockServiceLock
   */
  private DistributedLockService prLockService;

  /**
   * lock used to access prLockService
   */
  private final Object prLockServiceLock = new Object();
  
  /**
   * DistributedLockService for GatewaySenders. Remains null until the
   * first GatewaySender is created. Destroyed by GemFireCache when closing
   * the cache.
   * @guarded.By gatewayLockServiceLock
   */
  private volatile DistributedLockService gatewayLockService;
  
  /**
   * Lock used to access gatewayLockService
   */
  private final Object gatewayLockServiceLock = new Object();

  private final InternalResourceManager resourceManager;

  private final AtomicReference<BackupManager> backupManager = new AtomicReference<BackupManager>();

  private HeapEvictor heapEvictor = null;
  
  private OffHeapEvictor offHeapEvictor = null;

  private final Object heapEvictorLock = new Object();
  
  private final Object offHeapEvictorLock = new Object();

  private ResourceEventsListener listener;

  /**
   * Enabled when CacheExistsException issues arise in debugging
   *
   * @see #creationStack
   */
  private static final boolean DEBUG_CREATION_STACK = false;

  private volatile QueryMonitor queryMonitor;

  private final Object queryMonitorLock = new Object();

  private final PersistentMemberManager persistentMemberManager;

  private ClientMetadataService clientMetadatService = null;

  private final Object clientMetaDatServiceLock = new Object();

  private volatile boolean isShutDownAll = false;

  private final ResourceAdvisor resourceAdvisor;
  private final JmxManagerAdvisor jmxAdvisor;

  private final int serialNumber;

  private final TXEntryStateFactory txEntryStateFactory;

  private final CacheConfig cacheConfig;
  
  private final DiskStoreMonitor diskMonitor;
  
  // Stores the properties used to initialize declarables.
  private final Map<Declarable, Properties> declarablePropertiesMap = new ConcurrentHashMap<Declarable, Properties>();

  /** {@link PropertyResolver} to resolve ${} type property strings */
  protected static PropertyResolver resolver;

  protected static boolean xmlParameterizationEnabled = !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "xml.parameterization.disabled");

  public static Runnable internalBeforeApplyChanges;

  public static Runnable internalBeforeNonTXBasicPut;

  /**
   * the memcachedServer instance that is started when {@link DistributionConfig#getMemcachedPort()}
   * is specified
   */
  private GemFireMemcachedServer memcachedServer;
  
  /**
   * Redis server is started when {@link DistributionConfig#getRedisPort()} is set
   */
  private GeodeRedisServer redisServer;
  
  /**
   * {@link ExtensionPoint} support.
   * @since GemFire 8.1
   */
  private SimpleExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<Cache>(this, this);
  
  private final CqService cqService;
  
  private final Set<RegionListener> regionListeners = new ConcurrentHashSet<RegionListener>();
  
  private final Map<Class<? extends CacheService>, CacheService> services = new HashMap<Class<? extends CacheService>, CacheService>();
  
  public static final int DEFAULT_CLIENT_FUNCTION_TIMEOUT = 0;

  private static int clientFunctionTimeout;

  private final static Boolean DISABLE_AUTO_EVICTION = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disableAutoEviction");

  private static SecurityService securityService = SecurityService.getSecurityService();

  static {
    // this works around jdk bug 6427854, reported in ticket #44434
    String propertyName = "sun.nio.ch.bugLevel";
    String value = System.getProperty(propertyName);
    if (value == null) {
      System.setProperty(propertyName, "");
    }
  }

  /**
   * Invokes mlockall().  Locks  all pages mapped into the address space of the 
   * calling process.  This includes the pages of the code, data and stack segment, 
   * as well as shared libraries, user space kernel data, shared memory, and 
   * memory-mapped files.  All mapped pages are guaranteed to be resident in RAM 
   * when the call returns successfully; the pages are guaranteed to stay in RAM 
   * until later unlocked.
   * 
   * @param flags
   *    MCL_CURRENT 1 - Lock all pages which are currently mapped into the 
   *    address space of the process.
   *    
   *    MCL_FUTURE  2 - Lock all pages which will become mapped into the address 
   *    space of the process in the future.  These could be for instance new 
   *    pages required by a growing heap and stack as well as new memory mapped 
   *    files or shared memory regions.
   *    
   * @return 
   *    0 if success, non-zero if error and errno set
   *    
   */
  private static native int mlockall(int flags);

  public static void lockMemory() {
    int result = 0;
    try {
      Native.register(Platform.C_LIBRARY_NAME);
      result = mlockall(1);
      if (result == 0) {
        return;
      }
    } catch (Throwable t) {
      throw new IllegalStateException("Error trying to lock memory", t);
    }

    int errno = Native.getLastError();
    String msg = "mlockall failed: " + errno;
    if (errno == 1 || errno == 12) {  // EPERM || ENOMEM
      msg = "Unable to lock memory due to insufficient free space or privileges.  " 
          + "Please check the RLIMIT_MEMLOCK soft resource limit (ulimit -l) and " 
          + "increase the available memory if needed";
    }
    throw new IllegalStateException(msg);
  }
  
  /**
   * This is for debugging cache-open issues (esp. {@link org.apache.geode.cache.CacheExistsException})
   */
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("GemFireCache[");
    sb.append("id = " + System.identityHashCode(this));
    sb.append("; isClosing = " + this.isClosing);
    sb.append("; isShutDownAll = " + this.isShutDownAll);
    sb.append("; created = " + this.creationDate);
    sb.append("; server = " + this.isServer);
    sb.append("; copyOnRead = " + this.copyOnRead);
    sb.append("; lockLease = " + this.lockLease);
    sb.append("; lockTimeout = " + this.lockTimeout);
    // sb.append("; rootRegions = (" + this.rootRegions + ")");
    // sb.append("; cacheServers = (" + this.cacheServers + ")");
    // sb.append("; regionAttributes = (" + this.listRegionAttributes());
    // sb.append("; gatewayHub = " + gatewayHub);
    if (this.creationStack != null) {
      sb.append("\nCreation context:\n");
      OutputStream os = new OutputStream() {
        @Override
        public void write(int i) {
          sb.append((char) i);
        }
      };
      PrintStream ps = new PrintStream(os);
      this.creationStack.printStackTrace(ps);
    }
    sb.append("]");
    return sb.toString();
  }

  // ////////////////////// Constructors /////////////////////////

  /** Map of Futures used to track Regions that are being reinitialized */
  private final ConcurrentMap reinitializingRegions = new ConcurrentHashMap();

  /** Returns the last created instance of GemFireCache */
  public static GemFireCacheImpl getInstance() {
    return instance;
  }
  
  /* Used for testing, retain the old instance in the test and re-set the value when test completes*/
  public static GemFireCacheImpl setInstanceForTests(GemFireCacheImpl cache) {
    GemFireCacheImpl oldInstance = instance;
	  instance = cache;
	  return oldInstance;
  }

  /**
   * Returns an existing instance. If a cache does not exist
   * throws a cache closed exception.
   * 
   * @return the existing cache
   * @throws CacheClosedException
   *           if an existing cache can not be found.
   */
  public static final GemFireCacheImpl getExisting() {
    final GemFireCacheImpl result = instance;
    if (result != null && !result.isClosing) {
      return result;
    }
    if (result != null) {
      throw result.getCacheClosedException(LocalizedStrings
        .CacheFactory_THE_CACHE_HAS_BEEN_CLOSED.toLocalizedString(), null);
    }
    throw new CacheClosedException(LocalizedStrings
        .CacheFactory_A_CACHE_HAS_NOT_YET_BEEN_CREATED.toLocalizedString());
  }

  /**
   * Returns an existing instance. If a cache does not exist throws an exception.
   *
   * @param reason
   *          the reason an existing cache is being requested.
   * @return the existing cache
   * @throws CacheClosedException
   *           if an existing cache can not be found.
   */
  public static GemFireCacheImpl getExisting(String reason) {
    GemFireCacheImpl result = getInstance();
    if (result == null) {
      throw new CacheClosedException(reason);
    }
    return result;
  }

  /**
   * Pdx is allowed to obtain the cache even while it is being closed
   */
  public static GemFireCacheImpl getForPdx(String reason) {
    GemFireCacheImpl result = pdxInstance;
    if (result == null) {
      throw new CacheClosedException(reason);
    }
    return result;
  }

  // /**
  // * @deprecated remove when Lise allows a Hydra VM to
  // * be re-created
  // */
  // public static void clearInstance() {
  // System.err.println("DEBUG: do not commit GemFireCache#clearInstance");
  // instance = null;
  // }

  public static GemFireCacheImpl createClient(DistributedSystem system, PoolFactory pf, CacheConfig cacheConfig) {
    return basicCreate(system, true, cacheConfig, pf, true, ASYNC_EVENT_LISTENERS, null);
  }

  public static GemFireCacheImpl create(DistributedSystem system, CacheConfig cacheConfig) {
    return basicCreate(system, true, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }

  public static GemFireCacheImpl createWithAsyncEventListeners(DistributedSystem system, CacheConfig cacheConfig, TypeRegistry typeRegistry) {
    return basicCreate(system, true, cacheConfig, null, false, true, typeRegistry);
  }
  
 public static Cache create(DistributedSystem system, boolean existingOk, CacheConfig cacheConfig) {
    return basicCreate(system, existingOk, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }

  private static GemFireCacheImpl basicCreate(DistributedSystem system, boolean existingOk, CacheConfig cacheConfig, PoolFactory pf, boolean isClient, boolean asyncEventListeners, TypeRegistry typeRegistry)
  throws CacheExistsException, TimeoutException, CacheWriterException,
  GatewayException,
  RegionExistsException 
  {
    try {
      synchronized (GemFireCacheImpl.class) {
        GemFireCacheImpl instance = checkExistingCache(existingOk, cacheConfig);
        if (instance == null) {
          instance = new GemFireCacheImpl(isClient, pf, system, cacheConfig, asyncEventListeners, typeRegistry);
          instance.initialize();
        }
        return instance;
      }
    } catch (CacheXmlException | IllegalArgumentException e) {
      logger.error(e.getLocalizedMessage());
      throw e;
    } catch (Error | RuntimeException e) {
      logger.error(e);
      throw e;
    }
  }

  private static GemFireCacheImpl checkExistingCache(boolean existingOk, CacheConfig cacheConfig) {
    GemFireCacheImpl instance = getInstance();

    if (instance != null && !instance.isClosed()) {
      if (existingOk) {
        // Check if cache configuration matches.
        cacheConfig.validateCacheConfig(instance);
        return instance;
      } else {
        // instance.creationStack argument is for debugging...
        throw new CacheExistsException(instance, LocalizedStrings.CacheFactory_0_AN_OPEN_CACHE_ALREADY_EXISTS.toLocalizedString(instance), instance.creationStack);
      }
    }
    return null;
  }

  /**
   * Creates a new instance of GemFireCache and populates it according to the <code>cache.xml</code>, if appropriate.
   * @param typeRegistry: currently only unit tests set this parameter to a non-null value
   */
  private GemFireCacheImpl(boolean isClient, PoolFactory pf, DistributedSystem system, CacheConfig cacheConfig, boolean asyncEventListeners, TypeRegistry typeRegistry) {
    this.isClient = isClient;
    this.clientpf = pf;
    this.cacheConfig = cacheConfig; // do early for bug 43213
    this.pdxRegistry = typeRegistry;

    // Synchronized to prevent a new cache from being created
    // before an old one has finished closing
    synchronized (GemFireCacheImpl.class) {
      
      // start JTA transaction manager within this synchronized block
      // to prevent race with cache close. fixes bug 43987
      JNDIInvoker.mapTransactions(system);
      this.system = (InternalDistributedSystem) system;
      this.dm = this.system.getDistributionManager();
      if (!this.isClient && PoolManager.getAll().isEmpty()) {
        // We only support management on members of a distributed system
        // Should do this:     if (!getSystem().isLoner()) {
        // but it causes quickstart.CqClientTest to hang
        this.listener = new ManagementListener();
        this.system.addResourceListener(listener);
        if (this.system.isLoner()) {
          this.system.getInternalLogWriter().info(LocalizedStrings.GemFireCacheImpl_RUNNING_IN_LOCAL_MODE);
        }
      } else {
        getLogger().info("Running in client mode");
        this.listener = null;
      }

      // Don't let admin-only VMs create Cache's just yet.
      DM dm = this.system.getDistributionManager();
      if (dm instanceof DistributionManager) {
        if (((DistributionManager) dm).getDMType() == DistributionManager.ADMIN_ONLY_DM_TYPE) {
          throw new IllegalStateException(LocalizedStrings.GemFireCache_CANNOT_CREATE_A_CACHE_IN_AN_ADMINONLY_VM
              .toLocalizedString());
        }
      }

      this.rootRegions = new HashMap();
      
      this.cqService = CqServiceProvider.create(this);

      initReliableMessageQueueFactory();

      // Create the CacheStatistics
      this.cachePerfStats = new CachePerfStats(system);
      CachePerfStats.enableClockStats = this.system.getConfig().getEnableTimeStatistics();

      this.txMgr = new TXManagerImpl(this.cachePerfStats, this);
      dm.addMembershipListener(this.txMgr);

      this.creationDate = new Date();

      this.persistentMemberManager = new PersistentMemberManager();

      if (asyncEventListeners) {
        final ThreadGroup group = LoggingThreadGroup.createThreadGroup("Message Event Threads",logger);
        ThreadFactory tf = new ThreadFactory() {
          public Thread newThread(final Runnable command) {
            final Runnable r = new Runnable() {
              public void run() {
                ConnectionTable.threadWantsSharedResources();
                command.run();
              }
            };
            Thread thread = new Thread(group, r, "Message Event Thread");
            thread.setDaemon(true);
            return thread;
          }
        };
        ArrayBlockingQueue q = new ArrayBlockingQueue(EVENT_QUEUE_LIMIT);
        this.eventThreadPool = new PooledExecutorWithDMStats(q, EVENT_THREAD_LIMIT, this.cachePerfStats.getEventPoolHelper(), tf, 1000);
      } else {
        this.eventThreadPool = null;
      }

      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      this.resourceAdvisor = ResourceAdvisor.createResourceAdvisor(this);
      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      this.jmxAdvisor = JmxManagerAdvisor.createJmxManagerAdvisor(new JmxManagerAdvisee(this));
      
      resourceManager = InternalResourceManager.createResourceManager(this);
      this.serialNumber = DistributionAdvisor.createSerialNumber();

      getResourceManager().addResourceListener(ResourceType.HEAP_MEMORY, getHeapEvictor());
      
      /*
       * Only bother creating an off-heap evictor if we have off-heap memory enabled.
       */
      if(null != getOffHeapStore()) {
        getResourceManager().addResourceListener(ResourceType.OFFHEAP_MEMORY, getOffHeapEvictor());
      }
      
      recordedEventSweeper = EventTracker.startTrackerServices(this);
      tombstoneService = TombstoneService.initialize(this);

      TypeRegistry.init();
      basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
      TypeRegistry.open();

      if (!isClient()) {
        // Initialize the QRM thread freqeuncy to default (1 second )to prevent spill
        // over from previous Cache , as the interval is stored in a static
        // volatile field.
        HARegionQueue.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
      }
      FunctionService.registerFunction(new PRContainsValueFunction());
      this.expirationScheduler = new ExpirationScheduler(this.system);

      // uncomment following line when debugging CacheExistsException
      if (DEBUG_CREATION_STACK) {
        this.creationStack = new Exception(LocalizedStrings.GemFireCache_CREATED_GEMFIRECACHE_0.toLocalizedString(toString()));
      }

      this.txEntryStateFactory = TXEntryState.getFactory();
      if (xmlParameterizationEnabled) {
        /** If product properties file is available replace properties from there */
        Properties userProps = this.system.getConfig().getUserDefinedProps();
        if (userProps != null && !userProps.isEmpty()) {
          resolver = new CacheXmlPropertyResolver(false, PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, userProps);
        } else {
          resolver = new CacheXmlPropertyResolver(false, PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, null);
        }
      }
     
      SystemFailure.signalCacheCreate();
      
      diskMonitor = new DiskStoreMonitor();
    } // synchronized
  }
  
  public boolean isRESTServiceRunning() {
    return isRESTServiceRunning;
  }

  public void setRESTServiceRunning(boolean isRESTServiceRunning) {
    this.isRESTServiceRunning = isRESTServiceRunning;
  }

  /**
   * Used by Hydra tests to get handle of Rest Agent  
   * @return RestAgent
   */
  public RestAgent getRestAgent() {
    return restAgent;
  }
  
  /*****
   * Request the shared configuration from the locator(s) which have the Cluster config service running
   */
  public ConfigurationResponse requestSharedConfiguration() {
    //Request the shared configuration from the locator(s)
    final DistributionConfig config = this.system.getConfig();

    if (!(dm instanceof DistributionManager))
      return null;

    // do nothing if this vm is/has locator or this is a client
    if( ((DistributionManager)dm).getDMType() == DistributionManager.LOCATOR_DM_TYPE
      || isClient
      || Locator.getLocator() !=null )
      return null;

    Map<InternalDistributedMember, Collection<String>> scl = this.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration();

    //If there are no locators with Shared configuration, that means the system has been started without shared configuration
    //then do not make requests to the locators
    if(scl.isEmpty()) {
      logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_NO_LOCATORS_FOUND_WITH_SHARED_CONFIGURATION));
      return null;
    }

    String groupsString = config.getGroups();
    ConfigurationResponse response = null;
    List<String> locatorConnectionStrings = getSharedConfigLocatorConnectionStringList();

    try {
      response = ClusterConfigurationLoader.requestConfigurationFromLocators(system.getConfig(), locatorConnectionStrings);

      //log the configuration received from the locator
      logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_RECEIVED_SHARED_CONFIGURATION_FROM_LOCATORS));
      logger.info(response.describeConfig());

      Configuration clusterConfig = response.getRequestedConfiguration().get(SharedConfiguration.CLUSTER_CONFIG);
      Properties clusterSecProperties = (clusterConfig==null) ? new Properties():clusterConfig.getGemfireProperties();

      // If not using shared configuration, return null or throw an exception is locator is secured
      if(!config.getUseSharedConfiguration()){
        if (clusterSecProperties.containsKey(ConfigurationProperties.SECURITY_MANAGER)) {
          throw new GemFireConfigException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION_2.toLocalizedString());
        } else {
          logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_NOT_USING_SHARED_CONFIGURATION));
          return null;
        }
      }

      Properties serverSecProperties = config.getSecurityProps();
      //check for possible mis-configuration
      if (isMisConfigured(clusterSecProperties, serverSecProperties, ConfigurationProperties.SECURITY_MANAGER)
       || isMisConfigured(clusterSecProperties, serverSecProperties, ConfigurationProperties.SECURITY_POST_PROCESSOR)) {
        throw new GemFireConfigException(LocalizedStrings.GEMFIRE_CACHE_SECURITY_MISCONFIGURATION.toLocalizedString());
      }
      return response;

    } catch (ClusterConfigurationNotAvailableException e) {
      throw new GemFireConfigException(LocalizedStrings.GemFireCache_SHARED_CONFIGURATION_NOT_AVAILABLE.toLocalizedString(), e);
    } catch (UnknownHostException e) {
      throw new GemFireConfigException(e.getLocalizedMessage(), e);
    }
  }

  public void deployJarsRecevedFromClusterConfiguration(ConfigurationResponse response){
    try{
      ClusterConfigurationLoader.deployJarsReceivedFromClusterConfiguration(this, response);
    } catch (IOException e) {
      throw new GemFireConfigException(LocalizedStrings.GemFireCache_EXCEPTION_OCCURED_WHILE_DEPLOYING_JARS_FROM_SHARED_CONDFIGURATION.toLocalizedString(), e);
    } catch (ClassNotFoundException e) {
      throw new GemFireConfigException(LocalizedStrings.GemFireCache_EXCEPTION_OCCURED_WHILE_DEPLOYING_JARS_FROM_SHARED_CONDFIGURATION.toLocalizedString(), e);
    }
  }


  // When called, clusterProps and serverProps and key could not be null
  public static boolean isMisConfigured(Properties clusterProps, Properties serverProps, String key){
    String clusterPropValue = clusterProps.getProperty(key);
    String serverPropValue = serverProps.getProperty(key);

    // if this server prop is not specified, this is always OK.
    if(StringUtils.isBlank(serverPropValue))
      return false;

    // server props is not blank, but cluster props is blank, NOT OK.
    if(StringUtils.isBlank(clusterPropValue))
      return true;

    // at this point check for eqality
    return !clusterPropValue.equals(serverPropValue);
  }

  public List<String> getSharedConfigLocatorConnectionStringList() {
    List<String> locatorConnectionStringList = new ArrayList<String>();
    
    Map<InternalDistributedMember, Collection<String>> scl = this.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration();

    //If there are no locators with Shared configuration, that means the system has been started without shared configuration 
    //then do not make requests to the locators
    if (!scl.isEmpty()) {
      Set<Entry<InternalDistributedMember, Collection<String>>> locs =  scl.entrySet();
      
      for (Entry<InternalDistributedMember, Collection<String>> loc : locs) {
        Collection<String> locStrings = loc.getValue();
        Iterator<String> locStringIter = locStrings.iterator();
        
        while (locStringIter.hasNext()) {
          locatorConnectionStringList.add(locStringIter.next());
        }
      }
    }
    return locatorConnectionStringList;
  }
  
  
  

 
  /**
   * Used by unit tests to force cache creation to use a test generated cache.xml
   */
  public static File testCacheXml = null;

  /**
   * @return true if cache is created using a ClientCacheFactory
   * @see #hasPool()
   */
  public boolean isClient() {
    return this.isClient;
  }

  /**
   * Method to check for GemFire client. In addition to checking for ClientCacheFactory, this method checks for any
   * defined pools.
   *
   * @return true if the cache has pools declared
   */
  public boolean hasPool() {
    return this.isClient || !getAllPools().isEmpty();
  }

  private Collection<Pool> getAllPools() {
    Collection<Pool> pools = PoolManagerImpl.getPMI().getMap().values();
    for (Iterator<Pool> itr = pools.iterator(); itr.hasNext();) {
      PoolImpl pool = (PoolImpl) itr.next();
      if (pool.isUsedByGateway()) {
        itr.remove();
      }
    }
    return pools;
  }

  /**
   * May return null (even on a client).
   */
  public Pool getDefaultPool() {
    return this.defaultPool;
  }

  private void setDefaultPool(Pool v) {
    this.defaultPool = v;
  }

  /**
   * Perform initialization, solve the early escaped reference problem by putting publishing references to this instance
   * in this method (vs. the constructor).
   */
  private void initialize() {
    if (GemFireCacheImpl.instance != null) {
      Assert.assertTrue(GemFireCacheImpl.instance == null, "Cache instance already in place: " + instance);
    }
    GemFireCacheImpl.instance = this;
    GemFireCacheImpl.pdxInstance = this;
    
    MinimumSystemRequirements.checkAndLog();
    
    for (Iterator<CacheLifecycleListener> iter = cacheLifecycleListeners.iterator(); iter.hasNext();) {
      CacheLifecycleListener listener = (CacheLifecycleListener) iter.next();
      listener.cacheCreated(this);
    }
    
    ClassPathLoader.setLatestToDefault();

    //request and check cluster configuration
    ConfigurationResponse configurationResponse = requestSharedConfiguration();
    deployJarsRecevedFromClusterConfiguration(configurationResponse);

    // apply the cluster's properties configuration and initialize security using that configuration
    ClusterConfigurationLoader.applyClusterPropertiesConfiguration(this, configurationResponse, system.getConfig());
    securityService.initSecurity(system.getConfig().getSecurityProps());
       
    SystemMemberCacheEventProcessor.send(this, Operation.CACHE_CREATE);
    this.resourceAdvisor.initializationGate();
    
    //Register function that we need to execute to fetch available REST service endpoints in DS
    FunctionService.registerFunction(new FindRestEnabledServersFunction());

    // moved this after initializeDeclarativeCache because in the future
    // distributed system creation will not happen until we have read
    // cache.xml file.
    // For now this needs to happen before cache.xml otherwise
    // we will not be ready for all the events that cache.xml
    // processing can deliver (region creation, etc.).
    // This call may need to be moved inside initializeDeclarativeCache.
    /** Entry to GemFire Management service **/
    this.jmxAdvisor.initializationGate();

    // this starts up the ManagementService, register and federate the internal beans
    system.handleResourceEvent(ResourceEvent.CACHE_CREATE, this);

    boolean completedCacheXml = false;

    initializeServices();
    
    try {
      //Deploy all the jars from the deploy working dir.
      new JarDeployer(this.system.getConfig().getDeployWorkingDir()).loadPreviouslyDeployedJars();
      ClusterConfigurationLoader.applyClusterXmlConfiguration(this, configurationResponse, system.getConfig());
      initializeDeclarativeCache();
      completedCacheXml = true;
    } finally {
      if (!completedCacheXml) {
        // so initializeDeclarativeCache threw an exception
        try {
          close(); // fix for bug 34041
        } catch (Throwable ignore) {
          // I don't want init to throw an exception that came from the close.
          // I want it to throw the original exception that came from initializeDeclarativeCache.
        }
      }
    }
    
    this.clientpf = null;
    
    startColocatedJmxManagerLocator();
    
    startMemcachedServer();
    
    startRedisServer();
    
    startRestAgentServer(this);

    int time = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT",
        DEFAULT_CLIENT_FUNCTION_TIMEOUT);
    clientFunctionTimeout = time >= 0 ? time : DEFAULT_CLIENT_FUNCTION_TIMEOUT;

    isInitialized = true;
  }

  /**
   * Initialize any services that provided as extensions to the cache using the
   * service loader mechanism.
   */
  private void initializeServices() {
    ServiceLoader<CacheService> loader = ServiceLoader.load(CacheService.class);
    for(CacheService service : loader) {
      service.init(this);
      this.services.put(service.getInterface(), service);
      system.handleResourceEvent(ResourceEvent.CACHE_SERVICE_CREATE, service);
    }
  }

  private boolean isNotJmxManager(){
    return (this.system.getConfig().getJmxManagerStart() != true);
  }
  
  private boolean isServerNode(){
    return (this.system.getDistributedMember().getVmKind() != DistributionManager.LOCATOR_DM_TYPE
         && this.system.getDistributedMember().getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE
         && !isClient());
  }
  
  private void startRestAgentServer(GemFireCacheImpl cache) {
    if (this.system.getConfig().getStartDevRestApi()
        && isNotJmxManager()
        && isServerNode()) {
      this.restAgent = new RestAgent(this.system.getConfig());
      restAgent.start(cache);
    } else {
      this.restAgent = null;
    }
  }
  
  private void startMemcachedServer() {
    int port = system.getConfig().getMemcachedPort();
    if (port != 0) {
      String protocol = system.getConfig().getMemcachedProtocol();
      assert protocol != null;
      String bindAddress = system.getConfig().getMemcachedBindAddress();
      assert bindAddress != null;
      if (bindAddress.equals(DistributionConfig.DEFAULT_MEMCACHED_BIND_ADDRESS)) {
        logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCacheImpl_STARTING_GEMFIRE_MEMCACHED_SERVER_ON_PORT_0_FOR_1_PROTOCOL,
                new Object[] { port, protocol }));
      } else {
        logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCacheImpl_STARTING_GEMFIRE_MEMCACHED_SERVER_ON_BIND_ADDRESS_0_PORT_1_FOR_2_PROTOCOL,
                new Object[] { bindAddress, port, protocol }));
      }
      this.memcachedServer = new GemFireMemcachedServer(bindAddress, port, Protocol.valueOf(protocol.toUpperCase()));
      this.memcachedServer.start();
    }
  }
  
  private void startRedisServer() {
    int port = system.getConfig().getRedisPort();
    if (port != 0) {
      String bindAddress = system.getConfig().getRedisBindAddress();
      assert bindAddress != null;
      if (bindAddress.equals(DistributionConfig.DEFAULT_REDIS_BIND_ADDRESS)) {
        getLoggerI18n().info(LocalizedStrings.GemFireCacheImpl_STARTING_GEMFIRE_REDIS_SERVER_ON_PORT_0,
            new Object[] { port });
      } else {
        getLoggerI18n().info(LocalizedStrings.GemFireCacheImpl_STARTING_GEMFIRE_REDIS_SERVER_ON_BIND_ADDRESS_0_PORT_1,
            new Object[] { bindAddress, port });
      }
      this.redisServer = new GeodeRedisServer(bindAddress, port);
      this.redisServer.start();
    }
  }


  public URL getCacheXmlURL() {
    if (this.getMyId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
      return null;
    }
    File xmlFile = testCacheXml;
    if (xmlFile == null) {
      xmlFile = this.system.getConfig().getCacheXmlFile();
    }
    if ("".equals(xmlFile.getName())) {
      return null;
    }

    URL url = null;
    if (!xmlFile.exists() || !xmlFile.isFile()) {
      // do a resource search
      String resource = xmlFile.getPath();
      resource = resource.replaceAll("\\\\", "/");
      if (resource.length() > 1 && resource.startsWith("/")) {
        resource = resource.substring(1);
      }
      url = ClassPathLoader.getLatest().getResource(getClass(), resource);
    } else {
      try {
        url = xmlFile.toURL();
      } catch (IOException ex) {
        throw new CacheXmlException(
            LocalizedStrings.GemFireCache_COULD_NOT_CONVERT_XML_FILE_0_TO_AN_URL.toLocalizedString(xmlFile), ex);
      }
    }
    if (url == null) {
      File defaultFile = DistributionConfig.DEFAULT_CACHE_XML_FILE;
      if (!xmlFile.equals(defaultFile)) {
        if (!xmlFile.exists()) {
          throw new CacheXmlException(LocalizedStrings.GemFireCache_DECLARATIVE_CACHE_XML_FILERESOURCE_0_DOES_NOT_EXIST
              .toLocalizedString(xmlFile));
        } else /* if (!xmlFile.isFile()) */{
          throw new CacheXmlException(LocalizedStrings.GemFireCache_DECLARATIVE_XML_FILE_0_IS_NOT_A_FILE.toLocalizedString(xmlFile));
        }
      }
    }

    return url;
  }

  /**
   * Initializes the contents of this <code>Cache</code> according to the declarative caching XML file specified by the
   * given <code>DistributedSystem</code>. Note that this operation cannot be performed in the constructor because
   * creating regions in the cache, etc. uses the cache itself (which isn't initialized until the constructor returns).
   *
   * @throws CacheXmlException
   *           If something goes wrong while parsing the declarative caching XML file.
   * @throws TimeoutException
   *           If a {@link org.apache.geode.cache.Region#put(Object, Object)}times out while initializing the cache.
   * @throws CacheWriterException
   *           If a <code>CacheWriterException</code> is thrown while initializing the cache.
   * @throws RegionExistsException
   *           If the declarative caching XML file desribes a region that already exists (including the root region).
   * @throws GatewayException
   *           If a <code>GatewayException</code> is thrown while initializing the cache.
   *           
   * @see #loadCacheXml
   */
  private void initializeDeclarativeCache() throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    URL url = getCacheXmlURL();
    String cacheXmlDescription = this.cacheConfig.getCacheXMLDescription();
    if (url == null && cacheXmlDescription == null) {
      if (isClient()) {
        determineDefaultPool();
        initializeClientRegionShortcuts(this);
      } else {
        initializeRegionShortcuts(this);
      }
      initializePdxRegistry();
      readyDynamicRegionFactory();
      return; // nothing needs to be done
    }

    try {
      logCacheXML(url, cacheXmlDescription);
      InputStream stream = null;
      if (cacheXmlDescription != null) {
        if (logger.isTraceEnabled()) {
          logger.trace("initializing cache with generated XML: {}", cacheXmlDescription);
        }
        stream = new StringBufferInputStream(cacheXmlDescription);
      } else {
        stream = url.openStream();
      }
      loadCacheXml(stream);
      try {
        stream.close();
      } catch (IOException ignore) {
      }
    } catch (IOException ex) {
      throw new CacheXmlException(LocalizedStrings.GemFireCache_WHILE_OPENING_CACHE_XML_0_THE_FOLLOWING_ERROR_OCCURRED_1
          .toLocalizedString(new Object[] { url.toString(), ex }));

    } catch (CacheXmlException ex) {
      CacheXmlException newEx = new CacheXmlException(LocalizedStrings.GemFireCache_WHILE_READING_CACHE_XML_0_1
          .toLocalizedString(new Object[] { url, ex.getMessage() }));
      newEx.setStackTrace(ex.getStackTrace());
      newEx.initCause(ex.getCause());
      throw newEx;
    }
  }

  private void logCacheXML(URL url, String cacheXmlDescription) {
    if (cacheXmlDescription == null) {
      StringBuilder sb = new StringBuilder();
      try {
        final String EOLN = System.getProperty("line.separator");
        BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()));
        String l = br.readLine();
        while (l != null) {
          if (!l.isEmpty()) {
            sb.append(EOLN).append(l);
          }
          l = br.readLine();
        }
        br.close();
      } catch (IOException ignore) {
      }
      logger.info(LocalizedMessage.create(
          LocalizedStrings.GemFireCache_INITIALIZING_CACHE_USING__0__1, new Object[]{url.toString(), sb.toString()}));
    } else {
      logger.info(LocalizedMessage.create(
          LocalizedStrings.GemFireCache_INITIALIZING_CACHE_USING__0__1, new Object[] {"generated description from old cache", cacheXmlDescription}));
    }
  }

  public synchronized void initializePdxRegistry() {
    if (this.pdxRegistry == null) {
      //The member with locator is initialized with a NullTypePdxRegistration
      if (this.getMyId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
        this.pdxRegistry = new TypeRegistry(this, true);
      } else {
        this.pdxRegistry = new TypeRegistry(this, false);
      }
      this.pdxRegistry.initialize();
    }
  }

  /**
   * Call to make this vm's dynamic region factory ready. Public so it can be called from CacheCreation during xml
   * processing
   */
  public void readyDynamicRegionFactory() {
    try {
      ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).internalInit(this);
    } catch (CacheException ce) {
      throw new GemFireCacheException(LocalizedStrings.GemFireCache_DYNAMIC_REGION_INITIALIZATION_FAILED.toLocalizedString(), ce);
    }
  }

  /**
   * create diskstore factory with default attributes
   *
   * @since GemFire prPersistSprint2
   */
  public DiskStoreFactory createDiskStoreFactory() {
    return new DiskStoreFactoryImpl(this);
  }

  /**
   * create diskstore factory with predefined attributes
   *
   * @since GemFire prPersistSprint2
   */
  public DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs) {
    return new DiskStoreFactoryImpl(this, attrs);
  }

  protected class Stopper extends CancelCriterion {

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.CancelCriterion#cancelInProgress()
     */
    @Override
    public String cancelInProgress() {
      String reason = GemFireCacheImpl.this.getDistributedSystem().getCancelCriterion().cancelInProgress();
      if (reason != null) {
        return reason;
      }
      if (GemFireCacheImpl.this.disconnectCause != null) {
        return disconnectCause.getMessage();
      }
      if (GemFireCacheImpl.this.isClosing) {
        return "The cache is closed."; // this + ": closed";
      }
      return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.CancelCriterion#generateCancelledException(java.lang.Throwable)
     */
    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      RuntimeException result = getDistributedSystem().getCancelCriterion().generateCancelledException(e);
      if (result != null) {
        return result;
      }
      if (GemFireCacheImpl.this.disconnectCause == null) {
        // No root cause, specify the one given and be done with it.
        return new CacheClosedException(reason, e);
      }

      if (e == null) {
        // Caller did not specify any root cause, so just use our own.
        return new CacheClosedException(reason, GemFireCacheImpl.this.disconnectCause);
      }

      // Attempt to stick rootCause at tail end of the exception chain.
      Throwable nt = e;
      while (nt.getCause() != null) {
        nt = nt.getCause();
      }
      try {
        nt.initCause(GemFireCacheImpl.this.disconnectCause);
        return new CacheClosedException(reason, e);
      } catch (IllegalStateException e2) {
        // Bug 39496 (Jrockit related) Give up. The following
        // error is not entirely sane but gives the correct general picture.
        return new CacheClosedException(reason, GemFireCacheImpl.this.disconnectCause);
      }
    }
  }

  private final Stopper stopper = new Stopper();

  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  /** return true if the cache was closed due to being shunned by other members */
  public boolean forcedDisconnect() {
    return this.forcedDisconnect || this.system.forcedDisconnect();
  }

  /** return a CacheClosedException with the given reason */
  public CacheClosedException getCacheClosedException(String reason, Throwable cause) {
    CacheClosedException result;
    if (cause != null) {
      result = new CacheClosedException(reason, cause);
    } else if (this.disconnectCause != null) {
      result = new CacheClosedException(reason, this.disconnectCause);
    } else {
      result = new CacheClosedException(reason);
    }
    return result;
  }

  /** if the cache was forcibly closed this exception will reflect the cause */
  public Throwable getDisconnectCause() {
    return this.disconnectCause;
  }

  /**
   * Set to true during a cache close if user requested durable subscriptions to be kept.
   *
   * @since GemFire 5.7
   */
  private boolean keepAlive;

  /**
   * Returns true if durable subscriptions (registrations and queries) should be preserved.
   *
   * @since GemFire 5.7
   */
  public boolean keepDurableSubscriptionsAlive() {
    return this.keepAlive;
  }

  /**
   * break any potential circularity in {@link #loadEmergencyClasses()}
   */
  private static volatile boolean emergencyClassesLoaded = false;

  /**
   * Ensure that all the necessary classes for closing the cache are loaded
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  static public void loadEmergencyClasses() {
    if (emergencyClassesLoaded)
      return;
    emergencyClassesLoaded = true;
    InternalDistributedSystem.loadEmergencyClasses();
    AcceptorImpl.loadEmergencyClasses();
    PoolManagerImpl.loadEmergencyClasses();
  }

  /**
   * Close the distributed system, cache servers, and gateways. Clears the rootRegions and partitionedRegions map.
   * Marks the cache as closed.
   *
   * @see SystemFailure#emergencyClose()
   */
  static public void emergencyClose() {
    final boolean DEBUG = SystemFailure.TRACE_CLOSE;

    GemFireCacheImpl inst = GemFireCacheImpl.instance;
    if (inst == null) {
      if (DEBUG) {
        System.err.println("GemFireCache#emergencyClose: no instance");
      }
      return;
    }

    GemFireCacheImpl.instance = null;
    GemFireCacheImpl.pdxInstance = null;
    // leave the PdxSerializer set if we have one to prevent 43412
    // TypeRegistry.setPdxSerializer(null);

    // Shut down messaging first
    InternalDistributedSystem ids = inst.system;
    if (ids != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergencyClose InternalDistributedSystem");
      }
      ids.emergencyClose();
    }

    inst.disconnectCause = SystemFailure.getFailure();
    inst.isClosing = true;

    // Clear cache servers
    if (DEBUG) {
      System.err.println("DEBUG: Close cache servers");
    }
    {
      Iterator allCacheServersItr = inst.allCacheServers.iterator();
      while (allCacheServersItr.hasNext()) {
        CacheServerImpl bs = (CacheServerImpl) allCacheServersItr.next();
        AcceptorImpl ai = bs.getAcceptor();
        if (ai != null) {
          ai.emergencyClose();
        }
      }
    }

    if (DEBUG) {
      System.err.println("DEBUG: closing client resources");
    }
    PoolManagerImpl.emergencyClose();

    if (DEBUG) {
      System.err.println("DEBUG: closing gateway hubs");
    }
    
    // These are synchronized sets -- avoid potential deadlocks
    // instance.pathToRegion.clear(); // garbage collection
    // instance.gatewayHubs.clear();

    // rootRegions is intentionally *not* synchronized. The
    // implementation of clear() does not currently allocate objects.
    inst.rootRegions.clear();
    // partitionedRegions is intentionally *not* synchronized, The
    // implementation of clear() does not currently allocate objects.
    inst.partitionedRegions.clear();
    if (DEBUG) {
      System.err.println("DEBUG: done with cache emergency close");
    }
  }

  public boolean isCacheAtShutdownAll() {
    return isShutDownAll;
  }

  /**
   * Number of threads used to close PRs in shutdownAll. By default is the number of PRs in the cache
   */
  private static final int shutdownAllPoolSize = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "SHUTDOWN_ALL_POOL_SIZE", -1);

  void shutdownSubTreeGracefully(Map<String, PartitionedRegion> prSubMap) {
    for (final PartitionedRegion pr : prSubMap.values()) {
      shutDownOnePRGracefully(pr);
    }
  }

  public synchronized void shutDownAll() {
    boolean testIGE = Boolean.getBoolean("TestInternalGemFireError");

    if (testIGE) {
      InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString());
      throw assErr;
    }
    if (isCacheAtShutdownAll()) {
      // it's already doing shutdown by another thread
      return;
    }
    if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      try {
        CacheObserverHolder.getInstance().beforeShutdownAll();
      } finally {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      }
    }
    this.isShutDownAll = true;

    // bug 44031 requires multithread shutdownall should be grouped
    // by root region. However, shutDownAllDuringRecovery.conf test revealed that
    // we have to close colocated child regions first.
    // Now check all the PR, if anyone has colocate-with attribute, sort all the
    // PRs by colocation relationship and close them sequentially, otherwise still
    // group them by root region.
    TreeMap<String, Map<String, PartitionedRegion>> prTrees = getPRTrees();
    if (prTrees.size() > 1 && shutdownAllPoolSize != 1) {
      ExecutorService es = getShutdownAllExecutorService(prTrees.size());
      for (final Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
        es.execute(new Runnable() {
          public void run() {
            ConnectionTable.threadWantsSharedResources();
            shutdownSubTreeGracefully(prSubMap);
          }
        });
      } // for each root
      es.shutdown();
      try {
        es.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.debug("Shutdown all interrupted while waiting for PRs to be shutdown gracefully.");
      }

    } else {
      for (final Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
        shutdownSubTreeGracefully(prSubMap);
      }
    }

    close("Shut down all members", null, false, true);
  }

  private ExecutorService getShutdownAllExecutorService(int size) {
    final ThreadGroup thrGrp = LoggingThreadGroup.createThreadGroup("ShutdownAllGroup", logger);
    ThreadFactory thrFactory = new ThreadFactory() {
      private final AtomicInteger threadCount = new AtomicInteger(1);

      public Thread newThread(Runnable r) {
        Thread t = new Thread(thrGrp, r, "ShutdownAll-" + threadCount.getAndIncrement());
        t.setDaemon(true);
        return t;
      }
    };
    ExecutorService es = Executors.newFixedThreadPool(shutdownAllPoolSize == -1 ? size : shutdownAllPoolSize, thrFactory);
    return es;
  }

  private void shutDownOnePRGracefully(PartitionedRegion pr) {
    boolean acquiredLock = false;
    try {
      pr.acquireDestroyLock();
      acquiredLock = true;

      synchronized(pr.getRedundancyProvider()) {
      if (pr.isDataStore() && pr.getDataStore() != null && pr.getDataPolicy() == DataPolicy.PERSISTENT_PARTITION) {
        int numBuckets = pr.getTotalNumberOfBuckets();
        Map<InternalDistributedMember, PersistentMemberID> bucketMaps[] = new Map[numBuckets];
        PartitionedRegionDataStore prds = pr.getDataStore();

        // lock all the primary buckets
        Set<Entry<Integer, BucketRegion>> bucketEntries = prds.getAllLocalBuckets();
        for (Map.Entry e : bucketEntries) {
          BucketRegion br = (BucketRegion) e.getValue();
          if (br == null || br.isDestroyed) {
            // bucket region could be destroyed in race condition
            continue;
          }
          br.getBucketAdvisor().tryLockIfPrimary();

          // get map <InternalDistriutedMemeber, persistentID> for this bucket's
          // remote members
          bucketMaps[br.getId()] = br.getBucketAdvisor().adviseInitializedPersistentMembers();
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: PR {}: initialized persistent members for {}:{}", pr.getName(), br.getId(), bucketMaps[br.getId()]);
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("shutDownAll: All buckets for PR {} are locked.", pr.getName());
        }

        // send lock profile update to other members
        pr.setShutDownAllStatus(PartitionedRegion.PRIMARY_BUCKETS_LOCKED);
        new UpdateAttributesProcessor(pr).distribute(false);
        pr.getRegionAdvisor().waitForProfileStatus(PartitionedRegion.PRIMARY_BUCKETS_LOCKED);
        if (logger.isDebugEnabled()) {
          logger.debug("shutDownAll: PR {}: all bucketlock profiles received.", pr.getName());
        }

        // if async write, do flush
        if (!pr.getAttributes().isDiskSynchronous()) {
          // several PRs might share the same diskstore, we will only flush once
          // even flush is called several times.
          pr.getDiskStore().forceFlush();
          // send flush profile update to other members
          pr.setShutDownAllStatus(PartitionedRegion.DISK_STORE_FLUSHED);
          new UpdateAttributesProcessor(pr).distribute(false);
          pr.getRegionAdvisor().waitForProfileStatus(PartitionedRegion.DISK_STORE_FLUSHED);
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: PR {}: all flush profiles received.", pr.getName());
          }
        } // async write

        // persist other members to OFFLINE_EQUAL for each bucket region
        // iterate through all the bucketMaps and exclude the items whose
        // idm is no longer online
        Set<InternalDistributedMember> membersToPersistOfflineEqual = pr.getRegionAdvisor().adviseDataStore();
        for (Map.Entry e : bucketEntries) {
          BucketRegion br = (BucketRegion) e.getValue();
          if (br == null || br.isDestroyed) {
            // bucket region could be destroyed in race condition
            continue;
          }
          Map<InternalDistributedMember, PersistentMemberID> persistMap = getSubMapForLiveMembers(pr, membersToPersistOfflineEqual,
              bucketMaps[br.getId()]);
          if (persistMap != null) {
            br.getPersistenceAdvisor().persistMembersOfflineAndEqual(persistMap);
            if (logger.isDebugEnabled()) {
              logger.debug("shutDownAll: PR {}: pesisting bucket {}:{}", pr.getName(), br.getId(), persistMap);
            }
          }
        }

        // send persited profile update to other members, let all members to persist
        // before close the region
        pr.setShutDownAllStatus(PartitionedRegion.OFFLINE_EQUAL_PERSISTED);
        new UpdateAttributesProcessor(pr).distribute(false);
        pr.getRegionAdvisor().waitForProfileStatus(PartitionedRegion.OFFLINE_EQUAL_PERSISTED);
        if (logger.isDebugEnabled()) {
          logger.debug("shutDownAll: PR {}: all offline_equal profiles received.", pr.getName());
        }
      } // datastore

      // after done all steps for buckets, close pr
      // close accessor directly
      RegionEventImpl event = new RegionEventImpl(pr, Operation.REGION_CLOSE, null, false, getMyId(), true);
      try {
        // not to acquire lock
        pr.basicDestroyRegion(event, false, false, true);
      } catch (CacheWriterException e) {
        // not possible with local operation, CacheWriter not called
        throw new Error(LocalizedStrings.LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
            .toLocalizedString(), e);
      } catch (TimeoutException e) {
        // not possible with local operation, no distributed locks possible
        throw new Error(LocalizedStrings.LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
            .toLocalizedString(), e);
      }
      // pr.close();
      } // synchronized
    } catch (CacheClosedException cce) {
      logger.debug("Encounter CacheClosedException when shutDownAll is closing PR: {}:{}", pr.getFullPath(), cce.getMessage());
    } catch (CancelException ce) {
      logger.debug("Encounter CancelException when shutDownAll is closing PR: {}:{}", pr.getFullPath(), ce.getMessage());
    } catch (RegionDestroyedException rde) {
      logger.debug("Encounter CacheDestroyedException when shutDownAll is closing PR: {}:{}", pr.getFullPath(), rde.getMessage());
    } finally {
      if (acquiredLock) {
        pr.releaseDestroyLock();
      }
    }
  }

  private Map<InternalDistributedMember, PersistentMemberID> getSubMapForLiveMembers(PartitionedRegion pr,
      Set<InternalDistributedMember> membersToPersistOfflineEqual, Map<InternalDistributedMember, PersistentMemberID> bucketMap) {
    if (bucketMap == null) {
      return null;
    }
    Map<InternalDistributedMember, PersistentMemberID> persistMap = new HashMap();
    Iterator itor = membersToPersistOfflineEqual.iterator();
    while (itor.hasNext()) {
      InternalDistributedMember idm = (InternalDistributedMember) itor.next();
      if (bucketMap.containsKey(idm)) {
        persistMap.put(idm, bucketMap.get(idm));
      }
    }
    return persistMap;
  }

  public void close() {
    close(false);
  }

  public void close(boolean keepalive) {
    close("Normal disconnect", null, keepalive, false);
  }

  public void close(String reason, Throwable optionalCause) {
    close(reason, optionalCause, false, false);
  }

  /**
   * Gets or lazily creates the PartitionedRegion distributed lock service. This call will synchronize on this
   * GemFireCache.
   *
   * @return the PartitionedRegion distributed lock service
   */
  protected DistributedLockService getPartitionedRegionLockService() {
    synchronized (this.prLockServiceLock) {
      stopper.checkCancelInProgress(null);
      if (this.prLockService == null) {
        try {
          this.prLockService = DLockService.create(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME, getDistributedSystem(),
              true /* distributed */, true /* destroyOnDisconnect */, true /* automateFreeResources */);
        } catch (IllegalArgumentException e) {
          this.prLockService = DistributedLockService.getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
          if (this.prLockService == null) {
            throw e; // PARTITION_LOCK_SERVICE_NAME must be illegal!
          }
        }
      }
      return this.prLockService;
    }
  }

  /**
   * Gets or lazily creates the GatewaySender distributed lock service.
   * @return the GatewaySender distributed lock service
   */
  public DistributedLockService getGatewaySenderLockService() {
    if (this.gatewayLockService == null) {
      synchronized (this.gatewayLockServiceLock) {
        stopper.checkCancelInProgress(null);
        if (this.gatewayLockService == null) {
          try {
            this.gatewayLockService = DLockService.create(
                AbstractGatewaySender.LOCK_SERVICE_NAME, 
                getDistributedSystem(), 
                true /*distributed*/, 
                true /*destroyOnDisconnect*/, 
                true /*automateFreeResources*/);
          }
          catch (IllegalArgumentException e) {
            this.gatewayLockService = DistributedLockService.getServiceNamed(
                AbstractGatewaySender.LOCK_SERVICE_NAME);
            if (this.gatewayLockService == null) {
              throw e; // AbstractGatewaySender.LOCK_SERVICE_NAME must be illegal!
            }
          }
        }
      }
    }
    return this.gatewayLockService;
  }

  /**
   * Destroys the PartitionedRegion distributed lock service when closing the cache. Caller must be synchronized on this
   * GemFireCache.
   */
  private void destroyPartitionedRegionLockService() {
    try {
      DistributedLockService.destroy(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
    } catch (IllegalArgumentException e) {
      // DistributedSystem.disconnect may have already destroyed the DLS
    }
  }

  /**
   * Destroys the GatewaySender distributed lock service when closing
   * the cache. Caller must be synchronized on this GemFireCache.
   */
  private void destroyGatewaySenderLockService() {
    if (DistributedLockService
        .getServiceNamed(AbstractGatewaySender.LOCK_SERVICE_NAME) != null) {
      try {
        DistributedLockService.destroy(AbstractGatewaySender.LOCK_SERVICE_NAME);
      }
      catch (IllegalArgumentException e) {
        // DistributedSystem.disconnect may have already destroyed the DLS
      }
    }
  }

  public HeapEvictor getHeapEvictor() {
    synchronized (this.heapEvictorLock) {
      stopper.checkCancelInProgress(null);
      if (this.heapEvictor == null) {
        this.heapEvictor = new HeapEvictor(this);
      }
      return this.heapEvictor;
    }
  }

  public OffHeapEvictor getOffHeapEvictor() {
    synchronized (this.offHeapEvictorLock) {
      stopper.checkCancelInProgress(null);
      if (this.offHeapEvictor == null) {
        this.offHeapEvictor = new OffHeapEvictor(this);
      }
      return this.offHeapEvictor;
    }    
  }
  
  public PersistentMemberManager getPersistentMemberManager() {
    return persistentMemberManager;
  }

  public ClientMetadataService getClientMetadataService() {
    synchronized (this.clientMetaDatServiceLock) {
      stopper.checkCancelInProgress(null);
      if (this.clientMetadatService == null) {
        this.clientMetadatService = new ClientMetadataService(this);
      }
      return this.clientMetadatService;
    }
  }

  private final boolean DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");

  /**
   * close the cache
   *
   * @param reason
   *          the reason the cache is being closed
   * @param systemFailureCause
   *          whether this member was ejected from the distributed system
   * @param keepalive
   *          whoever added this should javadoc it
   */
  public void close(String reason, Throwable systemFailureCause, boolean keepalive) {
    close(reason, systemFailureCause, keepalive, false);
  }

  public void close(String reason, Throwable systemFailureCause, boolean keepalive, boolean keepDS) {
    securityService.close();

    if (isClosed()) {
      return;
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();

    synchronized (GemFireCacheImpl.class) {
      // bugfix for bug 36512 "GemFireCache.close is not thread safe"
      // ALL CODE FOR CLOSE SHOULD NOW BE UNDER STATIC SYNCHRONIZATION
      // OF synchronized (GemFireCache.class) {
      // static synchronization is necessary due to static resources
      if (isClosed()) {
        return;
      }

      /**
       * First close the ManagementService as it uses a lot of infra which will be closed by cache.close()
       **/
      system.handleResourceEvent(ResourceEvent.CACHE_REMOVE, this);
      if (this.listener != null) {
        this.system.removeResourceListener(listener);
        this.listener = null;
      }

      if (systemFailureCause != null) {
        this.forcedDisconnect = systemFailureCause instanceof ForcedDisconnectException;
        if (this.forcedDisconnect) {
          this.disconnectCause = new ForcedDisconnectException(reason);
        } else {
          this.disconnectCause = systemFailureCause;
        }
      }

      this.keepAlive = keepalive;
      isClosing = true;
      logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_0_NOW_CLOSING, this));

      // Before anything else...make sure that this instance is not
      // available to anyone "fishing" for a cache...
      if (GemFireCacheImpl.instance == this) {
        GemFireCacheImpl.instance = null;
      }

      // we don't clear the prID map if there is a system failure. Other
      // threads may be hung trying to communicate with the map locked
      if (systemFailureCause == null) {
        PartitionedRegion.clearPRIdMap();
      }
      TXStateProxy tx = null;
      try {

        if (this.txMgr != null) {
          tx = this.txMgr.internalSuspend();
        }

        // do this before closing regions
        resourceManager.close();

        try {
          this.resourceAdvisor.close();
        } catch (CancelException e) {
          // ignore
        } 
        try {
          this.jmxAdvisor.close();
        } catch (CancelException e) {
          // ignore
        }

        GatewaySenderAdvisor advisor = null;
        for (GatewaySender sender : this.getAllGatewaySenders()) {
          try {
            sender.stop();
            advisor = ((AbstractGatewaySender) sender).getSenderAdvisor();
            if (advisor != null) {
              if (isDebugEnabled) {
                logger.debug("Stopping the GatewaySender advisor");
              }
              advisor.close();
            }
          } catch (CancelException ce) {
          }
        }
        ParallelGatewaySenderQueue.cleanUpStatics(null);

        destroyGatewaySenderLockService();

        if (this.eventThreadPool != null) {
          if (isDebugEnabled) {
            logger.debug("{}: stopping event thread pool...", this);
          }
          this.eventThreadPool.shutdown();
        }

        /*
         * IMPORTANT: any operation during shut down that can time out (create a CancelException) must be inside of this
         * try block. If all else fails, we *must* ensure that the cache gets closed!
         */
        try {                              
          this.stopServers();

          stopMemcachedServer();
          
          stopRedisServer();

          // no need to track PR instances since we won't create any more
          // cacheServers or gatewayHubs
          if (this.partitionedRegions != null) {
            if (isDebugEnabled) {
              logger.debug("{}: clearing partitioned regions...", this);
            }
            synchronized (this.partitionedRegions) {
              int prSize = -this.partitionedRegions.size();
              this.partitionedRegions.clear();
              getCachePerfStats().incPartitionedRegions(prSize);
            }
          }

          prepareDiskStoresForClose();
          if (GemFireCacheImpl.pdxInstance == this) {
            GemFireCacheImpl.pdxInstance = null;
          }

          List rootRegionValues = null;
          synchronized (this.rootRegions) {
            rootRegionValues = new ArrayList(this.rootRegions.values());
          }
          {
            final Operation op;
            if (this.forcedDisconnect) {
              op = Operation.FORCED_DISCONNECT;
            } else if (isReconnecting()) {
              op = Operation.CACHE_RECONNECT;
            } else {
              op = Operation.CACHE_CLOSE;
            }

            LocalRegion prRoot = null;
            
            for (Iterator itr = rootRegionValues.iterator(); itr.hasNext();) {
              LocalRegion lr = (LocalRegion) itr.next();
              if (isDebugEnabled) {
                logger.debug("{}: processing region {}", this, lr.getFullPath());
              }
              if (PartitionedRegionHelper.PR_ROOT_REGION_NAME.equals(lr.getName())) {
                prRoot = lr;
              } else {
                if(lr.getName().contains(ParallelGatewaySenderQueue.QSTRING)){
                  continue; //this region will be closed internally by parent region
                }
                if (isDebugEnabled) {
                  logger.debug("{}: closing region {}...", this, lr.getFullPath());
                }
                try {
                  lr.handleCacheClose(op);
                } catch (Exception e) {
                  if (isDebugEnabled || !forcedDisconnect) {
                    logger.warn(LocalizedMessage.create(LocalizedStrings.GemFireCache_0_ERROR_CLOSING_REGION_1,
                        new Object[] { this, lr.getFullPath() }), e);
                  }
                }
              }
            } // for

            try {
              if (isDebugEnabled) {
                logger.debug("{}: finishing partitioned region close...", this);
              }
              PartitionedRegion.afterRegionsClosedByCacheClose(this);
              if (prRoot != null) {
                // do the PR meta root region last
                prRoot.handleCacheClose(op);
              }
            } catch (CancelException e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.GemFireCache_0_ERROR_IN_LAST_STAGE_OF_PARTITIONEDREGION_CACHE_CLOSE,
                  this), e);
            }
            destroyPartitionedRegionLockService();
          }

          closeDiskStores();
          diskMonitor.close();
          
          // Close the CqService Handle.
          try {
            if (isDebugEnabled) {
              logger.debug("{}: closing CQ service...", this);
            }
            cqService.close();
          } catch (Exception ex) {
            logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_FAILED_TO_GET_THE_CQSERVICE_TO_CLOSE_DURING_CACHE_CLOSE_1));
          }

          PoolManager.close(keepalive);

          if (isDebugEnabled) {
            logger.debug("{}: closing reliable message queue...", this);
          }
          try {
            getReliableMessageQueueFactory().close(true);
          } catch (CancelException e) {
            if (isDebugEnabled) {
              logger.debug("Ignored cancellation while closing reliable message queue", e);
            }
          }

          if (isDebugEnabled) {
            logger.debug("{}: notifying admins of close...", this);
          }
          try {
            SystemMemberCacheEventProcessor.send(this, Operation.CACHE_CLOSE);
          } catch (CancelException e) {
            if (logger.isDebugEnabled()) {
              logger.debug("Ignored cancellation while notifying admins");
            }
          }

          if (isDebugEnabled) {
            logger.debug("{}: stopping destroyed entries processor...", this);
          }
          this.tombstoneService.stop();

          // NOTICE: the CloseCache message is the *last* message you can send!
          DM dm = null;
          try {
            dm = system.getDistributionManager();
            dm.removeMembershipListener(this.txMgr);
          } catch (CancelException e) {
            // dm = null;
          }

          if (dm != null) { // Send CacheClosedMessage (and NOTHING ELSE) here
            if (isDebugEnabled) {
              logger.debug("{}: sending CloseCache to peers...", this);
            }
            Set otherMembers = dm.getOtherDistributionManagerIds();
            ReplyProcessor21 processor = new ReplyProcessor21(system, otherMembers);
            CloseCacheMessage msg = new CloseCacheMessage();
            msg.setRecipients(otherMembers);
            msg.setProcessorId(processor.getProcessorId());
            dm.putOutgoing(msg);
            try {
              processor.waitForReplies();
            } catch (InterruptedException ex) {
              // Thread.currentThread().interrupt(); // TODO ??? should we reset this bit later?
              // Keep going, make best effort to shut down.
            } catch (ReplyException ex) {
              // keep going
            }
            // set closed state after telling others and getting responses
            // to avoid complications with others still in the process of
            // sending messages
          }
          // NO MORE Distributed Messaging AFTER THIS POINT!!!!

          {
            ClientMetadataService cms = this.clientMetadatService;
            if (cms != null) {
              cms.close();
            }
            HeapEvictor he = this.heapEvictor;
            if (he != null) {
              he.close();
            }
          }
        } catch (CancelException e) {
          // make sure the disk stores get closed
          closeDiskStores();
          // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!

          // okay, we're taking too long to do this stuff, so let's
          // be mean to other processes and skip the rest of the messaging
          // phase
          // [bruce] the following code is unnecessary since someone put the
          // same actions in a finally block
          // if (!this.closed) {
          // this.closed = true;
          // this.txMgr.close();
          // if (GemFireCache.instance == this) {
          // GemFireCache.instance = null;
          // }
          // ((DynamicRegionFactoryImpl)DynamicRegionFactory.get()).close();
          // }
        }

        // Close the CqService Handle.
        try {
          cqService.close();
        } catch (Exception ex) {
          logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_FAILED_TO_GET_THE_CQSERVICE_TO_CLOSE_DURING_CACHE_CLOSE_2));
        }

        this.cachePerfStats.close();
        TXLockService.destroyServices();

        EventTracker.stopTrackerServices(this);

        synchronized (ccpTimerMutex) {
          if (this.ccpTimer != null) {
            this.ccpTimer.cancel();
          }
        }

        this.expirationScheduler.cancel();

        // Stop QueryMonitor if running.
        if (this.queryMonitor != null) {
          this.queryMonitor.stopMonitoring();
        }
        stopDiskStoreTaskPool();        

      } finally {
        // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!
        if (this.txMgr != null) {
          this.txMgr.close();
        }
        ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).close();
        if (this.txMgr != null) {
          this.txMgr.resume(tx);
        }
        TXCommitMessage.getTracker().clearForCacheClose();
      }
      // Added to close the TransactionManager's cleanup thread
      TransactionManagerImpl.refresh();
      
      if (!keepDS) {
        // keepDS is used by ShutdownAll. It will override DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE
        if (!DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE) {
          this.system.disconnect();
        }
      }
      TypeRegistry.close();
      // do this late to prevent 43412
      TypeRegistry.setPdxSerializer(null);
      
      for (Iterator iter = cacheLifecycleListeners.iterator(); iter.hasNext();) {
        CacheLifecycleListener listener = (CacheLifecycleListener) iter.next();
        listener.cacheClosed(this);
      }      
      stopRestAgentServer();
      // Fix for #49856
      SequenceLoggerImpl.signalCacheClose();
      SystemFailure.signalCacheClose();
      
    } // static synchronization on GemFireCache.class

  }
  
  // see Cache.isReconnecting()
  public boolean isReconnecting() {
    return this.system.isReconnecting();
  }

  // see Cache.waitUntilReconnected(long, TimeUnit)
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    boolean systemReconnected = this.system.waitUntilReconnected(time,  units);
    if (!systemReconnected) {
      return false;
    }
    GemFireCacheImpl cache = getInstance();
    if (cache == null || !cache.isInitialized()) {
      return false;
    }
    return true;
  }
  
  // see Cache.stopReconnecting()
  public void stopReconnecting() {
    this.system.stopReconnecting();
  }
  
  // see Cache.getReconnectedCache()
  public Cache getReconnectedCache() {
    GemFireCacheImpl c = GemFireCacheImpl.getInstance();
    if (c == this || !c.isInitialized()) {
      c = null;
    }
    return c;
  }

  private void stopMemcachedServer() {
    if (this.memcachedServer != null) {
      logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCacheImpl_MEMCACHED_SERVER_ON_PORT_0_IS_SHUTTING_DOWN,
          new Object[] { this.system.getConfig().getMemcachedPort() }));
      this.memcachedServer.shutdown();
    }
  }
  
  private void stopRedisServer() {
    if (redisServer != null)
      this.redisServer.shutdown();
  }
  
  private void stopRestAgentServer() {
    if ( this.restAgent != null) {
      logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCacheImpl_REST_SERVER_ON_PORT_0_IS_SHUTTING_DOWN,
          new Object[] { this.system.getConfig().getHttpServicePort() }));
      this.restAgent.stop();
    }
  }

  private void prepareDiskStoresForClose() {
    String pdxDSName = TypeRegistry.getPdxDiskStoreName(this);
    DiskStoreImpl pdxdsi = null;
    for (DiskStoreImpl dsi : this.diskStores.values()) {
      if (dsi.getName().equals(pdxDSName)) {
        pdxdsi = dsi;
      } else {
        dsi.prepareForClose();
      }
    }
    if (pdxdsi != null) {
      pdxdsi.prepareForClose();
    }
  }

  /**
   * Used to guard access to compactorPool and set to true when cache is shutdown.
   */
  private final AtomicBoolean diskStoreTaskSync = new AtomicBoolean(false);
  /**
   * Lazily initialized.
   */
  private ThreadPoolExecutor diskStoreTaskPool = null;

  private void createDiskStoreTaskPool() {
    int MAXT = DiskStoreImpl.MAX_CONCURRENT_COMPACTIONS;
    final ThreadGroup compactThreadGroup = LoggingThreadGroup.createThreadGroup("Oplog Compactor Thread Group", logger);
    /*final ThreadFactory compactThreadFactory = new ThreadFactory() {
      public Thread newThread(Runnable command) {
        Thread thread = new Thread(compactThreadGroup, command, "Idle OplogCompactor");
        thread.setDaemon(true);
        return thread;
      }
    };*/

    final ThreadFactory compactThreadFactory = GemfireCacheHelper.CreateThreadFactory(compactThreadGroup, "Idle OplogCompactor");
    this.diskStoreTaskPool = new ThreadPoolExecutor(MAXT, MAXT, 1, TimeUnit.SECONDS,
                                             new LinkedBlockingQueue(),
                                             compactThreadFactory);
  }

  private final ConcurrentMap<String, DiskStoreImpl> diskStores = new ConcurrentHashMap<String, DiskStoreImpl>();
  private final ConcurrentMap<String, DiskStoreImpl> regionOwnedDiskStores = new ConcurrentHashMap<String, DiskStoreImpl>();
  
  public void addDiskStore(DiskStoreImpl dsi) {
    this.diskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      getDiskStoreMonitor().addDiskStore(dsi);
    }
  }

  public void removeDiskStore(DiskStoreImpl dsi) {
    this.diskStores.remove(dsi.getName());
    this.regionOwnedDiskStores.remove(dsi.getName());
    /** Added for M&M **/
    if(!dsi.getOwnedByRegion())
    system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, dsi);
  }

  public void addRegionOwnedDiskStore(DiskStoreImpl dsi) {
    this.regionOwnedDiskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      getDiskStoreMonitor().addDiskStore(dsi);
    }
  }

  public void closeDiskStores() {
    Iterator<DiskStoreImpl> it = this.diskStores.values().iterator();
    while (it.hasNext()) {
      try {
        DiskStoreImpl dsi = it.next();
        if (logger.isDebugEnabled()) {
          logger.debug("closing {}", dsi);
        }
        dsi.close();
        /** Added for M&M **/
        system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, dsi);
      } catch (Exception e) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.Disk_Store_Exception_During_Cache_Close), e);
      }
      it.remove();
    }
  }

  /**
   * Used by unit tests to allow them to change the default disk store name.
   */
  public static void setDefaultDiskStoreName(String dsName) {
    DEFAULT_DS_NAME = dsName;
  }

  /**
   * Used by unit tests to undo a change to the default disk store name.
   */
  public static void unsetDefaultDiskStoreName() {
    DEFAULT_DS_NAME = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;
  }

  public static String getDefaultDiskStoreName() {
    return DEFAULT_DS_NAME;
  }

  public static String DEFAULT_DS_NAME = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;

  public DiskStoreImpl getOrCreateDefaultDiskStore() {
    DiskStoreImpl result = (DiskStoreImpl) findDiskStore(null);
    if (result == null) {
      synchronized (this) {
        result = (DiskStoreImpl) findDiskStore(null);
        if (result == null) {
          result = (DiskStoreImpl) createDiskStoreFactory().create(DEFAULT_DS_NAME);
        }
      }
    }
    return result;
  }

  /**
   * Returns the DiskStore by name
   *
   * @since GemFire prPersistSprint2
   */
  public DiskStore findDiskStore(String name) {
    if (name == null) {
      name = DEFAULT_DS_NAME;
    }
    return this.diskStores.get(name);
  }

  /**
   * Returns the DiskStore list
   *
   * @since GemFire prPersistSprint2
   */
  public Collection<DiskStoreImpl> listDiskStores() {
    return Collections.unmodifiableCollection(this.diskStores.values());
  }

  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault() {
    return Collections.unmodifiableCollection(listDiskStores());
  }

  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned() {
    HashSet<DiskStoreImpl> allDiskStores = new HashSet<DiskStoreImpl>();
    allDiskStores.addAll(this.diskStores.values());
    allDiskStores.addAll(this.regionOwnedDiskStores.values());
    return allDiskStores;
  }

  public boolean executeDiskStoreTask(DiskStoreTask r) {
    synchronized (this.diskStoreTaskSync) {
      if (!this.diskStoreTaskSync.get()) {
        if (this.diskStoreTaskPool == null) {
          createDiskStoreTaskPool();
        }
        try {
          this.diskStoreTaskPool.execute(r);
          return true;
        } catch (RejectedExecutionException ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("Ignored compact schedule during shutdown", ex);
          }
        }
      }
    }
    return false;
  }

 /* private static class DiskStoreFuture extends FutureTask {
    private DiskStoreTask task;

    public DiskStoreFuture(DiskStoreTask r) {
      super(r, null);
      this.task = r;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean result = super.cancel(mayInterruptIfRunning);
      if (result) {
        task.taskCancelled();
      }
      return result;
    }

  }*/

  private void stopDiskStoreTaskPool() {
    synchronized (this.diskStoreTaskSync) {
      this.diskStoreTaskSync.set(true);
      // All the regions have already been closed
      // so this pool shouldn't be doing anything.
      if (this.diskStoreTaskPool != null) {
        List<Runnable> l = this.diskStoreTaskPool.shutdownNow();
        for (Runnable runnable : l) {
          if (l instanceof DiskStoreTask) {
            ((DiskStoreTask) l).taskCancelled();
          }
        }
      }
      //this.diskStoreTaskPool = null;
    }
  }

  public int stopGatewaySenders(boolean byShutdownAll) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    int cnt = 0;
    closingGatewaySendersByShutdownAll = byShutdownAll;
    synchronized (allGatewaySendersLock) {
      GatewaySenderAdvisor advisor = null;
      Iterator<GatewaySender> itr = allGatewaySenders.iterator();
      while (itr.hasNext()) {
        GatewaySender sender = itr.next();
        if (isDebugEnabled) {
          logger.debug("{}: stopping gateway sender {}", this, sender);
        }
        try {
          sender.stop();
          advisor = ((AbstractGatewaySender)sender).getSenderAdvisor();
          if (advisor != null) {
            if (isDebugEnabled) {
              logger.debug("Stopping the GatewaySender advisor");
            }
            advisor.close();
          }
          cnt++;
        }
        catch (CancelException e) {
          if (isDebugEnabled) {
            logger.debug("Ignored cache closure while closing sender {}", sender, e);
          }
        }
      }
    } // synchronized

    destroyGatewaySenderLockService();
    
    if (isDebugEnabled) {
      logger.debug("{}: finished stopping {} gateway sender(s), total is {}", this, cnt, allGatewaySenders.size());
    }
    return cnt;
  }
  
  public int stopGatewayReceivers(boolean byShutdownAll) {
    int cnt = 0;
    closingGatewayReceiversByShutdownAll = byShutdownAll;
    synchronized (allGatewayReceiversLock) {
      Iterator<GatewayReceiver> itr = allGatewayReceivers.iterator();
      while (itr.hasNext()) {
        GatewayReceiver receiver = itr.next();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: stopping gateway receiver {}", this, receiver);
        }
        try {
          receiver.stop();
          cnt++;
        }
        catch (CancelException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Ignored cache closure while closing receiver {}", receiver, e);
          }
        }
      }
    } // synchronized

    if (logger.isDebugEnabled()) {
      logger.debug("{}: finished stopping {} gateway receiver(s), total is {}", this, cnt, allGatewayReceivers.size());
    }
    return cnt;
  }

  void stopServers() {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (isDebugEnabled) {
      logger.debug("{}: stopping cache servers...", this);
    }
    boolean stoppedCacheServer = false;
    Iterator allCacheServersIterator = this.allCacheServers.iterator();
    while (allCacheServersIterator.hasNext()) {
      CacheServerImpl bridge = (CacheServerImpl) allCacheServersIterator.next();
      if (isDebugEnabled) {
        logger.debug("stopping bridge {}", bridge);
      }
      try {
        bridge.stop();
      } catch (CancelException e) {
        if (isDebugEnabled) {
          logger.debug("Ignored cache closure while closing bridge {}", bridge, e);
        }
      }
      allCacheServers.remove(bridge);
      stoppedCacheServer = true;
    }
    if (stoppedCacheServer) {
      // now that all the cache servers have stopped empty the static pool of commBuffers it might have used.
      ServerConnection.emptyCommBufferPool();
    }
    
    // stop HA services if they had been started
    if (isDebugEnabled) {
      logger.debug("{}: stopping HA services...", this);
    }
    try {
      HARegionQueue.stopHAServices();
    } catch (CancelException e) {
      if (isDebugEnabled) {
        logger.debug("Ignored cache closure while closing HA services", e);
      }
    }

    if (isDebugEnabled) {
      logger.debug("{}: stopping client health monitor...", this);
    }
    try {
      ClientHealthMonitor.shutdownInstance();
    } catch (CancelException e) {
      if (isDebugEnabled) {
        logger.debug("Ignored cache closure while closing client health monitor", e);
      }
    }

    // Reset the unique id counter for durable clients.
    // If a durable client stops/starts its cache, it needs
    // to maintain the same unique id.
    ClientProxyMembershipID.resetUniqueIdCounter();

  }

  public InternalDistributedSystem getDistributedSystem() {
    return this.system;
  }

  /**
   * Returns the member id of my distributed system
   *
   * @since GemFire 5.0
   */
  public InternalDistributedMember getMyId() {
    return this.system.getDistributedMember();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.Cache#getMembers()
   */
  public Set<DistributedMember> getMembers() {
    return Collections.unmodifiableSet(this.dm.getOtherNormalDistributionManagerIds());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.Cache#getAdminMembers()
   */
  public Set<DistributedMember> getAdminMembers() {
    return this.dm.getAdminMemberSet();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.Cache#getMembers(org.apache.geode.cache.Region)
   */
  public Set<DistributedMember> getMembers(Region r) {
    if (r instanceof DistributedRegion) {
      DistributedRegion d = (DistributedRegion) r;
      return d.getDistributionAdvisor().adviseCacheOp();
    } else if (r instanceof PartitionedRegion) {
      PartitionedRegion p = (PartitionedRegion) r;
      return p.getRegionAdvisor().adviseAllPRNodes();
    } else {
      return Collections.EMPTY_SET;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.client.ClientCache#getCurrentServers()
   */
  public Set<InetSocketAddress> getCurrentServers() {
    Map<String, Pool> pools = PoolManager.getAll();
    Set result = null;
    for (Pool p : pools.values()) {
      PoolImpl pi = (PoolImpl) p;
      for (Object o : pi.getCurrentServers()) {
        ServerLocation sl = (ServerLocation) o;
        if (result == null) {
          result = new HashSet<DistributedMember>();
        }
        result.add(new InetSocketAddress(sl.getHostName(), sl.getPort()));
      }
    }
    if (result == null) {
      return Collections.EMPTY_SET;
    } else {
      return result;
    }
  }

  public LogWriter getLogger() {
    return this.system.getLogWriter();
  }

  public LogWriter getSecurityLogger() {
    return this.system.getSecurityLogWriter();
  }

  public LogWriterI18n getLoggerI18n() {
    return this.system.getInternalLogWriter();
  }
  
  public LogWriterI18n getSecurityLoggerI18n() {
    return this.system.getSecurityInternalLogWriter();
  }
  
  public InternalLogWriter getInternalLogWriter() {
    return this.system.getInternalLogWriter();
  }

  public InternalLogWriter getSecurityInternalLogWriter() {
    return this.system.getSecurityInternalLogWriter();
  }

  /**
   * get the threadid/sequenceid sweeper task for this cache
   *
   * @return the sweeper task
   */
  protected EventTracker.ExpiryTask getEventTrackerTask() {
    return this.recordedEventSweeper;
  }

  public CachePerfStats getCachePerfStats() {
    return this.cachePerfStats;
  }

  public String getName() {
    return this.system.getName();
  }

  /**
   * Get the list of all instances of properties for Declarables with the given class name.
   * 
   * @param className Class name of the declarable
   * @return List of all instances of properties found for the given declarable
   */
  public List<Properties> getDeclarableProperties(final String className) {
    List<Properties> propertiesList = new ArrayList<Properties>();
    synchronized (this.declarablePropertiesMap) {
      for (Map.Entry<Declarable, Properties> entry : this.declarablePropertiesMap.entrySet()) {
        if (entry.getKey().getClass().getName().equals(className)) {
          propertiesList.add(entry.getValue());
        }
      }
    }
    return propertiesList;
  }
  
  /**
   * Get the properties for the given declarable.
   * 
   * @param declarable The declarable
   * @return Properties found for the given declarable
   */
  public Properties getDeclarableProperties(final Declarable declarable) {
    return this.declarablePropertiesMap.get(declarable);
  }
  
  /**
   * Returns the date and time that this cache was created.
   *
   * @since GemFire 3.5
   */
  public Date getCreationDate() {
    return this.creationDate;
  }

  /**
   * Returns the number of seconds that have elapsed since the Cache was created.
   *
   * @since GemFire 3.5
   */
  public int getUpTime() {
    return (int) ((System.currentTimeMillis() - this.creationDate.getTime()) / 1000);
  }

  /**
   * All entry and region operations should be using this time rather than
   * System.currentTimeMillis(). Specially all version stamps/tags must be
   * populated with this timestamp.
   * 
   * @return distributed cache time.
   */
  @Override
  public long cacheTimeMillis() {
    if (this.system != null) {
      return this.system.getClock().cacheTimeMillis();
    } else {
      return System.currentTimeMillis();
    }
  }

  public Region createVMRegion(String name, RegionAttributes attrs) throws RegionExistsException, TimeoutException {
    return createRegion(name, attrs);
  }

  private PoolFactory createDefaultPF() {
    PoolFactory defpf = PoolManager.createFactory();
    try {
      String localHostName = SocketCreator.getHostName(SocketCreator.getLocalHost());
      defpf.addServer(localHostName, CacheServer.DEFAULT_PORT);
    } catch (UnknownHostException ex) {
      throw new IllegalStateException("Could not determine local host name");
    }
    return defpf;
  }

  /**
   * Used to set the default pool on a new GemFireCache.
   */
  public void determineDefaultPool() {
    if (!isClient()) {
      throw new UnsupportedOperationException();
    }
    Pool pool = null;
    // create the pool if it does not already exist
    if (this.clientpf == null) {
      Map<String, Pool> pools = PoolManager.getAll();
      if (pools.isEmpty()) {
        this.clientpf = createDefaultPF();
      } else if (pools.size() == 1) {
        // otherwise use a singleton.
        pool = pools.values().iterator().next();
      } else {
        if (pool == null) {
          // act as if the default pool was configured
          // and see if we can find an existing one that is compatible
          PoolFactoryImpl pfi = (PoolFactoryImpl) createDefaultPF();
          for (Pool p : pools.values()) {
            if (((PoolImpl) p).isCompatible(pfi.getPoolAttributes())) {
              pool = p;
              break;
            }
          }
          if (pool == null) {
            // if pool is still null then we will not have a default pool for this ClientCache
            setDefaultPool(null);
            return;
          }
        }
      }
    } else {
      PoolFactoryImpl pfi = (PoolFactoryImpl) this.clientpf;
      if (pfi.getPoolAttributes().locators.isEmpty() && pfi.getPoolAttributes().servers.isEmpty()) {
        try {
          String localHostName = SocketCreator.getHostName(SocketCreator.getLocalHost());
          pfi.addServer(localHostName, CacheServer.DEFAULT_PORT);
        } catch (UnknownHostException ex) {
          throw new IllegalStateException("Could not determine local host name");
        }
      }
      // look for a pool that already exists that is compatible with
      // our PoolFactory.
      // If we don't find one we will create a new one that meets our needs.
      Map<String, Pool> pools = PoolManager.getAll();
      for (Pool p : pools.values()) {
        if (((PoolImpl) p).isCompatible(pfi.getPoolAttributes())) {
          pool = p;
          break;
        }
      }
    }
    if (pool == null) {
      // create our pool with a unique name
      String poolName = "DEFAULT";
      int count = 1;
      Map<String, Pool> pools = PoolManager.getAll();
      while (pools.containsKey(poolName)) {
        poolName = "DEFAULT" + count;
        count++;
      }
      pool = this.clientpf.create(poolName);
    }
    setDefaultPool(pool);
  }

  /**
   * Used to see if a existing cache's pool is compatible with us.
   *
   * @return the default pool that is right for us
   */
  public Pool determineDefaultPool(PoolFactory pf) {
    Pool pool = null;
    // create the pool if it does not already exist
    if (pf == null) {
      Map<String, Pool> pools = PoolManager.getAll();
      if (pools.isEmpty()) {
        throw new IllegalStateException("Since a cache already existed a pool should also exist.");
      } else if (pools.size() == 1) {
        // otherwise use a singleton.
        pool = pools.values().iterator().next();
        if (getDefaultPool() != pool) {
          throw new IllegalStateException("Existing cache's default pool was not the same as the only existing pool");
        }
      } else {
        // just use the current default pool if one exists
        pool = getDefaultPool();
        if (pool == null) {
          // act as if the default pool was configured
          // and see if we can find an existing one that is compatible
          PoolFactoryImpl pfi = (PoolFactoryImpl) createDefaultPF();
          for (Pool p : pools.values()) {
            if (((PoolImpl) p).isCompatible(pfi.getPoolAttributes())) {
              pool = p;
              break;
            }
          }
          if (pool == null) {
            // if pool is still null then we will not have a default pool for this ClientCache
            return null;
          }
        }
      }
    } else {
      PoolFactoryImpl pfi = (PoolFactoryImpl) pf;
      if (pfi.getPoolAttributes().locators.isEmpty() && pfi.getPoolAttributes().servers.isEmpty()) {
        try {
          String localHostName = SocketCreator.getHostName(SocketCreator.getLocalHost());
          pfi.addServer(localHostName, CacheServer.DEFAULT_PORT);
        } catch (UnknownHostException ex) {
          throw new IllegalStateException("Could not determine local host name");
        }
      }
      PoolImpl defPool = (PoolImpl) getDefaultPool();
      if (defPool != null && defPool.isCompatible(pfi.getPoolAttributes())) {
        pool = defPool;
      } else {
        throw new IllegalStateException("Existing cache's default pool was not compatible");
      }
    }
    return pool;
  }

  public Region createRegion(String name, RegionAttributes attrs) throws RegionExistsException, TimeoutException {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    return basicCreateRegion(name, attrs);
  }

  public Region basicCreateRegion(String name, RegionAttributes attrs) throws RegionExistsException, TimeoutException {
    try {
      InternalRegionArguments ira = new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
          .setSnapshotInputStream(null).setImageTarget(null);

      if (attrs instanceof UserSpecifiedRegionAttributes) {
        ira.setIndexes(((UserSpecifiedRegionAttributes) attrs).getIndexes());
      }
      return createVMRegion(name, attrs, ira);
    } catch (IOException | ClassNotFoundException e) {
      // only if loading snapshot, not here
      InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString());
      assErr.initCause(e);
      throw assErr;
    }
  }

  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs, InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    if (getMyId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
      if (!internalRegionArgs.isUsedForMetaRegion() && internalRegionArgs.getInternalMetaRegion() == null) {
        throw new IllegalStateException("Regions can not be created in a locator.");
      }
    }
    stopper.checkCancelInProgress(null);
    LocalRegion.validateRegionName(name, internalRegionArgs);
    RegionAttributes<K, V> attrs = p_attrs;
    attrs = invokeRegionBefore(null, name, attrs, internalRegionArgs);
    if (attrs == null) {
      throw new IllegalArgumentException(LocalizedStrings.GemFireCache_ATTRIBUTES_MUST_NOT_BE_NULL.toLocalizedString());
    }
    
    LocalRegion rgn = null;
    // final boolean getDestroyLock = attrs.getDestroyLockFlag();
    final InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();
    final boolean recreate = internalRegionArgs.getRecreateFlag();

    final boolean isPartitionedRegion = attrs.getPartitionAttributes() != null;
    final boolean isReinitCreate = snapshotInputStream != null || imageTarget != null || recreate;

    final String regionPath = LocalRegion.calcFullPath(name, null);

    try {
      for (;;) {
        getCancelCriterion().checkCancelInProgress(null);

        Future future = null;
        synchronized (this.rootRegions) {
          rgn = (LocalRegion) this.rootRegions.get(name);
          if (rgn != null) {
            throw new RegionExistsException(rgn);
          }
          // check for case where a root region is being reinitialized and we
          // didn't
          // find a region, i.e. the new region is about to be created

          if (!isReinitCreate) { // fix bug 33523
            String fullPath = Region.SEPARATOR + name;
            future = (Future) this.reinitializingRegions.get(fullPath);
          }
          if (future == null) {
            if (internalRegionArgs.getInternalMetaRegion() != null) {
              rgn = internalRegionArgs.getInternalMetaRegion();
            } else if (isPartitionedRegion) {
              rgn = new PartitionedRegion(name, attrs, null, this, internalRegionArgs);
            } else {
              /*for (String senderId : attrs.getGatewaySenderIds()) {
                if (getGatewaySender(senderId) != null
                    && getGatewaySender(senderId).isParallel()) {
                  throw new IllegalStateException(
                      LocalizedStrings.AttributesFactory_PARALLELGATEWAYSENDER_0_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION
                          .toLocalizedString(senderId));
                }
              }*/
              if (attrs.getScope().isLocal()) {
                rgn = new LocalRegion(name, attrs, null, this, internalRegionArgs);
              } else {
                rgn = new DistributedRegion(name, attrs, null, this, internalRegionArgs);
              }
            }

            this.rootRegions.put(name, rgn);
            if (isReinitCreate) {
              regionReinitialized(rgn);
            }
            break;
          }
        } // synchronized

        boolean interrupted = Thread.interrupted();
        try { // future != null
          LocalRegion region = (LocalRegion) future.get(); // wait on Future
          throw new RegionExistsException(region);
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          throw new Error(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString(), e);
        } catch (CancellationException e) {
          // future was cancelled
        } finally {
          if (interrupted)
            Thread.currentThread().interrupt();
        }
      } // for
      
      boolean success = false;
      try {
        setRegionByPath(rgn.getFullPath(), rgn);
        rgn.initialize(snapshotInputStream, imageTarget, internalRegionArgs);
        success = true;
      } catch (CancelException e) {
        // don't print a call stack
        throw e;
      } catch (RedundancyAlreadyMetException e) {
        // don't log this
        throw e;
      } catch (final RuntimeException validationException) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.GemFireCache_INITIALIZATION_FAILED_FOR_REGION_0, rgn.getFullPath()), validationException);
        throw validationException;
      } finally {
        if (!success) {
          try {
            // do this before removing the region from
            // the root set to fix bug 41982.
            rgn.cleanupFailedInitialization();
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            SystemFailure.checkFailure();
            stopper.checkCancelInProgress(t);
            
            // bug #44672 - log the failure but don't override the original exception
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.GemFireCache_INIT_CLEANUP_FAILED_FOR_REGION_0, rgn.getFullPath()), t);

          } finally {
            // clean up if initialize fails for any reason
            setRegionByPath(rgn.getFullPath(), null);
            synchronized (this.rootRegions) {
              Region r = (Region) this.rootRegions.get(name);
              if (r == rgn) {
                this.rootRegions.remove(name);
              }
            } // synchronized
          }
        } // success
      }

      
      
      rgn.postCreateRegion();
    } catch (RegionExistsException ex) {
      // outside of sync make sure region is initialized to fix bug 37563
      LocalRegion r = (LocalRegion) ex.getRegion();
      r.waitOnInitialization(); // don't give out ref until initialized
      throw ex;
    }

    invokeRegionAfter(rgn);
    /**
     * Added for M&M . Putting the callback here to avoid creating RegionMBean in case of Exception
     **/
    if (!rgn.isInternalRegion()) {
      system.handleResourceEvent(ResourceEvent.REGION_CREATE, rgn);
    }

    return rgn;
  }

  public RegionAttributes invokeRegionBefore(LocalRegion parent,
      String name, RegionAttributes attrs, InternalRegionArguments internalRegionArgs) {
    for(RegionListener listener : regionListeners) {
      attrs = listener.beforeCreate(parent, name, attrs, internalRegionArgs);
    }
    return attrs;
  }
  
  public void invokeRegionAfter(LocalRegion region) {
    for(RegionListener listener : regionListeners) {
      listener.afterCreate(region);
    }
  }

  public final Region getRegion(String path) {
    return getRegion(path, false);
  }

  /**
   * returns a set of all current regions in the cache, including buckets
   *
   * @since GemFire 6.0
   */
  public Set<LocalRegion> getAllRegions() {
    Set<LocalRegion> result = new HashSet();
    synchronized (this.rootRegions) {
      for (Object r : this.rootRegions.values()) {
        if (r instanceof PartitionedRegion) {
          PartitionedRegion p = (PartitionedRegion) r;
          PartitionedRegionDataStore prds = p.getDataStore();
          if (prds != null) {
            Set<Entry<Integer, BucketRegion>> bucketEntries = p.getDataStore().getAllLocalBuckets();
            for (Map.Entry e : bucketEntries) {
              result.add((LocalRegion) e.getValue());
            }
          }
        } else if (r instanceof LocalRegion) {
          LocalRegion l = (LocalRegion) r;
          result.add(l);
          result.addAll(l.basicSubregions(true));
        }
      }
    }
    return result;
  }

  public Set<LocalRegion> getApplicationRegions() {
    Set<LocalRegion> result = new HashSet<LocalRegion>();
    synchronized (this.rootRegions) {
      for (Object r : this.rootRegions.values()) {
        LocalRegion rgn = (LocalRegion) r;
        if (rgn.isSecret() || rgn.isUsedForMetaRegion() || rgn instanceof HARegion || rgn.isUsedForPartitionedRegionAdmin()
            || rgn.isInternalRegion()/* rgn.isUsedForPartitionedRegionBucket() */) {
          continue; // Skip administrative PartitionedRegions
        }
        result.add(rgn);
        result.addAll(rgn.basicSubregions(true));
      }
    }
    return result;
  }

  void setRegionByPath(String path, LocalRegion r) {
    if (r == null) {
      this.pathToRegion.remove(path);
    } else {
      this.pathToRegion.put(path, r);
    }
  }

  /**
   * @throws IllegalArgumentException
   *           if path is not valid
   */
  private static void validatePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException(LocalizedStrings.GemFireCache_PATH_CANNOT_BE_NULL.toLocalizedString());
    }
    if (path.length() == 0) {
      throw new IllegalArgumentException(LocalizedStrings.GemFireCache_PATH_CANNOT_BE_EMPTY.toLocalizedString());
    }
    if (path.equals(Region.SEPARATOR)) {
      throw new IllegalArgumentException(LocalizedStrings.GemFireCache_PATH_CANNOT_BE_0.toLocalizedString(Region.SEPARATOR));
    }
  }

  public LocalRegion getRegionByPath(String path) {
    validatePath(path); // fix for bug 34892

    { // do this before checking the pathToRegion map
      LocalRegion result = getReinitializingRegion(path);
      if (result != null) {
        return result;
      }
    }
    return (LocalRegion) this.pathToRegion.get(path);
  }

  public LocalRegion getRegionByPathForProcessing(String path) {
    LocalRegion result = getRegionByPath(path);
    if (result == null) {
      stopper.checkCancelInProgress(null);
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT); // go through
      // initialization
      // latches
      try {
        String[] pathParts = parsePath(path);
        LocalRegion root;
        synchronized (this.rootRegions) {
          root = (LocalRegion) this.rootRegions.get(pathParts[0]);
          if (root == null)
            return null;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, calling getSubregion on root({}): {}", pathParts[0], pathParts[1]);
        }
        result = (LocalRegion) root.getSubregion(pathParts[1], true);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
    return result;
  }

  /**
   * @param returnDestroyedRegion
   *          if true, okay to return a destroyed region
   */
  public Region getRegion(String path, boolean returnDestroyedRegion) {
    stopper.checkCancelInProgress(null);
    {
      LocalRegion result = getRegionByPath(path);
      // Do not waitOnInitialization() for PR
      // if (result != null && !(result instanceof PartitionedRegion)) {
      if (result != null) {
        result.waitOnInitialization();
        if (!returnDestroyedRegion && result.isDestroyed()) {
          stopper.checkCancelInProgress(null);
          return null;
        } else {
          return result;
        }
      }
    }

    String[] pathParts = parsePath(path);
    LocalRegion root;
    synchronized (this.rootRegions) {
      root = (LocalRegion) this.rootRegions.get(pathParts[0]);
      if (root == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, no region found for {}", pathParts[0]);
        }
        stopper.checkCancelInProgress(null);
        return null;
      }
      if (!returnDestroyedRegion && root.isDestroyed()) {
        stopper.checkCancelInProgress(null);
        return null;
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("GemFireCache.getRegion, calling getSubregion on root({}): {}", pathParts[0], pathParts[1]);
    }
    return root.getSubregion(pathParts[1], returnDestroyedRegion);
  }

  /**
   * @param returnDestroyedRegion
   *          if true, okay to return a destroyed partitioned region
   */
  public final Region getPartitionedRegion(String path, boolean returnDestroyedRegion) {
    stopper.checkCancelInProgress(null);
    {
      LocalRegion result = getRegionByPath(path);
      // Do not waitOnInitialization() for PR
      if (result != null) {
        if (!(result instanceof PartitionedRegion)) {
          return null;
        } else {
          return result;
        }
      }
    }
 
    String[] pathParts = parsePath(path);
    LocalRegion root;
    LogWriterI18n logger = getLoggerI18n();
    synchronized (this.rootRegions) {
      root = (LocalRegion) this.rootRegions.get(pathParts[0]);
      if (root == null) {
        if (logger.fineEnabled()) {
          logger.fine("GemFireCache.getRegion, no region found for " + pathParts[0]);
        }
        stopper.checkCancelInProgress(null);
        return null;
      }
      if (!returnDestroyedRegion && root.isDestroyed()) {
        stopper.checkCancelInProgress(null);
        return null;
      }
    }
    if (logger.fineEnabled()) {
      logger.fine("GemFireCache.getPartitionedRegion, calling getSubregion on root(" + pathParts[0] + "): " + pathParts[1]);
    }
    Region result = root.getSubregion(pathParts[1], returnDestroyedRegion);
    if (result != null && !(result instanceof PartitionedRegion)) {
      return null;
    } else {
      return result;
    }
  }

  /** Return true if this region is initializing */
  boolean isGlobalRegionInitializing(String fullPath) {
    stopper.checkCancelInProgress(null);
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT); // go through
    // initialization
    // latches
    try {
      return isGlobalRegionInitializing((LocalRegion) getRegion(fullPath));
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  /** Return true if this region is initializing */
  boolean isGlobalRegionInitializing(LocalRegion region) {
    boolean result = region != null && region.scope.isGlobal() && !region.isInitialized();
    if (result) {
      if (logger.isDebugEnabled()) {
        logger.debug("GemFireCache.isGlobalRegionInitializing ({})", region.getFullPath());
      }
    }
    return result;
  }

  public Set rootRegions() {
    return rootRegions(false);
  }

  public final Set rootRegions(boolean includePRAdminRegions) {
    return rootRegions(includePRAdminRegions, true);
  }

  private final Set rootRegions(boolean includePRAdminRegions, boolean waitForInit) {
    stopper.checkCancelInProgress(null);
    Set regions = new HashSet();
    synchronized (this.rootRegions) {
      for (Iterator itr = this.rootRegions.values().iterator(); itr.hasNext();) {
        LocalRegion r = (LocalRegion) itr.next();
        // If this is an internal meta-region, don't return it to end user
        if (r.isSecret() || r.isUsedForMetaRegion() || r instanceof HARegion || !includePRAdminRegions
            && (r.isUsedForPartitionedRegionAdmin() || r.isUsedForPartitionedRegionBucket())) {
          continue; // Skip administrative PartitionedRegions
        }
        regions.add(r);
      }
    }
    if (waitForInit) {
      for (Iterator r = regions.iterator(); r.hasNext();) {
        LocalRegion lr = (LocalRegion) r.next();
        // lr.waitOnInitialization();
        if (!lr.checkForInitialization()) {
          r.remove();
        }
      }
    }
    return Collections.unmodifiableSet(regions);
  }

  /**
   * Called by ccn when a client goes away
   *
   * @since GemFire 5.7
   */
  public void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client) {
    try {
      if (isClosed())
        return;
      Iterator it = rootRegions(false, false).iterator();
      while (it.hasNext()) {
        LocalRegion lr = (LocalRegion) it.next();
        lr.cleanupForClient(ccn, client);
      }
    } catch (DistributedSystemDisconnectedException ignore) {
    }
  }

  public boolean isInitialized() {
    return this.isInitialized;
  }

  public boolean isClosed() {
    return this.isClosing;
  }

  public int getLockTimeout() {
    return this.lockTimeout;
  }

  public void setLockTimeout(int seconds) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);
    this.lockTimeout = seconds;
  }

  public int getLockLease() {
    return this.lockLease;
  }

  public void setLockLease(int seconds) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);
    this.lockLease = seconds;
  }

  public int getSearchTimeout() {
    return this.searchTimeout;
  }

  public void setSearchTimeout(int seconds) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);
    this.searchTimeout = seconds;
  }

  public int getMessageSyncInterval() {
    return HARegionQueue.getMessageSyncInterval();
  }

  public void setMessageSyncInterval(int seconds) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);
    if (seconds < 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.GemFireCache_THE_MESSAGESYNCINTERVAL_PROPERTY_FOR_CACHE_CANNOT_BE_NEGATIVE.toLocalizedString());
    }
    HARegionQueue.setMessageSyncInterval(seconds);
  }

  /**
   * Get a reference to a Region that is reinitializing, or null if that Region is not reinitializing or this thread is
   * interrupted. If a reinitializing region is found, then this method blocks until reinitialization is complete and
   * then returns the region.
   */
  LocalRegion getReinitializingRegion(String fullPath) {
    Future future = (Future) this.reinitializingRegions.get(fullPath);
    if (future == null) {
      return null;
    }
    try {
      LocalRegion region = (LocalRegion) future.get();
      region.waitOnInitialization();
      if (logger.isDebugEnabled()) {
        logger.debug("Returning manifested future for: {}", fullPath);
      }
      return region;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException e) {
      throw new Error(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString(), e);
    } catch (CancellationException e) {
      // future was cancelled
      logger.debug("future cancelled, returning null");
      return null;
    }
  }

  /**
   * Register the specified region name as reinitializing, creating and adding a Future for it to the map.
   *
   * @throws IllegalStateException
   *           if there is already a region by that name registered.
   */
  void regionReinitializing(String fullPath) {
    Object old = this.reinitializingRegions.putIfAbsent(fullPath, new FutureResult(this.stopper));
    if (old != null) {
      throw new IllegalStateException(LocalizedStrings.GemFireCache_FOUND_AN_EXISTING_REINITALIZING_REGION_NAMED_0
          .toLocalizedString(fullPath));
    }
  }

  /**
   * Set the reinitialized region and unregister it as reinitializing.
   *
   * @throws IllegalStateException
   *           if there is no region by that name registered as reinitializing.
   */
  void regionReinitialized(Region region) {
    String regionName = region.getFullPath();
    FutureResult future = (FutureResult) this.reinitializingRegions.get(regionName);
    if (future == null) {
      throw new IllegalStateException(LocalizedStrings.GemFireCache_COULD_NOT_FIND_A_REINITIALIZING_REGION_NAMED_0
          .toLocalizedString(regionName));
    }
    future.set(region);
    unregisterReinitializingRegion(regionName);
  }

  /**
   * Clear a reinitializing region, e.g. reinitialization failed.
   *
   * @throws IllegalStateException
   *           if cannot find reinitializing region registered by that name.
   */
  void unregisterReinitializingRegion(String fullPath) {
    /* Object previous = */this.reinitializingRegions.remove(fullPath);
    // if (previous == null) {
    // throw new IllegalStateException("Could not find a reinitializing region
    // named " +
    // fullPath);
    // }
  }

  // /////////////////////////////////////////////////////////////

  /**
   * Returns true if get should give a copy; false if a reference.
   *
   * @since GemFire 4.0
   */
  final boolean isCopyOnRead() {
    return this.copyOnRead;
  }

  /**
   * Implementation of {@link org.apache.geode.cache.Cache#setCopyOnRead}
   *
   * @since GemFire 4.0
   */
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
  }

  /**
   * Implementation of {@link org.apache.geode.cache.Cache#getCopyOnRead}
   *
   * @since GemFire 4.0
   */
  final public boolean getCopyOnRead() {
    return this.copyOnRead;
  }

  /**
   * Remove the specified root region
   *
   * @param rootRgn
   *          the region to be removed
   * @return true if root region was removed, false if not found
   */
  boolean removeRoot(LocalRegion rootRgn) {
    synchronized (this.rootRegions) {
      String rgnName = rootRgn.getName();
      LocalRegion found = (LocalRegion) this.rootRegions.get(rgnName);
      if (found == rootRgn) {
        LocalRegion previous = (LocalRegion) this.rootRegions.remove(rgnName);
        Assert.assertTrue(previous == rootRgn);
        return true;
      } else
        return false;
    }
  }

  /**
   * @return array of two Strings, the root name and the relative path from root If there is no relative path from root,
   *         then String[1] will be an empty string
   */
  static String[] parsePath(String p_path) {
    String path = p_path;
    validatePath(path);
    String[] result = new String[2];
    result[1] = "";
    // strip off root name from path
    int slashIndex = path.indexOf(Region.SEPARATOR_CHAR);
    if (slashIndex == 0) {
      path = path.substring(1);
      slashIndex = path.indexOf(Region.SEPARATOR_CHAR);
    }
    result[0] = path;
    if (slashIndex > 0) {
      result[0] = path.substring(0, slashIndex);
      result[1] = path.substring(slashIndex + 1);
    }
    return result;
  }

  /**
   * Makes note of a <code>CacheLifecycleListener</code>
   */
  public static void addCacheLifecycleListener(CacheLifecycleListener l) {
    synchronized (GemFireCacheImpl.class) {
      cacheLifecycleListeners.add(l);
    }
  }

  /**
   * Removes a <code>CacheLifecycleListener</code>
   *
   * @return Whether or not the listener was removed
   */
  public static boolean removeCacheLifecycleListener(CacheLifecycleListener l) {
    synchronized (GemFireCacheImpl.class) {
      return cacheLifecycleListeners.remove(l);
    }
  }
  
  public void addRegionListener(RegionListener l ) {
    this.regionListeners.add(l);
  }
  
  public void removeRegionListener(RegionListener l ) {
    this.regionListeners.remove(l);
  }
  
  @SuppressWarnings("unchecked")
  public <T extends CacheService> T getService(Class<T> clazz) {
    return (T) services.get(clazz);
  }

  /**
   * Creates the single instance of the Transation Manager for this cache. Returns the existing one upon request.
   *
   * @return the CacheTransactionManager instance.
   *
   * @since GemFire 4.0
   */
  public CacheTransactionManager getCacheTransactionManager() {
    return this.txMgr;
  }

  /**
   * @see CacheClientProxy
   * @guarded.By {@link #ccpTimerMutex}
   */
  private SystemTimer ccpTimer;

  /**
   * @see #ccpTimer
   */
  private final Object ccpTimerMutex = new Object();

  /**
   * Get cache-wide CacheClientProxy SystemTimer
   *
   * @return the timer, lazily created
   */
  public SystemTimer getCCPTimer() {
    synchronized (ccpTimerMutex) {
      if (ccpTimer != null) {
        return ccpTimer;
      }
      ccpTimer = new SystemTimer(getDistributedSystem(), true);
      if (this.isClosing) {
        ccpTimer.cancel(); // poison it, don't throw.
      }
      return ccpTimer;
    }
  }

  /**
   * @see LocalRegion
   */
  private final ExpirationScheduler expirationScheduler;

  /**
   * Get cache-wide ExpirationScheduler
   *
   * @return the scheduler, lazily created
   */
  public ExpirationScheduler getExpirationScheduler() {
    return this.expirationScheduler;
  }

  TXManagerImpl getTXMgr() {
    return this.txMgr;
  }

  /**
   * Returns the <code>Executor</code> (thread pool) that is used to execute cache event listeners.
   * Returns <code>null</code> if no pool exists.
   *
   * @since GemFire 3.5
   */
  Executor getEventThreadPool() {
    return this.eventThreadPool;
  }

  public CacheServer addCacheServer() {
    return addCacheServer(false);
  }

  public CacheServer addCacheServer(boolean isGatewayReceiver) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);

    CacheServerImpl bridge = new CacheServerImpl(this, isGatewayReceiver);
    allCacheServers.add(bridge);

    sendAddCacheServerProfileMessage();
    return bridge;
  }
  
  public void addGatewaySender(GatewaySender sender) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }

    stopper.checkCancelInProgress(null);

    synchronized (allGatewaySendersLock) {
      if (!allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((AbstractGatewaySender) sender).distribute(true);
        Set<GatewaySender> tmp = new HashSet<GatewaySender>(allGatewaySenders.size() + 1);
        if (!allGatewaySenders.isEmpty()) {
          tmp.addAll(allGatewaySenders);
        }
        tmp.add(sender);
        this.allGatewaySenders = Collections.unmodifiableSet(tmp);
      } else {
        throw new IllegalStateException(LocalizedStrings.GemFireCache_A_GATEWAYSENDER_WITH_ID_0_IS_ALREADY_DEFINED_IN_THIS_CACHE
            .toLocalizedString(sender.getId()));
      }
    }

    synchronized (this.rootRegions) {
      Set<LocalRegion> appRegions = getApplicationRegions();
      for (LocalRegion r : appRegions) {
        Set<String> senders = r.getAllGatewaySenderIds();
        if (senders.contains(sender.getId()) && !sender.isParallel()) {
          r.senderCreated();
        }
      }
    }

     if(!sender.isParallel()) {
       Region dynamicMetaRegion = getRegion(DynamicRegionFactory.dynamicRegionListName);
       if(dynamicMetaRegion == null) {
         if(logger.isDebugEnabled()) {
           logger.debug(" The dynamic region is null. ");
         }
      } else {
        dynamicMetaRegion.getAttributesMutator().addGatewaySenderId(sender.getId());
      }
    }
    if (!(sender.getRemoteDSId() < 0)) {
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_CREATE, sender);
    }
  }
  
  public void removeGatewaySender(GatewaySender sender) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    
    stopper.checkCancelInProgress(null);

    synchronized (allGatewaySendersLock) {
      if (allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((AbstractGatewaySender) sender, true).distribute(true);
        Set<GatewaySender> tmp = new HashSet<GatewaySender>(allGatewaySenders.size() - 1);
        if (!allGatewaySenders.isEmpty()) {
          tmp.addAll(allGatewaySenders);
        }
        tmp.remove(sender);
        this.allGatewaySenders = Collections.unmodifiableSet(tmp);
      }
    }
  }

  public void addGatewayReceiver(GatewayReceiver recv) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);
    synchronized (allGatewayReceiversLock) {
      Set<GatewayReceiver> tmp = new HashSet<GatewayReceiver>(allGatewayReceivers.size() + 1);
      if (!allGatewayReceivers.isEmpty()) {
        tmp.addAll(allGatewayReceivers);
      }
      tmp.add(recv);
      this.allGatewayReceivers = Collections.unmodifiableSet(tmp);
    }
  }

  public void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue) {
    this.allAsyncEventQueues.add(asyncQueue);
    if(!asyncQueue.isMetaQueue()) {
      this.allVisibleAsyncEventQueues.add(asyncQueue);
    }
    system
        .handleResourceEvent(ResourceEvent.ASYNCEVENTQUEUE_CREATE, asyncQueue);
  }
  
  /**
   * Returns List of GatewaySender (excluding the senders for internal use)
   * 
   * @return  List    List of GatewaySender objects
   */
  public Set<GatewaySender> getGatewaySenders() {
    Set<GatewaySender> tempSet = new HashSet<GatewaySender>();
    for (GatewaySender sender : allGatewaySenders) {
      if (!((AbstractGatewaySender)sender).isForInternalUse()) {
        tempSet.add(sender);
      }
    }
    return tempSet;
  }

  /**
   * Returns List of all GatewaySenders (including the senders for internal use)
   * 
   * @return  List    List of GatewaySender objects
   */
  public Set<GatewaySender> getAllGatewaySenders() {
    return this.allGatewaySenders;
  }

  public GatewaySender getGatewaySender(String Id) {
    for (GatewaySender sender : this.allGatewaySenders) {
      if (sender.getId().equals(Id)) {
        return sender;
      }
    }
    return null;
  }

  public Set<GatewayReceiver> getGatewayReceivers() {
    return this.allGatewayReceivers;
  }

  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return this.allVisibleAsyncEventQueues;
  }
  
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : this.allAsyncEventQueues) {
      if (asyncEventQueue.getId().equals(id)) {
        return asyncEventQueue;
      }
    }
    return null;
  }

  public void removeAsyncEventQueue(AsyncEventQueue asyncQueue) {
    if (isClient()) {
      throw new UnsupportedOperationException(
          "operation is not supported on a client cache");
    }
    // first remove the gateway sender of the queue
    if (asyncQueue instanceof AsyncEventQueueImpl) {
      removeGatewaySender(((AsyncEventQueueImpl)asyncQueue).getSender());
    }
    // using gateway senders lock since async queue uses a gateway sender
    synchronized (allGatewaySendersLock) {
      this.allAsyncEventQueues.remove(asyncQueue);
      this.allVisibleAsyncEventQueues.remove(asyncQueue);
    }
  }
  
  /* Cache API - get the conflict resolver for WAN */
  public GatewayConflictResolver getGatewayConflictResolver() {
    synchronized (this.allGatewayHubsLock) {
      return this.gatewayConflictResolver;
    }
  }
  
  /* Cache API - set the conflict resolver for WAN */
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    synchronized (this.allGatewayHubsLock) {
      this.gatewayConflictResolver = resolver;
    }
  }

  public List getCacheServers() {
    List cacheServersWithoutReceiver = null;
    if (!allCacheServers.isEmpty()) {
    Iterator allCacheServersIterator = allCacheServers.iterator();
    while (allCacheServersIterator.hasNext()) {
      CacheServerImpl cacheServer = (CacheServerImpl) allCacheServersIterator.next();
      // If CacheServer is a GatewayReceiver, don't return as part of CacheServers
      if (!cacheServer.isGatewayReceiver()) {
        if (cacheServersWithoutReceiver == null) {
          cacheServersWithoutReceiver = new ArrayList();
        }
        cacheServersWithoutReceiver.add(cacheServer);
      }
    }
    }
    if (cacheServersWithoutReceiver == null) {
      cacheServersWithoutReceiver = Collections.emptyList();
    }
    return cacheServersWithoutReceiver;
  }

  public List getCacheServersAndGatewayReceiver() {
    return allCacheServers;
  }

  /**
   * notify partitioned regions that this cache requires all of their events
   */
  public void requiresPREvents() {
    synchronized (this.partitionedRegions) {
      for (Iterator it = this.partitionedRegions.iterator(); it.hasNext();) {
        ((PartitionedRegion) it.next()).cacheRequiresNotification();
      }
    }
  }

  /**
   * add a partitioned region to the set of tracked partitioned regions. This is used to notify the regions when this
   * cache requires, or does not require notification of all region/entry events.
   */
  public void addPartitionedRegion(PartitionedRegion r) {
    synchronized (GemFireCacheImpl.class) {
      synchronized (this.partitionedRegions) {
        if (r.isDestroyed()) {
          if (logger.isDebugEnabled()) {
            logger.debug("GemFireCache#addPartitionedRegion did not add destroyed {}", r);
          }
          return;
        }
        if (this.partitionedRegions.add(r)) {
          getCachePerfStats().incPartitionedRegions(1);
        }
      }
    }
  }

  /**
   * Returns a set of all current partitioned regions for test hook.
   */
  public Set<PartitionedRegion> getPartitionedRegions() {
    synchronized (this.partitionedRegions) {
      return new HashSet<PartitionedRegion>(this.partitionedRegions);
    }
  }

  private TreeMap<String, Map<String, PartitionedRegion>> getPRTrees() {
    // prTree will save a sublist of PRs who are under the same root
    TreeMap<String, Map<String, PartitionedRegion>> prTrees = new TreeMap();
    TreeMap<String, PartitionedRegion> prMap = getPartitionedRegionMap();
    boolean hasColocatedRegion = false;
    for (PartitionedRegion pr : prMap.values()) {
      List<PartitionedRegion> childlist = ColocationHelper.getColocatedChildRegions(pr);
      if (childlist != null && childlist.size() > 0) {
        hasColocatedRegion = true;
        break;
      }
    }

    if (hasColocatedRegion) {
      LinkedHashMap<String, PartitionedRegion> orderedPrMap = orderByColocation(prMap);
      prTrees.put("ROOT", orderedPrMap);
    } else {
      for (PartitionedRegion pr : prMap.values()) {
        String rootName = pr.getRoot().getName();
        TreeMap<String, PartitionedRegion> prSubMap = (TreeMap<String, PartitionedRegion>) prTrees.get(rootName);
        if (prSubMap == null) {
          prSubMap = new TreeMap();
          prTrees.put(rootName, prSubMap);
        }
        prSubMap.put(pr.getFullPath(), pr);
      }
    }

    return prTrees;
  }

  private TreeMap<String, PartitionedRegion> getPartitionedRegionMap() {
    TreeMap<String, PartitionedRegion> prMap = new TreeMap();
    for (Map.Entry<String, Region> entry : ((Map<String,Region>)pathToRegion).entrySet()) {
      String regionName = (String) entry.getKey();
      Region region = entry.getValue();
      
      //Don't wait for non partitioned regions
      if(!(region instanceof PartitionedRegion)) {
        continue;
      }
      // Do a getRegion to ensure that we wait for the partitioned region
      //to finish initialization
      try {
        Region pr = getRegion(regionName);
        if (pr instanceof PartitionedRegion) {
          prMap.put(regionName, (PartitionedRegion) pr);
        }
      } catch (CancelException ce) {
        // if some region throws cancel exception during initialization,
        // then no need to shutdownall them gracefully
      }
    }

    return prMap;
  }

  private LinkedHashMap<String, PartitionedRegion> orderByColocation(TreeMap<String, PartitionedRegion> prMap) {
    LinkedHashMap<String, PartitionedRegion> orderedPrMap = new LinkedHashMap();
    for (PartitionedRegion pr : prMap.values()) {
      addColocatedChildRecursively(orderedPrMap, pr);
    }
    return orderedPrMap;
  }

  private void addColocatedChildRecursively(LinkedHashMap<String, PartitionedRegion> prMap, PartitionedRegion pr) {
    for (PartitionedRegion colocatedRegion : ColocationHelper.getColocatedChildRegions(pr)) {
      addColocatedChildRecursively(prMap, colocatedRegion);
    }
    prMap.put(pr.getFullPath(), pr);
  }

  /**
   * check to see if any cache components require notification from a partitioned region. Notification adds to the
   * messaging a PR must do on each put/destroy/invalidate operation and should be kept to a minimum
   *
   * @param r
   *          the partitioned region
   * @return true if the region should deliver all of its events to this cache
   */
  protected boolean requiresNotificationFromPR(PartitionedRegion r) {
    synchronized (GemFireCacheImpl.class) {
      boolean hasSerialSenders = hasSerialSenders(r);
      boolean result = hasSerialSenders;
      if (!result) {
        Iterator allCacheServersIterator = allCacheServers.iterator();
        while (allCacheServersIterator.hasNext()) {
          CacheServerImpl server = (CacheServerImpl) allCacheServersIterator.next();
          if (!server.getNotifyBySubscription()) {
            result = true;
            break;
          }
        }

      }
      return result;
    }
  }

  private boolean hasSerialSenders(PartitionedRegion r) {
    boolean hasSenders = false;
    Set<String> senders = r.getAllGatewaySenderIds();
    for (String sender : senders) {
      GatewaySender gs = this.getGatewaySender(sender);
      if (gs != null && !gs.isParallel()) {
        hasSenders = true;
        break;
      }
    }
    return hasSenders;
  }

  /**
   * remove a partitioned region from the set of tracked instances.
   *
   * @see #addPartitionedRegion(PartitionedRegion)
   */
  public void removePartitionedRegion(PartitionedRegion r) {
    synchronized (this.partitionedRegions) {
      if (this.partitionedRegions.remove(r)) {
        getCachePerfStats().incPartitionedRegions(-1);
      }
    }
  }

  public void setIsServer(boolean isServer) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    stopper.checkCancelInProgress(null);

    this.isServer = isServer;
  }

  public boolean isServer() {
    if (isClient()) {
      return false;
    }
    stopper.checkCancelInProgress(null);

    if (!this.isServer) {
      return (this.allCacheServers.size() > 0);
    } else {
      return true;
    }
  }

  public QueryService getQueryService() {
    if (isClient()) {
      Pool p = getDefaultPool();
      if (p == null) {
        throw new IllegalStateException("Client cache does not have a default pool. Use getQueryService(String poolName) instead.");
      } else {
        return p.getQueryService();
      }
    } else {
      return new DefaultQueryService(this);
    }
  }

  public QueryService getLocalQueryService() {
    return new DefaultQueryService(this);
  }

  /**
   * @return Context jndi context associated with the Cache.
   * @since GemFire 4.0
   */
  public Context getJNDIContext() {
    // if (isClient()) {
    // throw new UnsupportedOperationException("operation is not supported on a client cache");
    // }
    return JNDIInvoker.getJNDIContext();
  }

  /**
   * @return JTA TransactionManager associated with the Cache.
   * @since GemFire 4.0
   */
  public javax.transaction.TransactionManager getJTATransactionManager() {
    // if (isClient()) {
    // throw new UnsupportedOperationException("operation is not supported on a client cache");
    // }
    return JNDIInvoker.getTransactionManager();
  }

  /**
   * return the cq/interest information for a given region name, creating one if it doesn't exist
   */
  public FilterProfile getFilterProfile(String regionName) {
    LocalRegion r = (LocalRegion) getRegion(regionName, true);
    if (r != null) {
      return r.getFilterProfile();
    }
    return null;
  }

  public RegionAttributes getRegionAttributes(String id) {
    return (RegionAttributes) this.namedRegionAttributes.get(id);
  }

  public void setRegionAttributes(String id, RegionAttributes attrs) {
    if (attrs == null) {
      this.namedRegionAttributes.remove(id);
    } else {
      this.namedRegionAttributes.put(id, attrs);
    }
  }

  public Map listRegionAttributes() {
    return Collections.unmodifiableMap(this.namedRegionAttributes);
  }

  private static final ThreadLocal xmlCache = new ThreadLocal();

  /**
   * Returns the cache currently being xml initialized by the thread that calls this method. The result will be null if
   * the thread is not initializing a cache.
   */
  public static GemFireCacheImpl getXmlCache() {
    return (GemFireCacheImpl) xmlCache.get();
  }

  public void loadCacheXml(InputStream stream) throws TimeoutException, CacheWriterException, GatewayException,
      RegionExistsException {
    // make this cache available to callbacks being initialized during xml create
    final Object oldValue = xmlCache.get();
    xmlCache.set(this);
    try {
      CacheXmlParser xml;

      if (xmlParameterizationEnabled) {
        char[] buffer = new char[1024];
        Reader reader = new BufferedReader(new InputStreamReader(stream, "ISO-8859-1"));
        Writer stringWriter = new StringWriter();

        int n = -1;
        while ((n = reader.read(buffer)) != -1) {
          stringWriter.write(buffer, 0, n);
        }

        /** Now replace all replaceable system properties here using <code>PropertyResolver</code> */
        String replacedXmlString = resolver.processUnresolvableString(stringWriter.toString());

        /*
         * Turn the string back into the default encoding so that the XML parser can work correctly in the presence of
         * an "encoding" attribute in the XML prolog.
         */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(baos, "ISO-8859-1");
        writer.write(replacedXmlString);
        writer.flush();

        xml = CacheXmlParser.parse(new ByteArrayInputStream(baos.toByteArray()));
      } else {
        xml = CacheXmlParser.parse(stream);
      }
      xml.create(this);
    } catch (IOException e) {
      throw new CacheXmlException("Input Stream could not be read for system property substitutions.");
    } finally {
      xmlCache.set(oldValue);
    }
  }

  public void readyForEvents() {
    PoolManagerImpl.readyForEvents(this.system, false);
  }

  /**
   * This cache's reliable message queue factory. Should always have an instance of it.
   */
  private ReliableMessageQueueFactory rmqFactory;

  private List<File> backupFiles = Collections.emptyList();

  /**
   * Initializes the reliable message queue. Needs to be called at cache creation
   *
   * @throws IllegalStateException
   *           if the factory is in use
   */
  private void initReliableMessageQueueFactory() {
    synchronized (GemFireCacheImpl.class) {
      if (this.rmqFactory != null) {
        this.rmqFactory.close(false);
      }
      this.rmqFactory = new ReliableMessageQueueFactoryImpl();
    }
  }

  /**
   * Returns this cache's ReliableMessageQueueFactory.
   *
   * @since GemFire 5.0
   */
  public ReliableMessageQueueFactory getReliableMessageQueueFactory() {
    return this.rmqFactory;
  }

  public InternalResourceManager getResourceManager() {
    return getResourceManager(true);
  }

  public InternalResourceManager getResourceManager(boolean checkCancellationInProgress) {
    if (checkCancellationInProgress) {
      stopper.checkCancelInProgress(null);
    }
    return this.resourceManager;
  }

  public void setBackupFiles(List<File> backups) {
    this.backupFiles = backups;
  }

  public List<File> getBackupFiles() {
    return Collections.unmodifiableList(this.backupFiles);
  }

  public BackupManager startBackup(InternalDistributedMember sender) 
  throws IOException {
    BackupManager manager = new BackupManager(sender, this);
    if (!this.backupManager.compareAndSet(null, manager)) {
      // TODO prpersist internationalize this
      throw new IOException("Backup already in progress");
    }
    manager.start();
    return manager;
  }

  public void clearBackupManager() {
    this.backupManager.set(null);
  }

  public BackupManager getBackupManager() {
    return this.backupManager.get();
  }

  // //////////////////// Inner Classes //////////////////////

  // TODO make this a simple int guarded by riWaiters and get rid of the double-check
  private final AtomicInteger registerInterestsInProgress = new AtomicInteger();

  private final ArrayList<SimpleWaiter> riWaiters = new ArrayList<SimpleWaiter>();

  private TypeRegistry pdxRegistry; // never changes but is currently only
                                    // initialized in constructor by unit tests

  /**
   * update stats for completion of a registerInterest operation
   */
  public void registerInterestCompleted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    // GemFireCache.this.getCancelCriterion().checkCancelInProgress(null);
    if (GemFireCacheImpl.this.isClosing) {
      return; // just get out, all of the SimpleWaiters will die of their own accord
    }
    int cv = registerInterestsInProgress.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestCompleted: new value = {}", cv);
    }
    if (cv == 0) {
      synchronized (riWaiters) {
        // TODO double-check
        cv = registerInterestsInProgress.get();
        if (cv == 0) { // all clear
          if (logger.isDebugEnabled()) {
            logger.debug("registerInterestCompleted: Signalling end of register-interest");
          }
          Iterator it = riWaiters.iterator();
          while (it.hasNext()) {
            SimpleWaiter sw = (SimpleWaiter) it.next();
            sw.doNotify();
          }
          riWaiters.clear();
        } // all clear
      } // synchronized
    }
  }

  public void registerInterestStarted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    // GemFireCache.this.getCancelCriterion().checkCancelInProgress(null);
    int newVal = registerInterestsInProgress.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestsStarted: new count = {}", newVal);
    }
  }

  /**
   * update stats for initiation of a registerInterest operation
   */
  /**
   * Blocks until no register interests are in progress.
   */
  public void waitForRegisterInterestsInProgress() {
    // In *this* particular context, let the caller know that
    // his cache has been cancelled. doWait below would do that as
    // well, so this is just an early out.
    GemFireCacheImpl.this.getCancelCriterion().checkCancelInProgress(null);

    int count = registerInterestsInProgress.get();
    SimpleWaiter sw = null;
    if (count > 0) {
      synchronized (riWaiters) {
        // TODO double-check
        count = registerInterestsInProgress.get();
        if (count > 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("waitForRegisterInterestsInProgress: count ={}", count);
          }
          sw = new SimpleWaiter();
          riWaiters.add(sw);
        }
      } // synchronized
      if (sw != null) {
        sw.doWait();
      }
    }
  }

  /**
   * Wait for given sender queue to flush for given timeout.
   * 
   * @param id
   *          ID of GatewaySender or AsyncEventQueue
   * @param isAsyncListener
   *          true if this is for an AsyncEventQueue and false if for a
   *          GatewaySender
   * @param maxWaitTime
   *          maximum time to wait in seconds; zero or -ve means infinite wait
   * 
   * @return zero if maxWaitTime was not breached, -1 if queue could not be
   *         found or is closed, and elapsed time if timeout was breached
   */
  public int waitForSenderQueueFlush(String id, boolean isAsyncListener,
      int maxWaitTime) {
    getCancelCriterion().checkCancelInProgress(null);
    AbstractGatewaySender gatewaySender = null;
    if (isAsyncListener) {
      AsyncEventQueueImpl asyncQueue = (AsyncEventQueueImpl)
          getAsyncEventQueue(id);
      if (asyncQueue != null) {
        gatewaySender = (AbstractGatewaySender) asyncQueue.getSender();
      }
    }
    else {
      gatewaySender = (AbstractGatewaySender)getGatewaySender(id);
    }
    RegionQueue rq;
    final long startTime = System.currentTimeMillis();
    long elapsedTime;
    if (maxWaitTime <= 0) {
      maxWaitTime = Integer.MAX_VALUE;
    }
    while (gatewaySender != null && gatewaySender.isRunning()
        && (rq = gatewaySender.getQueue()) != null) {
      if (rq.size() == 0) {
        // return zero since it was not a timeout
        return 0;
      }
      try {
        Thread.sleep(500);
        getCancelCriterion().checkCancelInProgress(null);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        getCancelCriterion().checkCancelInProgress(ie);
      }
      // clear interrupted flag before retry
      Thread.interrupted();
      elapsedTime = System.currentTimeMillis() - startTime;
      if (elapsedTime >= (maxWaitTime * 1000L)) {
        // return elapsed time
        return (int)(elapsedTime / 1000L);
      }
    }
    return -1;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void setQueryMonitorRequiredForResourceManager(boolean required) {
    QUERY_MONITOR_REQUIRED_FOR_RESOURCE_MANAGER = required;
  }
  
  public boolean isQueryMonitorDisabledForLowMemory() {
    return QUERY_MONITOR_DISABLED_FOR_LOW_MEM;
  }
  
  /**
   * Returns the QueryMonitor instance based on system property MAX_QUERY_EXECUTION_TIME.
   * @since GemFire 6.0
   */
  public QueryMonitor getQueryMonitor() {
    //Check to see if monitor is required if ResourceManager critical heap percentage is set
    //@see org.apache.geode.cache.control.ResourceManager#setCriticalHeapPercentage(int)
    //or whether we override it with the system variable;
    boolean monitorRequired = !QUERY_MONITOR_DISABLED_FOR_LOW_MEM && QUERY_MONITOR_REQUIRED_FOR_RESOURCE_MANAGER;
    // Added for DUnit test purpose, which turns-on and off the this.TEST_MAX_QUERY_EXECUTION_TIME.
    if (!(this.MAX_QUERY_EXECUTION_TIME > 0 || this.TEST_MAX_QUERY_EXECUTION_TIME > 0 || monitorRequired)) {
      // if this.TEST_MAX_QUERY_EXECUTION_TIME is set, send the QueryMonitor.
      // Else send null, so that the QueryMonitor is turned-off.
      return null;
    }

    // Return the QueryMonitor service if MAX_QUERY_EXECUTION_TIME is set or it is required by the ResourceManager and not overriden by system property.
    if ((this.MAX_QUERY_EXECUTION_TIME > 0 || this.TEST_MAX_QUERY_EXECUTION_TIME > 0 || monitorRequired) && this.queryMonitor == null) {
      synchronized (queryMonitorLock) {
        if (this.queryMonitor == null) {
          int maxTime = MAX_QUERY_EXECUTION_TIME > TEST_MAX_QUERY_EXECUTION_TIME ? MAX_QUERY_EXECUTION_TIME
              : TEST_MAX_QUERY_EXECUTION_TIME;
          
          if (monitorRequired && maxTime < 0) {
            //this means that the resource manager is being used and we need to monitor query memory usage
            //If no max execution time has been set, then we will default to five hours
            maxTime = FIVE_HOURS;
          }

         
          this.queryMonitor = new QueryMonitor(maxTime);
          final LoggingThreadGroup group = LoggingThreadGroup.createThreadGroup("QueryMonitor Thread Group", logger);
          Thread qmThread = new Thread(group, this.queryMonitor, "QueryMonitor Thread");
          qmThread.setDaemon(true);
          qmThread.start();
          if (logger.isDebugEnabled()) {
            logger.debug("QueryMonitor thread started.");
          }
        }
      }
    }
    return this.queryMonitor;
  }

  /**
   * Simple class to allow waiters for register interest. Has at most one thread that ever calls wait.
   *
   * @since GemFire 5.7
   */
  private class SimpleWaiter {
    private boolean notified = false;

    SimpleWaiter() {
    }

    public void doWait() {
      synchronized (this) {
        while (!this.notified) {
          GemFireCacheImpl.this.getCancelCriterion().checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            this.wait(1000);
          } catch (InterruptedException ex) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    }

    public void doNotify() {
      synchronized (this) {
        this.notified = true;
        this.notifyAll();
      }
    }
  }

  private void sendAddCacheServerProfileMessage() {
    DM dm = this.getDistributedSystem().getDistributionManager();
    Set otherMembers = dm.getOtherDistributionManagerIds();
    AddCacheServerProfileMessage msg = new AddCacheServerProfileMessage();
    msg.operateOnLocalCache(this);
    if (!otherMembers.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending add cache server profile message to other members.");
      }
      ReplyProcessor21 rp = new ReplyProcessor21(dm, otherMembers);
      msg.setRecipients(otherMembers);
      msg.processorId = rp.getProcessorId();
      dm.putOutgoing(msg);

      // Wait for replies.
      try {
        rp.waitForReplies();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public TXManagerImpl getTxManager() {
    return this.txMgr;
  }

  /**
   * @since GemFire 6.5
   */
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut atts) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    } else {
      return new RegionFactoryImpl<K, V>(this, atts);
    }
  }

  /**
   * @since GemFire 6.5
   */
  public <K, V> RegionFactory<K, V> createRegionFactory() {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    return new RegionFactoryImpl<K, V>(this);
  }

  /**
   * @since GemFire 6.5
   */
  public <K, V> RegionFactory<K, V> createRegionFactory(String regionAttributesId) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    return new RegionFactoryImpl<K, V>(this, regionAttributesId);
  }

  /**
   * @since GemFire 6.5
   */
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionAttributes<K, V> regionAttributes) {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
    return new RegionFactoryImpl<K, V>(this, regionAttributes);
  }

  /**
   * @since GemFire 6.5
   */
  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(ClientRegionShortcut atts) {
    return new ClientRegionFactoryImpl<K, V>(this, atts);
  }

  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(String refid) {
    return new ClientRegionFactoryImpl<K, V>(this, refid);
  }

  /**
   * @since GemFire 6.5
   */
  public QueryService getQueryService(String poolName) {
    Pool p = PoolManager.find(poolName);
    if (p == null) {
      throw new IllegalStateException("Could not find a pool named " + poolName);
    } else {
      return p.getQueryService();
    }
  }

  public RegionService createAuthenticatedView(Properties properties) {
    Pool pool = getDefaultPool();
    if (pool == null) {
      throw new IllegalStateException("This cache does not have a default pool");
    }
    return createAuthenticatedCacheView(pool, properties);
  }

  public RegionService createAuthenticatedView(Properties properties, String poolName) {
    Pool pool = PoolManager.find(poolName);
    if (pool == null) {
      throw new IllegalStateException("Pool " + poolName + " does not exist");
    }
    return createAuthenticatedCacheView(pool, properties);
  }

  public RegionService createAuthenticatedCacheView(Pool pool, Properties properties) {
    if (pool.getMultiuserAuthentication()) {
      return ((PoolImpl) pool).createAuthenticatedCacheView(properties);
    } else {
      throw new IllegalStateException("The pool " + pool.getName() + " did not have multiuser-authentication set to true");
    }
  }

  public static void initializeRegionShortcuts(Cache c) {
    for (RegionShortcut pra : RegionShortcut.values()) {
      switch (pra) {
      case PARTITION: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_REDUNDANT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_PERSISTENT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_REDUNDANT_PERSISTENT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_REDUNDANT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_PERSISTENT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_REDUNDANT_PERSISTENT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_REDUNDANT_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE_PERSISTENT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE_PERSISTENT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.REPLICATE);
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.LOCAL);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_PERSISTENT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setScope(Scope.LOCAL);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.LOCAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setScope(Scope.LOCAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_PERSISTENT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setScope(Scope.LOCAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_PROXY: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setLocalMaxMemory(0);
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PARTITION_PROXY_REDUNDANT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setLocalMaxMemory(0);
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case REPLICATE_PROXY: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.EMPTY);
        af.setScope(Scope.DISTRIBUTED_ACK);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      default:
        throw new IllegalStateException("unhandled enum " + pra);
      }
    }
  }

  public static void initializeClientRegionShortcuts(Cache c) {
    for (ClientRegionShortcut pra : ClientRegionShortcut.values()) {
      switch (pra) {
      case LOCAL: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_PERSISTENT: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case LOCAL_PERSISTENT_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        c.setRegionAttributes(pra.toString(), af.create());
        break;
      }
      case PROXY: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.EMPTY);
        UserSpecifiedRegionAttributes ra = (UserSpecifiedRegionAttributes) af.create();
        ra.requiresPoolName = true;
        c.setRegionAttributes(pra.toString(), ra);
        break;
      }
      case CACHING_PROXY: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        UserSpecifiedRegionAttributes ra = (UserSpecifiedRegionAttributes) af.create();
        ra.requiresPoolName = true;
        c.setRegionAttributes(pra.toString(), ra);
        break;
      }
      case CACHING_PROXY_HEAP_LRU: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
        UserSpecifiedRegionAttributes ra = (UserSpecifiedRegionAttributes) af.create();
        ra.requiresPoolName = true;
        c.setRegionAttributes(pra.toString(), ra);
        break;
      }
      case CACHING_PROXY_OVERFLOW: {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.NORMAL);
        af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
        UserSpecifiedRegionAttributes ra = (UserSpecifiedRegionAttributes) af.create();
        ra.requiresPoolName = true;
        c.setRegionAttributes(pra.toString(), ra);
        break;
      }
      default:
        throw new IllegalStateException("unhandled enum " + pra);
      }
    }
  }

  public void beginDestroy(String path, DistributedRegion region) {
    this.regionsInDestroy.putIfAbsent(path, region);
  }

  public void endDestroy(String path, DistributedRegion region) {
    this.regionsInDestroy.remove(path, region);
  }

  public DistributedRegion getRegionInDestroy(String path) {
    return this.regionsInDestroy.get(path);
  }

  public TombstoneService getTombstoneService() {
    return this.tombstoneService;
  }

  public TypeRegistry getPdxRegistry() {
    return this.pdxRegistry;
  }

  public boolean getPdxReadSerialized() {
    return this.cacheConfig.pdxReadSerialized;
  }

  public PdxSerializer getPdxSerializer() {
    return this.cacheConfig.pdxSerializer;
  }

  public String getPdxDiskStore() {
    return this.cacheConfig.pdxDiskStore;
  }

  public boolean getPdxPersistent() {
    return this.cacheConfig.pdxPersistent;
  }

  public boolean getPdxIgnoreUnreadFields() {
    return this.cacheConfig.pdxIgnoreUnreadFields;
  }

  /**
   * Returns true if any of the GemFire services prefers PdxInstance. And application has not requested getObject() on
   * the PdxInstance.
   *
   */
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    if ((getPdxReadSerialized() || DefaultQuery.getPdxReadSerialized()) && PdxInstanceImpl.getPdxReadSerialized()) {
      return true;
    }
    return false;
  }

  public CacheConfig getCacheConfig() {
    return this.cacheConfig;
  }

  public DM getDistributionManager() {
    return this.dm;
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

  public DistributionAdvisor getDistributionAdvisor() {
    return getResourceAdvisor();
  }

  public ResourceAdvisor getResourceAdvisor() {
    return resourceAdvisor;
  }

  public Profile getProfile() {
    return resourceAdvisor.createProfile();
  }

  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  public InternalDistributedSystem getSystem() {
    return this.system;
  }

  public String getFullPath() {
    return "ResourceManager";
  }

  public void fillInProfile(Profile profile) {
    resourceManager.fillInProfile(profile);
  }

  public int getSerialNumber() {
    return this.serialNumber;
  }

  public TXEntryStateFactory getTXEntryStateFactory() {
    return this.txEntryStateFactory;
  }

  // test hook
  public void setPdxSerializer(PdxSerializer v) {
    this.cacheConfig.setPdxSerializer(v);
    basicSetPdxSerializer(v);
  }

  private void basicSetPdxSerializer(PdxSerializer v) {
    TypeRegistry.setPdxSerializer(v);
    if (v instanceof ReflectionBasedAutoSerializer) {
      AutoSerializableManager asm = (AutoSerializableManager) ((ReflectionBasedAutoSerializer) v).getManager();
      if (asm != null) {
        asm.setRegionService(this);
      }
    }
  }

  // test hook
  public void setReadSerialized(boolean v) {
    this.cacheConfig.setPdxReadSerialized(v);
  }

  public void setDeclarativeCacheConfig(CacheConfig cacheConfig) {
    this.cacheConfig.setDeclarativeConfig(cacheConfig);
    basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
  }

  /**
   * Add to the map of declarable properties.  Any properties that exactly match existing
   * properties for a class in the list will be discarded (no duplicate Properties allowed).
   * 
   * @param mapOfNewDeclarableProps Map of the declarable properties to add
   */
  public void addDeclarableProperties(final Map<Declarable, Properties> mapOfNewDeclarableProps) {
    synchronized (this.declarablePropertiesMap) {
      for (Map.Entry<Declarable, Properties> newEntry : mapOfNewDeclarableProps.entrySet()) {
        // Find and remove a Declarable from the map if an "equal" version is already stored
        Class clazz = newEntry.getKey().getClass();

        Object matchingDeclarable = null;
        for (Map.Entry<Declarable, Properties> oldEntry : this.declarablePropertiesMap.entrySet()) {
          if (clazz.getName().equals(oldEntry.getKey().getClass().getName()) && (newEntry.getValue().equals(oldEntry.getValue()) ||
              ((newEntry.getKey() instanceof Identifiable) && (((Identifiable) oldEntry.getKey()).getId().equals(((Identifiable) newEntry.getKey()).getId()))))) {
            matchingDeclarable = oldEntry.getKey();
            break;
          }
        }
        if (matchingDeclarable != null) {
          this.declarablePropertiesMap.remove(matchingDeclarable);
        }

        // Now add the new/replacement properties to the map
        this.declarablePropertiesMap.put(newEntry.getKey(), newEntry.getValue());
      }
    }
  }

  public static boolean isXmlParameterizationEnabled() {
    return xmlParameterizationEnabled;
  }

  public static void setXmlParameterizationEnabled(boolean isXmlParameterizationEnabled) {
    xmlParameterizationEnabled = isXmlParameterizationEnabled;
  }
    
  private Declarable initializer;
  private Properties initializerProps;

  /**
   * A factory for temporary result sets than can overflow to disk.
   */
  private TemporaryResultSetFactory resultSetFactory;

  public Declarable getInitializer() {
    return this.initializer;
  }

  public Properties getInitializerProps() {
    return this.initializerProps;
  }

  public void setInitializer(Declarable initializer, Properties initializerProps) {
    this.initializer = initializer;
    this.initializerProps = initializerProps;
  }

  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    return PdxInstanceFactoryImpl.newCreator(className, true);
  }

  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean b) {
    return PdxInstanceFactoryImpl.newCreator(className, b);
  }

  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    return PdxInstanceFactoryImpl.createPdxEnum(className, enumName, enumOrdinal, this);
  }
  
  public JmxManagerAdvisor getJmxManagerAdvisor() {
    return this.jmxAdvisor;
  }
  
  public CacheSnapshotService getSnapshotService() {
    return new CacheSnapshotServiceImpl(this);
  }
  
  private void startColocatedJmxManagerLocator() {
    InternalLocator loc = InternalLocator.getLocator();
    if (loc != null) {
      loc.startJmxManagerLocationService(this);
    }
  }
  
  public TemporaryResultSetFactory getResultSetFactory() {
    return this.resultSetFactory;
  }

  public MemoryAllocator getOffHeapStore() {
    return this.getSystem().getOffHeapStore();
  }

  public DiskStoreMonitor getDiskStoreMonitor() {
    return diskMonitor;
  }
  
  /**
   * @see Extensible#getExtensionPoint()
   * @since GemFire 8.1
   */
  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return extensionPoint;
  }

  public static int getClientFunctionTimeout() {
    return clientFunctionTimeout;
  }
  
  public CqService getCqService() {
    return this.cqService;
  }
}
