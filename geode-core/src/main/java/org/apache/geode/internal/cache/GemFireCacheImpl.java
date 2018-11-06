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

import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.getAnyInstance;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
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
import java.net.MalformedURLException;
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
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.transaction.TransactionManager;

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
import org.apache.geode.SerializationException;
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
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientRegionFactoryImpl;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.InternalQueryService;
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
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.internal.cache.execute.util.FindRestEnabledServersFunction;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.snapshot.CacheSnapshotServiceImpl;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAdvisor;
import org.apache.geode.internal.cache.wan.GatewaySenderQueueEntrySynchronizationListener;
import org.apache.geode.internal.cache.wan.WANServiceProvider;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheXmlPropertyResolver;
import org.apache.geode.internal.cache.xmlcache.PropertyResolver;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.jta.TransactionManagerImpl;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.sequencelog.SequenceLoggerImpl;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.internal.util.BlobHelper;
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
import org.apache.geode.pdx.internal.InternalPdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.redis.GeodeRedisServer;

// TODO: somebody Come up with more reasonable values for {@link #DEFAULT_LOCK_TIMEOUT}, etc.
/**
 * GemFire's implementation of a distributed {@link Cache}.
 */
@SuppressWarnings("deprecation")
public class GemFireCacheImpl implements InternalCache, InternalClientCache, HasCachePerfStats,
    DistributionAdvisee, CacheTime {
  private static final Logger logger = LogService.getLogger();

  /** The default number of seconds to wait for a distributed lock */
  public static final int DEFAULT_LOCK_TIMEOUT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultLockTimeout", 60);

  /**
   * The default duration (in seconds) of a lease on a distributed lock
   */
  public static final int DEFAULT_LOCK_LEASE =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultLockLease", 120);

  /** The default "copy on read" attribute value */
  public static final boolean DEFAULT_COPY_ON_READ = false;

  /**
   * The default amount of time to wait for a {@code netSearch} to complete
   */
  public static final int DEFAULT_SEARCH_TIMEOUT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.defaultSearchTimeout", 300);

  /**
   * The {@code CacheLifecycleListener} s that have been registered in this VM
   */
  private static final Set<CacheLifecycleListener> cacheLifecycleListeners = new HashSet<>();

  /**
   * Define gemfire.Cache.ASYNC_EVENT_LISTENERS=true to invoke event listeners in the background
   */
  private static final boolean ASYNC_EVENT_LISTENERS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Cache.ASYNC_EVENT_LISTENERS");

  /**
   * Name of the default pool.
   */
  public static final String DEFAULT_POOL_NAME = "DEFAULT";


  /**
   * The number of threads that the QueryMonitor will use to mark queries as cancelled
   * (see QueryMonitor class for reasons why a query might be cancelled).
   * That processing is very efficient, so we don't foresee needing to raise this above 1.
   */
  private static final int QUERY_MONITOR_THREAD_POOL_SIZE = 1;

  /**
   * If true then when a delta is applied the size of the entry value will be recalculated. If false
   * (the default) then the size of the entry value is unchanged by a delta application. Not a final
   * so that tests can change this value.
   *
   * TODO: move or static or encapsulate with interface methods
   *
   * @since GemFire h****** 6.1.2.9
   */
  static boolean DELTAS_RECALCULATE_SIZE =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DELTAS_RECALCULATE_SIZE");

  private static final int EVENT_QUEUE_LIMIT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.EVENT_QUEUE_LIMIT", 4096);

  static final int EVENT_THREAD_LIMIT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.EVENT_THREAD_LIMIT", 16);

  /**
   * System property to limit the max query-execution time. By default its turned off (-1), the time
   * is set in milliseconds.
   */
  public static int MAX_QUERY_EXECUTION_TIME =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "Cache.MAX_QUERY_EXECUTION_TIME", -1);

  /**
   * System property to disable query monitor even if resource manager is in use
   */
  private final boolean queryMonitorDisabledForLowMem = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");

  /**
   * Property set to true if resource manager heap percentage is set and query monitor is required
   */
  private static boolean queryMonitorRequiredForResourceManager = false;

  /** time in milliseconds */
  private static final int FIVE_HOURS = 5 * 60 * 60 * 1000;

  private static final Pattern DOUBLE_BACKSLASH = Pattern.compile("\\\\");

  private volatile ConfigurationResponse configurationResponse;

  private final InternalDistributedSystem system;

  private final DistributionManager dm;

  private final Map<String, InternalRegion> rootRegions;

  /**
   * True if this cache is being created by a ClientCacheFactory.
   */
  private final boolean isClient;

  private PoolFactory poolFactory;

  /**
   * It is not final to allow cache.xml parsing to set it.
   */
  private Pool defaultPool;

  private final ConcurrentMap<String, InternalRegion> pathToRegion = new ConcurrentHashMap<>();

  private volatile boolean isInitialized;

  volatile boolean isClosing = false; // used in Stopper inner class

  /** Amount of time (in seconds) to wait for a distributed lock */
  private int lockTimeout = DEFAULT_LOCK_TIMEOUT;

  /** Amount of time a lease of a distributed lock lasts */
  private int lockLease = DEFAULT_LOCK_LEASE;

  /** Amount of time to wait for a {@code netSearch} to complete */
  private int searchTimeout = DEFAULT_SEARCH_TIMEOUT;

  private final CachePerfStats cachePerfStats;

  /** Date on which this instances was created */
  private final Date creationDate;

  /** thread pool for event dispatching */
  private final ExecutorService eventThreadPool;

  /**
   * the list of all cache servers. CopyOnWriteArrayList is used to allow concurrent add, remove and
   * retrieval operations. It is assumed that the traversal operations on cache servers list vastly
   * outnumber the mutative operations such as add, remove.
   */
  private final List<CacheServerImpl> allCacheServers = new CopyOnWriteArrayList<>();

  /**
   * Controls updates to the list of all gateway senders
   *
   * @see #allGatewaySenders
   */
  private final Object allGatewaySendersLock = new Object();

  /**
   * the set of all gateway senders. It may be fetched safely (for enumeration), but updates must by
   * synchronized via {@link #allGatewaySendersLock}
   */
  private volatile Set<GatewaySender> allGatewaySenders = Collections.emptySet();

  /**
   * The list of all async event queues added to the cache. CopyOnWriteArrayList is used to allow
   * concurrent add, remove and retrieval operations.
   */
  private final Set<AsyncEventQueue> allVisibleAsyncEventQueues = new CopyOnWriteArraySet<>();

  /**
   * The list of all async event queues added to the cache. CopyOnWriteArrayList is used to allow
   * concurrent add, remove and retrieval operations.
   */
  private final Set<AsyncEventQueue> allAsyncEventQueues = new CopyOnWriteArraySet<>();

  /**
   * Controls updates to the list of all gateway receivers
   *
   * @see #allGatewayReceivers
   */
  private final Object allGatewayReceiversLock = new Object();

  /**
   * the list of all gateway Receivers. It may be fetched safely (for enumeration), but updates must
   * by synchronized via {@link #allGatewayReceiversLock}
   */
  private volatile Set<GatewayReceiver> allGatewayReceivers = Collections.emptySet();

  /**
   * PartitionedRegion instances (for required-events notification
   */
  private final Set<PartitionedRegion> partitionedRegions = new HashSet<>();

  /**
   * Fix for 42051 This is a map of regions that are in the process of being destroyed. We could
   * potentially leave the regions in the pathToRegion map, but that would entail too many changes
   * at this point in the release. We need to know which regions are being destroyed so that a
   * profile exchange can get the persistent id of the destroying region and know not to persist
   * that ID if it receives it as part of the persistent view.
   */
  private final ConcurrentMap<String, DistributedRegion> regionsInDestroy =
      new ConcurrentHashMap<>();

  private final Object allGatewayHubsLock = new Object();

  /**
   * conflict resolver for WAN, if any
   *
   * GuardedBy {@link #allGatewayHubsLock}
   */
  private GatewayConflictResolver gatewayConflictResolver;

  /** Is this is "server" cache? */
  private boolean isServer = false;

  /** transaction manager for this cache */
  private final TXManagerImpl transactionManager;

  private RestAgent restAgent;

  private boolean isRESTServiceRunning = false;

  /** Copy on Read feature for all read operations e.g. get */
  private volatile boolean copyOnRead = DEFAULT_COPY_ON_READ;

  /** The named region attributes registered with this cache. */
  private final Map<String, RegionAttributes<?, ?>> namedRegionAttributes =
      Collections.synchronizedMap(new HashMap<>());

  /**
   * if this cache was forced to close due to a forced-disconnect, we retain a
   * ForcedDisconnectException that can be used as the cause
   */
  private boolean forcedDisconnect;

  /**
   * if this cache was forced to close due to a forced-disconnect or system failure, this keeps
   * track of the reason
   */
  volatile Throwable disconnectCause; // used in Stopper inner class

  /** context where this cache was created -- for debugging, really... */
  private Exception creationStack = null;

  /**
   * a system timer task for cleaning up old bridge thread event entries
   */
  private final EventTrackerExpiryTask recordedEventSweeper;

  private final TombstoneService tombstoneService;

  /**
   * DistributedLockService for PartitionedRegions. Remains null until the first PartitionedRegion
   * is created. Destroyed by GemFireCache when closing the cache. Protected by synchronization on
   * this GemFireCache.
   *
   * GuardedBy prLockServiceLock
   */
  private DistributedLockService prLockService;

  /**
   * lock used to access prLockService
   */
  private final Object prLockServiceLock = new Object();

  /**
   * DistributedLockService for GatewaySenders. Remains null until the first GatewaySender is
   * created. Destroyed by GemFireCache when closing the cache.
   *
   * GuardedBy gatewayLockServiceLock
   */
  private volatile DistributedLockService gatewayLockService;

  /**
   * Lock used to access gatewayLockService
   */
  private final Object gatewayLockServiceLock = new Object();

  private final InternalResourceManager resourceManager;

  private final BackupService backupService;

  private HeapEvictor heapEvictor = null;

  private OffHeapEvictor offHeapEvictor = null;

  private final Object heapEvictorLock = new Object();

  private final Object offHeapEvictorLock = new Object();

  private ResourceEventsListener resourceEventsListener;

  /**
   * Enabled when CacheExistsException issues arise in debugging
   *
   * @see #creationStack
   */
  private static final boolean DEBUG_CREATION_STACK = false;

  private volatile QueryMonitor queryMonitor;

  private final Object queryMonitorLock = new Object();

  private final PersistentMemberManager persistentMemberManager;

  private ClientMetadataService clientMetadataService = null;

  private final Object clientMetaDatServiceLock = new Object();

  private final AtomicBoolean isShutDownAll = new AtomicBoolean();
  private final CountDownLatch shutDownAllFinished = new CountDownLatch(1);

  private final ResourceAdvisor resourceAdvisor;
  private final JmxManagerAdvisor jmxAdvisor;

  private final int serialNumber;

  private final TXEntryStateFactory txEntryStateFactory;

  private final CacheConfig cacheConfig;

  private final DiskStoreMonitor diskMonitor;

  /**
   * Stores the properties used to initialize declarables.
   */
  private final Map<Declarable, Properties> declarablePropertiesMap = new ConcurrentHashMap<>();

  /** {@link PropertyResolver} to resolve ${} type property strings */
  private final PropertyResolver resolver;

  private static final boolean XML_PARAMETERIZATION_ENABLED =
      !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "xml.parameterization.disabled");

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
   *
   * @since GemFire 8.1
   */
  private final SimpleExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<>(this, this);

  private final CqService cqService;

  private final Set<RegionListener> regionListeners = new ConcurrentHashSet<>();

  private final Map<Class<? extends CacheService>, CacheService> services = new HashMap<>();

  private final SecurityService securityService;

  private final Set<RegionEntrySynchronizationListener> synchronizationListeners =
      new ConcurrentHashSet<>();

  private final ClusterConfigurationLoader ccLoader = new ClusterConfigurationLoader();

  static {
    // this works around jdk bug 6427854, reported in ticket #44434
    String propertyName = "sun.nio.ch.bugLevel";
    String value = System.getProperty(propertyName);
    if (value == null) {
      System.setProperty(propertyName, "");
    }
  }

  /**
   * Invokes mlockall(). Locks all pages mapped into the address space of the calling process. This
   * includes the pages of the code, data and stack segment, as well as shared libraries, user space
   * kernel data, shared memory, and memory-mapped files. All mapped pages are guaranteed to be
   * resident in RAM when the call returns successfully; the pages are guaranteed to stay in RAM
   * until later unlocked.
   *
   * @param flags MCL_CURRENT 1 - Lock all pages which are currently mapped into the address space
   *        of the process.
   *
   *        MCL_FUTURE 2 - Lock all pages which will become mapped into the address space of the
   *        process in the future. These could be for instance new pages required by a growing heap
   *        and stack as well as new memory mapped files or shared memory regions.
   *
   * @return 0 if success, non-zero if error and errno set
   */
  private static native int mlockall(int flags);

  public static void lockMemory() {
    try {
      Native.register(Platform.C_LIBRARY_NAME);
      int result = mlockall(1);
      if (result == 0) {
        return;
      }
    } catch (Throwable t) {
      throw new IllegalStateException("Error trying to lock memory", t);
    }

    int lastError = Native.getLastError();
    String message = "mlockall failed: " + lastError;
    if (lastError == 1 || lastError == 12) { // EPERM || ENOMEM
      message = "Unable to lock memory due to insufficient free space or privileges.  "
          + "Please check the RLIMIT_MEMLOCK soft resource limit (ulimit -l) and "
          + "increase the available memory if needed";
    }
    throw new IllegalStateException(message);
  }

  /**
   * This is for debugging cache-open issues (esp. {@link CacheExistsException})
   */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GemFireCache[");
    sb.append("id = ").append(System.identityHashCode(this));
    sb.append("; isClosing = ").append(this.isClosing);
    sb.append("; isShutDownAll = ").append(isCacheAtShutdownAll());
    sb.append("; created = ").append(this.creationDate);
    sb.append("; server = ").append(this.isServer);
    sb.append("; copyOnRead = ").append(this.copyOnRead);
    sb.append("; lockLease = ").append(this.lockLease);
    sb.append("; lockTimeout = ").append(this.lockTimeout);
    if (this.creationStack != null) {
      // TODO: eliminate anonymous inner class and maybe move this to ExceptionUtils
      sb.append(System.lineSeparator()).append("Creation context:").append(System.lineSeparator());
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

  /** Map of Futures used to track Regions that are being reinitialized */
  private final ConcurrentMap reinitializingRegions = new ConcurrentHashMap();

  /**
   * Returns the last created instance of GemFireCache
   *
   * @deprecated use DM.getCache instead
   */
  @Deprecated
  public static GemFireCacheImpl getInstance() {
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system == null) {
      return null;
    }
    GemFireCacheImpl cache = (GemFireCacheImpl) system.getCache();
    if (cache == null) {
      return null;
    }

    if (cache.isClosing) {
      return null;
    }

    return cache;

  }

  /**
   * Returns an existing instance. If a cache does not exist throws a cache closed exception.
   *
   * @return the existing cache
   * @throws CacheClosedException if an existing cache can not be found.
   * @deprecated use DM.getExistingCache instead.
   */
  @Deprecated
  public static GemFireCacheImpl getExisting() {
    final GemFireCacheImpl result = getInstance();
    if (result != null && !result.isClosing) {
      return result;
    }
    if (result != null) {
      throw result.getCacheClosedException(
          "The cache has been closed.");
    }
    throw new CacheClosedException(
        "A cache has not yet been created.");
  }

  /**
   * Returns an existing instance. If a cache does not exist throws an exception.
   *
   * @param reason the reason an existing cache is being requested.
   * @return the existing cache
   * @throws CacheClosedException if an existing cache can not be found.
   * @deprecated use DM.getExistingCache instead.
   */
  @Deprecated
  public static GemFireCacheImpl getExisting(String reason) {
    GemFireCacheImpl result = getInstance();
    if (result == null) {
      throw new CacheClosedException(reason);
    }
    return result;
  }

  /**
   * Pdx is allowed to obtain the cache even while it is being closed
   *
   * @deprecated Rather than fishing for a cache with this static method, use a cache that is passed
   *             in to your method.
   */
  public static GemFireCacheImpl getForPdx(String reason) {

    InternalDistributedSystem system = getAnyInstance();
    if (system == null) {
      throw new CacheClosedException(reason);
    }
    GemFireCacheImpl cache = (GemFireCacheImpl) system.getCache();
    if (cache == null) {
      throw new CacheClosedException(reason);
    }

    return cache;
  }

  public static GemFireCacheImpl createClient(InternalDistributedSystem system, PoolFactory pf,
      CacheConfig cacheConfig) {
    return basicCreate(system, true, cacheConfig, pf, true, ASYNC_EVENT_LISTENERS, null);
  }

  public static GemFireCacheImpl create(InternalDistributedSystem system, CacheConfig cacheConfig) {
    return basicCreate(system, true, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }

  static GemFireCacheImpl createWithAsyncEventListeners(InternalDistributedSystem system,
      CacheConfig cacheConfig, TypeRegistry typeRegistry) {
    return basicCreate(system, true, cacheConfig, null, false, true, typeRegistry);
  }

  public static Cache create(InternalDistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig) {
    return basicCreate(system, existingOk, cacheConfig, null, false, ASYNC_EVENT_LISTENERS, null);
  }

  private static GemFireCacheImpl basicCreate(InternalDistributedSystem system, boolean existingOk,
      CacheConfig cacheConfig, PoolFactory pf, boolean isClient, boolean asyncEventListeners,
      TypeRegistry typeRegistry) throws CacheExistsException, TimeoutException,
      CacheWriterException, GatewayException, RegionExistsException {
    try {
      synchronized (GemFireCacheImpl.class) {
        GemFireCacheImpl instance = checkExistingCache(existingOk, cacheConfig, system);
        if (instance == null) {
          instance = new GemFireCacheImpl(isClient, pf, system, cacheConfig, asyncEventListeners,
              typeRegistry);
          system.setCache(instance);
          instance.initialize();
        } else {
          system.setCache(instance);
        }
        return instance;
      }
    } catch (CacheXmlException | IllegalArgumentException e) {
      logger.error(e.getLocalizedMessage()); // TODO: log the full stack trace or not?
      throw e;
    } catch (Error | RuntimeException e) {
      logger.error(e);
      throw e;
    }
  }

  private static GemFireCacheImpl checkExistingCache(boolean existingOk, CacheConfig cacheConfig,
      InternalDistributedSystem system) {
    GemFireCacheImpl instance =
        ALLOW_MULTIPLE_SYSTEMS ? (GemFireCacheImpl) system.getCache() : getInstance();

    if (instance != null && !instance.isClosed()) {
      if (existingOk) {
        // Check if cache configuration matches.
        cacheConfig.validateCacheConfig(instance);
        return instance;
      } else {
        // instance.creationStack argument is for debugging...
        throw new CacheExistsException(instance,
            String.format("%s: An open cache already exists.",
                instance),
            instance.creationStack);
      }
    }
    return null;
  }

  /**
   * Creates a new instance of GemFireCache and populates it according to the {@code cache.xml}, if
   * appropriate.
   *
   * @param typeRegistry: currently only unit tests set this parameter to a non-null value
   */
  private GemFireCacheImpl(boolean isClient, PoolFactory pf, InternalDistributedSystem system,
      CacheConfig cacheConfig, boolean asyncEventListeners, TypeRegistry typeRegistry) {
    this.isClient = isClient;
    this.poolFactory = pf;
    this.cacheConfig = cacheConfig; // do early for bug 43213
    this.pdxRegistry = typeRegistry;

    // Synchronized to prevent a new cache from being created
    // before an old one has finished closing
    synchronized (GemFireCacheImpl.class) {

      // start JTA transaction manager within this synchronized block
      // to prevent race with cache close. fixes bug 43987
      JNDIInvoker.mapTransactions(system);
      this.system = system;
      this.dm = this.system.getDistributionManager();

      if (!isClient) {
        this.configurationResponse = requestSharedConfiguration();

        // apply the cluster's properties configuration and initialize security using that
        // configuration
        ccLoader.applyClusterPropertiesConfiguration(this.configurationResponse,
            this.system.getConfig());

        this.securityService =
            SecurityServiceFactory.create(this.system.getConfig().getSecurityProps(), cacheConfig);
        this.system.setSecurityService(this.securityService);
      } else {
        // create a no-op security service for client
        this.securityService = SecurityServiceFactory.create();
      }

      if (!this.isClient && PoolManager.getAll().isEmpty()) {
        // We only support management on members of a distributed system
        // Should do this: if (!getSystem().isLoner()) {
        // but it causes quickstart.CqClientTest to hang
        this.resourceEventsListener = new ManagementListener(this.system);
        this.system.addResourceListener(this.resourceEventsListener);
        if (this.system.isLoner()) {
          this.system.getInternalLogWriter()
              .info("Running in local mode since no locators were specified.");
        }
      } else {
        logger.info("Running in client mode");
        this.resourceEventsListener = null;
      }

      // Don't let admin-only VMs create Cache's just yet.
      if (this.dm.getDMType() == ClusterDistributionManager.ADMIN_ONLY_DM_TYPE) {
        throw new IllegalStateException(
            "Cannot create a Cache in an admin-only VM.");
      }

      this.rootRegions = new HashMap<>();

      this.cqService = CqServiceProvider.create(this);

      // Create the CacheStatistics
      this.cachePerfStats = new CachePerfStats(system);
      CachePerfStats.enableClockStats = this.system.getConfig().getEnableTimeStatistics();

      this.transactionManager = new TXManagerImpl(this.cachePerfStats, this);
      this.dm.addMembershipListener(this.transactionManager);

      this.creationDate = new Date();

      this.persistentMemberManager = new PersistentMemberManager();

      if (asyncEventListeners) {
        this.eventThreadPool = LoggingExecutors.newThreadPoolWithFixedFeed("Message Event Thread",
            command -> {
              ConnectionTable.threadWantsSharedResources();
              command.run();
            }, EVENT_THREAD_LIMIT, this.cachePerfStats.getEventPoolHelper(), 1000,
            getThreadMonitorObj(),
            EVENT_QUEUE_LIMIT);
      } else {
        this.eventThreadPool = null;
      }

      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      this.resourceAdvisor = ResourceAdvisor.createResourceAdvisor(this);

      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      this.jmxAdvisor = JmxManagerAdvisor.createJmxManagerAdvisor(new JmxManagerAdvisee(this));

      this.resourceManager = InternalResourceManager.createResourceManager(this);
      this.serialNumber = DistributionAdvisor.createSerialNumber();

      getInternalResourceManager().addResourceListener(ResourceType.HEAP_MEMORY, getHeapEvictor());

      /*
       * Only bother creating an off-heap evictor if we have off-heap memory enabled.
       */
      if (null != getOffHeapStore()) {
        getInternalResourceManager().addResourceListener(ResourceType.OFFHEAP_MEMORY,
            getOffHeapEvictor());
      }

      this.recordedEventSweeper = createEventTrackerExpiryTask();
      this.tombstoneService = TombstoneService.initialize(this);

      TypeRegistry.init();
      basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
      TypeRegistry.open();

      if (!isClient()) {
        // Initialize the QRM thread frequency to default (1 second )to prevent spill
        // over from previous Cache , as the interval is stored in a static
        // volatile field.
        HARegionQueue.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
      }
      FunctionService.registerFunction(new PRContainsValueFunction());
      this.expirationScheduler = new ExpirationScheduler(this.system);

      // uncomment following line when debugging CacheExistsException
      if (DEBUG_CREATION_STACK) {
        this.creationStack = new Exception(
            String.format("Created GemFireCache %s", toString()));
      }

      this.txEntryStateFactory = TXEntryState.getFactory();
      if (XML_PARAMETERIZATION_ENABLED) {
        // If product properties file is available replace properties from there
        Properties userProps = this.system.getConfig().getUserDefinedProps();
        if (userProps != null && !userProps.isEmpty()) {
          this.resolver = new CacheXmlPropertyResolver(false,
              PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, userProps);
        } else {
          this.resolver = new CacheXmlPropertyResolver(false,
              PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, null);
        }
      } else {
        this.resolver = null;
      }

      SystemFailure.signalCacheCreate();

      this.diskMonitor = new DiskStoreMonitor(system.getConfig().getLogFile());

      addRegionEntrySynchronizationListener(new GatewaySenderQueueEntrySynchronizationListener());
      backupService = new BackupService(this);
    } // synchronized
  }

  @Override
  public void reLoadClusterConfiguration() throws IOException, ClassNotFoundException {
    this.configurationResponse = requestSharedConfiguration();
    if (this.configurationResponse != null) {
      ccLoader.deployJarsReceivedFromClusterConfiguration(this.configurationResponse);
      ccLoader.applyClusterPropertiesConfiguration(this.configurationResponse,
          this.system.getConfig());
      ccLoader.applyClusterXmlConfiguration(this, this.configurationResponse,
          this.system.getConfig().getGroups());
      initializeDeclarativeCache();
    }
  }

  /**
   * Initialize the EventTracker's timer task. This is stored for tracking and shutdown purposes
   */
  private EventTrackerExpiryTask createEventTrackerExpiryTask() {
    long lifetimeInMillis =
        Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "messageTrackingTimeout",
            PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT / 3);
    EventTrackerExpiryTask task = new EventTrackerExpiryTask(lifetimeInMillis);
    getCCPTimer().scheduleAtFixedRate(task, lifetimeInMillis, lifetimeInMillis);
    return task;
  }

  @Override
  public SecurityService getSecurityService() {
    return this.securityService;
  }

  @Override
  public boolean isRESTServiceRunning() {
    return this.isRESTServiceRunning;
  }

  @Override
  public void setRESTServiceRunning(boolean isRESTServiceRunning) {
    this.isRESTServiceRunning = isRESTServiceRunning;
  }

  /**
   * Used by Hydra tests to get handle of Rest Agent
   *
   */
  @Override
  public RestAgent getRestAgent() {
    return this.restAgent;
  }

  /**
   * Request the shared configuration from the locator(s) which have the Cluster config service
   * running
   */
  ConfigurationResponse requestSharedConfiguration() {
    final DistributionConfig config = this.system.getConfig();

    if (!(this.dm instanceof ClusterDistributionManager)) {
      return null;
    }

    // do nothing if this vm is/has locator or this is a client
    if (this.dm.getDMType() == ClusterDistributionManager.LOCATOR_DM_TYPE || this.isClient
        || Locator.getLocator() != null) {
      return null;
    }

    // can't simply return null if server is not using shared configuration, since we need to find
    // out if the locator is running in secure mode or not, if yes, then we need to throw an
    // exception if server is not using cluster config.

    Map<InternalDistributedMember, Collection<String>> locatorsWithClusterConfig =
        getDistributionManager().getAllHostedLocatorsWithSharedConfiguration();

    // If there are no locators with Shared configuration, that means the system has been started
    // without shared configuration then do not make requests to the locators.
    if (locatorsWithClusterConfig.isEmpty()) {
      logger.info("No locator(s) found with cluster configuration service");
      return null;
    }

    try {
      ConfigurationResponse response = ccLoader.requestConfigurationFromLocators(
          this.system.getConfig().getGroups(), locatorsWithClusterConfig.keySet());

      // log the configuration received from the locator
      logger.info("Received cluster configuration from the locator");
      logger.info(response.describeConfig());

      Configuration clusterConfig =
          response.getRequestedConfiguration().get(ConfigurationPersistenceService.CLUSTER_CONFIG);
      Properties clusterSecProperties =
          clusterConfig == null ? new Properties() : clusterConfig.getGemfireProperties();

      // If not using shared configuration, return null or throw an exception is locator is secured
      if (!config.getUseSharedConfiguration()) {
        if (clusterSecProperties.containsKey(ConfigurationProperties.SECURITY_MANAGER)) {
          throw new GemFireConfigException(
              "A server must use cluster configuration when joining a secured cluster.");
        } else {
          logger.info(
              "The cache has been created with use-cluster-configuration=false. It will not receive any cluster configuration");
          return null;
        }
      }

      Properties serverSecProperties = config.getSecurityProps();
      // check for possible mis-configuration
      if (isMisConfigured(clusterSecProperties, serverSecProperties,
          ConfigurationProperties.SECURITY_MANAGER)
          || isMisConfigured(clusterSecProperties, serverSecProperties,
              ConfigurationProperties.SECURITY_POST_PROCESSOR)) {
        throw new GemFireConfigException(
            "A server cannot specify its own security-manager or security-post-processor when using cluster configuration");
      }
      return response;

    } catch (ClusterConfigurationNotAvailableException e) {
      throw new GemFireConfigException(
          "cluster configuration service not available", e);
    } catch (UnknownHostException e) {
      throw new GemFireConfigException(e.getLocalizedMessage(), e);
    }
  }

  /**
   * When called, clusterProps and serverProps and key could not be null
   */
  static boolean isMisConfigured(Properties clusterProps, Properties serverProps, String key) {
    String clusterPropValue = clusterProps.getProperty(key);
    String serverPropValue = serverProps.getProperty(key);

    // if this server prop is not specified, this is always OK.
    if (StringUtils.isBlank(serverPropValue))
      return false;

    // server props is not blank, but cluster props is blank, NOT OK.
    if (StringUtils.isBlank(clusterPropValue))
      return true;

    // at this point check for equality
    return !clusterPropValue.equals(serverPropValue);
  }

  /**
   * Used by unit tests to force cache creation to use a test generated cache.xml
   */
  public static File testCacheXml = null;

  /**
   * @return true if cache is created using a ClientCacheFactory
   * @see #hasPool()
   */
  @Override
  public boolean isClient() {
    return this.isClient;
  }

  /**
   * Method to check for GemFire client. In addition to checking for ClientCacheFactory, this method
   * checks for any defined pools.
   *
   * @return true if the cache has pools declared
   */
  @Override
  public boolean hasPool() {
    return this.isClient || !getAllPools().isEmpty();
  }

  private static Collection<Pool> getAllPools() {
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
  @Override
  public synchronized Pool getDefaultPool() {
    if (this.defaultPool == null) {
      determineDefaultPool();
    }
    return this.defaultPool;
  }

  /**
   * Perform initialization, solve the early escaped reference problem by putting publishing
   * references to this instance in this method (vs. the constructor).
   */
  private void initialize() {
    for (CacheLifecycleListener listener : cacheLifecycleListeners) {
      listener.cacheCreated(this);
    }

    // set ClassPathLoader and then deploy cluster config jars
    ClassPathLoader.setLatestToDefault(this.system.getConfig().getDeployWorkingDir());

    try {
      ccLoader.deployJarsReceivedFromClusterConfiguration(this.configurationResponse);
    } catch (IOException | ClassNotFoundException e) {
      throw new GemFireConfigException(
          "Exception while deploying the jars received as a part of cluster Configuration",
          e);
    }

    SystemMemberCacheEventProcessor.send(this, Operation.CACHE_CREATE);
    this.resourceAdvisor.initializationGate();

    // Register function that we need to execute to fetch available REST service endpoints in DS
    FunctionService.registerFunction(new FindRestEnabledServersFunction());

    // moved this after initializeDeclarativeCache because in the future
    // distributed system creation will not happen until we have read
    // cache.xml file.
    // For now this needs to happen before cache.xml otherwise
    // we will not be ready for all the events that cache.xml
    // processing can deliver (region creation, etc.).
    // This call may need to be moved inside initializeDeclarativeCache.
    this.jmxAdvisor.initializationGate(); // Entry to GemFire Management service

    // this starts up the ManagementService, register and federate the internal beans
    this.system.handleResourceEvent(ResourceEvent.CACHE_CREATE, this);

    initializeServices();

    boolean completedCacheXml = false;
    try {
      if (!isClient) {
        applyJarAndXmlFromClusterConfig();
      }
      initializeDeclarativeCache();
      completedCacheXml = true;
    } catch (RuntimeException e) {
      logger.error("Cache initialization for {} failed because: {}", this, e); // fix GEODE-3038
      throw e;
    } finally {
      if (!completedCacheXml) {
        // so initializeDeclarativeCache threw an exception
        try {
          close(); // fix for bug 34041
        } catch (Throwable ignore) {
          // I don't want init to throw an exception that came from the close.
          // I want it to throw the original exception that came from initializeDeclarativeCache.
        }
        this.configurationResponse = null;
      }
    }

    startColocatedJmxManagerLocator();

    startMemcachedServer();

    startRedisServer();

    startRestAgentServer(this);

    this.isInitialized = true;
  }

  void applyJarAndXmlFromClusterConfig() {
    if (this.configurationResponse == null) {
      // Deploy all the jars from the deploy working dir.
      ClassPathLoader.getLatest().getJarDeployer().loadPreviouslyDeployedJarsFromDisk();
    }
    ccLoader.applyClusterXmlConfiguration(this, this.configurationResponse,
        this.system.getConfig().getGroups());
  }

  /**
   * Initialize any services that provided as extensions to the cache using the service loader
   * mechanism.
   */
  private void initializeServices() {
    ServiceLoader<CacheService> loader = ServiceLoader.load(CacheService.class);
    for (CacheService service : loader) {
      service.init(this);
      this.services.put(service.getInterface(), service);
      this.system.handleResourceEvent(ResourceEvent.CACHE_SERVICE_CREATE, service);
      logger.info("Initialized cache service {}", service.getClass().getName());
    }
  }

  private boolean isNotJmxManager() {
    return !this.system.getConfig().getJmxManagerStart();
  }

  private boolean isServerNode() {
    return this.system.getDistributedMember()
        .getVmKind() != ClusterDistributionManager.LOCATOR_DM_TYPE
        && this.system.getDistributedMember()
            .getVmKind() != ClusterDistributionManager.ADMIN_ONLY_DM_TYPE
        && !isClient();
  }

  private void startRestAgentServer(GemFireCacheImpl cache) {
    if (this.system.getConfig().getStartDevRestApi() && isNotJmxManager() && isServerNode()) {
      this.restAgent = new RestAgent(this.system.getConfig(), this.securityService);
      this.restAgent.start(cache);
    } else {
      this.restAgent = null;
    }
  }

  private void startMemcachedServer() {
    int port = this.system.getConfig().getMemcachedPort();
    if (port != 0) {
      String protocol = this.system.getConfig().getMemcachedProtocol();
      assert protocol != null;
      String bindAddress = this.system.getConfig().getMemcachedBindAddress();
      assert bindAddress != null;
      if (bindAddress.equals(DistributionConfig.DEFAULT_MEMCACHED_BIND_ADDRESS)) {
        logger.info("Starting GemFireMemcachedServer on port {} for {} protocol",
            new Object[] {port, protocol});
      } else {
        logger.info("Starting GemFireMemcachedServer on bind address {} on port {} for {} protocol",
            new Object[] {bindAddress, port, protocol});
      }
      this.memcachedServer =
          new GemFireMemcachedServer(bindAddress, port, Protocol.valueOf(protocol.toUpperCase()));
      this.memcachedServer.start();
    }
  }

  private void startRedisServer() {
    int port = this.system.getConfig().getRedisPort();
    if (port != 0) {
      String bindAddress = this.system.getConfig().getRedisBindAddress();
      assert bindAddress != null;
      if (bindAddress.equals(DistributionConfig.DEFAULT_REDIS_BIND_ADDRESS)) {
        getLogger().info(
            String.format("Starting GeodeRedisServer on port %s",
                new Object[] {port}));
      } else {
        getLogger().info(
            String.format("Starting GeodeRedisServer on bind address %s on port %s",
                new Object[] {bindAddress, port}));
      }
      this.redisServer = new GeodeRedisServer(bindAddress, port);
      this.redisServer.start();
    }
  }

  @Override
  public URL getCacheXmlURL() {
    if (this.getMyId().getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
      return null;
    }
    File xmlFile = testCacheXml;
    if (xmlFile == null) {
      xmlFile = this.system.getConfig().getCacheXmlFile();
    }
    if (xmlFile.getName().isEmpty()) {
      return null;
    }

    URL url;
    if (!xmlFile.exists() || !xmlFile.isFile()) {
      // do a resource search
      String resource = xmlFile.getPath();
      resource = DOUBLE_BACKSLASH.matcher(resource).replaceAll("/");
      if (resource.length() > 1 && resource.startsWith("/")) {
        resource = resource.substring(1);
      }
      url = ClassPathLoader.getLatest().getResource(getClass(), resource);
    } else {
      try {
        url = xmlFile.toURL();
      } catch (MalformedURLException ex) {
        throw new CacheXmlException(
            String.format("Could not convert XML file %s to an URL.",
                xmlFile),
            ex);
      }
    }
    if (url == null) {
      File defaultFile = DistributionConfig.DEFAULT_CACHE_XML_FILE;
      if (!xmlFile.equals(defaultFile)) {
        if (!xmlFile.exists()) {
          throw new CacheXmlException(
              String.format("Declarative Cache XML file/resource %s does not exist.",
                  xmlFile));
        } else {
          throw new CacheXmlException(
              String.format("Declarative XML file %s is not a file.",
                  xmlFile));
        }
      }
    }

    return url;
  }

  /**
   * Initializes the contents of this {@code Cache} according to the declarative caching XML file
   * specified by the given {@code DistributedSystem}. Note that this operation cannot be performed
   * in the constructor because creating regions in the cache, etc. uses the cache itself (which
   * isn't initialized until the constructor returns).
   *
   * @throws CacheXmlException If something goes wrong while parsing the declarative caching XML
   *         file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)}times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   *
   * @see #loadCacheXml
   */
  private void initializeDeclarativeCache()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    URL url = getCacheXmlURL();
    String cacheXmlDescription = this.cacheConfig.getCacheXMLDescription();
    if (url == null && cacheXmlDescription == null) {
      if (isClient()) {
        initializeClientRegionShortcuts(this);
      } else {
        initializeRegionShortcuts(this);
      }
      initializePdxRegistry();
      readyDynamicRegionFactory();
      return; // nothing needs to be done
    }

    InputStream stream = null;
    try {
      logCacheXML(url, cacheXmlDescription);
      if (cacheXmlDescription != null) {
        if (logger.isTraceEnabled()) {
          logger.trace("initializing cache with generated XML: {}", cacheXmlDescription);
        }
        stream = new StringBufferInputStream(cacheXmlDescription);
      } else {
        stream = url.openStream();
      }
      loadCacheXml(stream);

    } catch (IOException ex) {
      throw new CacheXmlException(
          String.format("While opening Cache XML %s the following error occurred %s",
              url.toString(), ex));

    } catch (CacheXmlException ex) {
      CacheXmlException newEx =
          new CacheXmlException(String.format("While reading Cache XML %s. %s",
              url, ex.getMessage()));
      /*
       * TODO: why use setStackTrace and initCause? removal breaks several tests: OplogRVVJUnitTest,
       * NewDeclarativeIndexCreationJUnitTest CacheXml70DUnitTest, CacheXml80DUnitTest,
       * CacheXml81DUnitTest, CacheXmlGeode10DUnitTest RegionManagementDUnitTest
       */
      newEx.setStackTrace(ex.getStackTrace());
      newEx.initCause(ex.getCause());
      throw newEx;

    } finally {
      closeQuietly(stream);
    }
  }

  private static void logCacheXML(URL url, String cacheXmlDescription) {
    if (cacheXmlDescription == null) {
      StringBuilder sb = new StringBuilder();
      BufferedReader br = null;
      try {
        final String lineSeparator = System.getProperty("line.separator");
        br = new BufferedReader(new InputStreamReader(url.openStream()));
        String line = br.readLine();
        while (line != null) {
          if (!line.isEmpty()) {
            sb.append(lineSeparator).append(line);
          }
          line = br.readLine();
        }
      } catch (IOException ignore) {
      } finally {
        closeQuietly(br);
      }
      logger.info("Initializing cache using {}:{}",
          new Object[] {url.toString(), sb.toString()});
    } else {
      logger.info(
          "Initializing cache using {}:{}",
          new Object[] {"generated description from old cache", cacheXmlDescription});
    }
  }

  public synchronized void initializePdxRegistry() {
    if (this.pdxRegistry == null) {
      // The member with locator is initialized with a NullTypePdxRegistration
      if (this.getMyId().getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
        this.pdxRegistry = new TypeRegistry(this, true);
      } else {
        this.pdxRegistry = new TypeRegistry(this, false);
      }
      this.pdxRegistry.initialize();
    }
  }

  /**
   * Call to make this vm's dynamic region factory ready. Public so it can be called from
   * CacheCreation during xml processing
   */
  public void readyDynamicRegionFactory() {
    try {
      ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).internalInit(this);
    } catch (CacheException ce) {
      throw new GemFireCacheException(
          "dynamic region initialization failed",
          ce);
    }
  }

  /**
   * create diskStore factory with default attributes
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public DiskStoreFactory createDiskStoreFactory() {
    return new DiskStoreFactoryImpl(this);
  }

  /**
   * create diskStore factory with predefined attributes
   *
   * @since GemFire prPersistSprint2
   */
  public DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs) {
    return new DiskStoreFactoryImpl(this, attrs);
  }

  class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      String reason = getDistributedSystem().getCancelCriterion().cancelInProgress();
      if (reason != null) {
        return reason;
      }
      if (GemFireCacheImpl.this.disconnectCause != null) {
        return GemFireCacheImpl.this.disconnectCause.getMessage();
      }
      if (GemFireCacheImpl.this.isClosing) {
        return "The cache is closed."; // this + ": closed";
      }
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable throwable) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      RuntimeException result =
          getDistributedSystem().getCancelCriterion().generateCancelledException(throwable);
      if (result != null) {
        return result;
      }
      if (GemFireCacheImpl.this.disconnectCause == null) {
        // No root cause, specify the one given and be done with it.
        return new CacheClosedException(reason, throwable);
      }

      if (throwable == null) {
        // Caller did not specify any root cause, so just use our own.
        return new CacheClosedException(reason, GemFireCacheImpl.this.disconnectCause);
      }

      // Attempt to stick rootCause at tail end of the exception chain.
      Throwable nt = throwable;
      while (nt.getCause() != null) {
        nt = nt.getCause();
      }
      try {
        nt.initCause(GemFireCacheImpl.this.disconnectCause);
        return new CacheClosedException(reason, throwable);
      } catch (IllegalStateException ignore) {
        // Bug 39496 (JRockit related) Give up. The following
        // error is not entirely sane but gives the correct general picture.
        return new CacheClosedException(reason, GemFireCacheImpl.this.disconnectCause);
      }
    }
  }

  private final Stopper stopper = new Stopper();

  @Override
  public CancelCriterion getCancelCriterion() {
    return this.stopper;
  }

  /** return true if the cache was closed due to being shunned by other members */
  @Override
  public boolean forcedDisconnect() {
    return this.forcedDisconnect || this.system.forcedDisconnect();
  }

  /** return a CacheClosedException with the given reason */
  @Override
  public CacheClosedException getCacheClosedException(String reason) {
    return getCacheClosedException(reason, null);
  }

  /** return a CacheClosedException with the given reason and cause */
  @Override
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
  @Override
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
  public static void loadEmergencyClasses() {
    if (emergencyClassesLoaded)
      return;
    emergencyClassesLoaded = true;
    InternalDistributedSystem.loadEmergencyClasses();
    AcceptorImpl.loadEmergencyClasses();
    PoolManagerImpl.loadEmergencyClasses();
  }

  /**
   * Close the distributed system, cache servers, and gateways. Clears the rootRegions and
   * partitionedRegions map. Marks the cache as closed.
   *
   * @see SystemFailure#emergencyClose()
   */
  public static void emergencyClose() {
    final boolean DEBUG = SystemFailure.TRACE_CLOSE;

    GemFireCacheImpl cache = getInstance();
    if (cache == null) {
      if (DEBUG) {
        System.err.println("GemFireCache#emergencyClose: no instance");
      }
      return;
    }

    // leave the PdxSerializer set if we have one to prevent 43412

    // Shut down messaging first
    InternalDistributedSystem ids = cache.system;
    if (ids != null) {
      if (DEBUG) {
        System.err.println("DEBUG: emergencyClose InternalDistributedSystem");
      }
      ids.emergencyClose();
    }

    cache.disconnectCause = SystemFailure.getFailure();
    cache.isClosing = true;

    // Clear cache servers
    if (DEBUG) {
      System.err.println("DEBUG: Close cache servers");
    }

    for (CacheServerImpl cacheServer : cache.allCacheServers) {
      AcceptorImpl acceptor = cacheServer.getAcceptor();
      if (acceptor != null) {
        acceptor.emergencyClose();
      }
    }

    if (DEBUG) {
      System.err.println("DEBUG: closing client resources");
    }
    PoolManagerImpl.emergencyClose();

    if (DEBUG) {
      System.err.println("DEBUG: closing gateway hubs");
    }

    // rootRegions is intentionally *not* synchronized. The
    // implementation of clear() does not currently allocate objects.
    cache.rootRegions.clear();

    // partitionedRegions is intentionally *not* synchronized, The
    // implementation of clear() does not currently allocate objects.
    cache.partitionedRegions.clear();
    if (DEBUG) {
      System.err.println("DEBUG: done with cache emergency close");
    }
  }

  @Override
  public boolean isCacheAtShutdownAll() {
    return this.isShutDownAll.get();
  }

  /**
   * Number of threads used to close PRs in shutdownAll. By default is the number of PRs in the
   * cache
   */
  private static final int shutdownAllPoolSize =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "SHUTDOWN_ALL_POOL_SIZE", -1);

  private void shutdownSubTreeGracefully(Map<String, PartitionedRegion> prSubMap) {
    for (final PartitionedRegion pr : prSubMap.values()) {
      shutDownOnePRGracefully(pr);
    }
  }

  @Override
  public void shutDownAll() {
    if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      try {
        CacheObserverHolder.getInstance().beforeShutdownAll();
      } finally {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      }
    }
    if (!this.isShutDownAll.compareAndSet(false, true)) {
      // it's already doing shutdown by another thread
      try {
        this.shutDownAllFinished.await();
      } catch (InterruptedException ignore) {
        logger.debug(
            "Shutdown all interrupted while waiting for another thread to do the shutDownAll");
        Thread.currentThread().interrupt();
      }
      return;
    }
    synchronized (GemFireCacheImpl.class) {
      try {
        boolean testIGE = Boolean.getBoolean("TestInternalGemFireError");

        if (testIGE) {
          throw new InternalGemFireError(
              "unexpected exception");
        }

        // bug 44031 requires multithread shutDownAll should be grouped
        // by root region. However, shutDownAllDuringRecovery.conf test revealed that
        // we have to close colocated child regions first.
        // Now check all the PR, if anyone has colocate-with attribute, sort all the
        // PRs by colocation relationship and close them sequentially, otherwise still
        // group them by root region.
        SortedMap<String, Map<String, PartitionedRegion>> prTrees = getPRTrees();
        if (prTrees.size() > 1 && shutdownAllPoolSize != 1) {
          ExecutorService es = getShutdownAllExecutorService(prTrees.size());
          for (final Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
            es.execute(() -> {
              ConnectionTable.threadWantsSharedResources();
              shutdownSubTreeGracefully(prSubMap);
            });
          } // for each root
          es.shutdown();
          try {
            es.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
          } catch (InterruptedException ignore) {
            logger
                .debug("Shutdown all interrupted while waiting for PRs to be shutdown gracefully.");
          }

        } else {
          for (final Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
            shutdownSubTreeGracefully(prSubMap);
          }
        }

        close("Shut down all members", null, false, true);
      } finally {
        this.shutDownAllFinished.countDown();
      }
    }
  }

  private ExecutorService getShutdownAllExecutorService(int size) {
    return LoggingExecutors
        .newFixedThreadPool("ShutdownAll-", true,
            shutdownAllPoolSize == -1 ? size : shutdownAllPoolSize);
  }

  private void shutDownOnePRGracefully(PartitionedRegion partitionedRegion) {
    boolean acquiredLock = false;
    try {
      partitionedRegion.acquireDestroyLock();
      acquiredLock = true;

      synchronized (partitionedRegion.getRedundancyProvider()) {
        if (partitionedRegion.isDataStore() && partitionedRegion.getDataStore() != null
            && partitionedRegion.getDataPolicy() == DataPolicy.PERSISTENT_PARTITION) {
          int numBuckets = partitionedRegion.getTotalNumberOfBuckets();
          Map<InternalDistributedMember, PersistentMemberID>[] bucketMaps = new Map[numBuckets];
          PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();

          // lock all the primary buckets
          Set<Entry<Integer, BucketRegion>> bucketEntries = dataStore.getAllLocalBuckets();
          for (Entry e : bucketEntries) {
            BucketRegion bucket = (BucketRegion) e.getValue();
            if (bucket == null || bucket.isDestroyed) {
              // bucket region could be destroyed in race condition
              continue;
            }
            bucket.getBucketAdvisor().tryLockIfPrimary();

            // get map <InternalDistributedMember, persistentID> for this bucket's
            // remote members
            bucketMaps[bucket.getId()] =
                bucket.getBucketAdvisor().adviseInitializedPersistentMembers();
            if (logger.isDebugEnabled()) {
              logger.debug("shutDownAll: PR {}: initialized persistent members for {}:{}",
                  partitionedRegion.getName(), bucket.getId(), bucketMaps[bucket.getId()]);
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: All buckets for PR {} are locked.",
                partitionedRegion.getName());
          }

          // send lock profile update to other members
          partitionedRegion.setShutDownAllStatus(PartitionedRegion.PRIMARY_BUCKETS_LOCKED);
          new UpdateAttributesProcessor(partitionedRegion).distribute(false);
          partitionedRegion.getRegionAdvisor()
              .waitForProfileStatus(PartitionedRegion.PRIMARY_BUCKETS_LOCKED);
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: PR {}: all bucketLock profiles received.",
                partitionedRegion.getName());
          }

          // if async write, do flush
          if (!partitionedRegion.getAttributes().isDiskSynchronous()) {
            // several PRs might share the same diskStore, we will only flush once
            // even flush is called several times.
            partitionedRegion.getDiskStore().forceFlush();
            // send flush profile update to other members
            partitionedRegion.setShutDownAllStatus(PartitionedRegion.DISK_STORE_FLUSHED);
            new UpdateAttributesProcessor(partitionedRegion).distribute(false);
            partitionedRegion.getRegionAdvisor()
                .waitForProfileStatus(PartitionedRegion.DISK_STORE_FLUSHED);
            if (logger.isDebugEnabled()) {
              logger.debug("shutDownAll: PR {}: all flush profiles received.",
                  partitionedRegion.getName());
            }
          } // async write

          // persist other members to OFFLINE_EQUAL for each bucket region
          // iterate through all the bucketMaps and exclude the items whose
          // idm is no longer online
          Set<InternalDistributedMember> membersToPersistOfflineEqual =
              partitionedRegion.getRegionAdvisor().adviseDataStore();
          for (Entry e : bucketEntries) {
            BucketRegion bucket = (BucketRegion) e.getValue();
            if (bucket == null || bucket.isDestroyed) {
              // bucket region could be destroyed in race condition
              continue;
            }
            Map<InternalDistributedMember, PersistentMemberID> persistMap =
                getSubMapForLiveMembers(membersToPersistOfflineEqual, bucketMaps[bucket.getId()]);
            if (persistMap != null) {
              bucket.getPersistenceAdvisor().persistMembersOfflineAndEqual(persistMap);
              if (logger.isDebugEnabled()) {
                logger.debug("shutDownAll: PR {}: persisting bucket {}:{}",
                    partitionedRegion.getName(), bucket.getId(), persistMap);
              }
            }
          }

          // send persisted profile update to other members, let all members to persist
          // before close the region
          partitionedRegion.setShutDownAllStatus(PartitionedRegion.OFFLINE_EQUAL_PERSISTED);
          new UpdateAttributesProcessor(partitionedRegion).distribute(false);
          partitionedRegion.getRegionAdvisor()
              .waitForProfileStatus(PartitionedRegion.OFFLINE_EQUAL_PERSISTED);
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: PR {}: all offline_equal profiles received.",
                partitionedRegion.getName());
          }
        } // dataStore

        // after done all steps for buckets, close partitionedRegion
        // close accessor directly
        RegionEventImpl event = new RegionEventImpl(partitionedRegion, Operation.REGION_CLOSE, null,
            false, getMyId(), true);
        try {
          // not to acquire lock
          partitionedRegion.basicDestroyRegion(event, false, false, true);
        } catch (CacheWriterException e) {
          // not possible with local operation, CacheWriter not called
          throw new Error(
              "CacheWriterException should not be thrown in localDestroyRegion",
              e);
        } catch (TimeoutException e) {
          // not possible with local operation, no distributed locks possible
          throw new Error(
              "TimeoutException should not be thrown in localDestroyRegion",
              e);
        }
      } // synchronized
    } catch (CacheClosedException cce) {
      logger.debug("Encounter CacheClosedException when shutDownAll is closing PR: {}:{}",
          partitionedRegion.getFullPath(), cce.getMessage());
    } catch (CancelException ce) {
      logger.debug("Encounter CancelException when shutDownAll is closing PR: {}:{}",
          partitionedRegion.getFullPath(), ce.getMessage());
    } catch (RegionDestroyedException rde) {
      logger.debug("Encounter CacheDestroyedException when shutDownAll is closing PR: {}:{}",
          partitionedRegion.getFullPath(), rde.getMessage());
    } finally {
      if (acquiredLock) {
        partitionedRegion.releaseDestroyLock();
      }
    }
  }

  private static Map<InternalDistributedMember, PersistentMemberID> getSubMapForLiveMembers(
      Set<InternalDistributedMember> membersToPersistOfflineEqual,
      Map<InternalDistributedMember, PersistentMemberID> bucketMap) {
    if (bucketMap == null) {
      return null;
    }
    Map<InternalDistributedMember, PersistentMemberID> persistMap = new HashMap<>();
    for (InternalDistributedMember member : membersToPersistOfflineEqual) {
      if (bucketMap.containsKey(member)) {
        persistMap.put(member, bucketMap.get(member));
      }
    }
    return persistMap;
  }

  @Override
  public void close() {
    close(false);
  }

  @Override
  public void close(String reason, boolean keepAlive, boolean keepDS) {
    close(reason, null, keepAlive, keepDS);
  }

  @Override
  public void close(boolean keepAlive) {
    close("Normal disconnect", null, keepAlive, false);
  }

  @Override
  public void close(String reason, Throwable optionalCause) {
    close(reason, optionalCause, false, false);
  }

  /**
   * Gets or lazily creates the PartitionedRegion distributed lock service. This call will
   * synchronize on this GemFireCache.
   *
   * @return the PartitionedRegion distributed lock service
   */
  @Override
  public DistributedLockService getPartitionedRegionLockService() {
    synchronized (this.prLockServiceLock) {
      this.stopper.checkCancelInProgress(null);
      if (this.prLockService == null) {
        try {
          this.prLockService =
              DLockService.create(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME,
                  getInternalDistributedSystem(), true /* distributed */,
                  true /* destroyOnDisconnect */, true /* automateFreeResources */);
        } catch (IllegalArgumentException e) {
          this.prLockService = DistributedLockService
              .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
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
   *
   * @return the GatewaySender distributed lock service
   */
  @Override
  public DistributedLockService getGatewaySenderLockService() {
    if (this.gatewayLockService == null) {
      synchronized (this.gatewayLockServiceLock) {
        this.stopper.checkCancelInProgress(null);
        if (this.gatewayLockService == null) {
          try {
            this.gatewayLockService = DLockService.create(AbstractGatewaySender.LOCK_SERVICE_NAME,
                getInternalDistributedSystem(), true /* distributed */,
                true /* destroyOnDisconnect */, true /* automateFreeResources */);
          } catch (IllegalArgumentException e) {
            this.gatewayLockService =
                DistributedLockService.getServiceNamed(AbstractGatewaySender.LOCK_SERVICE_NAME);
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
   * Destroys the PartitionedRegion distributed lock service when closing the cache. Caller must be
   * synchronized on this GemFireCache.
   */
  private void destroyPartitionedRegionLockService() {
    try {
      DistributedLockService.destroy(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
    } catch (IllegalArgumentException ignore) {
      // DistributedSystem.disconnect may have already destroyed the DLS
    }
  }

  /**
   * Destroys the GatewaySender distributed lock service when closing the cache. Caller must be
   * synchronized on this GemFireCache.
   */
  private void destroyGatewaySenderLockService() {
    if (DistributedLockService.getServiceNamed(AbstractGatewaySender.LOCK_SERVICE_NAME) != null) {
      try {
        DistributedLockService.destroy(AbstractGatewaySender.LOCK_SERVICE_NAME);
      } catch (IllegalArgumentException ignore) {
        // DistributedSystem.disconnect may have already destroyed the DLS
      }
    }
  }

  public HeapEvictor getHeapEvictor() {
    synchronized (this.heapEvictorLock) {
      this.stopper.checkCancelInProgress(null);
      if (this.heapEvictor == null) {
        this.heapEvictor = new HeapEvictor(this);
      }
      return this.heapEvictor;
    }
  }

  public OffHeapEvictor getOffHeapEvictor() {
    synchronized (this.offHeapEvictorLock) {
      this.stopper.checkCancelInProgress(null);
      if (this.offHeapEvictor == null) {
        this.offHeapEvictor = new OffHeapEvictor(this);
      }
      return this.offHeapEvictor;
    }
  }

  /** Used by test to inject an evictor */
  void setOffHeapEvictor(OffHeapEvictor evictor) {
    this.offHeapEvictor = evictor;
  }

  /** Used by test to inject an evictor */
  void setHeapEvictor(HeapEvictor evictor) {
    this.heapEvictor = evictor;
  }

  @Override
  public PersistentMemberManager getPersistentMemberManager() {
    return this.persistentMemberManager;
  }

  @Override
  public ClientMetadataService getClientMetadataService() {
    synchronized (this.clientMetaDatServiceLock) {
      this.stopper.checkCancelInProgress(null);
      if (this.clientMetadataService == null) {
        this.clientMetadataService = new ClientMetadataService(this);
      }
      return this.clientMetadataService;
    }
  }

  private final boolean DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");

  public void close(String reason, Throwable systemFailureCause, boolean keepAlive,
      boolean keepDS) {
    securityService.close();

    if (isClosed()) {
      return;
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();

    synchronized (GemFireCacheImpl.class) {
      // fix for bug 36512 "GemFireCache.close is not thread safe"
      // ALL CODE FOR CLOSE SHOULD NOW BE UNDER STATIC SYNCHRONIZATION
      // OF synchronized (GemFireCache.class) {
      // static synchronization is necessary due to static resources
      if (isClosed()) {
        return;
      }

      /*
       * First close the ManagementService as it uses a lot of infra which will be closed by
       * cache.close()
       */
      this.system.handleResourceEvent(ResourceEvent.CACHE_REMOVE, this);
      if (this.resourceEventsListener != null) {
        this.system.removeResourceListener(this.resourceEventsListener);
        this.resourceEventsListener = null;
      }

      if (systemFailureCause != null) {
        this.forcedDisconnect = systemFailureCause instanceof ForcedDisconnectException;
        if (this.forcedDisconnect) {
          this.disconnectCause = new ForcedDisconnectException(reason);
        } else {
          this.disconnectCause = systemFailureCause;
        }
      }

      this.keepAlive = keepAlive;
      this.isClosing = true;
      logger.info("{}: Now closing.", this);

      // we don't clear the prID map if there is a system failure. Other
      // threads may be hung trying to communicate with the map locked
      if (systemFailureCause == null) {
        PartitionedRegion.clearPRIdMap();
      }
      TXStateProxy tx = null;
      try {

        if (this.transactionManager != null) {
          tx = this.transactionManager.pauseTransaction();
        }

        // do this before closing regions
        this.resourceManager.close();

        try {
          this.resourceAdvisor.close();
        } catch (CancelException ignore) {
          // ignore
        }
        try {
          this.jmxAdvisor.close();
        } catch (CancelException ignore) {
          // ignore
        }

        for (GatewaySender sender : this.allGatewaySenders) {
          try {
            sender.stop();
            GatewaySenderAdvisor advisor = ((AbstractGatewaySender) sender).getSenderAdvisor();
            if (advisor != null) {
              if (isDebugEnabled) {
                logger.debug("Stopping the GatewaySender advisor");
              }
              advisor.close();
            }
          } catch (CancelException ignore) {
          }
        }

        destroyGatewaySenderLockService();

        if (this.eventThreadPool != null) {
          if (isDebugEnabled) {
            logger.debug("{}: stopping event thread pool...", this);
          }
          this.eventThreadPool.shutdown();
        }

        /*
         * IMPORTANT: any operation during shut down that can time out (create a CancelException)
         * must be inside of this try block. If all else fails, we *must* ensure that the cache gets
         * closed!
         */
        try {
          this.stopServers();

          stopMemcachedServer();

          stopRedisServer();

          stopRestAgentServer();

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

          List<InternalRegion> rootRegionValues;
          synchronized (this.rootRegions) {
            rootRegionValues = new ArrayList<>(this.rootRegions.values());
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

            InternalRegion prRoot = null;

            for (InternalRegion lr : rootRegionValues) {
              if (isDebugEnabled) {
                logger.debug("{}: processing region {}", this, lr.getFullPath());
              }
              if (PartitionedRegionHelper.PR_ROOT_REGION_NAME.equals(lr.getName())) {
                prRoot = lr;
              } else {
                if (lr.getName().contains(ParallelGatewaySenderQueue.QSTRING)) {
                  continue; // this region will be closed internally by parent region
                }
                if (isDebugEnabled) {
                  logger.debug("{}: closing region {}...", this, lr.getFullPath());
                }
                try {
                  lr.handleCacheClose(op);
                } catch (RuntimeException e) {
                  if (isDebugEnabled || !this.forcedDisconnect) {
                    logger.warn(String.format("%s: error closing region %s",
                        new Object[] {this, lr.getFullPath()}),
                        e);
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
              logger.warn(String.format("%s: error in last stage of PartitionedRegion cache close",
                  this),
                  e);
            }
            destroyPartitionedRegionLockService();
          }

          closeDiskStores();
          this.diskMonitor.close();

          // Close the CqService Handle.
          try {
            if (isDebugEnabled) {
              logger.debug("{}: closing CQ service...", this);
            }
            this.cqService.close();
          } catch (RuntimeException ignore) {
            logger.info("Failed to get the CqService, to close during cache close (1).");
          }

          PoolManager.close(keepAlive);

          if (isDebugEnabled) {
            logger.debug("{}: notifying admins of close...", this);
          }
          try {
            SystemMemberCacheEventProcessor.send(this, Operation.CACHE_CLOSE);
          } catch (CancelException ignore) {
            if (logger.isDebugEnabled()) {
              logger.debug("Ignored cancellation while notifying admins");
            }
          }

          if (isDebugEnabled) {
            logger.debug("{}: stopping destroyed entries processor...", this);
          }
          this.tombstoneService.stop();

          // NOTICE: the CloseCache message is the *last* message you can send!
          DistributionManager distributionManager = null;
          try {
            distributionManager = this.system.getDistributionManager();
            distributionManager.removeMembershipListener(this.transactionManager);
          } catch (CancelException ignore) {
            // distributionManager = null;
          }

          if (distributionManager != null) { // Send CacheClosedMessage (and NOTHING ELSE) here
            if (isDebugEnabled) {
              logger.debug("{}: sending CloseCache to peers...", this);
            }
            Set otherMembers = distributionManager.getOtherDistributionManagerIds();
            ReplyProcessor21 processor = new ReplyProcessor21(this.system, otherMembers);
            CloseCacheMessage msg = new CloseCacheMessage();
            msg.setRecipients(otherMembers);
            msg.setProcessorId(processor.getProcessorId());
            distributionManager.putOutgoing(msg);
            try {
              processor.waitForReplies();
            } catch (InterruptedException ignore) {
              // Thread.currentThread().interrupt(); // TODO ??? should we reset this bit later?
              // Keep going, make best effort to shut down.
            } catch (ReplyException ignore) {
              // keep going
            }
            // set closed state after telling others and getting responses
            // to avoid complications with others still in the process of
            // sending messages
          }
          // NO MORE Distributed Messaging AFTER THIS POINT!!!!

          ClientMetadataService cms = this.clientMetadataService;
          if (cms != null) {
            cms.close();
          }
          closeHeapEvictor();
          closeOffHeapEvictor();
        } catch (CancelException ignore) {
          // make sure the disk stores get closed
          closeDiskStores();
          // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!
        }

        // Close the CqService Handle.
        try {
          this.cqService.close();
        } catch (RuntimeException ignore) {
          logger.info("Failed to get the CqService, to close during cache close (2).");
        }

        this.cachePerfStats.close();
        TXLockService.destroyServices();
        getEventTrackerTask().cancel();

        synchronized (this.ccpTimerMutex) {
          if (this.ccpTimer != null) {
            this.ccpTimer.cancel();
          }
        }

        this.expirationScheduler.cancel();

        // Stop QueryMonitor if running.
        if (this.queryMonitor != null) {
          this.queryMonitor.stopMonitoring();
        }

      } finally {
        // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!
        if (this.transactionManager != null) {
          this.transactionManager.close();
        }
        ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).close();
        if (this.transactionManager != null) {
          this.transactionManager.unpauseTransaction(tx);
        }
        TXCommitMessage.getTracker().clearForCacheClose();
      }
      // Added to close the TransactionManager's cleanup thread
      TransactionManagerImpl.refresh();

      if (!keepDS) {
        // keepDS is used by ShutdownAll. It will override DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE
        if (!this.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE) {
          this.system.disconnect();
        }
      }
      TypeRegistry.close();
      // do this late to prevent 43412
      TypeRegistry.setPdxSerializer(null);

      for (CacheLifecycleListener listener : cacheLifecycleListeners) {
        listener.cacheClosed(this);
      }
      // Fix for #49856
      SequenceLoggerImpl.signalCacheClose();
      SystemFailure.signalCacheClose();

    } // static synchronization on GemFireCache.class

  }

  private void closeOffHeapEvictor() {
    OffHeapEvictor evictor = this.offHeapEvictor;
    if (evictor != null) {
      evictor.close();
    }
  }

  private void closeHeapEvictor() {
    HeapEvictor evictor = this.heapEvictor;
    if (evictor != null) {
      evictor.close();
    }
  }

  @Override
  public boolean isReconnecting() {
    return this.system.isReconnecting();
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    boolean systemReconnected = this.system.waitUntilReconnected(time, units);
    if (!systemReconnected) {
      return false;
    }
    GemFireCacheImpl cache = getInstance();
    return cache != null && cache.isInitialized();
  }

  @Override
  public void stopReconnecting() {
    this.system.stopReconnecting();
  }

  @Override
  public Cache getReconnectedCache() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == this || cache != null && !cache.isInitialized()) {
      cache = null;
    }
    return cache;
  }

  private void stopMemcachedServer() {
    if (this.memcachedServer != null) {
      logger.info("GemFireMemcachedServer on port {} is shutting down",
          new Object[] {this.system.getConfig().getMemcachedPort()});
      this.memcachedServer.shutdown();
    }
  }

  private void stopRedisServer() {
    if (this.redisServer != null)
      this.redisServer.shutdown();
  }

  private void stopRestAgentServer() {
    if (this.restAgent != null) {
      logger.info("Rest Server on port {} is shutting down",
          new Object[] {this.system.getConfig().getHttpServicePort()});
      this.restAgent.stop();
    }
  }

  private void prepareDiskStoresForClose() {
    String pdxDSName = TypeRegistry.getPdxDiskStoreName(this);
    DiskStoreImpl pdxDiskStore = null;
    for (DiskStoreImpl dsi : this.diskStores.values()) {
      if (dsi.getName().equals(pdxDSName)) {
        pdxDiskStore = dsi;
      } else {
        dsi.prepareForClose();
      }
    }
    if (pdxDiskStore != null) {
      pdxDiskStore.prepareForClose();
    }
  }

  private final ConcurrentMap<String, DiskStoreImpl> diskStores = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, DiskStoreImpl> regionOwnedDiskStores =
      new ConcurrentHashMap<>();

  @Override
  public void addDiskStore(DiskStoreImpl dsi) {
    this.diskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      this.diskMonitor.addDiskStore(dsi);
    }
  }

  @Override
  public void removeDiskStore(DiskStoreImpl diskStore) {
    this.diskStores.remove(diskStore.getName());
    this.regionOwnedDiskStores.remove(diskStore.getName());
    // Added for M&M
    if (!diskStore.getOwnedByRegion())
      this.system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, diskStore);
  }

  @Override
  public void addRegionOwnedDiskStore(DiskStoreImpl dsi) {
    this.regionOwnedDiskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      this.diskMonitor.addDiskStore(dsi);
    }
  }

  @Override
  public void closeDiskStores() {
    Iterator<DiskStoreImpl> it = this.diskStores.values().iterator();
    while (it.hasNext()) {
      try {
        DiskStoreImpl dsi = it.next();
        if (logger.isDebugEnabled()) {
          logger.debug("closing {}", dsi);
        }
        dsi.close();
        // Added for M&M
        this.system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, dsi);
      } catch (RuntimeException e) {
        logger.fatal("Cache close caught an exception during disk store close", e);
      }
      it.remove();
    }
  }

  /**
   * Used by unit tests to allow them to change the default disk store name.
   */
  public static void setDefaultDiskStoreName(String dsName) {
    defaultDiskStoreName = dsName;
  }

  public static String getDefaultDiskStoreName() {
    return defaultDiskStoreName;
  }

  // TODO: remove static from defaultDiskStoreName and move methods to InternalCache
  private static String defaultDiskStoreName = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;

  @Override
  public DiskStoreImpl getOrCreateDefaultDiskStore() {
    DiskStoreImpl result = (DiskStoreImpl) findDiskStore(null);
    if (result == null) {
      synchronized (this) {
        result = (DiskStoreImpl) findDiskStore(null);
        if (result == null) {
          result = (DiskStoreImpl) createDiskStoreFactory().create(defaultDiskStoreName);
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
  @Override
  public DiskStore findDiskStore(String name) {
    if (name == null) {
      name = defaultDiskStoreName;
    }
    return this.diskStores.get(name);
  }

  /**
   * Returns the DiskStore list
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public Collection<DiskStore> listDiskStores() {
    return Collections.unmodifiableCollection(this.diskStores.values());
  }

  @Override
  public Collection<DiskStore> listDiskStoresIncludingRegionOwned() {
    Collection<DiskStore> allDiskStores = new HashSet<>();
    allDiskStores.addAll(this.diskStores.values());
    allDiskStores.addAll(this.regionOwnedDiskStores.values());
    return allDiskStores;
  }

  private void stopServers() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: stopping cache servers...", this);
    }

    boolean stoppedCacheServer = false;

    for (CacheServerImpl cacheServer : this.allCacheServers) {
      if (isDebugEnabled) {
        logger.debug("stopping bridge {}", cacheServer);
      }
      try {
        cacheServer.stop();
      } catch (CancelException e) {
        if (isDebugEnabled) {
          logger.debug("Ignored cache closure while closing bridge {}", cacheServer, e);
        }
      }
      this.allCacheServers.remove(cacheServer);
      stoppedCacheServer = true;
    }
    if (stoppedCacheServer) {
      // now that all the cache servers have stopped empty the static pool of commBuffers it might
      // have used.
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

  @Override
  public DistributedSystem getDistributedSystem() {
    return this.system;
  }

  @Override
  public InternalDistributedSystem getInternalDistributedSystem() {
    return this.system;
  }

  /**
   * Returns the member id of my distributed system
   *
   * @since GemFire 5.0
   */
  @Override
  public InternalDistributedMember getMyId() {
    return this.system.getDistributedMember();
  }

  @Override
  public Set<DistributedMember> getMembers() {
    return Collections
        .unmodifiableSet((Set) this.dm.getOtherNormalDistributionManagerIds());
  }

  @Override
  public Set<DistributedMember> getAdminMembers() {
    return (Set) this.dm.getAdminMemberSet();
  }

  @Override
  public Set<DistributedMember> getMembers(Region region) {
    if (region instanceof DistributedRegion) {
      DistributedRegion distributedRegion = (DistributedRegion) region;
      return (Set<DistributedMember>) distributedRegion.getDistributionAdvisor().adviseCacheOp();
    } else if (region instanceof PartitionedRegion) {
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      return (Set<DistributedMember>) partitionedRegion.getRegionAdvisor().adviseAllPRNodes();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public Set<InetSocketAddress> getCurrentServers() {
    Map<String, Pool> pools = PoolManager.getAll();
    Set<InetSocketAddress> result = null;
    for (Pool pool : pools.values()) {
      PoolImpl poolImpl = (PoolImpl) pool;
      for (ServerLocation serverLocation : poolImpl.getCurrentServers()) {
        if (result == null) {
          result = new HashSet<>();
        }
        result.add(new InetSocketAddress(serverLocation.getHostName(), serverLocation.getPort()));
      }
    }
    if (result == null) {
      return Collections.emptySet();
    } else {
      return result;
    }
  }

  @Override
  public LogWriter getLogger() {
    return this.system.getLogWriter();
  }

  @Override
  public LogWriter getSecurityLogger() {
    return this.system.getSecurityLogWriter();
  }

  @Override
  public LogWriterI18n getLoggerI18n() {
    return this.system.getInternalLogWriter();
  }

  @Override
  public LogWriterI18n getSecurityLoggerI18n() {
    return this.system.getSecurityInternalLogWriter();
  }

  @Override
  public InternalLogWriter getInternalLogWriter() {
    return this.system.getInternalLogWriter();
  }

  @Override
  public InternalLogWriter getSecurityInternalLogWriter() {
    return this.system.getSecurityInternalLogWriter();
  }

  /**
   * get the threadId/sequenceId sweeper task for this cache
   *
   * @return the sweeper task
   */
  @Override
  public EventTrackerExpiryTask getEventTrackerTask() {
    return this.recordedEventSweeper;
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    return this.cachePerfStats;
  }

  @Override
  public String getName() {
    return this.system.getName();
  }

  /**
   * Get the list of all instances of properties for Declarables with the given class name.
   *
   * @param className Class name of the declarable
   * @return List of all instances of properties found for the given declarable
   */
  @Override
  public List<Properties> getDeclarableProperties(final String className) {
    List<Properties> propertiesList = new ArrayList<>();
    synchronized (this.declarablePropertiesMap) {
      for (Entry<Declarable, Properties> entry : this.declarablePropertiesMap.entrySet()) {
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
  @Override
  public Properties getDeclarableProperties(final Declarable declarable) {
    return this.declarablePropertiesMap.get(declarable);
  }

  /**
   * Returns the number of seconds that have elapsed since the Cache was created.
   *
   * @since GemFire 3.5
   */
  public int getUpTime() {
    return (int) (System.currentTimeMillis() - this.creationDate.getTime()) / 1000;
  }

  /**
   * All entry and region operations should be using this time rather than
   * System.currentTimeMillis(). Specially all version stamps/tags must be populated with this
   * timestamp.
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

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return createRegion(name, aRegionAttributes);
  }

  private PoolFactory createDefaultPF() {
    PoolFactory defaultPoolFactory = PoolManager.createFactory();
    try {
      String localHostName = SocketCreator.getHostName(SocketCreator.getLocalHost());
      defaultPoolFactory.addServer(localHostName, CacheServer.DEFAULT_PORT);
    } catch (UnknownHostException ex) {
      throw new IllegalStateException("Could not determine local host name", ex);
    }
    return defaultPoolFactory;
  }

  private Pool findFirstCompatiblePool(Map<String, Pool> pools) {
    // act as if the default pool was configured
    // and see if we can find an existing one that is compatible
    PoolFactoryImpl pfi = (PoolFactoryImpl) createDefaultPF();
    for (Pool p : pools.values()) {
      if (((PoolImpl) p).isCompatible(pfi.getPoolAttributes())) {
        return p;
      }
    }
    return null;
  }

  private void addLocalHostAsServer(PoolFactory poolFactory) {
    PoolFactoryImpl poolFactoryImpl = (PoolFactoryImpl) poolFactory;
    if (poolFactoryImpl.getPoolAttributes().locators.isEmpty()
        && poolFactoryImpl.getPoolAttributes().servers.isEmpty()) {
      try {
        String localHostName = SocketCreator.getHostName(SocketCreator.getLocalHost());
        poolFactoryImpl.addServer(localHostName, CacheServer.DEFAULT_PORT);
      } catch (UnknownHostException ex) {
        throw new IllegalStateException("Could not determine local host name", ex);
      }
    }
  }

  /**
   * Used to set the default pool on a new GemFireCache.
   */
  public synchronized void determineDefaultPool() {
    if (!isClient()) {
      throw new UnsupportedOperationException();
    }
    PoolFactory defaultPoolFactory = this.poolFactory;

    Pool pool = null;
    // create the pool if it does not already exist
    if (defaultPoolFactory == null) {
      Map<String, Pool> pools = PoolManager.getAll();
      if (pools.isEmpty()) {
        defaultPoolFactory = createDefaultPF();
      } else if (pools.size() == 1) {
        // otherwise use a singleton.
        pool = pools.values().iterator().next();
      } else {
        pool = findFirstCompatiblePool(pools);
        if (pool == null) {
          // if pool is still null then we will not have a default pool for this ClientCache
          this.defaultPool = null;
          return;
        }
      }
    } else {
      addLocalHostAsServer(defaultPoolFactory);

      // look for a pool that already exists that is compatible with
      // our PoolFactory.
      // If we don't find one we will create a new one that meets our needs.
      Map<String, Pool> pools = PoolManager.getAll();
      for (Pool p : pools.values()) {
        if (((PoolImpl) p)
            .isCompatible(((PoolFactoryImpl) defaultPoolFactory).getPoolAttributes())) {
          pool = p;
          break;
        }
      }
    }
    if (pool == null) {
      // create our pool with a unique name
      String poolName = DEFAULT_POOL_NAME;
      int count = 1;
      Map<String, Pool> pools = PoolManager.getAll();
      while (pools.containsKey(poolName)) {
        poolName = DEFAULT_POOL_NAME + count;
        count++;
      }
      pool = defaultPoolFactory.create(poolName);
    }
    this.defaultPool = pool;
  }

  /**
   * Determine whether the specified pool factory matches the pool factory used by this cache.
   *
   * @param poolFactory Prospective pool factory.
   * @throws IllegalStateException When the specified pool factory does not match.
   */
  public void validatePoolFactory(PoolFactory poolFactory) {
    // If the specified pool factory is null, by definition there is no pool factory to validate.
    if (poolFactory != null && !Objects.equals(this.poolFactory, poolFactory)) {
      throw new IllegalStateException("Existing cache's default pool was not compatible");
    }
  }

  @Override
  public <K, V> Region<K, V> createRegion(String name, RegionAttributes<K, V> aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    throwIfClient();
    return basicCreateRegion(name, aRegionAttributes);
  }

  public <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException {
    try {
      InternalRegionArguments ira = new InternalRegionArguments().setDestroyLockFlag(true)
          .setRecreateFlag(false).setSnapshotInputStream(null).setImageTarget(null);

      if (attrs instanceof UserSpecifiedRegionAttributes) {
        ira.setIndexes(((UserSpecifiedRegionAttributes) attrs).getIndexes());
      }
      return createVMRegion(name, attrs, ira);
    } catch (IOException | ClassNotFoundException e) {
      // only if loading snapshot, not here
      throw new InternalGemFireError(
          "unexpected exception", e);
    }
  }

  // TODO: createVMRegion method is too complex for IDE to analyze
  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {

    if (getMyId().getVmKind() == ClusterDistributionManager.LOCATOR_DM_TYPE) {
      if (!internalRegionArgs.isUsedForMetaRegion()
          && internalRegionArgs.getInternalMetaRegion() == null) {
        throw new IllegalStateException("Regions can not be created in a locator.");
      }
    }
    this.stopper.checkCancelInProgress(null);
    RegionNameValidation.validate(name, internalRegionArgs);
    RegionAttributes<K, V> attrs = p_attrs;
    attrs = invokeRegionBefore(null, name, attrs, internalRegionArgs);
    if (attrs == null) {
      throw new IllegalArgumentException(
          "Attributes must not be null");
    }

    InternalRegion region;
    final InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();
    final boolean recreate = internalRegionArgs.getRecreateFlag();

    final boolean isPartitionedRegion = attrs.getPartitionAttributes() != null;
    final boolean isReInitCreate = snapshotInputStream != null || imageTarget != null || recreate;

    try {
      for (;;) {
        getCancelCriterion().checkCancelInProgress(null);

        Future<InternalRegion> future = null;
        synchronized (this.rootRegions) {
          region = this.rootRegions.get(name);
          if (region != null) {
            throw new RegionExistsException(region);
          }
          // check for case where a root region is being reinitialized and we
          // didn't
          // find a region, i.e. the new region is about to be created

          if (!isReInitCreate) { // fix bug 33523
            String fullPath = Region.SEPARATOR + name;
            future = (Future) this.reinitializingRegions.get(fullPath);
          }
          if (future == null) {
            if (internalRegionArgs.getInternalMetaRegion() != null) {
              region = internalRegionArgs.getInternalMetaRegion();
            } else if (isPartitionedRegion) {
              region = new PartitionedRegion(name, attrs, null, this, internalRegionArgs);
            } else {
              // Abstract region depends on the default pool existing so lazily initialize it
              // if necessary.
              if (Objects.equals(attrs.getPoolName(), DEFAULT_POOL_NAME)) {
                determineDefaultPool();
              }
              if (attrs.getScope().isLocal()) {
                region = new LocalRegion(name, attrs, null, this, internalRegionArgs);
              } else {
                region = new DistributedRegion(name, attrs, null, this, internalRegionArgs);
              }
            }

            this.rootRegions.put(name, region);
            if (isReInitCreate) {
              regionReinitialized(region);
            }
            break;
          }
        } // synchronized

        boolean interrupted = Thread.interrupted();
        try { // future != null
          throw new RegionExistsException(future.get());
        } catch (InterruptedException ignore) {
          interrupted = true;
        } catch (ExecutionException e) {
          throw new Error("unexpected exception",
              e);
        } catch (CancellationException e) {
          // future was cancelled
          if (logger.isTraceEnabled()) {
            logger.trace("future cancelled", e);
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for

      boolean success = false;
      try {
        setRegionByPath(region.getFullPath(), region);
        region.preInitialize();
        region.initialize(snapshotInputStream, imageTarget, internalRegionArgs);
        success = true;
      } catch (CancelException | RedundancyAlreadyMetException e) {
        // don't print a call stack
        throw e;
      } catch (RuntimeException validationException) {
        logger.warn(String.format("Initialization failed for Region %s", region.getFullPath()),
            validationException);
        throw validationException;
      } finally {
        if (!success) {
          try {
            // do this before removing the region from
            // the root set to fix bug 41982.
            region.cleanupFailedInitialization();
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            SystemFailure.checkFailure();
            this.stopper.checkCancelInProgress(t);

            // bug #44672 - log the failure but don't override the original exception
            logger.warn(String.format("Initialization failed for Region %s",
                region.getFullPath()),
                t);

          } finally {
            // clean up if initialize fails for any reason
            setRegionByPath(region.getFullPath(), null);
            synchronized (this.rootRegions) {
              Region rootRegion = this.rootRegions.get(name);
              if (rootRegion == region) {
                this.rootRegions.remove(name);
              }
            } // synchronized
          }
        } // success
      }

      region.postCreateRegion();
    } catch (RegionExistsException ex) {
      // outside of sync make sure region is initialized to fix bug 37563
      InternalRegion internalRegion = (InternalRegion) ex.getRegion();
      internalRegion.waitOnInitialization(); // don't give out ref until initialized
      throw ex;
    }

    invokeRegionAfter(region);

    // Added for M&M . Putting the callback here to avoid creating RegionMBean in case of Exception
    if (!region.isInternalRegion()) {
      this.system.handleResourceEvent(ResourceEvent.REGION_CREATE, region);
    }

    return region;
  }

  @Override
  public <K, V> RegionAttributes<K, V> invokeRegionBefore(InternalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs) {
    for (RegionListener listener : this.regionListeners) {
      attrs =
          (RegionAttributes<K, V>) listener.beforeCreate(parent, name, attrs, internalRegionArgs);
    }
    return attrs;
  }

  @Override
  public void invokeRegionAfter(InternalRegion region) {
    for (RegionListener listener : this.regionListeners) {
      listener.afterCreate(region);
    }
  }

  @Override
  public void invokeBeforeDestroyed(InternalRegion region) {
    for (RegionListener listener : this.regionListeners) {
      listener.beforeDestroyed(region);
    }
  }

  @Override
  public void invokeCleanupFailedInitialization(InternalRegion region) {
    for (RegionListener listener : this.regionListeners) {
      listener.cleanupFailedInitialization(region);
    }
  }

  @Override
  public Region getRegion(String path) {
    return getRegion(path, false);
  }

  /**
   * returns a set of all current regions in the cache, including buckets
   *
   * @since GemFire 6.0
   */
  public Set<InternalRegion> getAllRegions() {
    Set<InternalRegion> result = new HashSet<>();
    synchronized (this.rootRegions) {
      for (Region region : this.rootRegions.values()) {
        if (region instanceof PartitionedRegion) {
          PartitionedRegion partitionedRegion = (PartitionedRegion) region;
          PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
          if (dataStore != null) {
            Set<Entry<Integer, BucketRegion>> bucketEntries =
                partitionedRegion.getDataStore().getAllLocalBuckets();
            for (Entry entry : bucketEntries) {
              result.add((InternalRegion) entry.getValue());
            }
          }
        } else if (region instanceof InternalRegion) {
          InternalRegion internalRegion = (InternalRegion) region;
          result.add(internalRegion);
          result.addAll(internalRegion.basicSubregions(true));
        }
      }
    }
    return result;
  }

  @Override
  public Set<InternalRegion> getApplicationRegions() {
    Set<InternalRegion> result = new HashSet<>();
    synchronized (this.rootRegions) {
      for (Object region : this.rootRegions.values()) {
        InternalRegion internalRegion = (InternalRegion) region;
        if (internalRegion.isInternalRegion()) {
          continue; // Skip internal regions
        }
        result.add(internalRegion);
        result.addAll(internalRegion.basicSubregions(true));
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean hasPersistentRegion() {
    synchronized (this.rootRegions) {
      for (InternalRegion region : this.rootRegions.values()) {
        if (region.getDataPolicy().withPersistence()) {
          return true;
        }
        for (InternalRegion subRegion : (Set<InternalRegion>) region.basicSubregions(true)) {
          if (subRegion.getDataPolicy().withPersistence()) {
            return true;
          }
        }
      }
      return false;
    }
  }

  @Override
  public void setRegionByPath(String path, InternalRegion r) {
    if (r == null) {
      this.pathToRegion.remove(path);
    } else {
      this.pathToRegion.put(path, r);
    }
  }

  /**
   * @throws IllegalArgumentException if path is not valid
   */
  private static void validatePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException(
          "path cannot be null");
    }
    if (path.isEmpty()) {
      throw new IllegalArgumentException(
          "path cannot be empty");
    }
    if (path.equals(Region.SEPARATOR)) {
      throw new IllegalArgumentException(
          String.format("path cannot be ' %s '", Region.SEPARATOR));
    }
  }

  @Override
  public InternalRegion getRegionByPath(String path) {
    validatePath(path); // fix for bug 34892

    // do this before checking the pathToRegion map
    InternalRegion result = getReinitializingRegion(path);
    if (result != null) {
      return result;
    }
    return this.pathToRegion.get(path);
  }

  @Override
  public InternalRegion getRegionByPathForProcessing(String path) {
    InternalRegion result = getRegionByPath(path);
    if (result == null) {
      this.stopper.checkCancelInProgress(null);
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT); // go through
      // initialization latches
      try {
        String[] pathParts = parsePath(path);
        InternalRegion rootRegion;
        synchronized (this.rootRegions) {
          rootRegion = this.rootRegions.get(pathParts[0]);
          if (rootRegion == null)
            return null;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, calling getSubregion on rootRegion({}): {}",
              pathParts[0], pathParts[1]);
        }
        result = (InternalRegion) rootRegion.getSubregion(pathParts[1], true);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
    return result;
  }

  /**
   * @param returnDestroyedRegion if true, okay to return a destroyed region
   */
  @Override
  public Region getRegion(String path, boolean returnDestroyedRegion) {
    this.stopper.checkCancelInProgress(null);

    InternalRegion result = getRegionByPath(path);
    // Do not waitOnInitialization() for PR
    if (result != null) {
      result.waitOnInitialization();
      if (!returnDestroyedRegion && result.isDestroyed()) {
        this.stopper.checkCancelInProgress(null);
        return null;
      } else {
        return result;
      }
    }

    String[] pathParts = parsePath(path);
    InternalRegion rootRegion;
    synchronized (this.rootRegions) {
      rootRegion = this.rootRegions.get(pathParts[0]);
      if (rootRegion == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, no region found for {}", pathParts[0]);
        }
        this.stopper.checkCancelInProgress(null);
        return null;
      }
      if (!returnDestroyedRegion && rootRegion.isDestroyed()) {
        this.stopper.checkCancelInProgress(null);
        return null;
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("GemFireCache.getRegion, calling getSubregion on rootRegion({}): {}",
          pathParts[0], pathParts[1]);
    }
    return rootRegion.getSubregion(pathParts[1], returnDestroyedRegion);
  }

  /** Return true if this region is initializing */
  @Override
  public boolean isGlobalRegionInitializing(String fullPath) {
    this.stopper.checkCancelInProgress(null);
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT); // go through
    // initialization latches
    try {
      return isGlobalRegionInitializing((InternalRegion) getRegion(fullPath));
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  /** Return true if this region is initializing */
  private boolean isGlobalRegionInitializing(InternalRegion region) {
    boolean result = region != null && region.getScope().isGlobal() && !region.isInitialized();
    if (result) {
      if (logger.isDebugEnabled()) {
        logger.debug("GemFireCache.isGlobalRegionInitializing ({})", region.getFullPath());
      }
    }
    return result;
  }

  @Override
  public Set<Region<?, ?>> rootRegions() {
    return rootRegions(false);
  }

  public Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions) {
    return rootRegions(includePRAdminRegions, true);
  }

  private Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions, boolean waitForInit) {
    this.stopper.checkCancelInProgress(null);
    Set<Region<?, ?>> regions = new HashSet<>();
    synchronized (this.rootRegions) {
      for (InternalRegion region : this.rootRegions.values()) {
        // If this is an internal meta-region, don't return it to end user
        if (region.isSecret() || region.isUsedForMetaRegion()
            || !includePRAdminRegions && (region.isUsedForPartitionedRegionAdmin()
                || region.isUsedForPartitionedRegionBucket())) {
          continue; // Skip administrative PartitionedRegions
        }
        regions.add(region);
      }
    }
    if (waitForInit) {
      for (Iterator<Region<?, ?>> iterator = regions.iterator(); iterator.hasNext();) {
        InternalRegion region = (InternalRegion) iterator.next();
        if (!region.checkForInitialization()) {
          iterator.remove();
        }
      }
    }
    return Collections.unmodifiableSet(regions);
  }

  /**
   * Called by notifier when a client goes away
   *
   * @since GemFire 5.7
   */
  @Override
  public void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client) {
    try {
      if (isClosed()) {
        return;
      }
      for (Object region : rootRegions(false, false)) {
        InternalRegion internalRegion = (InternalRegion) region;
        internalRegion.cleanupForClient(ccn, client);
      }
    } catch (DistributedSystemDisconnectedException ignore) {
    }
  }

  private boolean isInitialized() {
    return this.isInitialized;
  }

  @Override
  public boolean isClosed() {
    return this.isClosing;
  }

  @Override
  public int getLockTimeout() {
    return this.lockTimeout;
  }

  @Override
  public void setLockTimeout(int seconds) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    this.lockTimeout = seconds;
  }

  @Override
  public int getLockLease() {
    return this.lockLease;
  }

  @Override
  public void setLockLease(int seconds) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    this.lockLease = seconds;
  }

  @Override
  public int getSearchTimeout() {
    return this.searchTimeout;
  }

  @Override
  public void setSearchTimeout(int seconds) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    this.searchTimeout = seconds;
  }

  @Override
  public int getMessageSyncInterval() {
    return HARegionQueue.getMessageSyncInterval();
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    if (seconds < 0) {
      throw new IllegalArgumentException(
          "The 'messageSyncInterval' property for cache cannot be negative");
    }
    HARegionQueue.setMessageSyncInterval(seconds);
  }

  /**
   * Get a reference to a Region that is reinitializing, or null if that Region is not
   * reinitializing or this thread is interrupted. If a reinitializing region is found, then this
   * method blocks until reinitialization is complete and then returns the region.
   */
  @Override
  public InternalRegion getReinitializingRegion(String fullPath) {
    Future future = (Future) this.reinitializingRegions.get(fullPath);
    if (future == null) {
      return null;
    }
    try {
      InternalRegion region = (InternalRegion) future.get();
      region.waitOnInitialization();
      if (logger.isDebugEnabled()) {
        logger.debug("Returning manifested future for: {}", fullPath);
      }
      return region;
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException e) {
      throw new Error("unexpected exception", e);
    } catch (CancellationException ignore) {
      // future was cancelled
      logger.debug("future cancelled, returning null");
      return null;
    }
  }

  /**
   * Register the specified region name as reinitializing, creating and adding a Future for it to
   * the map.
   *
   * @throws IllegalStateException if there is already a region by that name registered.
   */
  @Override
  public void regionReinitializing(String fullPath) {
    Object old = this.reinitializingRegions.putIfAbsent(fullPath, new FutureResult(this.stopper));
    if (old != null) {
      throw new IllegalStateException(
          String.format("Found an existing reinitalizing region named %s",
              fullPath));
    }
  }

  /**
   * Set the reinitialized region and unregister it as reinitializing.
   *
   * @throws IllegalStateException if there is no region by that name registered as reinitializing.
   */
  @Override
  public void regionReinitialized(Region region) {
    String regionName = region.getFullPath();
    FutureResult future = (FutureResult) this.reinitializingRegions.get(regionName);
    if (future == null) {
      throw new IllegalStateException(
          String.format("Could not find a reinitializing region named %s",
              regionName));
    }
    future.set(region);
    unregisterReinitializingRegion(regionName);
  }

  /**
   * Clear a reinitializing region, e.g. reinitialization failed.
   *
   * @throws IllegalStateException if cannot find reinitializing region registered by that name.
   */
  @Override
  public void unregisterReinitializingRegion(String fullPath) {
    this.reinitializingRegions.remove(fullPath);
  }

  /**
   * Returns true if get should give a copy; false if a reference.
   *
   * @since GemFire 4.0
   */
  @Override
  public boolean isCopyOnRead() {
    return this.copyOnRead;
  }

  /**
   * Implementation of {@link Cache#setCopyOnRead}
   *
   * @since GemFire 4.0
   */
  @Override
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
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

  /**
   * Remove the specified root region
   *
   * @param rootRgn the region to be removed
   * @return true if root region was removed, false if not found
   */
  @Override
  public boolean removeRoot(InternalRegion rootRgn) {
    synchronized (this.rootRegions) {
      String regionName = rootRgn.getName();
      InternalRegion found = this.rootRegions.get(regionName);
      if (found == rootRgn) {
        InternalRegion previous = this.rootRegions.remove(regionName);
        Assert.assertTrue(previous == rootRgn);
        return true;
      } else
        return false;
    }
  }

  /**
   * @return array of two Strings, the root name and the relative path from root. If there is no
   *         relative path from root, then String[1] will be an empty string
   */
  static String[] parsePath(String path) {
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
   * Makes note of a {@code CacheLifecycleListener}
   */
  public static void addCacheLifecycleListener(CacheLifecycleListener listener) {
    synchronized (GemFireCacheImpl.class) {
      cacheLifecycleListeners.add(listener);
    }
  }

  /**
   * Removes a {@code CacheLifecycleListener}
   *
   * @return Whether or not the listener was removed
   */
  public static boolean removeCacheLifecycleListener(CacheLifecycleListener listener) {
    synchronized (GemFireCacheImpl.class) {
      return cacheLifecycleListeners.remove(listener);
    }
  }

  @Override
  public void addRegionListener(RegionListener regionListener) {
    this.regionListeners.add(regionListener);
  }

  @Override
  public void removeRegionListener(RegionListener regionListener) {
    this.regionListeners.remove(regionListener);
  }

  @Override
  public Set<RegionListener> getRegionListeners() {
    return Collections.unmodifiableSet(this.regionListeners);
  }

  @Override
  public <T extends CacheService> T getService(Class<T> clazz) {
    return clazz.cast(this.services.get(clazz));
  }

  @Override
  public Collection<CacheService> getServices() {
    return Collections.unmodifiableCollection(this.services.values());
  }

  /**
   * Creates the single instance of the Transaction Manager for this cache. Returns the existing one
   * upon request.
   *
   * @return the CacheTransactionManager instance.
   *
   * @since GemFire 4.0
   */
  @Override
  public CacheTransactionManager getCacheTransactionManager() {
    return this.transactionManager;
  }

  /**
   * GuardedBy {@link #ccpTimerMutex}
   *
   * @see CacheClientProxy
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
  @Override
  public SystemTimer getCCPTimer() {
    synchronized (this.ccpTimerMutex) {
      if (this.ccpTimer != null) {
        return this.ccpTimer;
      }
      this.ccpTimer = new SystemTimer(getDistributedSystem(), true);
      if (this.isClosing) {
        this.ccpTimer.cancel(); // poison it, don't throw.
      }
      return this.ccpTimer;
    }
  }

  /**
   * For use by unit tests to inject a mocked ccpTimer
   */
  void setCCPTimer(SystemTimer ccpTimer) {
    this.ccpTimer = ccpTimer;
  }

  static final int PURGE_INTERVAL = 1000;

  private int cancelCount = 0;

  /**
   * Does a periodic purge of the CCPTimer to prevent a large number of cancelled tasks from
   * building up in it. See GEODE-2485.
   */
  @Override
  public void purgeCCPTimer() {
    synchronized (this.ccpTimerMutex) {
      if (this.ccpTimer != null) {
        this.cancelCount++;
        if (this.cancelCount == PURGE_INTERVAL) {
          this.cancelCount = 0;
          this.ccpTimer.timerPurge();
        }
      }
    }
  }

  private final ExpirationScheduler expirationScheduler;

  /**
   * Get cache-wide ExpirationScheduler
   *
   * @return the scheduler, lazily created
   */
  @Override
  public ExpirationScheduler getExpirationScheduler() {
    return this.expirationScheduler;
  }

  @Override
  public TXManagerImpl getTXMgr() {
    return this.transactionManager;
  }

  /**
   * Returns the {@code Executor} (thread pool) that is used to execute cache event listeners.
   * Returns {@code null} if no pool exists.
   *
   * @since GemFire 3.5
   */
  @Override
  public Executor getEventThreadPool() {
    return this.eventThreadPool;
  }

  @Override
  public CacheServer addCacheServer() {
    return addCacheServer(false);
  }

  @Override
  public CacheServer addCacheServer(boolean isGatewayReceiver) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);

    CacheServerImpl cacheServer = new CacheServerImpl(this, isGatewayReceiver);
    this.allCacheServers.add(cacheServer);

    sendAddCacheServerProfileMessage();
    return cacheServer;
  }

  public boolean removeCacheServer(CacheServer cacheServer) {
    boolean removed = this.allCacheServers.remove(cacheServer);
    sendRemoveCacheServerProfileMessage();
    return removed;
  }

  @Override
  public void addGatewaySender(GatewaySender sender) {
    throwIfClient();

    this.stopper.checkCancelInProgress(null);

    synchronized (this.allGatewaySendersLock) {
      if (!this.allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((DistributionAdvisee) sender).distribute(true);
        Set<GatewaySender> newSenders = new HashSet<>(this.allGatewaySenders.size() + 1);
        if (!this.allGatewaySenders.isEmpty()) {
          newSenders.addAll(this.allGatewaySenders);
        }
        newSenders.add(sender);
        this.allGatewaySenders = Collections.unmodifiableSet(newSenders);
      } else {
        throw new IllegalStateException(
            String.format("A GatewaySender with id %s is already defined in this cache.",
                sender.getId()));
      }
    }

    synchronized (this.rootRegions) {
      Set<InternalRegion> applicationRegions = getApplicationRegions();
      for (InternalRegion region : applicationRegions) {
        Set<String> senders = region.getAllGatewaySenderIds();
        if (senders.contains(sender.getId()) && !sender.isParallel()) {
          region.senderCreated();
        }
      }
    }

    if (!sender.isParallel()) {
      Region dynamicMetaRegion = getRegion(DynamicRegionFactory.dynamicRegionListName);
      if (dynamicMetaRegion == null) {
        if (logger.isDebugEnabled()) {
          logger.debug(" The dynamic region is null. ");
        }
      } else {
        dynamicMetaRegion.getAttributesMutator().addGatewaySenderId(sender.getId());
      }
    }
    if (!(sender.getRemoteDSId() < 0)) {
      this.system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_CREATE, sender);
    }
  }

  @Override
  public void removeGatewaySender(GatewaySender sender) {
    throwIfClient();

    this.stopper.checkCancelInProgress(null);

    synchronized (this.allGatewaySendersLock) {
      if (this.allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((DistributionAdvisee) sender, true).distribute(true);
        Set<GatewaySender> newSenders = new HashSet<>(this.allGatewaySenders.size() - 1);
        if (!this.allGatewaySenders.isEmpty()) {
          newSenders.addAll(this.allGatewaySenders);
        }
        newSenders.remove(sender);
        this.allGatewaySenders = Collections.unmodifiableSet(newSenders);
      }
    }
    if (!(sender.getRemoteDSId() < 0)) {
      this.system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_REMOVE, sender);
    }
  }

  @Override
  public void addGatewayReceiver(GatewayReceiver receiver) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    synchronized (this.allGatewayReceiversLock) {
      Set<GatewayReceiver> newReceivers = new HashSet<>(this.allGatewayReceivers.size() + 1);
      if (!this.allGatewayReceivers.isEmpty()) {
        newReceivers.addAll(this.allGatewayReceivers);
      }
      newReceivers.add(receiver);
      this.allGatewayReceivers = Collections.unmodifiableSet(newReceivers);
    }
  }

  public void removeGatewayReceiver(GatewayReceiver receiver) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);
    synchronized (this.allGatewayReceiversLock) {
      Set<GatewayReceiver> newReceivers = new HashSet<>(this.allGatewayReceivers.size() + 1);
      if (!this.allGatewayReceivers.isEmpty()) {
        newReceivers.addAll(this.allGatewayReceivers);
      }
      newReceivers.remove(receiver);
      this.allGatewayReceivers = Collections.unmodifiableSet(newReceivers);
    }
  }

  @Override
  public void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue) {
    this.allAsyncEventQueues.add(asyncQueue);
    if (!asyncQueue.isMetaQueue()) {
      this.allVisibleAsyncEventQueues.add(asyncQueue);
    }
    this.system.handleResourceEvent(ResourceEvent.ASYNCEVENTQUEUE_CREATE, asyncQueue);
  }

  /**
   * Returns List of GatewaySender (excluding the senders for internal use)
   *
   * @return List List of GatewaySender objects
   */
  @Override
  public Set<GatewaySender> getGatewaySenders() {
    Set<GatewaySender> senders = new HashSet<>();
    for (GatewaySender sender : this.allGatewaySenders) {
      if (!((AbstractGatewaySender) sender).isForInternalUse()) {
        senders.add(sender);
      }
    }
    return senders;
  }

  /**
   * Returns List of all GatewaySenders (including the senders for internal use)
   *
   * @return List List of GatewaySender objects
   */
  @Override
  public Set<GatewaySender> getAllGatewaySenders() {
    return this.allGatewaySenders;
  }

  @Override
  public GatewaySender getGatewaySender(String id) {
    for (GatewaySender sender : this.allGatewaySenders) {
      if (sender.getId().equals(id)) {
        return sender;
      }
    }
    return null;
  }

  @Override
  public Set<GatewayReceiver> getGatewayReceivers() {
    return this.allGatewayReceivers;
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return getAsyncEventQueues(true);
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly) {
    return visibleOnly ? this.allVisibleAsyncEventQueues : this.allAsyncEventQueues;
  }

  @Override
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : this.allAsyncEventQueues) {
      if (asyncEventQueue.getId().equals(id)) {
        return asyncEventQueue;
      }
    }
    return null;
  }

  @Override
  public void removeAsyncEventQueue(AsyncEventQueue asyncQueue) {
    throwIfClient();
    // first remove the gateway sender of the queue
    if (asyncQueue instanceof AsyncEventQueueImpl) {
      removeGatewaySender(((AsyncEventQueueImpl) asyncQueue).getSender());
    }
    // using gateway senders lock since async queue uses a gateway sender
    synchronized (this.allGatewaySendersLock) {
      this.allAsyncEventQueues.remove(asyncQueue);
      this.allVisibleAsyncEventQueues.remove(asyncQueue);
    }
    this.system.handleResourceEvent(ResourceEvent.ASYNCEVENTQUEUE_REMOVE, asyncQueue);
  }

  /** get the conflict resolver for WAN */
  @Override
  public GatewayConflictResolver getGatewayConflictResolver() {
    synchronized (this.allGatewayHubsLock) {
      return this.gatewayConflictResolver;
    }
  }

  /** set the conflict resolver for WAN */
  @Override
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    synchronized (this.allGatewayHubsLock) {
      this.gatewayConflictResolver = resolver;
    }
  }

  @Override
  public List<CacheServer> getCacheServers() {
    List<CacheServer> cacheServersWithoutReceiver = null;
    if (!this.allCacheServers.isEmpty()) {
      for (CacheServerImpl cacheServer : this.allCacheServers) {
        // If CacheServer is a GatewayReceiver, don't return as part of CacheServers
        if (!cacheServer.isGatewayReceiver()) {
          if (cacheServersWithoutReceiver == null) {
            cacheServersWithoutReceiver = new ArrayList<>();
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

  @Override
  public List getCacheServersAndGatewayReceiver() {
    return this.allCacheServers;
  }

  /**
   * add a partitioned region to the set of tracked partitioned regions. This is used to notify the
   * regions when this cache requires, or does not require notification of all region/entry events.
   */
  public void addPartitionedRegion(PartitionedRegion region) {
    synchronized (this.partitionedRegions) {
      if (region.isDestroyed()) {
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache#addPartitionedRegion did not add destroyed {}", region);
        }
        return;
      }
      if (this.partitionedRegions.add(region)) {
        getCachePerfStats().incPartitionedRegions(1);
      }
    }
  }

  /**
   * Returns a set of all current partitioned regions for test hook.
   */
  @Override
  public Set<PartitionedRegion> getPartitionedRegions() {
    synchronized (this.partitionedRegions) {
      return new HashSet<>(this.partitionedRegions);
    }
  }

  private SortedMap<String, Map<String, PartitionedRegion>> getPRTrees() {
    // prTree will save a sublist of PRs who are under the same root
    SortedMap<String, PartitionedRegion> prMap = getPartitionedRegionMap();
    boolean hasColocatedRegion = false;
    for (PartitionedRegion pr : prMap.values()) {
      List<PartitionedRegion> childList = ColocationHelper.getColocatedChildRegions(pr);
      if (childList != null && !childList.isEmpty()) {
        hasColocatedRegion = true;
        break;
      }
    }

    TreeMap<String, Map<String, PartitionedRegion>> prTrees = new TreeMap<>();
    if (hasColocatedRegion) {
      Map<String, PartitionedRegion> orderedPrMap = orderByColocation(prMap);
      prTrees.put("ROOT", orderedPrMap);
    } else {
      for (PartitionedRegion pr : prMap.values()) {
        String rootName = pr.getRoot().getName();
        Map<String, PartitionedRegion> prSubMap = prTrees.get(rootName);
        if (prSubMap == null) {
          prSubMap = new TreeMap<>();
          prTrees.put(rootName, prSubMap);
        }
        prSubMap.put(pr.getFullPath(), pr);
      }
    }

    return prTrees;
  }

  private SortedMap<String, PartitionedRegion> getPartitionedRegionMap() {
    SortedMap<String, PartitionedRegion> prMap = new TreeMap<>();
    for (Entry<String, InternalRegion> entry : this.pathToRegion.entrySet()) {
      String regionName = entry.getKey();
      InternalRegion region = entry.getValue();

      // Don't wait for non partitioned regions
      if (!(region instanceof PartitionedRegion)) {
        continue;
      }
      // Do a getRegion to ensure that we wait for the partitioned region
      // to finish initialization
      try {
        Region pr = getRegion(regionName);
        if (pr instanceof PartitionedRegion) {
          prMap.put(regionName, (PartitionedRegion) pr);
        }
      } catch (CancelException ignore) {
        // if some region throws cancel exception during initialization,
        // then no need to shutDownAll them gracefully
      }
    }

    return prMap;
  }

  private Map<String, PartitionedRegion> orderByColocation(Map<String, PartitionedRegion> prMap) {
    LinkedHashMap<String, PartitionedRegion> orderedPrMap = new LinkedHashMap<>();
    for (PartitionedRegion pr : prMap.values()) {
      addColocatedChildRecursively(orderedPrMap, pr);
    }
    return orderedPrMap;
  }

  private void addColocatedChildRecursively(LinkedHashMap<String, PartitionedRegion> prMap,
      PartitionedRegion pr) {
    for (PartitionedRegion colocatedRegion : ColocationHelper.getColocatedChildRegions(pr)) {
      addColocatedChildRecursively(prMap, colocatedRegion);
    }
    prMap.put(pr.getFullPath(), pr);
  }

  /**
   * check to see if any cache components require notification from a partitioned region.
   * Notification adds to the messaging a PR must do on each put/destroy/invalidate operation and
   * should be kept to a minimum
   *
   * @param r the partitioned region
   * @return true if the region should deliver all of its events to this cache
   */
  @Override
  public boolean requiresNotificationFromPR(PartitionedRegion r) {
    boolean hasSerialSenders = hasSerialSenders(r);
    if (!hasSerialSenders) {
      for (CacheServerImpl server : this.allCacheServers) {
        if (!server.getNotifyBySubscription()) {
          hasSerialSenders = true;
          break;
        }
      }

    }
    return hasSerialSenders;
  }

  private boolean hasSerialSenders(PartitionedRegion region) {
    boolean hasSenders = false;
    Set<String> senders = region.getAllGatewaySenderIds();
    for (String sender : senders) {
      GatewaySender gatewaySender = this.getGatewaySender(sender);
      if (gatewaySender != null && !gatewaySender.isParallel()) {
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
  @Override
  public void removePartitionedRegion(PartitionedRegion region) {
    synchronized (this.partitionedRegions) {
      if (this.partitionedRegions.remove(region)) {
        getCachePerfStats().incPartitionedRegions(-1);
      }
    }
  }

  @Override
  public void setIsServer(boolean isServer) {
    throwIfClient();
    this.stopper.checkCancelInProgress(null);

    this.isServer = isServer;
  }

  @Override
  public boolean isServer() {
    if (isClient()) {
      return false;
    }
    this.stopper.checkCancelInProgress(null);

    return this.isServer || !this.allCacheServers.isEmpty();
  }

  @Override
  public InternalQueryService getQueryService() {
    if (!isClient()) {
      return new DefaultQueryService(this);
    }
    Pool defaultPool = getDefaultPool();
    if (defaultPool == null) {
      throw new IllegalStateException(
          "Client cache does not have a default pool. Use getQueryService(String poolName) instead.");
    }
    return (InternalQueryService) defaultPool.getQueryService();
  }

  @Override
  public QueryService getLocalQueryService() {
    return new DefaultQueryService(this);
  }

  /**
   * @return Context jndi context associated with the Cache.
   * @since GemFire 4.0
   */
  @Override
  public Context getJNDIContext() {
    return JNDIInvoker.getJNDIContext();
  }

  /**
   * @return JTA TransactionManager associated with the Cache.
   * @since GemFire 4.0
   */
  @Override
  public TransactionManager getJTATransactionManager() {
    return JNDIInvoker.getTransactionManager();
  }

  /**
   * return the cq/interest information for a given region name, creating one if it doesn't exist
   */
  @Override
  public FilterProfile getFilterProfile(String regionName) {
    InternalRegion r = (InternalRegion) getRegion(regionName, true);
    if (r != null) {
      return r.getFilterProfile();
    }
    return null;
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes(String id) {
    return (RegionAttributes<K, V>) this.namedRegionAttributes.get(id);
  }

  @Override
  public <K, V> void setRegionAttributes(String id, RegionAttributes<K, V> attrs) {
    if (attrs == null) {
      this.namedRegionAttributes.remove(id);
    } else {
      this.namedRegionAttributes.put(id, attrs);
    }
  }

  @Override
  public Map<String, RegionAttributes<?, ?>> listRegionAttributes() {
    return Collections.unmodifiableMap(this.namedRegionAttributes);
  }

  private static final ThreadLocal<GemFireCacheImpl> xmlCache = new ThreadLocal<>();

  @Override
  public void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    // make this cache available to callbacks being initialized during xml create
    final GemFireCacheImpl oldValue = xmlCache.get();
    xmlCache.set(this);

    Reader reader = null;
    Writer stringWriter = null;
    OutputStreamWriter writer = null;

    try {
      CacheXmlParser xml;

      if (XML_PARAMETERIZATION_ENABLED) {
        char[] buffer = new char[1024];
        reader = new BufferedReader(new InputStreamReader(is, "ISO-8859-1"));
        stringWriter = new StringWriter();

        int numChars;
        while ((numChars = reader.read(buffer)) != -1) {
          stringWriter.write(buffer, 0, numChars);
        }

        /*
         * Now replace all replaceable system properties here using {@code PropertyResolver}
         */
        String replacedXmlString = this.resolver.processUnresolvableString(stringWriter.toString());

        /*
         * Turn the string back into the default encoding so that the XML parser can work correctly
         * in the presence of an "encoding" attribute in the XML prolog.
         */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer = new OutputStreamWriter(baos, "ISO-8859-1");
        writer.write(replacedXmlString);
        writer.flush();

        xml = CacheXmlParser.parse(new ByteArrayInputStream(baos.toByteArray()));
      } else {
        xml = CacheXmlParser.parse(is);
      }
      xml.create(this);
    } catch (IOException e) {
      throw new CacheXmlException(
          "Input Stream could not be read for system property substitutions.", e);
    } finally {
      xmlCache.set(oldValue);
      closeQuietly(reader);
      closeQuietly(stringWriter);
      closeQuietly(writer);
    }
  }

  private static void closeQuietly(Closeable closeable) {
    try {
      if (closeable != null) {
        closeable.close();
      }
    } catch (IOException ignore) {
    }
  }

  @Override
  public void readyForEvents() {
    if (isClient()) {
      // If a durable client has been configured...
      if (Objects.nonNull(system) && Objects.nonNull(system.getConfig())
          && !Objects.equals(DistributionConfig.DEFAULT_DURABLE_CLIENT_ID,
              Objects.toString(system.getConfig().getDurableClientId(),
                  DistributionConfig.DEFAULT_DURABLE_CLIENT_ID))) {
        // Ensure that there is a pool to use for readyForEvents().
        if (Objects.isNull(defaultPool)) {
          determineDefaultPool();
        }
      }
    }
    PoolManagerImpl.readyForEvents(this.system, false);
  }

  private List<File> backupFiles = Collections.emptyList();

  @Override
  public ResourceManager getResourceManager() {
    return getInternalResourceManager(true);
  }

  @Override
  public InternalResourceManager getInternalResourceManager() {
    return getInternalResourceManager(true);
  }

  @Override
  public InternalResourceManager getInternalResourceManager(boolean checkCancellationInProgress) {
    if (checkCancellationInProgress) {
      this.stopper.checkCancelInProgress(null);
    }
    return this.resourceManager;
  }

  public void setBackupFiles(List<File> backups) {
    this.backupFiles = backups;
  }

  @Override
  public List<File> getBackupFiles() {
    return Collections.unmodifiableList(this.backupFiles);
  }

  @Override
  public BackupService getBackupService() {
    return backupService;
  }

  // TODO make this a simple int guarded by riWaiters and get rid of the double-check
  private final AtomicInteger registerInterestsInProgress = new AtomicInteger();

  private final List<SimpleWaiter> riWaiters = new ArrayList<>();

  // never changes but is currently only initialized in constructor by unit tests
  private TypeRegistry pdxRegistry;

  /**
   * update stats for completion of a registerInterest operation
   */
  @Override
  public void registerInterestCompleted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    if (GemFireCacheImpl.this.isClosing) {
      return; // just get out, all of the SimpleWaiters will die of their own accord
    }
    int numInProgress = this.registerInterestsInProgress.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestCompleted: new value = {}", numInProgress);
    }
    if (numInProgress == 0) {
      synchronized (this.riWaiters) {
        // TODO: get rid of double-check
        numInProgress = this.registerInterestsInProgress.get();
        if (numInProgress == 0) { // all clear
          if (logger.isDebugEnabled()) {
            logger.debug("registerInterestCompleted: Signalling end of register-interest");
          }
          for (SimpleWaiter sw : this.riWaiters) {
            sw.doNotify();
          }
          this.riWaiters.clear();
        } // all clear
      } // synchronized
    }
  }

  @Override
  public void registerInterestStarted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    int newVal = this.registerInterestsInProgress.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestsStarted: new count = {}", newVal);
    }
  }

  /**
   * Blocks until no register interests are in progress.
   */
  @Override
  public void waitForRegisterInterestsInProgress() {
    // In *this* particular context, let the caller know that
    // its cache has been cancelled. doWait below would do that as
    // well, so this is just an early out.
    getCancelCriterion().checkCancelInProgress(null);

    int count = this.registerInterestsInProgress.get();
    if (count > 0) {
      SimpleWaiter simpleWaiter = null;
      synchronized (this.riWaiters) {
        // TODO double-check
        count = this.registerInterestsInProgress.get();
        if (count > 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("waitForRegisterInterestsInProgress: count ={}", count);
          }
          simpleWaiter = new SimpleWaiter();
          this.riWaiters.add(simpleWaiter);
        }
      } // synchronized
      if (simpleWaiter != null) {
        simpleWaiter.doWait();
      }
    }
  }

  @Override
  @SuppressWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void setQueryMonitorRequiredForResourceManager(boolean required) {
    queryMonitorRequiredForResourceManager = required;
  }

  @Override
  public boolean isQueryMonitorDisabledForLowMemory() {
    return this.queryMonitorDisabledForLowMem;
  }

  /**
   * Returns the QueryMonitor instance based on system property MAX_QUERY_EXECUTION_TIME.
   *
   * @since GemFire 6.0
   */
  @Override
  public QueryMonitor getQueryMonitor() {
    // Check to see if monitor is required if ResourceManager critical heap percentage is set
    // or whether we override it with the system variable;
    boolean monitorRequired =
        !this.queryMonitorDisabledForLowMem && queryMonitorRequiredForResourceManager;
    // Added for DUnit test purpose, which turns-on and off the this.testMaxQueryExecutionTime.
    if (!(MAX_QUERY_EXECUTION_TIME > 0 || monitorRequired)) {
      // if this.testMaxQueryExecutionTime is set, send the QueryMonitor.
      // Else send null, so that the QueryMonitor is turned-off.
      return null;
    }

    // Return the QueryMonitor service if MAX_QUERY_EXECUTION_TIME is set or it is required by the
    // ResourceManager and not overridden by system property.
    boolean needQueryMonitor = MAX_QUERY_EXECUTION_TIME > 0 || monitorRequired;
    if (needQueryMonitor && this.queryMonitor == null) {
      synchronized (this.queryMonitorLock) {
        if (this.queryMonitor == null) {
          int maxTime = MAX_QUERY_EXECUTION_TIME;

          if (monitorRequired && maxTime < 0) {
            // this means that the resource manager is being used and we need to monitor query
            // memory usage
            // If no max execution time has been set, then we will default to five hours
            maxTime = FIVE_HOURS;
          }

          this.queryMonitor =
              new QueryMonitor(() -> (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
                  QUERY_MONITOR_THREAD_POOL_SIZE,
                  (runnable) -> new LoggingThread("QueryMonitor Thread", runnable)),
                  this,
                  maxTime);
          if (logger.isDebugEnabled()) {
            logger.debug("QueryMonitor thread started.");
          }
        }
      }
    }
    return this.queryMonitor;
  }

  /**
   * Simple class to allow waiters for register interest. Has at most one thread that ever calls
   * wait.
   *
   * @since GemFire 5.7
   */
  private class SimpleWaiter {
    private boolean notified;

    SimpleWaiter() {}

    void doWait() {
      synchronized (this) {
        while (!this.notified) {
          getCancelCriterion().checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            wait(1000);
          } catch (InterruptedException ignore) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    }

    void doNotify() {
      synchronized (this) {
        this.notified = true;
        notifyAll();
      }
    }
  }

  private void sendAddCacheServerProfileMessage() {
    Set otherMembers = this.dm.getOtherDistributionManagerIds();
    AddCacheServerProfileMessage message = new AddCacheServerProfileMessage();
    message.operateOnLocalCache(this);
    if (!otherMembers.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending add cache server profile message to other members.");
      }
      ReplyProcessor21 replyProcessor = new ReplyProcessor21(this.dm, otherMembers);
      message.setRecipients(otherMembers);
      message.processorId = replyProcessor.getProcessorId();
      this.dm.putOutgoing(message);

      // Wait for replies.
      try {
        replyProcessor.waitForReplies();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }


  private void sendRemoveCacheServerProfileMessage() {
    Set otherMembers = this.dm.getOtherDistributionManagerIds();
    RemoveCacheServerProfileMessage message = new RemoveCacheServerProfileMessage();
    message.operateOnLocalCache(this);

    // Remove this while loop when we release GEODE 2.0
    // This block prevents sending a message to old members that do not know about
    // the RemoveCacheServerProfileMessage
    Iterator memberIterator = otherMembers.iterator();
    while (memberIterator.hasNext()) {
      InternalDistributedMember member = (InternalDistributedMember) memberIterator.next();
      if (Version.GEODE_150.compareTo(member.getVersionObject()) > 0) {
        memberIterator.remove();
      }
    }

    if (!otherMembers.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending remove cache server profile message to other members.");
      }
      ReplyProcessor21 replyProcessor = new ReplyProcessor21(this.dm, otherMembers);
      message.setRecipients(otherMembers);
      message.processorId = replyProcessor.getProcessorId();
      this.dm.putOutgoing(message);

      // Wait for replies.
      try {
        replyProcessor.waitForReplies();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public TXManagerImpl getTxManager() {
    return this.transactionManager;
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionShortcut shortcut) {
    throwIfClient();
    return new RegionFactoryImpl<>(this, shortcut);
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory() {
    throwIfClient();
    return new RegionFactoryImpl<>(this);
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(String regionAttributesId) {
    throwIfClient();
    return new RegionFactoryImpl<>(this, regionAttributesId);
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> RegionFactory<K, V> createRegionFactory(RegionAttributes<K, V> regionAttributes) {
    throwIfClient();
    return new RegionFactoryImpl<>(this, regionAttributes);
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(ClientRegionShortcut shortcut) {
    return new ClientRegionFactoryImpl<>(this, shortcut);
  }

  @Override
  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(String regionAttributesId) {
    return new ClientRegionFactoryImpl<>(this, regionAttributesId);
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public QueryService getQueryService(String poolName) {
    Pool pool = PoolManager.find(poolName);
    if (pool == null) {
      throw new IllegalStateException("Could not find a pool named " + poolName);
    } else {
      return pool.getQueryService();
    }
  }

  @Override
  public RegionService createAuthenticatedView(Properties userSecurityProperties) {
    Pool pool = getDefaultPool();
    if (pool == null) {
      throw new IllegalStateException("This cache does not have a default pool");
    }
    return createAuthenticatedCacheView(pool, userSecurityProperties);
  }

  @Override
  public RegionService createAuthenticatedView(Properties userSecurityProperties, String poolName) {
    Pool pool = PoolManager.find(poolName);
    if (pool == null) {
      throw new IllegalStateException("Pool " + poolName + " does not exist");
    }
    return createAuthenticatedCacheView(pool, userSecurityProperties);
  }

  private static RegionService createAuthenticatedCacheView(Pool pool, Properties properties) {
    if (pool.getMultiuserAuthentication()) {
      return ((PoolImpl) pool).createAuthenticatedCacheView(properties);
    } else {
      throw new IllegalStateException(
          "The pool " + pool.getName() + " did not have multiuser-authentication set to true");
    }
  }

  public static void initializeRegionShortcuts(Cache cache) {
    for (RegionShortcut shortcut : RegionShortcut.values()) {
      switch (shortcut) {
        case PARTITION: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_REDUNDANT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_PERSISTENT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_REDUNDANT_PERSISTENT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_REDUNDANT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_PERSISTENT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_REDUNDANT_PERSISTENT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_REDUNDANT_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE_PERSISTENT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE_PERSISTENT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.REPLICATE);
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setScope(Scope.LOCAL);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_PERSISTENT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setScope(Scope.LOCAL);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setScope(Scope.LOCAL);
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setScope(Scope.LOCAL);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_PERSISTENT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setScope(Scope.LOCAL);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_PROXY: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setLocalMaxMemory(0);
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PARTITION_PROXY_REDUNDANT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PARTITION);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setLocalMaxMemory(0);
          paf.setRedundantCopies(1);
          af.setPartitionAttributes(paf.create());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case REPLICATE_PROXY: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.EMPTY);
          af.setScope(Scope.DISTRIBUTED_ACK);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        default:
          throw new IllegalStateException("unhandled enum " + shortcut);
      }
    }
  }

  public static void initializeClientRegionShortcuts(Cache cache) {
    for (ClientRegionShortcut shortcut : ClientRegionShortcut.values()) {
      switch (shortcut) {
        case LOCAL: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_PERSISTENT: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case LOCAL_PERSISTENT_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          cache.setRegionAttributes(shortcut.toString(), af.create());
          break;
        }
        case PROXY: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.EMPTY);
          UserSpecifiedRegionAttributes<?, ?> attributes =
              (UserSpecifiedRegionAttributes) af.create();
          attributes.requiresPoolName = true;
          cache.setRegionAttributes(shortcut.toString(), attributes);
          break;
        }
        case CACHING_PROXY: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          UserSpecifiedRegionAttributes<?, ?> attributes =
              (UserSpecifiedRegionAttributes) af.create();
          attributes.requiresPoolName = true;
          cache.setRegionAttributes(shortcut.toString(), attributes);
          break;
        }
        case CACHING_PROXY_HEAP_LRU: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes());
          UserSpecifiedRegionAttributes<?, ?> attributes =
              (UserSpecifiedRegionAttributes) af.create();
          attributes.requiresPoolName = true;
          cache.setRegionAttributes(shortcut.toString(), attributes);
          break;
        }
        case CACHING_PROXY_OVERFLOW: {
          AttributesFactory<?, ?> af = new AttributesFactory();
          af.setDataPolicy(DataPolicy.NORMAL);
          af.setEvictionAttributes(
              EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.OVERFLOW_TO_DISK));
          UserSpecifiedRegionAttributes<?, ?> attributes =
              (UserSpecifiedRegionAttributes) af.create();
          attributes.requiresPoolName = true;
          cache.setRegionAttributes(shortcut.toString(), attributes);
          break;
        }
        default:
          throw new IllegalStateException("unhandled enum " + shortcut);
      }
    }
  }

  @Override
  public void beginDestroy(String path, DistributedRegion region) {
    this.regionsInDestroy.putIfAbsent(path, region);
  }

  @Override
  public void endDestroy(String path, DistributedRegion region) {
    this.regionsInDestroy.remove(path, region);
  }

  @Override
  public DistributedRegion getRegionInDestroy(String path) {
    return this.regionsInDestroy.get(path);
  }

  @Override
  public TombstoneService getTombstoneService() {
    return this.tombstoneService;
  }

  @Override
  public TypeRegistry getPdxRegistry() {
    return this.pdxRegistry;
  }

  @Override
  public boolean getPdxReadSerialized() {
    return this.cacheConfig.pdxReadSerialized;
  }

  @Override
  public PdxSerializer getPdxSerializer() {
    return this.cacheConfig.pdxSerializer;
  }

  @Override
  public String getPdxDiskStore() {
    return this.cacheConfig.pdxDiskStore;
  }

  @Override
  public boolean getPdxPersistent() {
    return this.cacheConfig.pdxPersistent;
  }

  @Override
  public boolean getPdxIgnoreUnreadFields() {
    return this.cacheConfig.pdxIgnoreUnreadFields;
  }

  /**
   * Returns true if any of the GemFire services prefers PdxInstance. And application has not
   * requested getObject() on the PdxInstance.
   */
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    TypeRegistry pdxRegistry = this.getPdxRegistry();
    boolean pdxReadSerializedOverriden = false;
    if (pdxRegistry != null) {
      pdxReadSerializedOverriden = pdxRegistry.getPdxReadSerializedOverride();
    }

    return (getPdxReadSerialized() || pdxReadSerializedOverriden)
        && PdxInstanceImpl.getPdxReadSerialized();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return this.cacheConfig;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return this.dm;
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

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return getResourceAdvisor();
  }

  @Override
  public ResourceAdvisor getResourceAdvisor() {
    return this.resourceAdvisor;
  }

  @Override
  public Profile getProfile() {
    return this.resourceAdvisor.createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return this.system;
  }

  @Override
  public String getFullPath() {
    return "ResourceManager";
  }

  @Override
  public void fillInProfile(Profile profile) {
    this.resourceManager.fillInProfile(profile);
  }

  @Override
  public int getSerialNumber() {
    return this.serialNumber;
  }

  @Override
  public TXEntryStateFactory getTXEntryStateFactory() {
    return this.txEntryStateFactory;
  }

  // test hook
  public void setPdxSerializer(PdxSerializer serializer) {
    this.cacheConfig.setPdxSerializer(serializer);
    basicSetPdxSerializer(serializer);
  }

  private void basicSetPdxSerializer(PdxSerializer serializer) {
    TypeRegistry.setPdxSerializer(serializer);
    if (serializer instanceof ReflectionBasedAutoSerializer) {
      AutoSerializableManager autoSerializableManager =
          (AutoSerializableManager) ((ReflectionBasedAutoSerializer) serializer).getManager();
      if (autoSerializableManager != null) {
        autoSerializableManager.setRegionService(this);
      }
    }
  }

  @Override
  public void setReadSerializedForCurrentThread(boolean value) {
    PdxInstanceImpl.setPdxReadSerialized(value);
    this.setPdxReadSerializedOverride(value);
  }

  // test hook
  @Override
  public void setReadSerializedForTest(boolean value) {
    this.cacheConfig.setPdxReadSerialized(value);
  }

  public void setDeclarativeCacheConfig(CacheConfig cacheConfig) {
    this.cacheConfig.setDeclarativeConfig(cacheConfig);
    basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
  }

  /**
   * Add to the map of declarable properties. Any properties that exactly match existing properties
   * for a class in the list will be discarded (no duplicate Properties allowed).
   *
   * @param mapOfNewDeclarableProps Map of the declarable properties to add
   */
  public void addDeclarableProperties(final Map<Declarable, Properties> mapOfNewDeclarableProps) {
    synchronized (this.declarablePropertiesMap) {
      for (Entry<Declarable, Properties> newEntry : mapOfNewDeclarableProps.entrySet()) {
        // Find and remove a Declarable from the map if an "equal" version is already stored
        Class<? extends Declarable> clazz = newEntry.getKey().getClass();

        Declarable matchingDeclarable = null;
        for (Entry<Declarable, Properties> oldEntry : this.declarablePropertiesMap.entrySet()) {

          BiPredicate<Declarable, Declarable> isKeyIdentifiableAndSameIdPredicate =
              (Declarable oldKey, Declarable newKey) -> Identifiable.class.isInstance(newKey)
                  && ((Identifiable) oldKey).getId().equals(((Identifiable) newKey).getId());

          Supplier<Boolean> isKeyClassSame =
              () -> clazz.getName().equals(oldEntry.getKey().getClass().getName());
          Supplier<Boolean> isValueEqual = () -> newEntry.getValue().equals(oldEntry.getValue());
          Supplier<Boolean> isKeyIdentifiableAndSameId =
              () -> isKeyIdentifiableAndSameIdPredicate.test(oldEntry.getKey(), newEntry.getKey());

          if (isKeyClassSame.get() && (isValueEqual.get() || isKeyIdentifiableAndSameId.get())) {
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

  private Declarable initializer;

  private Properties initializerProps;

  @Override
  public Declarable getInitializer() {
    return this.initializer;
  }

  @Override
  public Properties getInitializerProps() {
    return this.initializerProps;
  }

  public void setInitializer(Declarable initializer, Properties initializerProps) {
    this.initializer = initializer;
    this.initializerProps = initializerProps;
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    return PdxInstanceFactoryImpl.newCreator(className, true, this);
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean expectDomainClass) {
    return PdxInstanceFactoryImpl.newCreator(className, expectDomainClass, this);
  }

  @Override
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    return PdxInstanceFactoryImpl.createPdxEnum(className, enumName, enumOrdinal, this);
  }

  public JmxManagerAdvisor getJmxManagerAdvisor() {
    return this.jmxAdvisor;
  }

  @Override
  public CacheSnapshotService getSnapshotService() {
    return new CacheSnapshotServiceImpl(this);
  }

  private void startColocatedJmxManagerLocator() {
    InternalLocator loc = InternalLocator.getLocator();
    if (loc != null) {
      loc.startJmxManagerLocationService(this);
    }
  }

  @Override
  public MemoryAllocator getOffHeapStore() {
    return this.getSystem().getOffHeapStore();
  }

  @Override
  public DiskStoreMonitor getDiskStoreMonitor() {
    return this.diskMonitor;
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
  public CqService getCqService() {
    return this.cqService;
  }

  public void addRegionEntrySynchronizationListener(RegionEntrySynchronizationListener listener) {
    this.synchronizationListeners.add(listener);
  }

  public void removeRegionEntrySynchronizationListener(
      RegionEntrySynchronizationListener listener) {
    this.synchronizationListeners.remove(listener);
  }

  @Override
  public void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize) {
    for (RegionEntrySynchronizationListener listener : this.synchronizationListeners) {
      try {
        listener.afterSynchronization(sender, region, entriesToSynchronize);
      } catch (Throwable t) {
        logger.warn(String.format(
            "Caught the following exception attempting to synchronize events from member=%s; regionPath=%s; entriesToSynchronize=%s:",
            new Object[] {sender, region.getFullPath(), entriesToSynchronize}),
            t);
      }
    }
  }

  @Override
  public Object convertPdxInstanceIfNeeded(Object obj, boolean preferCD) {
    Object result = obj;
    if (obj instanceof InternalPdxInstance) {
      InternalPdxInstance pdxInstance = (InternalPdxInstance) obj;
      if (preferCD) {
        try {
          result = new PreferBytesCachedDeserializable(pdxInstance.toBytes());
        } catch (IOException ignore) {
          // Could not convert pdx to bytes here; it will be tried again later
          // and an exception will be thrown there.
        }
      } else if (!this.getPdxReadSerialized()) {
        result = pdxInstance.getObject();
      }
    }
    return result;
  }

  @Override
  public Boolean getPdxReadSerializedOverride() {
    TypeRegistry pdxRegistry = this.getPdxRegistry();
    if (pdxRegistry != null) {
      return pdxRegistry.getPdxReadSerializedOverride();
    }
    return false;
  }

  @Override
  public void setPdxReadSerializedOverride(boolean pdxReadSerialized) {
    TypeRegistry pdxRegistry = this.getPdxRegistry();
    if (pdxRegistry != null) {
      pdxRegistry.setPdxReadSerializedOverride(pdxReadSerialized);
    }
  }

  @Override
  public void registerPdxMetaData(Object instance) {
    try {
      byte[] blob = BlobHelper.serializeToBlob(instance);
      if (blob.length == 0 || blob[0] != DSCODE.PDX.toByte()) {
        throw new SerializationException("The instance is not PDX serializable");
      }
    } catch (IOException e) {
      throw new SerializationException("Serialization failed", e);
    }
  }

  private void throwIfClient() {
    if (isClient()) {
      throw new UnsupportedOperationException("operation is not supported on a client cache");
    }
  }

  private final InternalCacheForClientAccess cacheForClients =
      new InternalCacheForClientAccess(this);

  @Override
  public InternalCache getCacheForProcessingClientRequests() {
    return cacheForClients;
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    if (this.dm != null) {
      return this.dm.getThreadMonitoring();
    } else {
      return null;
    }
  }

}
