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

import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.apache.geode.distributed.ConfigurationPersistenceService.CLUSTER_CONFIG;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.internal.ClusterDistributionManager.ADMIN_ONLY_DM_TYPE;
import static org.apache.geode.distributed.internal.ClusterDistributionManager.LOCATOR_DM_TYPE;
import static org.apache.geode.distributed.internal.DistributionConfig.DEFAULT_DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.getAnyInstance;
import static org.apache.geode.internal.cache.ColocationHelper.getColocatedChildRegions;
import static org.apache.geode.internal.cache.GemFireCacheImpl.UncheckedUtils.asDistributedMemberSet;
import static org.apache.geode.internal.cache.GemFireCacheImpl.UncheckedUtils.createMapArray;
import static org.apache.geode.internal.cache.GemFireCacheImpl.UncheckedUtils.uncheckedCast;
import static org.apache.geode.internal.cache.GemFireCacheImpl.UncheckedUtils.uncheckedRegionAttributes;
import static org.apache.geode.internal.cache.LocalRegion.setThreadInitLevelRequirement;
import static org.apache.geode.internal.cache.PartitionedRegion.DISK_STORE_FLUSHED;
import static org.apache.geode.internal.cache.PartitionedRegion.OFFLINE_EQUAL_PERSISTED;
import static org.apache.geode.internal.cache.PartitionedRegion.PRIMARY_BUCKETS_LOCKED;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME;
import static org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType.HEAP_MEMORY;
import static org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType.OFFHEAP_MEMORY;
import static org.apache.geode.internal.cache.util.UncheckedUtils.cast;
import static org.apache.geode.internal.logging.CoreLoggingExecutors.newThreadPoolWithFixedFeed;
import static org.apache.geode.internal.tcp.ConnectionTable.threadWantsSharedResources;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newFixedThreadPool;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
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
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.transaction.TransactionManager;

import com.sun.jna.Native;
import com.sun.jna.Platform;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.GemFireCacheException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.LogWriter;
import org.apache.geode.SerializationException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
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
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
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
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
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
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.ResourceAdvisor;
import org.apache.geode.internal.cache.event.EventTrackerExpiryTask;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.eviction.OffHeapEvictor;
import org.apache.geode.internal.cache.execute.util.FindRestEnabledServersFunction;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.locks.TXLockService;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.snapshot.CacheSnapshotServiceImpl;
import org.apache.geode.internal.cache.tier.Acceptor;
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
import org.apache.geode.internal.cache.xmlcache.CacheServerCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheXmlPropertyResolver;
import org.apache.geode.internal.cache.xmlcache.PropertyResolver;
import org.apache.geode.internal.config.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.jta.TransactionManagerImpl;
import org.apache.geode.internal.lang.ThrowableUtils;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.sequencelog.SequenceLoggerImpl;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.shared.StringPrintWriter;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsClockFactory;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.lang.Identifiable;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.JmxManagerAdvisee;
import org.apache.geode.management.internal.JmxManagerAdvisor;
import org.apache.geode.management.internal.RestAgent;
import org.apache.geode.management.internal.beans.ManagementListener;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.pdx.internal.InternalPdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * GemFire's implementation of a distributed {@link Cache}.
 */
public class GemFireCacheImpl implements InternalCache, InternalClientCache, HasCachePerfStats,
    DistributionAdvisee {
  private static final Logger logger = LogService.getLogger();

  /**
   * The default number of seconds to wait for a distributed lock
   */
  public static final int DEFAULT_LOCK_TIMEOUT =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.defaultLockTimeout", 60);

  /**
   * The default duration (in seconds) of a lease on a distributed lock
   */
  public static final int DEFAULT_LOCK_LEASE =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.defaultLockLease", 120);

  /**
   * The default "copy on read" attribute value
   */
  public static final boolean DEFAULT_COPY_ON_READ = false;

  /**
   * The default amount of time to wait for a {@code netSearch} to complete
   */
  public static final int DEFAULT_SEARCH_TIMEOUT =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.defaultSearchTimeout", 300);

  /**
   * Name of the default pool.
   */
  public static final String DEFAULT_POOL_NAME = "DEFAULT";

  @VisibleForTesting
  static final int EVENT_THREAD_LIMIT =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.EVENT_THREAD_LIMIT", 16);

  @VisibleForTesting
  static final int PURGE_INTERVAL = 1000;

  /**
   * The number of threads that the QueryMonitor will use to mark queries as cancelled (see
   * QueryMonitor class for reasons why a query might be cancelled). That processing is very
   * efficient, so we don't foresee needing to raise this above 1.
   */
  private static final int QUERY_MONITOR_THREAD_POOL_SIZE = 1;

  private static final int EVENT_QUEUE_LIMIT =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.EVENT_QUEUE_LIMIT", 4096);

  private static final int FIVE_HOURS_MILLIS = 5 * 60 * 60 * 1000;

  private static final Pattern DOUBLE_BACKSLASH = Pattern.compile("\\\\");

  /**
   * Enables {@link #creationStack} when CacheExistsException issues arise in debugging
   */
  private static final boolean DEBUG_CREATION_STACK = false;

  private static final boolean XML_PARAMETERIZATION_ENABLED =
      !Boolean.getBoolean(GEMFIRE_PREFIX + "xml.parameterization.disabled");

  /**
   * Number of threads used to close PRs in shutdownAll. By default is the number of PRs in the
   * cache.
   */
  private static final int shutdownAllPoolSize =
      Integer.getInteger(GEMFIRE_PREFIX + "SHUTDOWN_ALL_POOL_SIZE", -1);

  private static final ThreadLocal<GemFireCacheImpl> xmlCache = new ThreadLocal<>();

  /**
   * System property to limit the max query-execution time. By default its turned off (-1), the time
   * is set in milliseconds.
   */
  @MutableForTesting
  public static int MAX_QUERY_EXECUTION_TIME =
      Integer.getInteger(GEMFIRE_PREFIX + "Cache.MAX_QUERY_EXECUTION_TIME", -1);

  /**
   * Used by unit tests to force cache creation to use a test generated cache.xml
   */
  @MutableForTesting
  @VisibleForTesting
  public static File testCacheXml;

  /**
   * If true then when a delta is applied the size of the entry value will be recalculated. If false
   * (the default) then the size of the entry value is unchanged by a delta application. Not a final
   * so that tests can change this value.
   *
   * @since GemFire h****** 6.1.2.9
   */
  @MutableForTesting
  static boolean DELTAS_RECALCULATE_SIZE =
      Boolean.getBoolean(GEMFIRE_PREFIX + "DELTAS_RECALCULATE_SIZE");

  /**
   * The {@code CacheLifecycleListener} s that have been registered in this VM
   */
  @MakeNotStatic
  private static final Set<CacheLifecycleListener> cacheLifecycleListeners =
      new CopyOnWriteArraySet<>();

  /**
   * Property set to true if resource manager heap percentage is set and query monitor is required
   */
  @MakeNotStatic
  private static boolean queryMonitorRequiredForResourceManager;

  /**
   * TODO: remove static from defaultDiskStoreName and move methods to InternalCache
   */
  @MakeNotStatic
  private static String defaultDiskStoreName = DiskStoreFactory.DEFAULT_DISK_STORE_NAME;

  /**
   * System property to disable query monitor even if resource manager is in use
   */
  private final boolean queryMonitorDisabledForLowMem =
      Boolean.getBoolean(GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");

  private final InternalDistributedSystem system;

  private final DistributionManager dm;

  private final Map<String, InternalRegion> rootRegions;

  /**
   * True if this cache is being created by a ClientCacheFactory.
   */
  private final boolean isClient;

  private final PoolFactory poolFactory;

  private final ConcurrentMap<String, InternalRegion> pathToRegion = new ConcurrentHashMap<>();

  private final CachePerfStats cachePerfStats;

  /**
   * Date on which this instances was created
   */
  private final Date creationDate;

  /**
   * Thread pool for event dispatching
   */
  private final ExecutorService eventThreadPool;

  /**
   * List of all cache servers. CopyOnWriteArrayList is used to allow concurrent add, remove and
   * retrieval operations. It is assumed that the traversal operations on cache servers list vastly
   * outnumber the mutative operations such as add, remove.
   */
  private final List<InternalCacheServer> allCacheServers = new CopyOnWriteArrayList<>();

  /**
   * Unmodifiable view of "allCacheServers".
   */
  private final List<CacheServer> unmodifiableAllCacheServers = unmodifiableList(allCacheServers);

  /**
   * Controls updates to the list of all gateway senders {@link #allGatewaySenders}.
   */
  private final Object allGatewaySendersLock = new Object();

  /**
   * List of all async event queues added to the cache. CopyOnWriteArrayList is used to allow
   * concurrent add, remove and retrieval operations.
   */
  private final Set<AsyncEventQueue> allVisibleAsyncEventQueues = new CopyOnWriteArraySet<>();

  /**
   * List of all async event queues added to the cache. CopyOnWriteArrayList is used to allow
   * concurrent add, remove and retrieval operations.
   */
  private final Set<AsyncEventQueue> allAsyncEventQueues = new CopyOnWriteArraySet<>();

  private final AtomicReference<GatewayReceiver> gatewayReceiver = new AtomicReference<>();

  private final AtomicReference<InternalCacheServer> gatewayReceiverServer =
      new AtomicReference<>();

  /**
   * PartitionedRegion instances (for required-events notification
   */
  private final Set<PartitionedRegion> partitionedRegions = new HashSet<>();

  /**
   * Map of regions that are in the process of being destroyed. We could potentially leave the
   * regions in the pathToRegion map, but that would entail too many changes at this point in the
   * release. We need to know which regions are being destroyed so that a profile exchange can get
   * the persistent id of the destroying region and know not to persist that ID if it receives it as
   * part of the persistent view.
   */
  private final ConcurrentMap<String, DistributedRegion> regionsInDestroy =
      new ConcurrentHashMap<>();

  private final Object allGatewayHubsLock = new Object();

  /**
   * Transaction manager for this cache.
   */
  private final TXManagerImpl transactionManager;

  /**
   * Named region attributes registered with this cache.
   */
  private final Map<String, RegionAttributes<?, ?>> namedRegionAttributes =
      synchronizedMap(new HashMap<>());

  /**
   * System timer task for cleaning up old bridge thread event entries.
   */
  private final EventTrackerExpiryTask recordedEventSweeper;

  private final TombstoneService tombstoneService;

  /**
   * Synchronization mutex for prLockService.
   */
  private final Object prLockServiceLock = new Object();

  /**
   * Synchronization mutex for gatewayLockService.
   */
  private final Object gatewayLockServiceLock = new Object();

  private final InternalResourceManager resourceManager;

  private final BackupService backupService;

  private final Object heapEvictorLock = new Object();

  private final Object offHeapEvictorLock = new Object();

  private final Object queryMonitorLock = new Object();

  private final PersistentMemberManager persistentMemberManager;

  private final ClientMetadataService clientMetadataService;

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

  /**
   * Resolves ${} type property strings.
   */
  private final PropertyResolver resolver;

  /**
   * @since GemFire 8.1
   */
  private final ExtensionPoint<Cache> extensionPoint = new SimpleExtensionPoint<>(this, this);

  private final CqService cqService;

  private final Set<RegionListener> regionListeners = ConcurrentHashMap.newKeySet();

  private final Map<Class<? extends CacheService>, CacheService> services = new HashMap<>();

  private final SecurityService securityService;

  private final Set<RegionEntrySynchronizationListener> synchronizationListeners =
      ConcurrentHashMap.newKeySet();

  private final ClusterConfigurationLoader ccLoader = new ClusterConfigurationLoader();

  private final StatisticsClock statisticsClock;

  /**
   * Map of futures used to track regions that are being reinitialized.
   */
  private final ConcurrentMap<String, FutureResult<InternalRegion>> reinitializingRegions =
      new ConcurrentHashMap<>();

  private final HeapEvictorFactory heapEvictorFactory;
  private final Runnable typeRegistryClose;
  private final Function<InternalCache, String> typeRegistryGetPdxDiskStoreName;
  private final Consumer<PdxSerializer> typeRegistrySetPdxSerializer;
  private final TypeRegistryFactory typeRegistryFactory;
  private final Consumer<org.apache.geode.cache.execute.Function> functionServiceRegisterFunction;
  private final Function<Object, SystemTimer> systemTimerFactory;
  private final ReplyProcessor21Factory replyProcessor21Factory;

  private final Stopper stopper = new Stopper();

  private final boolean disableDisconnectDsOnCacheClose =
      Boolean.getBoolean(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");

  private final ConcurrentMap<String, DiskStoreImpl> diskStores = new ConcurrentHashMap<>();

  private final ConcurrentMap<String, DiskStoreImpl> regionOwnedDiskStores =
      new ConcurrentHashMap<>();

  /**
   * Synchronization mutex for {@link #ccpTimer}.
   */
  private final Object ccpTimerMutex = new Object();

  private final ExpirationScheduler expirationScheduler;

  /**
   * TODO make this a simple int guarded by riWaiters and get rid of the double-check
   */
  private final AtomicInteger registerInterestsInProgress = new AtomicInteger();

  private final List<SimpleWaiter> riWaiters = new ArrayList<>();

  private final InternalCacheForClientAccess cacheForClients =
      new InternalCacheForClientAccess(this);

  private volatile ConfigurationResponse configurationResponse;

  private volatile boolean isInitialized;

  private volatile boolean isClosing;

  /**
   * Set of all gateway senders. It may be fetched safely (for enumeration), but updates must by
   * synchronized via {@link #allGatewaySendersLock}
   */
  private volatile Set<GatewaySender> allGatewaySenders = emptySet();

  /**
   * Copy on Read feature for all read operations.
   */
  private volatile boolean copyOnRead = DEFAULT_COPY_ON_READ;

  /**
   * Reason this cache was forced to close due to a forced-disconnect or system failure.
   */
  private volatile Throwable disconnectCause;

  /**
   * DistributedLockService for GatewaySenders. Remains null until the first GatewaySender is
   * created. Destroyed by GemFireCache when closing the cache. Guarded by gatewayLockServiceLock.
   */
  private volatile DistributedLockService gatewayLockService;

  private volatile QueryMonitor queryMonitor;

  /**
   * Not final to allow cache.xml parsing to set it.
   */
  private Pool defaultPool;

  /**
   * Amount of time (in seconds) to wait for a distributed lock
   */
  private int lockTimeout = DEFAULT_LOCK_TIMEOUT;

  /**
   * Amount of time a lease of a distributed lock lasts
   */
  private int lockLease = DEFAULT_LOCK_LEASE;

  /**
   * Amount of time to wait for a {@code netSearch} to complete
   */
  private int searchTimeout = DEFAULT_SEARCH_TIMEOUT;

  /**
   * Conflict resolver for WAN, if any. Guarded by {@link #allGatewayHubsLock}.
   */
  private GatewayConflictResolver gatewayConflictResolver;

  /**
   * True if this is a server cache.
   */
  private boolean isServer;

  private RestAgent restAgent;

  private boolean isRESTServiceRunning;

  /**
   * True if this cache was forced to close due to a forced-disconnect.
   */
  private boolean forcedDisconnect;

  /**
   * Context where this cache was created for debugging.
   */
  private Exception creationStack;

  /**
   * DistributedLockService for PartitionedRegions. Remains null until the first PartitionedRegion
   * is created. Destroyed by GemFireCache when closing the cache. Protected by synchronization on
   * prLockService. Guarded by prLockServiceLock.
   */
  private DistributedLockService prLockService;

  private HeapEvictor heapEvictor;

  private OffHeapEvictor offHeapEvictor;

  private ResourceEventsListener resourceEventsListener;

  /**
   * Set to true during a cache close if user requested durable subscriptions to be kept.
   *
   * @since GemFire 5.7
   */
  private boolean keepAlive;

  /**
   * Timer for {@link CacheClientProxy}. Guarded by {@link #ccpTimerMutex}.
   */
  private SystemTimer ccpTimer;

  private int cancelCount;

  /**
   * Some tests pass value in via constructor. Product code sets value after construction.
   */
  private TypeRegistry pdxRegistry;

  private Declarable initializer;

  private Properties initializerProps;

  private List<File> backupFiles = emptyList();

  static {
    // this works around jdk bug 6427854
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
    if (lastError == 1 || lastError == 12) {
      // if EPERM || ENOMEM
      message = "Unable to lock memory due to insufficient free space or privileges.  "
          + "Please check the RLIMIT_MEMLOCK soft resource limit (ulimit -l) and "
          + "increase the available memory if needed";
    }
    throw new IllegalStateException(message);
  }

  /**
   * Returns the last created instance of GemFireCache.
   *
   * @deprecated use DM.getCache instead
   */
  @Deprecated
  public static GemFireCacheImpl getInstance() {
    InternalDistributedSystem system = getAnyInstance();
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
    GemFireCacheImpl result = getInstance();
    if (result != null && !result.isClosing) {
      return result;
    }
    if (result != null) {
      throw result.getCacheClosedException("The cache has been closed.");
    }
    throw new CacheClosedException("A cache has not yet been created.");
  }

  /**
   * Returns an existing instance. If a cache does not exist throws an exception.
   *
   * @param reason the reason an existing cache is being requested.
   * @return the existing cache
   * @throws CacheClosedException if an existing cache can not be found.
   * @deprecated Please use {@link DistributionManager#getExistingCache()} instead.
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
   * Returns instance to pdx even while it is being closed.
   *
   * @deprecated Please use a cache that is passed to your method instead.
   */
  @Deprecated
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

  /**
   * Creates a new instance of GemFireCache and populates it according to the {@code cache.xml}, if
   * appropriate.
   *
   * <p>
   * Currently only unit tests set the typeRegistry parameter to a non-null value
   */
  GemFireCacheImpl(boolean isClient, PoolFactory poolFactory,
      InternalDistributedSystem internalDistributedSystem, CacheConfig cacheConfig,
      boolean useAsyncEventListeners, TypeRegistry typeRegistry) {
    this(isClient,
        poolFactory,
        internalDistributedSystem,
        cacheConfig,
        useAsyncEventListeners,
        typeRegistry,
        JNDIInvoker::mapTransactions,
        SecurityServiceFactory::create,
        () -> PoolManager.getAll().isEmpty(),
        ManagementListener::new,
        CqServiceProvider::create,
        CachePerfStats::new,
        TXManagerImpl::new,
        PersistentMemberManager::new,
        ResourceAdvisor::createResourceAdvisor,
        JmxManagerAdvisee::new,
        JmxManagerAdvisor::createJmxManagerAdvisor,
        InternalResourceManager::createResourceManager,
        DistributionAdvisor::createSerialNumber,
        HeapEvictor::new,
        TypeRegistry::init,
        TypeRegistry::open,
        TypeRegistry::close,
        TypeRegistry::getPdxDiskStoreName,
        TypeRegistry::setPdxSerializer,
        TypeRegistry::new,
        HARegionQueue::setMessageSyncInterval,
        FunctionService::registerFunction,
        object -> new SystemTimer(object, true),
        TombstoneService::initialize,
        ExpirationScheduler::new,
        DiskStoreMonitor::new,
        GatewaySenderQueueEntrySynchronizationListener::new,
        BackupService::new,
        ClientMetadataService::new,
        TXEntryState.getFactory(),
        ReplyProcessor21::new);
  }

  @VisibleForTesting
  GemFireCacheImpl(boolean isClient,
      PoolFactory poolFactory,
      InternalDistributedSystem internalDistributedSystem,
      CacheConfig cacheConfig,
      boolean useAsyncEventListeners,
      TypeRegistry typeRegistry,
      Consumer<DistributedSystem> jndiTransactionMapper,
      InternalSecurityServiceFactory securityServiceFactory,
      Supplier<Boolean> isPoolManagerEmpty,
      Function<InternalDistributedSystem, ManagementListener> managementListenerFactory,
      Function<InternalCache, CqService> cqServiceFactory,
      CachePerfStatsFactory cachePerfStatsFactory,
      TXManagerImplFactory txManagerImplFactory,
      Supplier<PersistentMemberManager> persistentMemberManagerFactory,
      Function<DistributionAdvisee, ResourceAdvisor> resourceAdvisorFactory,
      Function<InternalCacheForClientAccess, JmxManagerAdvisee> jmxManagerAdviseeFactory,
      Function<JmxManagerAdvisee, JmxManagerAdvisor> jmxManagerAdvisorFactory,
      Function<InternalCache, InternalResourceManager> internalResourceManagerFactory,
      Supplier<Integer> serialNumberSupplier,
      HeapEvictorFactory heapEvictorFactory,
      Runnable typeRegistryInit,
      Runnable typeRegistryOpen,
      Runnable typeRegistryClose,
      Function<InternalCache, String> typeRegistryGetPdxDiskStoreName,
      Consumer<PdxSerializer> typeRegistrySetPdxSerializer,
      TypeRegistryFactory typeRegistryFactory,
      Consumer<Integer> haRegionQueueSetMessageSyncInterval,
      Consumer<org.apache.geode.cache.execute.Function> functionServiceRegisterFunction,
      Function<Object, SystemTimer> systemTimerFactory,
      Function<InternalCache, TombstoneService> tombstoneServiceFactory,
      Function<InternalDistributedSystem, ExpirationScheduler> expirationSchedulerFactory,
      Function<File, DiskStoreMonitor> diskStoreMonitorFactory,
      Supplier<RegionEntrySynchronizationListener> gatewaySenderQueueEntrySynchronizationListener,
      Function<InternalCache, BackupService> backupServiceFactory,
      Function<Cache, ClientMetadataService> clientMetadataServiceFactory,
      TXEntryStateFactory txEntryStateFactory,
      ReplyProcessor21Factory replyProcessor21Factory) {
    this.isClient = isClient;
    this.poolFactory = poolFactory;
    this.cacheConfig = cacheConfig;
    pdxRegistry = typeRegistry;

    this.heapEvictorFactory = heapEvictorFactory;
    this.typeRegistryClose = typeRegistryClose;
    this.typeRegistryGetPdxDiskStoreName = typeRegistryGetPdxDiskStoreName;
    this.typeRegistrySetPdxSerializer = typeRegistrySetPdxSerializer;
    this.typeRegistryFactory = typeRegistryFactory;
    this.functionServiceRegisterFunction = functionServiceRegisterFunction;
    this.systemTimerFactory = systemTimerFactory;
    this.replyProcessor21Factory = replyProcessor21Factory;

    // Synchronized to prevent a new cache from being created before an old one finishes closing
    synchronized (GemFireCacheImpl.class) {

      // start JTA transaction manager within synchronization to prevent race with cache close
      jndiTransactionMapper.accept(internalDistributedSystem);
      system = internalDistributedSystem;
      dm = system.getDistributionManager();

      if (!isClient) {
        configurationResponse = requestSharedConfiguration();

        // apply the cluster's properties config and initialize security using that config
        ccLoader.applyClusterPropertiesConfiguration(configurationResponse, system.getConfig());

        securityService =
            securityServiceFactory.create(system.getConfig().getSecurityProps(), cacheConfig);
        system.setSecurityService(securityService);
      } else {
        // create a no-op security service for client
        securityService = SecurityServiceFactory.create();
      }

      DistributionConfig systemConfig = internalDistributedSystem.getConfig();
      if (!this.isClient && isPoolManagerEmpty.get()) {
        // only support management on members of a distributed system
        boolean disableJmx = systemConfig.getDisableJmx();
        if (disableJmx) {
          logger.info("Running with JMX disabled.");
        } else {
          resourceEventsListener = managementListenerFactory.apply(system);
          system.addResourceListener(resourceEventsListener);
          if (system.isLoner()) {
            logger.info("Running in local mode since no locators were specified.");
          }
        }

      } else {
        logger.info("Running in client mode");
        resourceEventsListener = null;
      }

      // Don't let admin-only VMs create Cache's just yet.
      if (dm.getDMType() == ADMIN_ONLY_DM_TYPE) {
        throw new IllegalStateException("Cannot create a Cache in an admin-only VM.");
      }

      rootRegions = new HashMap<>();

      cqService = cqServiceFactory.apply(this);

      // Create the CacheStatistics
      statisticsClock = StatisticsClockFactory.clock(system.getConfig().getEnableTimeStatistics());
      cachePerfStats = cachePerfStatsFactory.create(
          internalDistributedSystem.getStatisticsManager(), statisticsClock);

      transactionManager = txManagerImplFactory.create(cachePerfStats, this, statisticsClock);
      dm.addMembershipListener(transactionManager);

      creationDate = new Date();

      persistentMemberManager = persistentMemberManagerFactory.get();

      if (useAsyncEventListeners) {
        eventThreadPool = newThreadPoolWithFixedFeed("Message Event Thread",
            command -> {
              threadWantsSharedResources();
              command.run();
            }, EVENT_THREAD_LIMIT, cachePerfStats.getEventPoolHelper(), 1000,
            getThreadMonitorObj(),
            EVENT_QUEUE_LIMIT);
      } else {
        eventThreadPool = null;
      }

      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      resourceAdvisor = resourceAdvisorFactory.apply(this);

      // Initialize the advisor here, but wait to exchange profiles until cache is fully built
      jmxAdvisor = jmxManagerAdvisorFactory.apply(jmxManagerAdviseeFactory.apply(cacheForClients));

      resourceManager = internalResourceManagerFactory.apply(this);
      serialNumber = serialNumberSupplier.get();

      getInternalResourceManager().addResourceListener(HEAP_MEMORY, getHeapEvictor());

      // Only bother creating an off-heap evictor if we have off-heap memory enabled.
      if (null != getOffHeapStore()) {
        getInternalResourceManager().addResourceListener(OFFHEAP_MEMORY, getOffHeapEvictor());
      }

      recordedEventSweeper = createEventTrackerExpiryTask();
      tombstoneService = tombstoneServiceFactory.apply(this);

      typeRegistryInit.run();
      basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
      typeRegistryOpen.run();

      if (!isClient()) {
        // Initialize the QRM thread frequency to default (1 second )to prevent spill over from
        // previous Cache, as the interval is stored in a static volatile field.
        haRegionQueueSetMessageSyncInterval.accept(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
      }
      functionServiceRegisterFunction.accept(new PRContainsValueFunction());
      expirationScheduler = expirationSchedulerFactory.apply(system);

      if (DEBUG_CREATION_STACK) {
        creationStack = new Exception(String.format("Created GemFireCache %s", toString()));
      }

      this.txEntryStateFactory = txEntryStateFactory;
      if (XML_PARAMETERIZATION_ENABLED) {
        // If product properties file is available replace properties from there
        Properties userProps = system.getConfig().getUserDefinedProps();
        if (userProps != null && !userProps.isEmpty()) {
          resolver = new CacheXmlPropertyResolver(false,
              PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, userProps);
        } else {
          resolver = new CacheXmlPropertyResolver(false,
              PropertyResolver.NO_SYSTEM_PROPERTIES_OVERRIDE, null);
        }
      } else {
        resolver = null;
      }

      SystemFailure.signalCacheCreate();

      diskMonitor = diskStoreMonitorFactory.apply(systemConfig.getLogFile());

      addRegionEntrySynchronizationListener(gatewaySenderQueueEntrySynchronizationListener.get());
      backupService = backupServiceFactory.apply(this);
    }

    clientMetadataService = clientMetadataServiceFactory.apply(this);
  }

  /**
   * This is for debugging cache-open issues such as {@link CacheExistsException}.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GemFireCache[");
    sb.append("id = ").append(System.identityHashCode(this));
    sb.append("; isClosing = ").append(isClosing);
    sb.append("; isShutDownAll = ").append(isCacheAtShutdownAll());
    sb.append("; created = ").append(creationDate);
    sb.append("; server = ").append(isServer);
    sb.append("; copyOnRead = ").append(copyOnRead);
    sb.append("; lockLease = ").append(lockLease);
    sb.append("; lockTimeout = ").append(lockTimeout);
    if (creationStack != null) {
      sb.append(lineSeparator()).append("Creation context:").append(lineSeparator());
      sb.append(getStackTrace(creationStack));
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public void throwCacheExistsException() {
    throw new CacheExistsException(this, String.format("%s: An open cache already exists.", this),
        creationStack);
  }

  @Override
  public MeterRegistry getMeterRegistry() {
    return system.getMeterRegistry();
  }

  @Override
  public void saveCacheXmlForReconnect() {
    // there are two versions of this method so it can be unit-tested
    boolean sharedConfigEnabled = getDistributionManager().getConfig().getUseSharedConfiguration();

    if (!Boolean.getBoolean(GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile")
        && !sharedConfigEnabled) {
      try {
        logger.info("generating XML to rebuild the cache after reconnect completes");
        StringPrintWriter pw = new StringPrintWriter();
        CacheXmlGenerator.generate((Cache) this, pw, false);
        String cacheXML = pw.toString();
        getCacheConfig().setCacheXMLDescription(cacheXML);
        logger.info("XML generation completed: {}", cacheXML);
      } catch (CancelException e) {
        logger.info("Unable to generate XML description for reconnect of cache due to exception",
            e);
      }
    } else if (sharedConfigEnabled && !getCacheServers().isEmpty()) {
      // we need to retain a cache-server description if this JVM was started by gfsh
      List<CacheServerCreation> list = new ArrayList<>(getCacheServers().size());
      for (Object o : getCacheServers()) {
        CacheServerImpl cs = (CacheServerImpl) o;
        if (cs.isDefaultServer()) {
          CacheServerCreation bsc = new CacheServerCreation(this, cs);
          list.add(bsc);
        }
      }
      getCacheConfig().setCacheServerCreation(list);
      logger.info("CacheServer configuration saved");
    }
  }

  @Override
  public void reLoadClusterConfiguration() throws IOException, ClassNotFoundException {
    configurationResponse = requestSharedConfiguration();
    if (configurationResponse != null) {
      ccLoader.deployJarsReceivedFromClusterConfiguration(configurationResponse);
      ccLoader.applyClusterPropertiesConfiguration(configurationResponse, system.getConfig());
      ccLoader.applyClusterXmlConfiguration(this, configurationResponse,
          system.getConfig().getGroups());
      initializeDeclarativeCache();
    }
  }

  /**
   * Initialize the EventTracker's timer task for tracking and shutdown purposes.
   */
  private EventTrackerExpiryTask createEventTrackerExpiryTask() {
    long lifetimeInMillis = Long.getLong(GEMFIRE_PREFIX + "messageTrackingTimeout",
        PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT / 3);
    EventTrackerExpiryTask task = new EventTrackerExpiryTask(lifetimeInMillis);
    getCCPTimer().scheduleAtFixedRate(task, lifetimeInMillis, lifetimeInMillis);
    return task;
  }

  @Override
  public SecurityService getSecurityService() {
    return securityService;
  }

  @Override
  public boolean isRESTServiceRunning() {
    return isRESTServiceRunning;
  }

  @Override
  public void setRESTServiceRunning(boolean isRESTServiceRunning) {
    this.isRESTServiceRunning = isRESTServiceRunning;
  }

  @Override
  @VisibleForTesting
  public RestAgent getRestAgent() {
    return restAgent;
  }

  /**
   * Request the cluster configuration from the locator(s) if cluster config service is running.
   */
  @VisibleForTesting
  ConfigurationResponse requestSharedConfiguration() {
    DistributionConfig config = system.getConfig();

    if (!(dm instanceof ClusterDistributionManager)) {
      return null;
    }

    // do nothing if this vm is/has locator or this is a client
    if (dm.getDMType() == LOCATOR_DM_TYPE || isClient || Locator.getLocator() != null) {
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
          system.getConfig().getGroups(), locatorsWithClusterConfig.keySet());

      // log the configuration received from the locator
      logger.info("Received cluster configuration from the locator");
      logger.info(response.describeConfig());

      Configuration clusterConfig = response.getRequestedConfiguration().get(CLUSTER_CONFIG);
      Properties clusterSecProperties =
          clusterConfig == null ? new Properties() : clusterConfig.getGemfireProperties();

      // If not using shared configuration, return null or throw an exception is locator is secured
      if (!config.getUseSharedConfiguration()) {
        if (clusterSecProperties.containsKey(SECURITY_MANAGER)) {
          throw new GemFireConfigException(
              "A server must use cluster configuration when joining a secured cluster.");
        }
        logger.info(
            "The cache has been created with use-cluster-configuration=false. It will not receive any cluster configuration");
        return null;
      }

      Properties serverSecProperties = config.getSecurityProps();
      // check for possible mis-configuration
      if (isMisConfigured(clusterSecProperties, serverSecProperties, SECURITY_MANAGER)
          || isMisConfigured(clusterSecProperties, serverSecProperties, SECURITY_POST_PROCESSOR)) {
        throw new GemFireConfigException(
            "A server cannot specify its own security-manager or security-post-processor when using cluster configuration");
      }
      return response;

    } catch (ClusterConfigurationNotAvailableException e) {
      throw new GemFireConfigException("cluster configuration service not available", e);
    } catch (UnknownHostException e) {
      throw new GemFireConfigException(e.getLocalizedMessage(), e);
    }
  }

  /**
   * Arguments must not be null.
   */
  @VisibleForTesting
  static boolean isMisConfigured(Properties clusterProps, Properties serverProps, String key) {
    requireNonNull(clusterProps);
    requireNonNull(serverProps);
    requireNonNull(key);

    String clusterPropValue = clusterProps.getProperty(key);
    String serverPropValue = serverProps.getProperty(key);

    // if this server prop is not specified, this is always OK.
    if (StringUtils.isBlank(serverPropValue)) {
      return false;
    }

    // server props is not blank, but cluster props is blank, NOT OK.
    if (StringUtils.isBlank(clusterPropValue)) {
      return true;
    }

    // at this point check for equality
    return !clusterPropValue.equals(serverPropValue);
  }

  @Override
  public boolean isClient() {
    return isClient;
  }

  @Override
  public boolean hasPool() {
    return isClient || !getAllPools().isEmpty();
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

  @Override
  public synchronized Pool getDefaultPool() {
    if (defaultPool == null) {
      determineDefaultPool();
    }
    return defaultPool;
  }

  @Override
  public void initialize() {
    for (CacheLifecycleListener listener : cacheLifecycleListeners) {
      listener.cacheCreated(this);
    }

    if (isClient()) {
      initializeClientRegionShortcuts(this);
    } else {
      initializeRegionShortcuts(this);
    }

    // set ClassPathLoader and then deploy cluster config jars
    ClassPathLoader.setLatestToDefault(system.getConfig().getDeployWorkingDir());

    try {
      ccLoader.deployJarsReceivedFromClusterConfiguration(configurationResponse);
    } catch (IOException | ClassNotFoundException e) {
      throw new GemFireConfigException(
          "Exception while deploying the jars received as a part of cluster Configuration",
          e);
    }

    SystemMemberCacheEventProcessor.send(this, Operation.CACHE_CREATE);
    resourceAdvisor.initializationGate();

    // Register function that we need to execute to fetch available REST service endpoints in DS
    functionServiceRegisterFunction.accept(new FindRestEnabledServersFunction());

    // Entry to GemFire Management service
    jmxAdvisor.initializationGate();

    initializeServices();

    // This starts up the ManagementService, registers and federates the internal beans. Since it
    // may be starting up web services, it relies on the prior step which would have started the
    // HttpService.
    system.handleResourceEvent(ResourceEvent.CACHE_CREATE, this);

    // Resource events, generated for started services. These events may depend on the prior
    // CACHE_CREATE event which is why they are split out separately.
    handleResourceEventsForCacheServices();

    boolean completedCacheXml = false;
    try {
      if (!isClient) {
        applyJarAndXmlFromClusterConfig();
      }
      initializeDeclarativeCache();
      completedCacheXml = true;
    } catch (RuntimeException e) {
      logger.error("Cache initialization for {} failed because: {}", this, e);
      throw e;
    } finally {
      if (!completedCacheXml) {
        // so initializeDeclarativeCache threw an exception
        try {
          close();
        } catch (Throwable ignore) {
          // I don't want init to throw an exception that came from the close.
          // I want it to throw the original exception that came from initializeDeclarativeCache.
        }
        configurationResponse = null;
      }
    }

    system.handleResourceEvent(ResourceEvent.CLUSTER_CONFIGURATION_APPLIED, this);

    startColocatedJmxManagerLocator();

    startRestAgentServer(this);

    isInitialized = true;
  }

  @VisibleForTesting
  void applyJarAndXmlFromClusterConfig() {
    if (configurationResponse == null) {
      // Deploy all the jars from the deploy working dir.
      ClassPathLoader.getLatest().getJarDeployer().loadPreviouslyDeployedJarsFromDisk();
    }
    ccLoader.applyClusterXmlConfiguration(this, configurationResponse,
        system.getConfig().getGroups());
  }

  /**
   * Initialize any services provided as extensions to the cache using service loader.
   */
  private void initializeServices() {
    ServiceLoader<CacheService> loader = ServiceLoader.load(CacheService.class);
    for (CacheService service : loader) {
      try {
        if (service.init(this)) {
          services.put(service.getInterface(), service);
          logger.info("Initialized cache service {}", service.getClass().getName());
        }
      } catch (Exception ex) {
        logger.warn("Cache service " + service.getClass().getName() + " failed to initialize", ex);
      }
    }
  }

  private void handleResourceEventsForCacheServices() {
    for (CacheService service : services.values()) {
      system.handleResourceEvent(ResourceEvent.CACHE_SERVICE_CREATE, service);
    }
  }

  private boolean isServerNode() {
    return system.getDistributedMember().getVmKind() != LOCATOR_DM_TYPE
        && system.getDistributedMember().getVmKind() != ADMIN_ONLY_DM_TYPE
        && !isClient();
  }

  private void startRestAgentServer(InternalCache cache) {
    if (system.getConfig().getStartDevRestApi() && isServerNode()) {
      restAgent = new RestAgent(system.getConfig(), securityService);
      restAgent.start(cache);
    } else {
      restAgent = null;
    }
  }

  @Override
  public URL getCacheXmlURL() {
    if (getMyId().getVmKind() == LOCATOR_DM_TYPE) {
      return null;
    }

    File xmlFile = testCacheXml;
    if (xmlFile == null) {
      xmlFile = system.getConfig().getCacheXmlFile();
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
            String.format("Could not convert XML file %s to an URL.", xmlFile), ex);
      }
    }

    if (url == null) {
      File defaultFile = DistributionConfig.DEFAULT_CACHE_XML_FILE;
      if (!xmlFile.equals(defaultFile)) {
        if (!xmlFile.exists()) {
          throw new CacheXmlException(
              String.format("Declarative Cache XML file/resource %s does not exist.", xmlFile));
        }
        throw new CacheXmlException(
            String.format("Declarative XML file %s is not a file.", xmlFile));
      }
    }

    return url;
  }

  /**
   * Initialize the contents of this {@code Cache} according to the declarative caching XML file
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
   * @see #loadCacheXml
   */
  private void initializeDeclarativeCache()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    URL url = getCacheXmlURL();
    String cacheXmlDescription = cacheConfig.getCacheXMLDescription();
    if (url == null && cacheXmlDescription == null) {
      initializePdxRegistry();
      readyDynamicRegionFactory();
      // nothing needs to be done
      return;
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
      throw new CacheXmlException(String.format(
          "While opening Cache XML %s the following error occurred %s", url.toString(), ex));

    } catch (CacheXmlException ex) {
      throw new CacheXmlException(
          String.format("While reading Cache XML %s. %s", url, ex.getMessage()), ex.getCause());

    } finally {
      closeQuietly(stream);
    }
  }

  private static void logCacheXML(URL url, String cacheXmlDescription) {
    if (cacheXmlDescription == null) {
      StringBuilder sb = new StringBuilder();
      BufferedReader br = null;
      try {
        br = new BufferedReader(new InputStreamReader(url.openStream()));
        String line = br.readLine();
        while (line != null) {
          if (!line.isEmpty()) {
            sb.append(lineSeparator()).append(line);
          }
          line = br.readLine();
        }
      } catch (IOException ignore) {
      } finally {
        closeQuietly(br);
      }
      logger.info("Initializing cache using {}:{}", url, sb);

    } else {
      logger.info("Initializing cache using {}:{}", "generated description from old cache",
          cacheXmlDescription);
    }
  }

  @Override
  public synchronized void initializePdxRegistry() {
    if (pdxRegistry == null) {
      // The member with locator is initialized with a NullTypePdxRegistration
      if (getMyId().getVmKind() == LOCATOR_DM_TYPE) {
        pdxRegistry = typeRegistryFactory.create(this, true);
      } else {
        pdxRegistry = typeRegistryFactory.create(this, false);
      }
      pdxRegistry.initialize();
    }
  }

  @Override
  public void readyDynamicRegionFactory() {
    try {
      ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).internalInit(this);
    } catch (CacheException ce) {
      throw new GemFireCacheException("dynamic region initialization failed", ce);
    }
  }

  @Override
  public DiskStoreFactory createDiskStoreFactory() {
    return new DiskStoreFactoryImpl(this);
  }

  @Override
  public DiskStoreFactory createDiskStoreFactory(DiskStoreAttributes attrs) {
    return new DiskStoreFactoryImpl(this, attrs);
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  @Override
  public boolean forcedDisconnect() {
    return forcedDisconnect || system.forcedDisconnect();
  }

  @Override
  public CacheClosedException getCacheClosedException(String reason) {
    return getCacheClosedException(reason, null);
  }

  @Override
  public CacheClosedException getCacheClosedException(String reason, Throwable cause) {
    CacheClosedException result;
    if (cause != null) {
      result = new CacheClosedException(reason, cause);
    } else if (disconnectCause != null) {
      result = new CacheClosedException(reason, disconnectCause);
    } else {
      result = new CacheClosedException(reason);
    }
    return result;
  }

  @Override
  @VisibleForTesting
  public Throwable getDisconnectCause() {
    return disconnectCause;
  }

  @Override
  public boolean keepDurableSubscriptionsAlive() {
    return keepAlive;
  }

  /**
   * Close the distributed system, cache servers, and gateways. Clears the rootRegions and
   * partitionedRegions map. Marks the cache as closed.
   *
   * @see SystemFailure#emergencyClose()
   */
  public static void emergencyClose() {
    GemFireCacheImpl cache = getInstance();
    if (cache == null) {
      return;
    }

    // leave the PdxSerializer set if we have one

    // Shut down messaging first
    InternalDistributedSystem ids = cache.system;
    if (ids != null) {
      ids.emergencyClose();
    }

    cache.disconnectCause = SystemFailure.getFailure();
    cache.isClosing = true;

    for (InternalCacheServer cacheServer : cache.allCacheServers) {
      Acceptor acceptor = cacheServer.getAcceptor();
      if (acceptor != null) {
        acceptor.emergencyClose();
      }
    }

    closeGateWayReceiverServers(cache);

    PoolManagerImpl.emergencyClose();

    // rootRegions is intentionally *not* synchronized. The
    // implementation of clear() does not currently allocate objects.
    cache.rootRegions.clear();

    // partitionedRegions is intentionally *not* synchronized, The
    // implementation of clear() does not currently allocate objects.
    cache.partitionedRegions.clear();
  }

  private static void closeGateWayReceiverServers(GemFireCacheImpl cache) {
    InternalCacheServer receiverServer = cache.gatewayReceiverServer.get();
    if (receiverServer != null) {
      Acceptor acceptor = receiverServer.getAcceptor();
      if (acceptor != null) {
        acceptor.emergencyClose();
      }
    }
  }

  @Override
  public boolean isCacheAtShutdownAll() {
    return isShutDownAll.get();
  }

  private void shutdownSubTreeGracefully(Map<String, PartitionedRegion> prSubMap) {
    for (PartitionedRegion pr : prSubMap.values()) {
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

    if (!isShutDownAll.compareAndSet(false, true)) {
      // it's already doing shutdown by another thread
      try {
        shutDownAllFinished.await();
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
          throw new InternalGemFireError("unexpected exception");
        }

        // multithread shutDownAll should be grouped by root region. However, we have to close
        // colocated child regions first. Now check all the PR, if anyone has colocate-with
        // attribute, sort all the PRs by colocation relationship and close them sequentially,
        // otherwise still group them by root region.

        Map<String, Map<String, PartitionedRegion>> prTrees = getPRTrees();
        if (prTrees.size() > 1 && shutdownAllPoolSize != 1) {
          ExecutorService es = getShutdownAllExecutorService(prTrees.size());
          for (Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
            // for each root
            es.execute(() -> {
              threadWantsSharedResources();
              shutdownSubTreeGracefully(prSubMap);
            });
          }
          es.shutdown();
          try {
            es.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
          } catch (InterruptedException ignore) {
            logger
                .debug("Shutdown all interrupted while waiting for PRs to be shutdown gracefully.");
          }

        } else {
          for (Map<String, PartitionedRegion> prSubMap : prTrees.values()) {
            shutdownSubTreeGracefully(prSubMap);
          }
        }

        close("Shut down all members", null, false, true);
      } finally {
        shutDownAllFinished.countDown();
      }
    }
  }

  private ExecutorService getShutdownAllExecutorService(int size) {
    return newFixedThreadPool("ShutdownAll-", true,
        shutdownAllPoolSize == -1 ? size : shutdownAllPoolSize);
  }

  private void shutDownOnePRGracefully(PartitionedRegion partitionedRegion) {
    boolean acquiredLock = false;
    try {
      partitionedRegion.acquireDestroyLock();
      acquiredLock = true;

      synchronized (partitionedRegion.getRedundancyProvider()) {
        if (partitionedRegion.isDataStore()
            && partitionedRegion.getDataStore() != null
            && partitionedRegion.getDataPolicy() == DataPolicy.PERSISTENT_PARTITION) {
          int numBuckets = partitionedRegion.getTotalNumberOfBuckets();
          Map<InternalDistributedMember, PersistentMemberID>[] bucketMaps =
              createMapArray(numBuckets);
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

            // get map <InternalDistributedMember, persistentID> for this bucket's remote members
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
          partitionedRegion.setShutDownAllStatus(PRIMARY_BUCKETS_LOCKED);
          new UpdateAttributesProcessor(partitionedRegion).distribute(false);
          partitionedRegion.getRegionAdvisor().waitForProfileStatus(PRIMARY_BUCKETS_LOCKED);
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
            partitionedRegion.setShutDownAllStatus(DISK_STORE_FLUSHED);
            new UpdateAttributesProcessor(partitionedRegion).distribute(false);
            partitionedRegion.getRegionAdvisor().waitForProfileStatus(DISK_STORE_FLUSHED);
            if (logger.isDebugEnabled()) {
              logger.debug("shutDownAll: PR {}: all flush profiles received.",
                  partitionedRegion.getName());
            }
          }

          // persist other members to OFFLINE_EQUAL for each bucket region iterate through all the
          // bucketMaps and exclude the items whose idm is no longer online
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
          partitionedRegion.setShutDownAllStatus(OFFLINE_EQUAL_PERSISTED);
          new UpdateAttributesProcessor(partitionedRegion).distribute(false);
          partitionedRegion.getRegionAdvisor().waitForProfileStatus(OFFLINE_EQUAL_PERSISTED);
          if (logger.isDebugEnabled()) {
            logger.debug("shutDownAll: PR {}: all offline_equal profiles received.",
                partitionedRegion.getName());
          }
        }

        // after done all steps for buckets, close partitionedRegion close accessor directly
        RegionEventImpl event = new RegionEventImpl(partitionedRegion, Operation.REGION_CLOSE, null,
            false, getMyId(), true);
        try {
          // not to acquire lock
          partitionedRegion.basicDestroyRegion(event, false, false, true);
        } catch (CacheWriterException e) {
          // not possible with local operation, CacheWriter not called
          throw new Error("CacheWriterException should not be thrown in localDestroyRegion", e);
        } catch (TimeoutException e) {
          // not possible with local operation, no distributed locks possible
          throw new Error("TimeoutException should not be thrown in localDestroyRegion", e);
        }
      }
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

  @Override
  public DistributedLockService getPartitionedRegionLockService() {
    synchronized (prLockServiceLock) {
      stopper.checkCancelInProgress(null);
      if (prLockService == null) {
        try {
          prLockService = DLockService.create(PARTITION_LOCK_SERVICE_NAME,
              getInternalDistributedSystem(), true, true, true);
        } catch (IllegalArgumentException e) {
          prLockService = DistributedLockService.getServiceNamed(PARTITION_LOCK_SERVICE_NAME);
          if (prLockService == null) {
            // PARTITION_LOCK_SERVICE_NAME must be illegal!
            throw e;
          }
        }
      }
      return prLockService;
    }
  }

  @Override
  public DistributedLockService getGatewaySenderLockService() {
    if (gatewayLockService == null) {
      synchronized (gatewayLockServiceLock) {
        stopper.checkCancelInProgress(null);
        if (gatewayLockService == null) {
          try {
            gatewayLockService = DLockService.create(AbstractGatewaySender.LOCK_SERVICE_NAME,
                getInternalDistributedSystem(), true, true, true);
          } catch (IllegalArgumentException e) {
            gatewayLockService =
                DistributedLockService.getServiceNamed(AbstractGatewaySender.LOCK_SERVICE_NAME);
            if (gatewayLockService == null) {
              // AbstractGatewaySender.LOCK_SERVICE_NAME must be illegal!
              throw e;
            }
          }
        }
      }
    }
    return gatewayLockService;
  }

  /**
   * Destroys the PartitionedRegion distributed lock service when closing the cache. Caller must be
   * synchronized on this GemFireCache.
   */
  private void destroyPartitionedRegionLockService() {
    try {
      DistributedLockService.destroy(PARTITION_LOCK_SERVICE_NAME);
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

  @Override
  @VisibleForTesting
  public HeapEvictor getHeapEvictor() {
    synchronized (heapEvictorLock) {
      stopper.checkCancelInProgress(null);
      if (heapEvictor == null) {
        heapEvictor = heapEvictorFactory.create(this, statisticsClock);
      }
      return heapEvictor;
    }
  }

  @Override
  @VisibleForTesting
  public OffHeapEvictor getOffHeapEvictor() {
    synchronized (offHeapEvictorLock) {
      stopper.checkCancelInProgress(null);
      if (offHeapEvictor == null) {
        offHeapEvictor = new OffHeapEvictor(this, statisticsClock);
      }
      return offHeapEvictor;
    }
  }

  /**
   * Used by test to inject an evictor.
   */
  @VisibleForTesting
  void setOffHeapEvictor(OffHeapEvictor evictor) {
    offHeapEvictor = evictor;
  }

  /**
   * Used by test to inject an evictor.
   */
  @VisibleForTesting
  void setHeapEvictor(HeapEvictor evictor) {
    heapEvictor = evictor;
  }

  @Override
  public PersistentMemberManager getPersistentMemberManager() {
    return persistentMemberManager;
  }

  @Override
  public ClientMetadataService getClientMetadataService() {
    stopper.checkCancelInProgress(null);

    return clientMetadataService;
  }

  @Override
  public void close(String reason, Throwable systemFailureCause, boolean keepAlive,
      boolean keepDS) {
    securityService.close();

    if (isClosed()) {
      return;
    }

    if (!keepDS && systemFailureCause == null
        && (isReconnecting() || system.getReconnectedSystem() != null)) {
      logger.debug(
          "Cache is shutting down distributed system connection. isReconnecting={} reconnectedSystem={} keepAlive={} keepDS={}",
          isReconnecting(), system.getReconnectedSystem(), keepAlive, keepDS);

      system.stopReconnectingNoDisconnect();
      if (system.getReconnectedSystem() != null) {
        system.getReconnectedSystem().disconnect();
      }
      return;
    }

    boolean isDebugEnabled = logger.isDebugEnabled();

    synchronized (GemFireCacheImpl.class) {
      // ALL CODE FOR CLOSE SHOULD NOW BE UNDER STATIC SYNCHRONIZATION OF GemFireCacheImpl.class
      // static synchronization is necessary due to static resources
      if (isClosed()) {
        return;
      }

      // First close the ManagementService
      system.handleResourceEvent(ResourceEvent.CACHE_REMOVE, this);
      if (resourceEventsListener != null) {
        system.removeResourceListener(resourceEventsListener);
        resourceEventsListener = null;
      }

      if (systemFailureCause != null) {
        forcedDisconnect = systemFailureCause instanceof ForcedDisconnectException;
        if (forcedDisconnect) {
          disconnectCause = new ForcedDisconnectException(reason);
        } else {
          disconnectCause = systemFailureCause;
        }
      }

      this.keepAlive = keepAlive;
      isClosing = true;
      logger.info("{}: Now closing.", this);

      // we don't clear the prID map if there is a system failure. Other
      // threads may be hung trying to communicate with the map locked
      if (systemFailureCause == null) {
        PartitionedRegion.clearPRIdMap();
      }

      TXStateProxy tx = null;
      try {
        if (transactionManager != null) {
          tx = transactionManager.pauseTransaction();
        }

        // do this before closing regions
        resourceManager.close();

        try {
          resourceAdvisor.close();
        } catch (CancelException ignore) {
        }
        try {
          jmxAdvisor.close();
        } catch (CancelException ignore) {
        }

        for (GatewaySender sender : allGatewaySenders) {
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

        if (eventThreadPool != null) {
          if (isDebugEnabled) {
            logger.debug("{}: stopping event thread pool...", this);
          }
          eventThreadPool.shutdown();
        }

        // IMPORTANT: any operation during shut down that can time out (create a CancelException)
        // must be inside of this try block. If all else fails, we *must* ensure that the cache gets
        // closed!
        try {
          stopServers();

          stopServices();

          // no need to track PR instances
          if (isDebugEnabled) {
            logger.debug("{}: clearing partitioned regions...", this);
          }
          synchronized (partitionedRegions) {
            int prSize = -partitionedRegions.size();
            partitionedRegions.clear();
            getCachePerfStats().incPartitionedRegions(prSize);
          }

          prepareDiskStoresForClose();

          List<InternalRegion> rootRegionValues;
          synchronized (rootRegions) {
            rootRegionValues = new ArrayList<>(rootRegions.values());
          }

          Operation op;
          if (forcedDisconnect) {
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
                // this region will be closed internally by parent region
                continue;
              }
              if (isDebugEnabled) {
                logger.debug("{}: closing region {}...", this, lr.getFullPath());
              }
              try {
                lr.handleCacheClose(op);
              } catch (RuntimeException e) {
                if (isDebugEnabled || !forcedDisconnect) {
                  logger.warn(String.format("%s: error closing region %s", this, lr.getFullPath()),
                      e);
                }
              }
            }
          }

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
            logger.warn(
                String.format("%s: error in last stage of PartitionedRegion cache close", this), e);
          }
          destroyPartitionedRegionLockService();

          closeDiskStores();
          diskMonitor.close();

          // Close the CqService Handle.
          try {
            if (isDebugEnabled) {
              logger.debug("{}: closing CQ service...", this);
            }
            cqService.close();
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
          tombstoneService.stop();

          // NOTICE: the CloseCache message is the *last* message you can send!
          DistributionManager distributionManager = null;
          try {
            distributionManager = system.getDistributionManager();
            distributionManager.removeMembershipListener(transactionManager);
          } catch (CancelException ignore) {
          }

          if (distributionManager != null) {
            // Send CacheClosedMessage (and NOTHING ELSE) here
            if (isDebugEnabled) {
              logger.debug("{}: sending CloseCache to peers...", this);
            }
            Set<InternalDistributedMember> otherMembers =
                distributionManager.getOtherDistributionManagerIds();
            ReplyProcessor21 processor = replyProcessor21Factory.create(system, otherMembers);
            CloseCacheMessage msg = new CloseCacheMessage();
            msg.setRecipients(otherMembers);
            msg.setProcessorId(processor.getProcessorId());
            distributionManager.putOutgoing(msg);

            try {
              processor.waitForReplies();
            } catch (InterruptedException ignore) {
              // TODO: reset interrupt flag later?
              // Keep going, make best effort to shut down.
            } catch (ReplyException ignore) {
              // keep going
            }
            // set closed state after telling others and getting responses to avoid complications
            // with others still in the process of sending messages
          }
          // NO MORE Distributed Messaging AFTER THIS POINT!!!!

          ClientMetadataService cms = clientMetadataService;
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
          cqService.close();
        } catch (RuntimeException ignore) {
          logger.info("Failed to get the CqService, to close during cache close (2).");
        }

        cachePerfStats.close();
        TXLockService.destroyServices();
        getEventTrackerTask().cancel();

        synchronized (ccpTimerMutex) {
          if (ccpTimer != null) {
            ccpTimer.cancel();
          }
        }

        expirationScheduler.cancel();

        // Stop QueryMonitor if running.
        if (queryMonitor != null) {
          queryMonitor.stopMonitoring();
        }

      } finally {
        // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!
        if (transactionManager != null) {
          transactionManager.close();
        }
        ((DynamicRegionFactoryImpl) DynamicRegionFactory.get()).close();
        if (transactionManager != null) {
          transactionManager.unpauseTransaction(tx);
        }
        TXCommitMessage.getTracker().clearForCacheClose();
      }

      // Added to close the TransactionManager's cleanup thread
      TransactionManagerImpl.refresh();

      if (!keepDS) {
        // keepDS is used by ShutdownAll. It will override disableDisconnectDsOnCacheClose
        if (!disableDisconnectDsOnCacheClose) {
          system.disconnect();
        }
      }

      typeRegistryClose.run();
      typeRegistrySetPdxSerializer.accept(null);

      for (CacheLifecycleListener listener : cacheLifecycleListeners) {
        listener.cacheClosed(this);
      }

      SequenceLoggerImpl.signalCacheClose();
      SystemFailure.signalCacheClose();
    }
  }

  private void stopServices() {
    for (CacheService service : services.values()) {
      try {
        service.close();
      } catch (Throwable t) {
        logger.warn("Error stopping service " + service, t);
      }
    }
  }

  private void closeOffHeapEvictor() {
    OffHeapEvictor evictor = offHeapEvictor;
    if (evictor != null) {
      evictor.close();
    }
  }

  private void closeHeapEvictor() {
    HeapEvictor evictor = heapEvictor;
    if (evictor != null) {
      evictor.close();
    }
  }

  @Override
  public boolean isReconnecting() {
    return system.isReconnecting();
  }

  @Override
  public boolean waitUntilReconnected(long time, TimeUnit units) throws InterruptedException {
    try {
      boolean systemReconnected = system.waitUntilReconnected(time, units);
      if (!systemReconnected) {
        return false;
      }
      GemFireCacheImpl cache = getInstance();
      return cache != null && cache.isInitialized();
    } catch (CancelException e) {
      throw new CacheClosedException("Cache could not be recreated", e);
    }
  }

  @Override
  public void stopReconnecting() {
    system.stopReconnecting();
  }

  @Override
  public Cache getReconnectedCache() {
    GemFireCacheImpl cache = getInstance();
    if (cache == this || cache != null && !cache.isInitialized()) {
      cache = null;
    }
    return cache;
  }

  private void prepareDiskStoresForClose() {
    String pdxDSName = typeRegistryGetPdxDiskStoreName.apply(this);
    DiskStoreImpl pdxDiskStore = null;
    for (DiskStoreImpl dsi : diskStores.values()) {
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

  @Override
  public void addDiskStore(DiskStoreImpl dsi) {
    diskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      diskMonitor.addDiskStore(dsi);
    }
  }

  @Override
  public void removeDiskStore(DiskStoreImpl diskStore) {
    diskStores.remove(diskStore.getName());
    regionOwnedDiskStores.remove(diskStore.getName());
    if (!diskStore.getOwnedByRegion()) {
      system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, diskStore);
    }
  }

  @Override
  public void addRegionOwnedDiskStore(DiskStoreImpl dsi) {
    regionOwnedDiskStores.put(dsi.getName(), dsi);
    if (!dsi.isOffline()) {
      diskMonitor.addDiskStore(dsi);
    }
  }

  @Override
  @VisibleForTesting
  public void closeDiskStores() {
    Iterator<DiskStoreImpl> it = diskStores.values().iterator();
    while (it.hasNext()) {
      try {
        DiskStoreImpl dsi = it.next();
        if (logger.isDebugEnabled()) {
          logger.debug("closing {}", dsi);
        }
        dsi.close();
        system.handleResourceEvent(ResourceEvent.DISKSTORE_REMOVE, dsi);
      } catch (RuntimeException e) {
        logger.fatal("Cache close caught an exception during disk store close", e);
      }
      it.remove();
    }
  }

  /**
   * Used by unit tests to allow them to change the default disk store name.
   */
  @VisibleForTesting
  public static void setDefaultDiskStoreName(String dsName) {
    defaultDiskStoreName = dsName;
  }

  public static String getDefaultDiskStoreName() {
    return defaultDiskStoreName;
  }

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

  @Override
  public DiskStore findDiskStore(String name) {
    if (name == null) {
      name = defaultDiskStoreName;
    }
    return diskStores.get(name);
  }

  @Override
  public Collection<DiskStore> listDiskStores() {
    return unmodifiableCollection(diskStores.values());
  }

  @Override
  public Collection<DiskStore> listDiskStoresIncludingRegionOwned() {
    Collection<DiskStore> allDiskStores = new HashSet<>();
    allDiskStores.addAll(diskStores.values());
    allDiskStores.addAll(regionOwnedDiskStores.values());
    return allDiskStores;
  }

  private void stopServers() {
    boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: stopping cache servers...", this);
    }

    boolean stoppedCacheServer = false;

    for (InternalCacheServer cacheServer : allCacheServers) {
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
      allCacheServers.remove(cacheServer);
      stoppedCacheServer = true;
    }

    InternalCacheServer receiverServer = gatewayReceiverServer.getAndSet(null);
    if (receiverServer != null) {
      if (isDebugEnabled) {
        logger.debug("stopping gateway receiver server {}", receiverServer);
      }
      try {
        receiverServer.stop();
      } catch (CancelException e) {
        if (isDebugEnabled) {
          logger.debug("Ignored cache closure while closing gateway receiver server {}",
              receiverServer, e);
        }
      }
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

    // Reset the unique id counter for durable clients. If a durable client stops/starts its cache,
    // it needs to maintain the same unique id.
    ClientProxyMembershipID.resetUniqueIdCounter();
  }

  @Override
  public DistributedSystem getDistributedSystem() {
    return system;
  }

  @Override
  public InternalDistributedSystem getInternalDistributedSystem() {
    return system;
  }

  @Override
  public InternalDistributedMember getMyId() {
    return system.getDistributedMember();
  }

  @Override
  public Set<DistributedMember> getMembers() {
    return unmodifiableSet(dm.getOtherNormalDistributionManagerIds());
  }

  @Override
  public Set<DistributedMember> getAdminMembers() {
    return asDistributedMemberSet(dm.getAdminMemberSet());
  }

  @Override
  public Set<DistributedMember> getMembers(Region region) {
    if (region instanceof DistributedRegion) {
      DistributedRegion distributedRegion = (DistributedRegion) region;
      return asDistributedMemberSet(distributedRegion.getDistributionAdvisor().adviseCacheOp());
    }
    if (region instanceof PartitionedRegion) {
      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      return asDistributedMemberSet(partitionedRegion.getRegionAdvisor().adviseAllPRNodes());
    }
    return emptySet();
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
        result.add(InetSocketAddress.createUnresolved(serverLocation.getHostName(),
            serverLocation.getPort()));
      }
    }
    if (result == null) {
      return emptySet();
    }
    return result;
  }

  @Override
  public LogWriter getLogger() {
    return system.getLogWriter();
  }

  @Override
  public LogWriter getSecurityLogger() {
    return system.getSecurityLogWriter();
  }

  @Override
  public LogWriterI18n getLoggerI18n() {
    return system.getInternalLogWriter();
  }

  @Override
  public LogWriterI18n getSecurityLoggerI18n() {
    return system.getSecurityInternalLogWriter();
  }

  @Deprecated
  @Override
  public InternalLogWriter getInternalLogWriter() {
    return system.getInternalLogWriter();
  }

  @Deprecated
  @Override
  public InternalLogWriter getSecurityInternalLogWriter() {
    return system.getSecurityInternalLogWriter();
  }

  @Override
  public EventTrackerExpiryTask getEventTrackerTask() {
    return recordedEventSweeper;
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    return cachePerfStats;
  }

  @Override
  public String getName() {
    return system.getName();
  }

  @Override
  public List<Properties> getDeclarableProperties(String className) {
    List<Properties> propertiesList = new ArrayList<>();
    synchronized (declarablePropertiesMap) {
      for (Entry<Declarable, Properties> entry : declarablePropertiesMap.entrySet()) {
        if (entry.getKey().getClass().getName().equals(className)) {
          propertiesList.add(entry.getValue());
        }
      }
    }
    return propertiesList;
  }

  @Override
  public Properties getDeclarableProperties(Declarable declarable) {
    return declarablePropertiesMap.get(declarable);
  }

  @Override
  public long getUpTime() {
    return (System.currentTimeMillis() - creationDate.getTime()) / 1000;
  }

  @Override
  public long cacheTimeMillis() {
    if (system != null) {
      return system.getClock().cacheTimeMillis();
    }
    return System.currentTimeMillis();
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    return createRegion(name, aRegionAttributes);
  }

  private PoolFactory createDefaultPF() {
    PoolFactory defaultPoolFactory = PoolManager.createFactory();
    try {
      String localHostName = LocalHostUtil.getLocalHostName();
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
        String localHostName = LocalHostUtil.getLocalHostName();
        poolFactoryImpl.addServer(localHostName, CacheServer.DEFAULT_PORT);
      } catch (UnknownHostException ex) {
        throw new IllegalStateException("Could not determine local host name", ex);
      }
    }
  }

  /**
   * Set the default pool on a new GemFireCache.
   */
  private synchronized void determineDefaultPool() {
    if (!isClient()) {
      throw new UnsupportedOperationException();
    }

    PoolFactory defaultPoolFactory = poolFactory;

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
          defaultPool = null;
          return;
        }
      }

    } else {
      addLocalHostAsServer(defaultPoolFactory);

      // look for a pool that already exists that is compatible with our PoolFactory.
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

    defaultPool = pool;
  }

  @Override
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

  @Override
  public <K, V> Region<K, V> basicCreateRegion(String name, RegionAttributes<K, V> attrs)
      throws RegionExistsException, TimeoutException {
    try {
      InternalRegionArguments ira = new InternalRegionArguments()
          .setDestroyLockFlag(true)
          .setRecreateFlag(false)
          .setSnapshotInputStream(null)
          .setImageTarget(null);

      if (attrs instanceof UserSpecifiedRegionAttributes) {
        ira.setIndexes(((UserSpecifiedRegionAttributes) attrs).getIndexes());
      }
      return createVMRegion(name, attrs, ira);
    } catch (IOException | ClassNotFoundException e) {
      throw new InternalGemFireError("unexpected exception", e);
    }
  }

  @Override
  public <K, V> Region<K, V> createVMRegion(String name, RegionAttributes<K, V> p_attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {
    if (getMyId().getVmKind() == LOCATOR_DM_TYPE) {
      if (!internalRegionArgs.isUsedForMetaRegion()
          && internalRegionArgs.getInternalMetaRegion() == null) {
        throw new IllegalStateException("Regions can not be created in a locator.");
      }
    }

    stopper.checkCancelInProgress(null);

    RegionNameValidation.validate(name, internalRegionArgs);
    RegionAttributes<K, V> attrs = p_attrs;
    attrs = invokeRegionBefore(null, name, attrs, internalRegionArgs);
    if (attrs == null) {
      throw new IllegalArgumentException("Attributes must not be null");
    }

    InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();

    boolean recreate = internalRegionArgs.getRecreateFlag();
    boolean isPartitionedRegion = attrs.getPartitionAttributes() != null;
    boolean isReInitCreate = snapshotInputStream != null || imageTarget != null || recreate;

    InternalRegion region;
    try {
      for (;;) {
        getCancelCriterion().checkCancelInProgress(null);

        Future<InternalRegion> future = null;
        synchronized (rootRegions) {
          region = rootRegions.get(name);
          if (region != null) {
            throw new RegionExistsException(region);
          }
          // check for case where a root region is being reinitialized
          // and we didn't find a region, i.e. the new region is about to be created

          if (!isReInitCreate) {
            String fullPath = Region.SEPARATOR + name;
            future = reinitializingRegions.get(fullPath);
          }
          if (future == null) {
            if (internalRegionArgs.getInternalMetaRegion() != null) {
              region = internalRegionArgs.getInternalMetaRegion();
            } else if (isPartitionedRegion) {
              region = new PartitionedRegion(name, attrs, null, this, internalRegionArgs,
                  statisticsClock, ColocationLoggerFactory.create());
            } else {
              // Abstract region depends on the default pool existing so lazily initialize it
              // if necessary.
              if (Objects.equals(attrs.getPoolName(), DEFAULT_POOL_NAME)) {
                determineDefaultPool();
              }
              if (attrs.getScope().isLocal()) {
                region =
                    new LocalRegion(name, attrs, null, this, internalRegionArgs, statisticsClock);
              } else {
                region = new DistributedRegion(name, attrs, null, this, internalRegionArgs,
                    statisticsClock);
              }
            }

            rootRegions.put(name, region);
            if (isReInitCreate) {
              regionReinitialized(region);
            }
            break;
          }
        }

        boolean interrupted = Thread.interrupted();
        try {
          throw new RegionExistsException(future.get());
        } catch (InterruptedException ignore) {
          interrupted = true;
        } catch (ExecutionException e) {
          throw new Error("unexpected exception", e);
        } catch (CancellationException e) {
          if (logger.isTraceEnabled()) {
            logger.trace("future cancelled", e);
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }

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
            // do this before removing the region from the root set
            region.cleanupFailedInitialization();
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable t) {
            SystemFailure.checkFailure();
            stopper.checkCancelInProgress(t);

            // log the failure but don't override the original exception
            logger.warn(String.format("Initialization failed for Region %s", region.getFullPath()),
                t);

          } finally {
            // clean up if initialize fails for any reason
            setRegionByPath(region.getFullPath(), null);
            synchronized (rootRegions) {
              Region rootRegion = rootRegions.get(name);
              if (rootRegion == region) {
                rootRegions.remove(name);
              }
            }
          }
        }
      }

      region.postCreateRegion();
    } catch (RegionExistsException ex) {
      // outside of sync make sure region is initialized
      InternalRegion internalRegion = (InternalRegion) ex.getRegion();
      internalRegion.waitOnInitialization(); // don't give out ref until initialized
      throw ex;
    }

    invokeRegionAfter(region);

    // Putting the callback here to avoid creating RegionMBean in case of Exception
    if (!region.isInternalRegion()) {
      system.handleResourceEvent(ResourceEvent.REGION_CREATE, region);
    }

    return cast(region);
  }

  @Override
  public <K, V> RegionAttributes<K, V> invokeRegionBefore(InternalRegion parent, String name,
      RegionAttributes<K, V> attrs, InternalRegionArguments internalRegionArgs) {
    for (RegionListener listener : regionListeners) {
      attrs =
          uncheckedRegionAttributes(listener.beforeCreate(parent, name, attrs, internalRegionArgs));
    }
    return attrs;
  }

  @Override
  public void invokeRegionAfter(InternalRegion region) {
    for (RegionListener listener : regionListeners) {
      listener.afterCreate(region);
    }
  }

  @Override
  public void invokeBeforeDestroyed(InternalRegion region) {
    for (RegionListener listener : regionListeners) {
      listener.beforeDestroyed(region);
    }
  }

  @Override
  public void invokeCleanupFailedInitialization(InternalRegion region) {
    for (RegionListener listener : regionListeners) {
      listener.cleanupFailedInitialization(region);
    }
  }

  @Override
  public <K, V> Region<K, V> getRegion(String path) {
    return getRegion(path, false);
  }

  @Override
  public Set<InternalRegion> getAllRegions() {
    Set<InternalRegion> result = new HashSet<>();
    synchronized (rootRegions) {
      for (Region<?, ?> region : rootRegions.values()) {
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
    synchronized (rootRegions) {
      for (Object region : rootRegions.values()) {
        InternalRegion internalRegion = (InternalRegion) region;
        if (internalRegion.isInternalRegion()) {
          // Skip internal regions
          continue;
        }
        result.add(internalRegion);
        result.addAll(internalRegion.basicSubregions(true));
      }
    }
    return result;
  }

  @Override
  public boolean hasPersistentRegion() {
    synchronized (rootRegions) {
      for (InternalRegion region : rootRegions.values()) {
        if (region.getDataPolicy().withPersistence()) {
          return true;
        }
        for (InternalRegion subRegion : region.basicSubregions(true)) {
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
      pathToRegion.remove(path);
    } else {
      pathToRegion.put(path, r);
    }
  }

  /**
   * @throws IllegalArgumentException if path is not valid
   */
  private static void validatePath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("path cannot be null");
    }
    if (path.isEmpty()) {
      throw new IllegalArgumentException("path cannot be empty");
    }
    if (path.equals(Region.SEPARATOR)) {
      throw new IllegalArgumentException(String.format("path cannot be ' %s '", Region.SEPARATOR));
    }
  }

  @Override
  public <K, V> Region<K, V> getRegionByPath(String path) {
    return cast(getInternalRegionByPath(path));
  }

  @Override
  public InternalRegion getInternalRegionByPath(String path) {
    validatePath(path);

    // do this before checking the pathToRegion map
    InternalRegion result = getReinitializingRegion(path);
    if (result != null) {
      return result;
    }
    return pathToRegion.get(path);
  }

  @Override
  public InternalRegion getRegionByPathForProcessing(String path) {
    Region<?, ?> result = getRegionByPath(path);
    if (result == null) {
      stopper.checkCancelInProgress(null);

      // go through initialization latches
      InitializationLevel oldLevel = setThreadInitLevelRequirement(InitializationLevel.ANY_INIT);
      try {
        String[] pathParts = parsePath(path);
        InternalRegion rootRegion;
        synchronized (rootRegions) {
          rootRegion = rootRegions.get(pathParts[0]);
          if (rootRegion == null) {
            return null;
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, calling getSubregion on rootRegion({}): {}",
              pathParts[0], pathParts[1]);
        }
        result = rootRegion.getSubregion(pathParts[1], true);
      } finally {
        setThreadInitLevelRequirement(oldLevel);
      }
    }
    return (InternalRegion) result;
  }

  @Override
  public <K, V> Region<K, V> getRegion(String path, boolean returnDestroyedRegion) {
    stopper.checkCancelInProgress(null);

    InternalRegion result = getInternalRegionByPath(path);

    // Do not waitOnInitialization() for PR
    if (result != null) {
      result.waitOnInitialization();
      if (!returnDestroyedRegion && result.isDestroyed()) {
        stopper.checkCancelInProgress(null);
        return null;
      }
      return cast(result);
    }

    String[] pathParts = parsePath(path);
    InternalRegion rootRegion;
    synchronized (rootRegions) {
      rootRegion = rootRegions.get(pathParts[0]);
      if (rootRegion == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache.getRegion, no region found for {}", pathParts[0]);
        }
        stopper.checkCancelInProgress(null);
        return null;
      }
      if (!returnDestroyedRegion && rootRegion.isDestroyed()) {
        stopper.checkCancelInProgress(null);
        return null;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("GemFireCache.getRegion, calling getSubregion on rootRegion({}): {}",
          pathParts[0], pathParts[1]);
    }
    return cast(rootRegion.getSubregion(pathParts[1], returnDestroyedRegion));
  }

  @Override
  public boolean isGlobalRegionInitializing(String fullPath) {
    stopper.checkCancelInProgress(null);
    // go through initialization latches
    InitializationLevel oldLevel = setThreadInitLevelRequirement(InitializationLevel.ANY_INIT);
    try {
      return isGlobalRegionInitializing((InternalRegion) getRegion(fullPath));
    } finally {
      setThreadInitLevelRequirement(oldLevel);
    }
  }

  /**
   * Return true if the region is initializing.
   */
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

  @Override
  public Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions) {
    return rootRegions(includePRAdminRegions, true);
  }

  private Set<Region<?, ?>> rootRegions(boolean includePRAdminRegions, boolean waitForInit) {
    stopper.checkCancelInProgress(null);
    Set<Region<?, ?>> regions = new HashSet<>();
    synchronized (rootRegions) {
      for (InternalRegion region : rootRegions.values()) {
        // If this is an internal meta-region, don't return it to end user
        if (region.isSecret()
            || region.isUsedForMetaRegion()
            || !includePRAdminRegions
                && (region.isUsedForPartitionedRegionAdmin()
                    || region.isUsedForPartitionedRegionBucket())) {
          // Skip administrative PartitionedRegions
          continue;
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
    return unmodifiableSet(regions);
  }

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
    return isInitialized;
  }

  @Override
  public boolean isClosed() {
    return isClosing;
  }

  @Override
  public int getLockTimeout() {
    return lockTimeout;
  }

  @Override
  public void setLockTimeout(int seconds) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    lockTimeout = seconds;
  }

  @Override
  public int getLockLease() {
    return lockLease;
  }

  @Override
  public void setLockLease(int seconds) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    lockLease = seconds;
  }

  @Override
  public int getSearchTimeout() {
    return searchTimeout;
  }

  @Override
  public void setSearchTimeout(int seconds) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    searchTimeout = seconds;
  }

  @Override
  public int getMessageSyncInterval() {
    return HARegionQueue.getMessageSyncInterval();
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    if (seconds < 0) {
      throw new IllegalArgumentException(
          "The 'messageSyncInterval' property for cache cannot be negative");
    }
    HARegionQueue.setMessageSyncInterval(seconds);
  }

  @Override
  public InternalRegion getReinitializingRegion(String fullPath) {
    Future<InternalRegion> future = reinitializingRegions.get(fullPath);
    if (future == null) {
      return null;
    }
    try {
      InternalRegion region = future.get();
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

  @Override
  public void regionReinitializing(String fullPath) {
    Object old = reinitializingRegions.putIfAbsent(fullPath, new FutureResult<>(stopper));
    if (old != null) {
      throw new IllegalStateException(
          String.format("Found an existing reinitializing region named %s",
              fullPath));
    }
  }

  @Override
  public void regionReinitialized(Region<?, ?> region) {
    String regionName = region.getFullPath();
    FutureResult<InternalRegion> future = reinitializingRegions.get(regionName);
    if (future == null) {
      throw new IllegalStateException(
          String.format("Could not find a reinitializing region named %s",
              regionName));
    }
    future.set((InternalRegion) region);
    unregisterReinitializingRegion(regionName);
  }

  @Override
  public void unregisterReinitializingRegion(String fullPath) {
    reinitializingRegions.remove(fullPath);
  }

  @Override
  public boolean isCopyOnRead() {
    return copyOnRead;
  }

  @Override
  public void setCopyOnRead(boolean copyOnRead) {
    this.copyOnRead = copyOnRead;
  }

  @Override
  public boolean getCopyOnRead() {
    return copyOnRead;
  }

  @Override
  public boolean removeRoot(InternalRegion rootRgn) {
    synchronized (rootRegions) {
      String regionName = rootRgn.getName();
      InternalRegion found = rootRegions.get(regionName);
      if (found == rootRgn) {
        InternalRegion previous = rootRegions.remove(regionName);
        Assert.assertTrue(previous == rootRgn);
        return true;
      }
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
   * Add the {@code CacheLifecycleListener}.
   */
  public static void addCacheLifecycleListener(CacheLifecycleListener listener) {
    synchronized (GemFireCacheImpl.class) {
      cacheLifecycleListeners.add(listener);
    }
  }

  /**
   * Remove the {@code CacheLifecycleListener}.
   *
   * @return true if the listener was removed
   */
  public static boolean removeCacheLifecycleListener(CacheLifecycleListener listener) {
    synchronized (GemFireCacheImpl.class) {
      return cacheLifecycleListeners.remove(listener);
    }
  }

  @Override
  public void addRegionListener(RegionListener regionListener) {
    regionListeners.add(regionListener);
  }

  @Override
  public void removeRegionListener(RegionListener regionListener) {
    regionListeners.remove(regionListener);
  }

  @Override
  public Set<RegionListener> getRegionListeners() {
    return unmodifiableSet(regionListeners);
  }

  @Override
  public <T extends CacheService> T getService(Class<T> clazz) {
    return clazz.cast(services.get(clazz));
  }

  @Override
  public <T extends CacheService> Optional<T> getOptionalService(Class<T> clazz) {
    return Optional.ofNullable(getService(clazz));
  }

  @Override
  public Collection<CacheService> getServices() {
    return unmodifiableCollection(services.values());
  }

  @Override
  public CacheTransactionManager getCacheTransactionManager() {
    return transactionManager;
  }

  @Override
  public SystemTimer getCCPTimer() {
    synchronized (ccpTimerMutex) {
      if (ccpTimer != null) {
        return ccpTimer;
      }
      ccpTimer = systemTimerFactory.apply(getDistributedSystem());
      if (isClosing) {
        // poison it, don't throw.
        ccpTimer.cancel();
      }
      return ccpTimer;
    }
  }

  /**
   * For use by unit tests to inject a mocked ccpTimer
   */
  @VisibleForTesting
  void setCCPTimer(SystemTimer ccpTimer) {
    this.ccpTimer = ccpTimer;
  }

  @Override
  public void purgeCCPTimer() {
    synchronized (ccpTimerMutex) {
      if (ccpTimer != null) {
        cancelCount++;
        if (cancelCount == PURGE_INTERVAL) {
          cancelCount = 0;
          ccpTimer.timerPurge();
        }
      }
    }
  }

  @Override
  public ExpirationScheduler getExpirationScheduler() {
    return expirationScheduler;
  }

  @Override
  public TXManagerImpl getTXMgr() {
    return transactionManager;
  }

  @Override
  public Executor getEventThreadPool() {
    return eventThreadPool;
  }

  @Override
  public CacheServer addCacheServer() {
    throwIfClient();
    stopper.checkCancelInProgress(null);

    InternalCacheServer server = new ServerBuilder(this, securityService,
        StatisticsClockFactory.disabledClock()).createServer();
    allCacheServers.add(server);

    sendAddCacheServerProfileMessage();
    return server;
  }

  @Override
  @VisibleForTesting
  public boolean removeCacheServer(CacheServer cacheServer) {
    boolean removed = allCacheServers.remove(cacheServer);
    sendRemoveCacheServerProfileMessage();
    return removed;
  }

  @Override
  public void addGatewaySender(GatewaySender sender) {
    throwIfClient();

    stopper.checkCancelInProgress(null);

    synchronized (allGatewaySendersLock) {
      if (!allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((DistributionAdvisee) sender).distribute(true);
        Set<GatewaySender> newSenders = new HashSet<>(allGatewaySenders.size() + 1);
        if (!allGatewaySenders.isEmpty()) {
          newSenders.addAll(allGatewaySenders);
        }
        newSenders.add(sender);
        allGatewaySenders = unmodifiableSet(newSenders);
      } else {
        throw new IllegalStateException(
            String.format("A GatewaySender with id %s is already defined in this cache.",
                sender.getId()));
      }
    }

    synchronized (rootRegions) {
      Set<InternalRegion> applicationRegions = getApplicationRegions();
      for (InternalRegion region : applicationRegions) {
        Set<String> senders = region.getAllGatewaySenderIds();
        if (senders.contains(sender.getId()) && !sender.isParallel()) {
          region.senderCreated();
        }
      }
    }

    if (!sender.isParallel()) {
      Region<?, ?> dynamicMetaRegion = getRegion(DynamicRegionFactory.DYNAMIC_REGION_LIST_NAME);
      if (dynamicMetaRegion == null) {
        if (logger.isDebugEnabled()) {
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

  @Override
  public void removeGatewaySender(GatewaySender sender) {
    throwIfClient();

    stopper.checkCancelInProgress(null);

    synchronized (allGatewaySendersLock) {
      if (allGatewaySenders.contains(sender)) {
        new UpdateAttributesProcessor((DistributionAdvisee) sender, true).distribute(true);
        Set<GatewaySender> newSenders = new HashSet<>(allGatewaySenders.size() - 1);
        if (!allGatewaySenders.isEmpty()) {
          newSenders.addAll(allGatewaySenders);
        }
        newSenders.remove(sender);
        allGatewaySenders = unmodifiableSet(newSenders);
      }
    }
    if (!(sender.getRemoteDSId() < 0)) {
      system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_REMOVE, sender);
    }
  }

  @Override
  public InternalCacheServer addGatewayReceiverServer(GatewayReceiver receiver) {
    throwIfClient();
    stopper.checkCancelInProgress(null);

    requireNonNull(receiver, "GatewayReceiver must be supplied to add a server endpoint.");
    requireNonNull(gatewayReceiver.get(),
        "GatewayReceiver must be added before adding a server endpoint.");

    InternalCacheServer receiverServer = new ServerBuilder(this, securityService,
        StatisticsClockFactory.disabledClock())
            .forGatewayReceiver(receiver).createServer();
    gatewayReceiverServer.set(receiverServer);

    sendAddCacheServerProfileMessage();
    return receiverServer;
  }

  @Override
  public boolean removeGatewayReceiverServer(InternalCacheServer receiverServer) {
    boolean removed = gatewayReceiverServer.compareAndSet(receiverServer, null);
    sendRemoveCacheServerProfileMessage();
    return removed;
  }

  @Override
  public void addGatewayReceiver(GatewayReceiver receiver) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    requireNonNull(receiver, "GatewayReceiver must be supplied.");
    gatewayReceiver.set(receiver);
  }

  @Override
  public void removeGatewayReceiver(GatewayReceiver receiver) {
    throwIfClient();
    stopper.checkCancelInProgress(null);
    gatewayReceiver.set(null);
  }

  @Override
  public void addAsyncEventQueue(AsyncEventQueueImpl asyncQueue) {
    allAsyncEventQueues.add(asyncQueue);
    if (!asyncQueue.isMetaQueue()) {
      allVisibleAsyncEventQueues.add(asyncQueue);
    }
    system.handleResourceEvent(ResourceEvent.ASYNCEVENTQUEUE_CREATE, asyncQueue);
  }

  @Override
  public Set<GatewaySender> getGatewaySenders() {
    Set<GatewaySender> senders = new HashSet<>();
    for (GatewaySender sender : allGatewaySenders) {
      if (!((AbstractGatewaySender) sender).isForInternalUse()) {
        senders.add(sender);
      }
    }
    return senders;
  }

  @Override
  public Set<GatewaySender> getAllGatewaySenders() {
    return allGatewaySenders;
  }

  @Override
  public GatewaySender getGatewaySender(String id) {
    for (GatewaySender sender : allGatewaySenders) {
      if (sender.getId().equals(id)) {
        return sender;
      }
    }
    return null;
  }

  @Override
  public Set<GatewayReceiver> getGatewayReceivers() {
    GatewayReceiver receiver = gatewayReceiver.get();
    if (receiver == null) {
      return emptySet();
    }
    return Collections.singleton(receiver);
  }

  @Override
  public Set<AsyncEventQueue> getAsyncEventQueues() {
    return getAsyncEventQueues(true);
  }

  @Override
  @VisibleForTesting
  public Set<AsyncEventQueue> getAsyncEventQueues(boolean visibleOnly) {
    return visibleOnly ? allVisibleAsyncEventQueues : allAsyncEventQueues;
  }

  @Override
  public AsyncEventQueue getAsyncEventQueue(String id) {
    for (AsyncEventQueue asyncEventQueue : allAsyncEventQueues) {
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
    if (asyncQueue instanceof InternalAsyncEventQueue) {
      removeGatewaySender(((InternalAsyncEventQueue) asyncQueue).getSender());
    }
    // using gateway senders lock since async queue uses a gateway sender
    synchronized (allGatewaySendersLock) {
      allAsyncEventQueues.remove(asyncQueue);
      allVisibleAsyncEventQueues.remove(asyncQueue);
    }
    system.handleResourceEvent(ResourceEvent.ASYNCEVENTQUEUE_REMOVE, asyncQueue);
  }

  @Override
  public GatewayConflictResolver getGatewayConflictResolver() {
    synchronized (allGatewayHubsLock) {
      return gatewayConflictResolver;
    }
  }

  @Override
  public void setGatewayConflictResolver(GatewayConflictResolver resolver) {
    synchronized (allGatewayHubsLock) {
      gatewayConflictResolver = resolver;
    }
  }

  @Override
  public List<CacheServer> getCacheServers() {
    return unmodifiableAllCacheServers;
  }

  @Override
  public List<InternalCacheServer> getCacheServersAndGatewayReceiver() {
    List<InternalCacheServer> allServers = new ArrayList<>(allCacheServers);

    InternalCacheServer receiverServer = gatewayReceiverServer.get();
    if (receiverServer != null) {
      allServers.add(receiverServer);
    }

    return unmodifiableList(allServers);
  }

  @Override
  public void addPartitionedRegion(PartitionedRegion region) {
    synchronized (partitionedRegions) {
      if (region.isDestroyed()) {
        if (logger.isDebugEnabled()) {
          logger.debug("GemFireCache#addPartitionedRegion did not add destroyed {}", region);
        }
        return;
      }
      if (partitionedRegions.add(region)) {
        getCachePerfStats().incPartitionedRegions(1);
      }
    }
  }

  @Override
  public Set<PartitionedRegion> getPartitionedRegions() {
    synchronized (partitionedRegions) {
      return new HashSet<>(partitionedRegions);
    }
  }

  private Map<String, Map<String, PartitionedRegion>> getPRTrees() {
    // prTree will save a sublist of PRs who are under the same root
    Map<String, PartitionedRegion> prMap = getPartitionedRegionMap();
    boolean hasColocatedRegion = false;
    for (PartitionedRegion pr : prMap.values()) {
      List<PartitionedRegion> childList = getColocatedChildRegions(pr);
      if (childList != null && !childList.isEmpty()) {
        hasColocatedRegion = true;
        break;
      }
    }

    Map<String, Map<String, PartitionedRegion>> prTrees = new TreeMap<>();
    if (hasColocatedRegion) {
      Map<String, PartitionedRegion> orderedPrMap = orderByColocation(prMap);
      prTrees.put("ROOT", orderedPrMap);
    } else {
      for (PartitionedRegion pr : prMap.values()) {
        String rootName = pr.getRoot().getName();
        Map<String, PartitionedRegion> prSubMap =
            prTrees.computeIfAbsent(rootName, k -> new TreeMap<>());
        prSubMap.put(pr.getFullPath(), pr);
      }
    }

    return prTrees;
  }

  private Map<String, PartitionedRegion> getPartitionedRegionMap() {
    Map<String, PartitionedRegion> prMap = new TreeMap<>();
    for (Entry<String, InternalRegion> entry : pathToRegion.entrySet()) {
      String regionName = entry.getKey();
      InternalRegion region = entry.getValue();

      // Don't wait for non partitioned regions
      if (!(region instanceof PartitionedRegion)) {
        continue;
      }
      // call getRegion to ensure that we wait for the partitioned region to finish initialization
      try {
        Region<?, ?> pr = getRegion(regionName);
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
    Map<String, PartitionedRegion> orderedPrMap = new LinkedHashMap<>();
    for (PartitionedRegion pr : prMap.values()) {
      addColocatedChildRecursively(orderedPrMap, pr);
    }
    return orderedPrMap;
  }

  private void addColocatedChildRecursively(Map<String, PartitionedRegion> prMap,
      PartitionedRegion pr) {
    for (PartitionedRegion colocatedRegion : getColocatedChildRegions(pr)) {
      addColocatedChildRecursively(prMap, colocatedRegion);
    }
    prMap.put(pr.getFullPath(), pr);
  }

  @Override
  public boolean requiresNotificationFromPR(PartitionedRegion region) {
    boolean hasSerialSenders = hasSerialSenders(region);

    if (!hasSerialSenders) {
      for (InternalCacheServer server : allCacheServers) {
        if (!server.getNotifyBySubscription()) {
          hasSerialSenders = true;
          break;
        }
      }
    }

    if (!hasSerialSenders) {
      InternalCacheServer receiverServer = gatewayReceiverServer.get();
      if (receiverServer != null && !receiverServer.getNotifyBySubscription()) {
        hasSerialSenders = true;
      }
    }

    return hasSerialSenders;
  }

  private boolean hasSerialSenders(InternalRegion region) {
    boolean hasSenders = false;
    Set<String> senders = region.getAllGatewaySenderIds();
    for (String sender : senders) {
      GatewaySender gatewaySender = getGatewaySender(sender);
      if (gatewaySender != null && !gatewaySender.isParallel()) {
        hasSenders = true;
        break;
      }
    }
    return hasSenders;
  }

  @Override
  public void removePartitionedRegion(PartitionedRegion region) {
    synchronized (partitionedRegions) {
      if (partitionedRegions.remove(region)) {
        getCachePerfStats().incPartitionedRegions(-1);
      }
    }
  }

  @Override
  public void setIsServer(boolean isServer) {
    throwIfClient();
    stopper.checkCancelInProgress(null);

    this.isServer = isServer;
  }

  @Override
  public boolean isServer() {
    if (isClient()) {
      return false;
    }
    stopper.checkCancelInProgress(null);

    return isServer || !allCacheServers.isEmpty();
  }

  @Override
  public QueryService getQueryService() {
    if (!isClient()) {
      return getLocalQueryService();
    }
    Pool defaultPool = getDefaultPool();
    if (defaultPool == null) {
      throw new IllegalStateException(
          "Client cache does not have a default pool. Use getQueryService(String poolName) instead.");
    }
    return defaultPool.getQueryService();
  }

  @Override
  public InternalQueryService getInternalQueryService() {
    return (InternalQueryService) getQueryService();
  }

  @Override
  public JSONFormatter getJsonFormatter() {
    // only ProxyCache implementation needs a JSONFormatter that has reference to userAttributes
    return new JSONFormatter();
  }

  @Override
  public QueryService getLocalQueryService() {
    return new DefaultQueryService(this);
  }

  @Override
  public Context getJNDIContext() {
    return JNDIInvoker.getJNDIContext();
  }

  @Override
  public TransactionManager getJTATransactionManager() {
    return JNDIInvoker.getTransactionManager();
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
  public <K, V> RegionAttributes<K, V> getRegionAttributes(String id) {
    return GemFireCacheImpl.UncheckedUtils.<K, V>uncheckedCast(namedRegionAttributes).get(id);
  }

  @Override
  public <K, V> void setRegionAttributes(String id, RegionAttributes<K, V> attrs) {
    if (attrs == null) {
      namedRegionAttributes.remove(id);
    } else {
      namedRegionAttributes.put(id, attrs);
    }
  }

  @Override
  public <K, V> Map<String, RegionAttributes<K, V>> listRegionAttributes() {
    return unmodifiableMap(uncheckedCast(namedRegionAttributes));
  }

  @Override
  public void loadCacheXml(InputStream is)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    // make this cache available to callbacks being initialized during xml create
    GemFireCacheImpl oldValue = xmlCache.get();
    xmlCache.set(this);

    Reader reader = null;
    Writer stringWriter = null;
    OutputStreamWriter writer = null;

    try {
      CacheXmlParser xml;

      if (XML_PARAMETERIZATION_ENABLED) {
        reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.ISO_8859_1));
        stringWriter = new StringWriter();

        int numChars;
        char[] buffer = new char[1024];
        while ((numChars = reader.read(buffer)) != -1) {
          stringWriter.write(buffer, 0, numChars);
        }

        // Now replace all replaceable system properties here using {@code PropertyResolver}
        String replacedXmlString = resolver.processUnresolvableString(stringWriter.toString());

        // Turn the string back into the default encoding so that the XML parser can work correctly
        // in the presence of an "encoding" attribute in the XML prolog.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writer = new OutputStreamWriter(baos, StandardCharsets.ISO_8859_1);
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
      if (Objects.nonNull(system)
          && Objects.nonNull(system.getConfig())
          && !Objects.equals(DEFAULT_DURABLE_CLIENT_ID,
              Objects.toString(system.getConfig().getDurableClientId(),
                  DEFAULT_DURABLE_CLIENT_ID))) {
        // Ensure that there is a pool to use for readyForEvents().
        if (Objects.isNull(defaultPool)) {
          determineDefaultPool();
        }
      }
    }
    PoolManagerImpl.readyForEvents(system, false);
  }

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
      stopper.checkCancelInProgress(null);
    }
    return resourceManager;
  }

  @Override
  public void setBackupFiles(List<File> backups) {
    backupFiles = backups;
  }

  @Override
  public List<File> getBackupFiles() {
    return unmodifiableList(backupFiles);
  }

  @Override
  public BackupService getBackupService() {
    return backupService;
  }

  @Override
  public void registerInterestCompleted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    if (isClosing) {
      // just get out, all of the SimpleWaiters will die of their own accord
      return;
    }

    int numInProgress = registerInterestsInProgress.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestCompleted: new value = {}", numInProgress);
    }
    if (numInProgress == 0) {
      synchronized (riWaiters) {
        numInProgress = registerInterestsInProgress.get();
        if (numInProgress == 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("registerInterestCompleted: Signalling end of register-interest");
          }
          for (SimpleWaiter sw : riWaiters) {
            sw.doNotify();
          }
          riWaiters.clear();
        }
      }
    }
  }

  @Override
  public void registerInterestStarted() {
    // Don't do a cancellation check, it's just a moot point, that's all
    int newVal = registerInterestsInProgress.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("registerInterestsStarted: new count = {}", newVal);
    }
  }

  @Override
  public void waitForRegisterInterestsInProgress() {
    // In *this* particular context, let the caller know that its cache has been cancelled.
    // doWait below would do that as well, so this is just an early out.
    getCancelCriterion().checkCancelInProgress(null);

    int count = registerInterestsInProgress.get();
    if (count > 0) {
      SimpleWaiter simpleWaiter = null;
      synchronized (riWaiters) {
        count = registerInterestsInProgress.get();
        if (count > 0) {
          if (logger.isDebugEnabled()) {
            logger.debug("waitForRegisterInterestsInProgress: count ={}", count);
          }
          simpleWaiter = new SimpleWaiter();
          riWaiters.add(simpleWaiter);
        }
      }
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
    return queryMonitorDisabledForLowMem;
  }

  @Override
  public QueryMonitor getQueryMonitor() {
    // Check to see if monitor is required if ResourceManager critical heap percentage is set
    // or whether we override it with the system variable;
    boolean monitorRequired =
        !queryMonitorDisabledForLowMem && queryMonitorRequiredForResourceManager;
    // Added for DUnit test purpose, which turns-on and off the this.testMaxQueryExecutionTime.
    if (!(MAX_QUERY_EXECUTION_TIME > 0 || monitorRequired)) {
      // if this.testMaxQueryExecutionTime is set, send the QueryMonitor.
      // Else send null, so that the QueryMonitor is turned-off.
      return null;
    }

    // Return the QueryMonitor service if MAX_QUERY_EXECUTION_TIME is set or it is required by the
    // ResourceManager and not overridden by system property.
    if (queryMonitor == null) {
      synchronized (queryMonitorLock) {
        if (queryMonitor == null) {
          int maxTime = MAX_QUERY_EXECUTION_TIME;

          if (monitorRequired && maxTime < 0) {
            // this means that the resource manager is being used and we need to monitor query
            // memory usage

            // If no max execution time has been set, then we will default to five hours
            maxTime = FIVE_HOURS_MILLIS;
          }

          queryMonitor =
              new QueryMonitor((ScheduledThreadPoolExecutor) newScheduledThreadPool(
                  QUERY_MONITOR_THREAD_POOL_SIZE,
                  runnable -> new LoggingThread("QueryMonitor Thread", runnable)),
                  this,
                  maxTime);
          if (logger.isDebugEnabled()) {
            logger.debug("QueryMonitor thread started.");
          }
        }
      }
    }
    return queryMonitor;
  }

  private void sendAddCacheServerProfileMessage() {
    Set<InternalDistributedMember> otherMembers = dm.getOtherDistributionManagerIds();
    AddCacheServerProfileMessage message = new AddCacheServerProfileMessage();
    message.operateOnLocalCache(this);

    if (!otherMembers.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending add cache server profile message to other members.");
      }
      ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, otherMembers);
      message.setRecipients(otherMembers);
      message.processorId = replyProcessor.getProcessorId();
      dm.putOutgoing(message);

      // Wait for replies.
      try {
        replyProcessor.waitForReplies();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }
  }


  private void sendRemoveCacheServerProfileMessage() {
    Set<InternalDistributedMember> otherMembers = dm.getOtherDistributionManagerIds();
    RemoveCacheServerProfileMessage message = new RemoveCacheServerProfileMessage();
    message.operateOnLocalCache(this);

    // This block prevents sending a message to old members that do not know about
    // the RemoveCacheServerProfileMessage
    otherMembers.removeIf(member -> Version.GEODE_1_5_0.compareTo(member.getVersionObject()) > 0);

    if (!otherMembers.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Sending remove cache server profile message to other members.");
      }
      ReplyProcessor21 replyProcessor = new ReplyProcessor21(dm, otherMembers);
      message.setRecipients(otherMembers);
      message.processorId = replyProcessor.getProcessorId();
      dm.putOutgoing(message);

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
    return transactionManager;
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

  @Override
  public QueryService getQueryService(String poolName) {
    Pool pool = PoolManager.find(poolName);
    if (pool == null) {
      throw new IllegalStateException("Could not find a pool named " + poolName);
    }
    return pool.getQueryService();
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
    }
    throw new IllegalStateException(
        "The pool " + pool.getName() + " did not have multiuser-authentication set to true");
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
    regionsInDestroy.putIfAbsent(path, region);
  }

  @Override
  public void endDestroy(String path, DistributedRegion region) {
    regionsInDestroy.remove(path, region);
  }

  @Override
  public DistributedRegion getRegionInDestroy(String path) {
    return regionsInDestroy.get(path);
  }

  @Override
  public TombstoneService getTombstoneService() {
    return tombstoneService;
  }

  @Override
  public TypeRegistry getPdxRegistry() {
    return pdxRegistry;
  }

  @Override
  public boolean getPdxReadSerialized() {
    return cacheConfig.pdxReadSerialized;
  }

  @Override
  public PdxSerializer getPdxSerializer() {
    return cacheConfig.pdxSerializer;
  }

  @Override
  public String getPdxDiskStore() {
    return cacheConfig.pdxDiskStore;
  }

  @Override
  public boolean getPdxPersistent() {
    return cacheConfig.pdxPersistent;
  }

  @Override
  public boolean getPdxIgnoreUnreadFields() {
    return cacheConfig.pdxIgnoreUnreadFields;
  }

  @Override
  public boolean getPdxReadSerializedByAnyGemFireServices() {
    TypeRegistry pdxRegistry = getPdxRegistry();
    boolean pdxReadSerializedOverridden = false;
    if (pdxRegistry != null) {
      pdxReadSerializedOverridden = pdxRegistry.getPdxReadSerializedOverride();
    }

    return (getPdxReadSerialized() || pdxReadSerializedOverridden)
        && PdxInstanceImpl.getPdxReadSerialized();
  }

  @Override
  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return dm;
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
    return resourceAdvisor;
  }

  @Override
  public Profile getProfile() {
    return resourceAdvisor.createProfile();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return system;
  }

  @Override
  public String getFullPath() {
    return "ResourceManager";
  }

  @Override
  public void fillInProfile(Profile profile) {
    resourceManager.fillInProfile(profile);
  }

  @Override
  public int getSerialNumber() {
    return serialNumber;
  }

  @Override
  public TXEntryStateFactory getTXEntryStateFactory() {
    return txEntryStateFactory;
  }

  @VisibleForTesting
  public void setPdxSerializer(PdxSerializer serializer) {
    cacheConfig.setPdxSerializer(serializer);
    basicSetPdxSerializer(serializer);
  }

  private void basicSetPdxSerializer(PdxSerializer serializer) {
    typeRegistrySetPdxSerializer.accept(serializer);
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
    setPdxReadSerializedOverride(value);
  }

  @Deprecated
  @Override
  @VisibleForTesting
  public void setReadSerializedForTest(boolean value) {
    cacheConfig.setPdxReadSerialized(value);
  }

  @Override
  public void setDeclarativeCacheConfig(CacheConfig cacheConfig) {
    this.cacheConfig.setDeclarativeConfig(cacheConfig);
    basicSetPdxSerializer(this.cacheConfig.getPdxSerializer());
  }

  @Override
  public void addDeclarableProperties(Map<Declarable, Properties> mapOfNewDeclarableProps) {
    synchronized (declarablePropertiesMap) {
      for (Entry<Declarable, Properties> newEntry : mapOfNewDeclarableProps.entrySet()) {
        // Find and remove a Declarable from the map if an "equal" version is already stored
        Class<? extends Declarable> clazz = newEntry.getKey().getClass();

        Declarable matchingDeclarable = null;
        for (Entry<Declarable, Properties> oldEntry : declarablePropertiesMap.entrySet()) {

          BiPredicate<Declarable, Declarable> isKeyIdentifiableAndSameIdPredicate =
              (oldKey, newKey) -> newKey instanceof Identifiable
                  && ((Identifiable) oldKey).getId().equals(((Identifiable) newKey).getId());

          Supplier<Boolean> isKeyClassSame =
              () -> clazz.getName().equals(oldEntry.getKey().getClass().getName());
          Supplier<Boolean> isValueEqual =
              () -> newEntry.getValue().equals(oldEntry.getValue());
          Supplier<Boolean> isKeyIdentifiableAndSameId =
              () -> isKeyIdentifiableAndSameIdPredicate.test(oldEntry.getKey(), newEntry.getKey());

          if (isKeyClassSame.get() && (isValueEqual.get() || isKeyIdentifiableAndSameId.get())) {
            matchingDeclarable = oldEntry.getKey();
            break;
          }
        }
        if (matchingDeclarable != null) {
          declarablePropertiesMap.remove(matchingDeclarable);
        }

        // Now add the new/replacement properties to the map
        declarablePropertiesMap.put(newEntry.getKey(), newEntry.getValue());
      }
    }
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

  @Override
  public JmxManagerAdvisor getJmxManagerAdvisor() {
    return jmxAdvisor;
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
    return getSystem().getOffHeapStore();
  }

  @Override
  public DiskStoreMonitor getDiskStoreMonitor() {
    return diskMonitor;
  }

  @Override
  public ExtensionPoint<Cache> getExtensionPoint() {
    return extensionPoint;
  }

  @Override
  public CqService getCqService() {
    return cqService;
  }

  private void addRegionEntrySynchronizationListener(RegionEntrySynchronizationListener listener) {
    synchronizationListeners.add(listener);
  }

  @Override
  public void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize) {
    for (RegionEntrySynchronizationListener listener : synchronizationListeners) {
      try {
        listener.afterSynchronization(sender, region, entriesToSynchronize);
      } catch (Throwable t) {
        logger.warn(String.format(
            "Caught the following exception attempting to synchronize events from member=%s; regionPath=%s; entriesToSynchronize=%s:",
            sender, region.getFullPath(), entriesToSynchronize), t);
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
      } else if (!getPdxReadSerialized()) {
        result = pdxInstance.getObject();
      }
    }
    return result;
  }

  @Override
  public Boolean getPdxReadSerializedOverride() {
    TypeRegistry pdxRegistry = getPdxRegistry();
    if (pdxRegistry != null) {
      return pdxRegistry.getPdxReadSerializedOverride();
    }
    return false;
  }

  @Override
  public void setPdxReadSerializedOverride(boolean pdxReadSerialized) {
    TypeRegistry pdxRegistry = getPdxRegistry();
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

  @Override
  public InternalCacheForClientAccess getCacheForProcessingClientRequests() {
    return cacheForClients;
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    if (dm != null) {
      return dm.getThreadMonitoring();
    }
    return null;
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    return statisticsClock;
  }

  @VisibleForTesting
  void setDisconnectCause(Throwable disconnectCause) {
    this.disconnectCause = disconnectCause;
  }

  private class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      String reason = getDistributedSystem().getCancelCriterion().cancelInProgress();
      if (reason != null) {
        return reason;
      }
      if (disconnectCause != null) {
        return disconnectCause.getMessage();
      }
      if (isClosing) {
        return "The cache is closed.";
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
      if (disconnectCause == null) {
        // No root cause, specify the one given and be done with it.
        return new CacheClosedException(reason, throwable);
      }

      if (throwable == null) {
        // Caller did not specify any root cause, so just use our own.
        return new CacheClosedException(reason, disconnectCause);
      }

      // Attempt to stick rootCause at tail end of the exception chain.
      try {
        ThrowableUtils.setRootCause(throwable, disconnectCause);
        return new CacheClosedException(reason, throwable);
      } catch (IllegalStateException ignore) {
        // Give up. The following error is not entirely sane but gives the correct general picture.
        return new CacheClosedException(reason, disconnectCause);
      }
    }
  }

  /**
   * Support for waiters of register interest. Only one thread at most ever calls wait.
   *
   * @since GemFire 5.7
   */
  private class SimpleWaiter {

    private boolean notified;

    private void doWait() {
      synchronized (this) {
        while (!notified) {
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

    private void doNotify() {
      synchronized (this) {
        notified = true;
        notifyAll();
      }
    }
  }

  @SuppressWarnings("unchecked")
  static class UncheckedUtils {

    static Map<InternalDistributedMember, PersistentMemberID>[] createMapArray(int size) {
      return new Map[size];
    }

    static Set<DistributedMember> asDistributedMemberSet(
        Set<InternalDistributedMember> internalDistributedMembers) {
      return (Set) internalDistributedMembers;
    }

    static <K, V> RegionAttributes<K, V> uncheckedRegionAttributes(RegionAttributes region) {
      return region;
    }

    static <K, V> Map<String, RegionAttributes<K, V>> uncheckedCast(
        Map<String, RegionAttributes<?, ?>> namedRegionAttributes) {
      return (Map) namedRegionAttributes;
    }
  }

  @FunctionalInterface
  @VisibleForTesting
  interface TXManagerImplFactory {
    TXManagerImpl create(CachePerfStats cachePerfStats, InternalCache cache,
        StatisticsClock statisticsClock);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface InternalSecurityServiceFactory {
    SecurityService create(Properties properties, CacheConfig cacheConfig);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface CachePerfStatsFactory {
    CachePerfStats create(StatisticsFactory factory, StatisticsClock clock);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface TypeRegistryFactory {
    TypeRegistry create(InternalCache cache, boolean disableTypeRegistry);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface HeapEvictorFactory {
    HeapEvictor create(InternalCache cache, StatisticsClock statisticsClock);
  }

  @FunctionalInterface
  @VisibleForTesting
  interface ReplyProcessor21Factory {
    ReplyProcessor21 create(InternalDistributedSystem system,
        Collection<InternalDistributedMember> initMembers);
  }
}
