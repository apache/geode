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

import static org.apache.geode.internal.lang.SystemUtils.getLineSeparator;
import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.DeltaSerializationException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.LogWriter;
import org.apache.geode.Statistics;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.internal.SystemMemberCacheEventProcessor;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.DiskWriteAttributesFactory;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.FailedSynchronizationException;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionReinitializedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.persistence.ConflictingPersistentDataException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexMaintenanceException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.AbstractRegionMap.ARMLockTestHook;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.InitialImageOperation.GIIStatus;
import org.apache.geode.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.MemoryThresholds;
import org.apache.geode.internal.cache.control.ResourceListener;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.event.EventTracker;
import org.apache.geode.internal.cache.event.NonDistributedEventTracker;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.DistributedRegionFunctionResultSender;
import org.apache.geode.internal.cache.execute.LocalResultCollector;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.RedundancyAlreadyMetException;
import org.apache.geode.internal.cache.persistence.DefaultDiskDirs;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.query.IndexMap;
import org.apache.geode.internal.cache.persistence.query.mock.IndexMapImpl;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientHealthMonitor;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientTombstoneMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventCallbackArgument;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.ReferenceCountHelper;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.internal.util.concurrent.FutureResult;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;
import org.apache.geode.internal.util.concurrent.StoppableReadWriteLock;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;

/**
 * Implementation of a local scoped-region. Note that this class has a different meaning starting
 * with 3.0. In previous versions, a LocalRegion was the representation of a region in the VM.
 * Starting with 3.0, a LocalRegion is a non-distributed region. The subclass DistributedRegion adds
 * distribution behavior.
 */
@SuppressWarnings("deprecation")
public class LocalRegion extends AbstractRegion implements LoaderHelperFactory,
    ResourceListener<MemoryEvent>, DiskExceptionHandler, DiskRecoveryStore {

  // package-private to avoid synthetic accessor
  static final Logger logger = LogService.getLogger();

  @Override
  public boolean isRegionInvalid() {
    return regionInvalid;
  }

  /**
   * Set to true after an invalidate region expiration so we don't get multiple expirations
   */
  @Override
  public void setRegionInvalid(boolean regionInvalid) {
    this.regionInvalid = regionInvalid;
  }

  /**
   * Prevents access to this region until it is done initializing, except for some special
   * initializing operations such as replying to create region messages In JDK 1.5 we will use
   * java.util.concurrent.CountDownLatch instead of org.apache.geode.internal.util.CountDownLatch.
   */
  @Override
  public StoppableCountDownLatch getInitializationLatchBeforeGetInitialImage() {
    return initializationLatchBeforeGetInitialImage;
  }

  @Override
  public StoppableCountDownLatch getInitializationLatchAfterGetInitialImage() {
    return initializationLatchAfterGetInitialImage;
  }

  /**
   * Internal interface used to simulate failures when performing entry operations
   *
   * @since GemFire 5.7
   */
  public interface TestCallable {
    void call(LocalRegion r, Operation op, RegionEntry re);
  }

  // view types for iterators
  public enum IteratorType {
    KEYS, VALUES, ENTRIES
  }

  // initialization level
  public static final int AFTER_INITIAL_IMAGE = 0;

  public static final int BEFORE_INITIAL_IMAGE = 1;

  public static final int ANY_INIT = 2;

  /**
   * thread local to indicate that this thread should bypass the initialization Latch
   */
  private static final ThreadLocal<Integer> initializationThread = new ThreadLocal();

  /* thread local to indicate its for persist data convert tool */
  static final ThreadLocal isConversion = new ThreadLocal();

  // user attributes //
  private Object regionUserAttribute;

  // TODO: shouldn't this be an identity map whose key is a RegionEntry?
  Map entryUserAttributes;

  private final String regionName;

  protected final LocalRegion parentRegion;

  /**
   * set to true only if isDestroyed is also true and region is about to be recreated due to
   * reinitialization by loading of a snapshot, etc.
   */
  private volatile boolean reinitialized_old = false;

  protected volatile boolean isDestroyed = false;

  /**
   * In case of parallel wan, when a destroy is called on userPR, it waits for parallelQueue to
   * drain and then destroys parallelQueue. In this time if operation like put happens on userPR
   * then it will keep on building parallel queue increasing time of userPR to get destroyed.this
   * volatile boolean will block such put operation by throwing RegionDestroyedException
   */
  volatile boolean isDestroyedForParallelWAN = false;

  /**
   * set to true after snapshot is loaded, to help get initial image make sure this is the right
   * incarnation of this region
   */
  private volatile boolean reinitialized_new = false;

  /** Lock used to prevent multiple concurrent destroy region operations */
  private Semaphore destroyLock;

  /** GuardedBy regionExpiryLock. */
  private RegionTTLExpiryTask regionTTLExpiryTask = null;

  /** GuardedBy regionExpiryLock. */
  private RegionIdleExpiryTask regionIdleExpiryTask = null;

  private final Object regionExpiryLock = new Object();

  /**
   * GuardedBy regionExpiryLock. Keeps track of how many txs are writing to this region.
   */
  private int txRefCount;

  private final ConcurrentHashMap<RegionEntry, EntryExpiryTask> entryExpiryTasks =
      new ConcurrentHashMap<>();

  private volatile boolean regionInvalid = false;

  /**
   * TODO: make this private and introduce wrappers
   */
  public final RegionMap entries;

  /**
   * Set to true if this region supports transaction else false.
   */
  private final boolean supportsTX;

  /**
   * tracks region-level version information for members
   */
  private final RegionVersionVector versionVector;

  private static final Pattern[] QUERY_PATTERNS = new Pattern[] {
      Pattern.compile("^\\(*select .*",
          Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL),
      Pattern.compile("^import .*",
          Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL)};

  public static final String EXPIRY_MS_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "EXPIRY_UNITS_MS";

  /**
   * Used by unit tests to set expiry to milliseconds instead of the default seconds. Used in
   * ExpiryTask.
   *
   * @since GemFire 5.0
   */
  final boolean EXPIRY_UNITS_MS;

  // Indicates that the entries are in fact initialized. It turns out
  // you can't trust the assignment of a volatile (as indicated above)
  // to mean that the the thing being assigned is fully formed, only
  // those things *before* the assignment are fully formed. mthomas 10/02/2005
  private final boolean entriesInitialized;

  /**
   * contains Regions themselves // marked volatile to make sure it is fully initialized before
   * being // accessed; (actually should be final)
   */
  protected volatile ConcurrentMap subregions;

  private final Object subregionsLock = new Object();

  private final StoppableCountDownLatch initializationLatchBeforeGetInitialImage;

  private final StoppableCountDownLatch initializationLatchAfterGetInitialImage;

  /**
   * Used to hold off cache listener events until the afterRegionCreate is called
   *
   * @since GemFire 5.0
   */
  private final StoppableCountDownLatch afterRegionCreateEventLatch;

  /**
   * Set to true the first time isInitialized returns true.
   */
  private volatile boolean initialized = false; // added for bug 30223

  /** Used for accessing region data on disk */
  private final DiskRegion diskRegion;

  /**
   * Used by transactions to suspend entry expiration while a transaction is in progress on a
   * region. This field is only initialized if expiration is configured and transactions are
   * possible.
   */
  private volatile StoppableReadWriteLock txExpirationLock;

  /**
   * Used for serializing netSearch and netLoad on a per key basis. CM <Object, Future>
   */
  private final ConcurrentMap getFutures = new ConcurrentHashMap();

  /**
   * TODO: This boolean needs to be made true if the test needs to receive a synchronous callback
   * just after clear on map is done. Its visibility is default so that only tests present in
   * org.apache.geode.internal.cache will be able to see it
   */
  public static boolean ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

  /**
   * A flag used to indicate that this Region is being used as an administrative Region, holding
   * meta-data for a PartitionedRegion
   */
  private final boolean isUsedForPartitionedRegionAdmin;

  private final boolean isUsedForPartitionedRegionBucket;

  private final boolean isUsedForMetaRegion;

  private final boolean isMetaRegionWithTransactions;

  private final boolean isUsedForSerialGatewaySenderQueue;

  private final boolean isUsedForParallelGatewaySenderQueue;

  private final AbstractGatewaySender serialGatewaySender;

  /**
   * The factory used to create the LoaderHelper when a loader is invoked
   */
  final LoaderHelperFactory loaderHelperFactory;

  /**
   * Allow for different CachePerfStats locations... primarily for PartitionedRegions
   */
  private final CachePerfStats cachePerfStats;

  private final boolean hasOwnStats;

  private final ImageState imageState;

  private final EventTracker eventTracker;

  /**
   * Register interest count to track if any register interest is in progress for this region. This
   * count will be incremented when register interest starts and decremented when register interest
   * finishes.
   * <p>
   * since always written while holding an exclusive write lock and only read while holding a read
   * lock it does not need to be atomic or protected by any other sync.
   * <p>
   * GuardedBy {@link #imageState}
   */
  private int riCnt = 0;

  /**
   * Map of subregion full paths to serial numbers. These are subregions that were destroyed when
   * this region was destroyed. This map remains null until this region is destroyed.
   */
  private volatile HashMap destroyedSubregionSerialNumbers;

  /**
   * This boolean is true when a member who has this region is running low on memory. It is used to
   * reject region operations.
   */
  private final AtomicBoolean memoryThresholdReached = new AtomicBoolean(false);

  /**
   * Lock for updating PR MetaData on client side
   * <p>
   * TODO: move this to ClientMetadataService into {@code Map<Region, Lock>}
   */
  private final Lock clientMetaDataLock = new ReentrantLock();

  /**
   * Lock for updating the cache service profile for the region.
   */
  private final Lock cacheServiceProfileUpdateLock = new ReentrantLock();

  public void executeSynchronizedOperationOnCacheProfiles(Runnable operation) {
    cacheServiceProfileUpdateLock.lock();
    try {
      operation.run();
    } finally {
      cacheServiceProfileUpdateLock.unlock();
    }
  }

  /**
   * There seem to be cases where a region can be created and yet the distributed system is not yet
   * in place...
   */
  protected class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      // This grossness is necessary because there are instances where the
      // region can exist without having a cache (XML creation)
      checkFailure();
      Cache cache = LocalRegion.this.getCache();
      if (cache == null) {
        return LocalizedStrings.LocalRegion_THE_CACHE_IS_NOT_AVAILABLE.toLocalizedString();
      }
      return cache.getCancelCriterion().cancelInProgress();
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      // This grossness is necessary because there are instances where the
      // region can exist without having a cache (XML creation)
      checkFailure();
      Cache cache = LocalRegion.this.getCache();
      if (cache == null) {
        return new CacheClosedException("No cache", e);
      }
      return cache.getCancelCriterion().generateCancelledException(e);
    }

  }

  protected final CancelCriterion stopper = createStopper();

  // TODO: change createStopper to be private (fix EventTrackerTest)
  public CancelCriterion createStopper() {
    return new Stopper();
  }

  protected CancelCriterion getStopper() {
    return this.stopper;
  }

  private final TestCallable testCallable;

  /**
   * ThreadLocal used to set the current region being initialized.
   *
   * Currently used by the OpLog layer.
   */
  private static final ThreadLocal<LocalRegion> initializingRegion = new ThreadLocal<LocalRegion>();

  /**
   * Get the current initializing region as set in the ThreadLocal.
   *
   * Note that this value is cleared after the initialization of LocalRegion is done so is valid
   * only for the duration of region creation and initialization.
   */
  static LocalRegion getInitializingRegion() {
    return initializingRegion.get();
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return this.stopper;
  }

  private final CopyOnWriteHashMap<String, CacheServiceProfile> cacheServiceProfiles =
      new CopyOnWriteHashMap<>();

  private static String calcFullPath(String regionName, LocalRegion parentRegion) {
    StringBuilder buf = null;
    if (parentRegion == null) {
      buf = new StringBuilder(regionName.length() + 1);
    } else {
      String parentFull = parentRegion.getFullPath();
      buf = new StringBuilder(parentFull.length() + regionName.length() + 1);
      buf.append(parentFull);
    }
    buf.append(SEPARATOR).append(regionName);
    return buf.toString();
  }

  /**
   * Creates new region
   */
  protected LocalRegion(String regionName, RegionAttributes attrs, LocalRegion parentRegion,
      InternalCache cache, InternalRegionArguments internalRegionArgs) throws DiskAccessException {
    super(cache, attrs, regionName, internalRegionArgs);

    // Initialized here (and defers to parent) to fix GEODE-128
    this.EXPIRY_UNITS_MS = parentRegion != null ? parentRegion.EXPIRY_UNITS_MS
        : Boolean.getBoolean(EXPIRY_MS_PROPERTY);

    Assert.assertTrue(regionName != null, "regionName must not be null");
    this.sharedDataView = buildDataView();
    this.regionName = regionName;
    this.parentRegion = parentRegion;
    this.fullPath = calcFullPath(regionName, parentRegion);

    String myName = getFullPath();
    if (internalRegionArgs.getPartitionedRegion() != null) {
      myName = internalRegionArgs.getPartitionedRegion().getFullPath();
    }
    this.offHeap = attrs.getOffHeap() || Boolean.getBoolean(myName + ":OFF_HEAP");
    if (getOffHeap()) {
      if (cache.getOffHeapStore() == null) {
        throw new IllegalStateException(
            LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED
                .toLocalizedString(myName));
      }
    }

    this.initializationLatchBeforeGetInitialImage = new StoppableCountDownLatch(this.stopper, 1);
    this.initializationLatchAfterGetInitialImage = new StoppableCountDownLatch(this.stopper, 1);
    this.afterRegionCreateEventLatch = new StoppableCountDownLatch(this.stopper, 1);

    if (internalRegionArgs.getUserAttribute() != null) {
      setUserAttribute(internalRegionArgs.getUserAttribute());
    }
    initializingRegion.set(this);

    if (internalRegionArgs.getCachePerfStatsHolder() != null) {
      this.hasOwnStats = false;
      this.cachePerfStats = internalRegionArgs.getCachePerfStatsHolder().getCachePerfStats();
    } else {
      if (attrs.getPartitionAttributes() != null || isInternalRegion()
          || internalRegionArgs.isUsedForMetaRegion()) {
        this.hasOwnStats = false;
        this.cachePerfStats = cache.getCachePerfStats();
      } else {
        this.hasOwnStats = true;
        this.cachePerfStats = new RegionPerfStats(cache, cache.getCachePerfStats(), regionName);
      }
    }

    this.diskStoreImpl = findDiskStore(attrs, internalRegionArgs);
    this.diskRegion = createDiskRegion(internalRegionArgs);
    this.entries = createRegionMap(internalRegionArgs);
    this.entriesInitialized = true;
    this.subregions = new ConcurrentHashMap();

    // we only need a destroy lock if this is a root
    if (parentRegion == null) {
      initRoot();
    }

    if (internalRegionArgs.getLoaderHelperFactory() != null) {
      this.loaderHelperFactory = internalRegionArgs.getLoaderHelperFactory();
    } else {
      this.loaderHelperFactory = this;
    }

    this.isUsedForPartitionedRegionAdmin = internalRegionArgs.isUsedForPartitionedRegionAdmin();
    this.isUsedForPartitionedRegionBucket = internalRegionArgs.isUsedForPartitionedRegionBucket();
    this.isUsedForMetaRegion = internalRegionArgs.isUsedForMetaRegion();
    this.isMetaRegionWithTransactions = internalRegionArgs.isMetaRegionWithTransactions();
    this.isUsedForSerialGatewaySenderQueue = internalRegionArgs.isUsedForSerialGatewaySenderQueue();
    this.isUsedForParallelGatewaySenderQueue =
        internalRegionArgs.isUsedForParallelGatewaySenderQueue();
    this.serialGatewaySender = internalRegionArgs.getSerialGatewaySender();
    if (internalRegionArgs.getCacheServiceProfiles() != null) {
      addCacheServiceProfiles(internalRegionArgs);
    }

    if (!isUsedForMetaRegion && !isUsedForPartitionedRegionAdmin
        && !isUsedForPartitionedRegionBucket && !isUsedForSerialGatewaySenderQueue
        && !isUsedForParallelGatewaySenderQueue) {
      this.filterProfile = new FilterProfile(this);
    }

    // initialize client to server proxy
    this.serverRegionProxy = this.getPoolName() != null ? new ServerRegionProxy(this) : null;
    this.imageState = new UnsharedImageState(this.serverRegionProxy != null,
        getDataPolicy().withReplication() || getDataPolicy().isPreloaded(),
        getAttributes().getDataPolicy().withPersistence(), this.stopper);

    // prevent internal regions from participating in a TX, bug 38709
    this.supportsTX = !isSecret() && !isUsedForPartitionedRegionAdmin() && !isUsedForMetaRegion()
        || isMetaRegionWithTransactions();

    this.testCallable = internalRegionArgs.getTestCallable();
    eventTracker = createEventTracker();

    versionVector = createRegionVersionVector();
  }

  private void addCacheServiceProfiles(InternalRegionArguments internalRegionArgs) {
    cacheServiceProfileUpdateLock.lock();
    try {
      this.cacheServiceProfiles.putAll(internalRegionArgs.getCacheServiceProfiles());
    } finally {
      cacheServiceProfileUpdateLock.unlock();
    }
  }

  protected EventTracker createEventTracker() {
    return NonDistributedEventTracker.getInstance();
  }

  private RegionMap createRegionMap(InternalRegionArguments internalRegionArgs) {
    RegionMap result = null;
    if (this.diskRegion != null) {
      result = this.diskRegion.useExistingRegionMap(this);
    }
    if (result == null) {
      RegionMap.Attributes ma = new RegionMap.Attributes();
      ma.statisticsEnabled = this.statisticsEnabled;
      ma.loadFactor = this.loadFactor;
      ma.initialCapacity = this.initialCapacity;
      ma.concurrencyLevel = this.concurrencyLevel;
      result = RegionMapFactory.createVM(this, ma, internalRegionArgs);
    }
    return result;
  }

  protected InternalDataView buildDataView() {
    return new LocalRegionDataView();
  }

  /**
   * Other region classes may track events using different mechanisms than EventTrackers or may not
   * track events at all
   */
  public EventTracker getEventTracker() {
    return eventTracker;
  }

  /** returns the regions version-vector */
  @Override
  public RegionVersionVector getVersionVector() {
    return this.versionVector;
  }

  /** returns object used to guard the size() operation during tombstone removal */
  Object getSizeGuard() {
    if (!this.getConcurrencyChecksEnabled()) {
      return new Object();
    } else {
      return this.fullPath; // avoids creating another sync object - could be anything unique to
                            // this region
    }
  }

  protected RegionVersionVector createRegionVersionVector() {
    if (getConcurrencyChecksEnabled()) {
      return createVersionVector();
    }
    return null;
  }

  /** initializes a new version vector for this region */
  private RegionVersionVector createVersionVector() {
    RegionVersionVector regionVersionVector = RegionVersionVector.create(getVersionMember(), this);

    if (this.getDataPolicy().withPersistence()) {
      // copy the versions that we have recovered from disk into
      // the version vector.
      RegionVersionVector diskVector = diskRegion.getRegionVersionVector();
      regionVersionVector.recordVersions(diskVector.getCloneForTransmission());
    } else if (!this.getDataPolicy().withStorage()) {
      // version vectors are currently only necessary in empty regions for
      // tracking canonical member IDs
      regionVersionVector.turnOffRecordingForEmptyRegion();
    }
    if (serverRegionProxy != null) {
      regionVersionVector.setIsClientVector();
    }
    cache.getDistributionManager().addMembershipListener(regionVersionVector);
    return regionVersionVector;
  }

  @Override
  protected void updateEntryExpiryPossible() {
    super.updateEntryExpiryPossible();
    if (!isEntryExpiryPossible()) {
      // since expiration is no longer possible cleanup the tasks
      cancelAllEntryExpiryTasks();
    }
  }

  boolean isCacheClosing() {
    return this.cache.isClosed();
  }

  public RegionEntry getRegionEntry(Object key) {
    return this.entries.getEntry(key);
  }

  /**
   * Test hook - returns the version stamp for an entry in the form of a version tag
   *
   * @return the entry version information
   */
  public VersionTag getVersionTag(Object key) {
    Region.Entry entry = getEntry(key, true);
    VersionTag tag = null;
    if (entry != null && entry instanceof EntrySnapshot) {
      tag = ((EntrySnapshot) entry).getVersionTag();
    } else if (entry != null && entry instanceof NonTXEntry) {
      tag = ((NonTXEntry) entry).getRegionEntry().getVersionStamp().asVersionTag();
    }
    return tag;
  }

  /**
   * removes any destroyed entries from the region and clear the destroyedKeys assert: Caller must
   * be holding writeLock on is
   */
  private void destroyEntriesAndClearDestroyedKeysSet() {
    ImageState is = getImageState();
    Iterator iter = is.getDestroyedEntries();
    while (iter.hasNext()) {
      Object key = iter.next();
      // destroy the entry which has value Token.DESTROYED
      // If it is Token.DESTROYED then only destroy it.
      this.entries.removeIfDestroyed(key); // fixes bug 41957
    }
  }

  /**
   * @since GemFire 5.7
   */
  final ServerRegionProxy serverRegionProxy;

  private final InternalDataView sharedDataView;

  @Override
  public ServerRegionProxy getServerProxy() {
    return this.serverRegionProxy;
  }

  @Override
  public boolean hasServerProxy() {
    return this.serverRegionProxy != null;
  }

  /** Returns true if the ExpiryTask is currently allowed to expire. */
  protected boolean isExpirationAllowed(ExpiryTask expiry) {
    return true;
  }

  void performExpiryTimeout(ExpiryTask expiryTask) throws CacheException {
    if (expiryTask != null) {
      expiryTask.basicPerformTimeout(false);
    }
  }

  private void initRoot() {
    this.destroyLock = new Semaphore(1);
  }

  public void handleMarker() {
    RegionEventImpl event = new RegionEventImpl(this, Operation.MARKER, null, false, getMyId(),
        false /* generate EventID */);

    dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_LIVE, event);
  }

  @Override
  public AttributesMutator getAttributesMutator() {
    checkReadiness();
    return this;
  }

  @Override
  public Region createSubregion(String subregionName, RegionAttributes aRegionAttributes)
      throws RegionExistsException, TimeoutException {
    try {
      return createSubregion(subregionName, aRegionAttributes,
          new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false));
    } catch (IOException | ClassNotFoundException e) {
      // only happens when loading a snapshot, not here
      throw new InternalGemFireError(
          LocalizedStrings.LocalRegion_UNEXPECTED_EXCEPTION.toLocalizedString(), e);
    }
  }

  /**
   * Returns the member id of my distributed system
   *
   * @since GemFire 5.0
   */
  @Override
  public InternalDistributedMember getMyId() {
    return this.cache.getInternalDistributedSystem().getDistributedMember();
  }

  @Override
  public VersionSource getVersionMember() {
    if (this.getDataPolicy().withPersistence()) {
      return getDiskStore().getDiskStoreID();
    } else {
      return this.cache.getInternalDistributedSystem().getDistributedMember();
    }
  }

  // TODO: createSubregion method is too complex for IDE to analyze
  @Override
  public Region createSubregion(String subregionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException {

    checkReadiness();
    RegionAttributes regionAttributes = attrs;
    // TODO: attrs is reassigned but never used
    attrs = this.cache.invokeRegionBefore(this, subregionName, attrs, internalRegionArgs);

    final InputStream snapshotInputStream = internalRegionArgs.getSnapshotInputStream();
    final boolean getDestroyLock = internalRegionArgs.getDestroyLockFlag();
    final InternalDistributedMember imageTarget = internalRegionArgs.getImageTarget();

    LocalRegion newRegion = null;
    try {
      if (getDestroyLock)
        acquireDestroyLock();
      LocalRegion existing = null;
      try {
        if (isDestroyed()) {
          if (this.reinitialized_old) {
            throw new RegionReinitializedException(toString(), getFullPath());
          }
          throw new RegionDestroyedException(toString(), getFullPath());
        }
        RegionNameValidation.validate(subregionName, internalRegionArgs);

        validateSubregionAttributes(regionAttributes);

        // lock down the subregionsLock
        // to prevent other threads from adding a region to it in toRegion
        // but don't wait on initialization while synchronized (distributed
        // deadlock)
        synchronized (this.subregionsLock) {

          existing = (LocalRegion) this.subregions.get(subregionName);

          if (existing == null) {
            if (regionAttributes.getScope().isDistributed()
                && internalRegionArgs.isUsedForPartitionedRegionBucket()) {
              final PartitionedRegion pr = internalRegionArgs.getPartitionedRegion();
              internalRegionArgs.setUserAttribute(pr.getUserAttribute());
              if (pr.isShadowPR()) {
                newRegion = new BucketRegionQueue(subregionName, regionAttributes, this, this.cache,
                    internalRegionArgs);
              } else {
                newRegion = new BucketRegion(subregionName, regionAttributes, this, this.cache,
                    internalRegionArgs);
              }
            } else if (regionAttributes.getPartitionAttributes() != null) {
              newRegion = new PartitionedRegion(subregionName, regionAttributes, this, this.cache,
                  internalRegionArgs);
            } else {
              boolean local = regionAttributes.getScope().isLocal();
              newRegion = local
                  ? new LocalRegion(subregionName, regionAttributes, this, this.cache,
                      internalRegionArgs)
                  : new DistributedRegion(subregionName, regionAttributes, this, this.cache,
                      internalRegionArgs);
            }
            Object previousValue = this.subregions.putIfAbsent(subregionName, newRegion);

            Assert.assertTrue(previousValue == null);

            Assert.assertTrue(!newRegion.isInitialized());

            if (logger.isDebugEnabled()) {
              logger.debug("Subregion created: {}", newRegion.getFullPath());
            }
            if (snapshotInputStream != null || imageTarget != null
                || internalRegionArgs.getRecreateFlag()) {
              this.cache.regionReinitialized(newRegion); // fix for bug 33534
            }

          } // endif: existing == null
        } // end synchronization
      } finally {
        if (getDestroyLock) {
          releaseDestroyLock();
        }
      }

      // Fix for bug 42127 - moved to outside of the destroy lock.
      if (existing != null) {
        // now outside of synchronization we must wait for appropriate
        // initialization on existing region before returning a reference to
        // it
        existing.waitOnInitialization();
        // fix for bug 32570
        throw new RegionExistsException(existing);
      }

      boolean success = false;
      try {
        newRegion.checkReadiness();
        this.cache.setRegionByPath(newRegion.getFullPath(), newRegion);
        if (regionAttributes instanceof UserSpecifiedRegionAttributes) {
          internalRegionArgs
              .setIndexes(((UserSpecifiedRegionAttributes) regionAttributes).getIndexes());
        }

        // releases initialization Latches
        newRegion.initialize(snapshotInputStream, imageTarget, internalRegionArgs);

        // register the region with resource manager to get memory events
        if (!newRegion.isInternalRegion()) {
          if (!newRegion.isDestroyed) {
            this.cache.getInternalResourceManager().addResourceListener(ResourceType.MEMORY,
                newRegion);

            if (!newRegion.getOffHeap()) {
              newRegion.initialCriticalMembers(
                  this.cache.getInternalResourceManager().getHeapMonitor().getState().isCritical(),
                  this.cache.getResourceAdvisor().adviseCriticalMembers());
            } else {
              newRegion.initialCriticalMembers(
                  this.cache.getInternalResourceManager().getHeapMonitor().getState().isCritical()
                      || this.cache.getInternalResourceManager().getOffHeapMonitor().getState()
                          .isCritical(),
                  this.cache.getResourceAdvisor().adviseCriticalMembers());
            }

            // synchronization would be done on ManagementAdapter.regionOpLock
            // instead of destroyLock in LocalRegion? ManagementAdapter is one
            // of the Resource Event listeners

            InternalDistributedSystem system = this.cache.getInternalDistributedSystem();
            system.handleResourceEvent(ResourceEvent.REGION_CREATE, newRegion);
          }
        }
        success = true;
      } catch (CancelException | RegionDestroyedException | RedundancyAlreadyMetException e) {
        // don't print a call stack
        throw e;
      } catch (RuntimeException validationException) {
        logger
            .warn(
                LocalizedMessage.create(
                    LocalizedStrings.LocalRegion_INITIALIZATION_FAILED_FOR_REGION_0, getFullPath()),
                validationException);
        throw validationException;
      } finally {
        if (!success) {
          this.cache.setRegionByPath(newRegion.getFullPath(), null);
          initializationFailed(newRegion);
          this.cache.getInternalResourceManager(false).removeResourceListener(newRegion);
        }
      }

      newRegion.postCreateRegion();
    } finally {
      // make sure region initialization latch is open regardless
      // before returning;
      // if the latch is not open at this point, then an exception must
      // have occurred
      if (newRegion != null && !newRegion.isInitialized()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region initialize latch is closed, Error must have occurred");
        }
      }
    }

    this.cache.invokeRegionAfter(newRegion);
    return newRegion;
  }

  @Override
  public void create(Object key, Object value, Object aCallbackArgument)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    long startPut = CachePerfStats.getStatTime();
    @Released
    EntryEventImpl event = newCreateEntryEvent(key, value, aCallbackArgument);
    try {
      validatedCreate(event, startPut);
    } finally {
      event.release();
    }
  }

  private void validatedCreate(EntryEventImpl event, long startPut)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    if (event.getEventId() == null && generateEventID()) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    // Fix for 42448 - Only make create with null a local invalidate for
    // normal regions. Otherwise, it will become a distributed invalidate.
    if (getDataPolicy() == DataPolicy.NORMAL) {
      event.setLocalInvalid(true);
    }
    discoverJTA();
    if (!basicPut(event, true, // ifNew
        false, // ifOld
        null, // expectedOldValue
        true // requireOldValue TODO txMerge why is oldValue required for
             // create? I think so that the EntryExistsException will have it.
    )) {
      throw new EntryExistsException(event.getKey().toString(), event.getOldValue());
    } else {
      if (!getDataView().isDeferredStats()) {
        getCachePerfStats().endPut(startPut, false);
      }
    }
  }

  @Retained
  private EntryEventImpl newCreateEntryEvent(Object key, Object value, Object aCallbackArgument) {

    validateArguments(key, value, aCallbackArgument);
    checkReadiness();
    checkForLimitedOrNoAccess();

    return EntryEventImpl
        .create(this, Operation.CREATE, key, value, aCallbackArgument, false, getMyId())
        /* to distinguish genuine create */.setCreate(true);
  }

  /**
   * The default Region implementation will generate EvenTID in the EntryEvent object. This method
   * is overridden in special Region objects like HARegion or
   * SingleWriteSingleReadRegionQueue.SingleReadWriteMetaRegion to return false as the event
   * propagation from those regions do not need EventID objects
   *
   * @return boolean indicating whether to generate eventID or not
   */
  @Override
  public boolean generateEventID() {
    return !isUsedForPartitionedRegionAdmin();
  }

  @Override
  public Object destroy(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    @Released
    EntryEventImpl event = newDestroyEntryEvent(key, aCallbackArgument);
    try {
      return validatedDestroy(key, event);
    } finally {
      event.release();
    }
  }

  /**
   * Destroys entry without performing validations. Call this after validating key, callback arg,
   * and runtime state.
   */
  public Object validatedDestroy(Object key, EntryEventImpl event)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    if (event.getEventId() == null && generateEventID()) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    basicDestroy(event, true, // cacheWrite
        null); // expectedOldValue
    if (event.isOldValueOffHeap()) {
      return null;
    } else {
      return handleNotAvailable(event.getOldValue());
    }
  }

  @Retained
  EntryEventImpl newDestroyEntryEvent(Object key, Object aCallbackArgument) {
    validateKey(key);
    checkReadiness();
    checkForLimitedOrNoAccess();

    return EntryEventImpl.create(this, Operation.DESTROY, key, null/* newValue */,
        aCallbackArgument, false, getMyId());
  }

  @Override
  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    this.cache.invokeBeforeDestroyed(this);
    getDataView().checkSupportsRegionDestroy();
    checkForLimitedOrNoAccess();

    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_DESTROY, aCallbackArgument,
        false, getMyId(), generateEventID());
    basicDestroyRegion(event, true);
  }

  @Override
  public InternalDataView getDataView() {
    final TXStateInterface tx = getTXState();
    if (tx == null) {
      return this.sharedDataView;
    }
    return tx;
  }

  /**
   * Fetch the de-serialized value from non-transactional state.
   *
   * @param keyInfo to which the value is associated
   * @param updateStats true if the entry stats should be updated.
   * @param disableCopyOnRead if true then disable copy on read
   * @param preferCachedDeserializable true if the preferred result form is CachedDeserializable
   * @param clientEvent client's event, if any (for version tag retrieval)
   * @param returnTombstones whether destroyed entries should be returned
   * @param retainResult if true then the result may be a retained off-heap reference
   * @return the value for the given key
   */
  public Object getDeserializedValue(RegionEntry regionEntry, final KeyInfo keyInfo,
      final boolean updateStats, boolean disableCopyOnRead, boolean preferCachedDeserializable,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean retainResult) {
    if (this.diskRegion != null) {
      this.diskRegion.setClearCountReference();
    }
    try {
      if (regionEntry == null) {
        regionEntry = this.entries.getEntry(keyInfo.getKey());
      }
      // skip updating the stats if the value is null
      // TODO - We need to clean up the callers of the this class so that we can
      // update the statistics here, where it would make more sense.
      if (regionEntry == null) {
        return null;
      }
      final Object value;
      if (clientEvent != null && regionEntry.getVersionStamp() != null) {
        // defer the lruUpdateCallback to prevent a deadlock (see bug 51121).
        final boolean disabled = this.entries.disableLruUpdateCallback();
        try {
          synchronized (regionEntry) { // bug #51059 value & version must be obtained atomically
            clientEvent.setVersionTag(regionEntry.getVersionStamp().asVersionTag());
            value = getDeserialized(regionEntry, updateStats, disableCopyOnRead,
                preferCachedDeserializable, retainResult);
          }
        } finally {
          if (disabled) {
            this.entries.enableLruUpdateCallback();
          }
          try {
            this.entries.lruUpdateCallback();
          } catch (DiskAccessException dae) {
            this.handleDiskAccessException(dae);
            throw dae;
          }
        }
      } else {
        value = getDeserialized(regionEntry, updateStats, disableCopyOnRead,
            preferCachedDeserializable, retainResult);
      }
      if (logger.isTraceEnabled() && !(this instanceof HARegion)) {
        logger.trace(
            "getDeserializedValue for {} returning version: {} returnTombstones: {} value: {}",
            keyInfo.getKey(), regionEntry.getVersionStamp() == null ? "null"
                : regionEntry.getVersionStamp().asVersionTag(),
            returnTombstones, value);
      }
      return value;
    } finally {
      if (this.diskRegion != null) {
        this.diskRegion.removeClearCountReference();
      }
    }
  }

  /**
   * @param disableCopyOnRead if true then do not make a copy on read
   * @param preferCachedDeserializable true if the preferred result form is CachedDeserializable
   * @param retainResult if true then the result may be a retained off-heap reference
   * @return the value found, which can be
   *         <ul>
   *         <li>null if the value was removed from the region entry
   *         <li>Token.INVALID if the value of the region entry is invalid
   *         <li>Token.LOCAL_INVALID if the value of the region entry is local invalid
   *         </ul>
   */
  @Retained
  Object getDeserialized(RegionEntry regionEntry, boolean updateStats, boolean disableCopyOnRead,
      boolean preferCachedDeserializable, boolean retainResult) {
    assert !retainResult || preferCachedDeserializable;
    boolean disabledLRUCallback = this.entries.disableLruUpdateCallback();
    try {
      @Retained
      Object value;
      try {
        if (retainResult) {
          value = regionEntry.getValueRetain(this);
        } else {
          value = regionEntry.getValue(this);
        }
      } catch (DiskAccessException dae) {
        this.handleDiskAccessException(dae);
        throw dae;
      }

      // skip updating the stats if the value is null
      if (value == null) {
        return null;
      }
      if (value instanceof CachedDeserializable) {
        if (!preferCachedDeserializable) {
          if (isCopyOnRead()) {
            if (disableCopyOnRead) {
              value = ((CachedDeserializable) value).getDeserializedForReading();
            } else {
              value = ((CachedDeserializable) value).getDeserializedWritableCopy(this, regionEntry);
            }
          } else {
            value = ((CachedDeserializable) value).getDeserializedValue(this, regionEntry);
          }
        }
      } else if (!disableCopyOnRead) {
        value = conditionalCopy(value);
      }

      if (updateStats) {
        updateStatsForGet(regionEntry, value != null && !Token.isInvalid(value));
      }
      return value;
    } catch (IllegalArgumentException i) {
      throw new IllegalArgumentException(LocalizedStrings.DONT_RELEASE
          .toLocalizedString("Error while deserializing value for key=" + regionEntry.getKey()), i);
    } finally {
      if (disabledLRUCallback) {
        this.entries.enableLruUpdateCallback();
        this.entries.lruUpdateCallback();
      }
    }
  }

  @Override
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      EntryEventImpl clientEvent) throws TimeoutException, CacheLoaderException {
    Object result =
        get(key, aCallbackArgument, generateCallbacks, false, false, null, clientEvent, false);
    if (Token.isInvalid(result)) {
      result = null;
    }
    return result;
  }

  /**
   * @see BucketRegion#getSerialized(KeyInfo, boolean, boolean, ClientProxyMembershipID,
   *      EntryEventImpl, boolean)
   */
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws TimeoutException, CacheLoaderException {
    return get(key, aCallbackArgument, generateCallbacks, disableCopyOnRead, preferCD,
        requestingClient, clientEvent, returnTombstones, false, false);
  }

  /**
   * The result of this operation may be an off-heap reference that the caller must release
   */
  @Retained
  public Object getRetained(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws TimeoutException, CacheLoaderException {
    return getRetained(key, aCallbackArgument, generateCallbacks, disableCopyOnRead,
        requestingClient, clientEvent, returnTombstones, false);
  }

  /**
   * The result of this operation may be an off-heap reference that the caller must release.
   *
   * @param opScopeIsLocal if true then just check local storage for a value; if false then try to
   *        find the value if it is not local
   */
  @Retained
  private Object getRetained(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean opScopeIsLocal)
      throws TimeoutException, CacheLoaderException {
    return get(key, aCallbackArgument, generateCallbacks, disableCopyOnRead, true, requestingClient,
        clientEvent, returnTombstones, opScopeIsLocal, false /* see GEODE-1291 */);
  }

  /**
   * @param opScopeIsLocal if true then just check local storage for a value; if false then try to
   *        find the value if it is not local
   * @param retainResult if true then the result may be a retained off-heap reference.
   */
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean opScopeIsLocal,
      boolean retainResult) throws TimeoutException, CacheLoaderException {
    assert !retainResult || preferCD;
    validateKey(key);
    checkReadiness();
    checkForNoAccess();
    discoverJTA();
    CachePerfStats stats = getCachePerfStats();
    long start = stats.startGet();
    boolean isMiss = true;
    try {
      KeyInfo keyInfo = getKeyInfo(key, aCallbackArgument);
      Object value = getDataView().getDeserializedValue(keyInfo, this, true, disableCopyOnRead,
          preferCD, clientEvent, returnTombstones, retainResult);
      final boolean isCreate = value == null;
      isMiss = value == null || Token.isInvalid(value)
          || (!returnTombstones && value == Token.TOMBSTONE);
      // Note: if the value was Token.DESTROYED then getDeserialized
      // returns null so we don't need the following in the above expression:
      // || (isRegInterestInProgress() && Token.isDestroyed(value))
      // because (value == null) will be true in this case.
      if (isMiss) {
        // to fix bug 51509 raise the precedence of opScopeIsLocal
        // if scope is local and there is no loader, then
        // don't go further to try and get value
        if (!opScopeIsLocal
            && ((getScope().isDistributed()) || hasServerProxy() || basicGetLoader() != null)) {
          // serialize search/load threads if not in txn
          value = getDataView().findObject(keyInfo, this, isCreate, generateCallbacks, value,
              disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);
          if (!returnTombstones && value == Token.TOMBSTONE) {
            value = null;
          }
        } else { // local scope with no loader, still might need to update stats
          if (isCreate) {
            recordMiss(null, key);
          }
          value = null;
        }
      }
      return value;
    } finally {
      stats.endGet(start, isMiss);
    }
  }

  /**
   * Update region and potentially entry stats for the miss case
   *
   * @param re optional region entry, fetched if null
   * @param key the key used to fetch the region entry
   */
  public void recordMiss(final RegionEntry re, Object key) {
    if (!this.statisticsEnabled) {
      return;
    }
    final RegionEntry e;
    if (re == null && !isTX()) {
      e = basicGetEntry(key);
    } else {
      e = re;
    }
    updateStatsForGet(e, false);
  }

  /**
   * optimized to only allow one thread to do a search/load, other threads wait on a future
   *
   * @param isCreate true if call found no entry; false if updating an existing entry
   * @param localValue the value retrieved from the region for this object.
   * @param disableCopyOnRead if true then do not make a copy
   * @param preferCD true if the preferred result form is CachedDeserializable
   * @param clientEvent the client event, if any
   * @param returnTombstones whether to return tombstones
   */
  @Retained
  Object nonTxnFindObject(KeyInfo keyInfo, boolean isCreate, boolean generateCallbacks,
      Object localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws TimeoutException, CacheLoaderException {

    @Retained
    Object result = null;
    FutureResult thisFuture = new FutureResult(this.stopper);
    Future otherFuture = (Future) this.getFutures.putIfAbsent(keyInfo.getKey(), thisFuture);
    // only one thread can get their future into the map for this key at a time
    if (otherFuture != null) {
      try {
        Object[] valueAndVersion = (Object[]) otherFuture.get();
        if (valueAndVersion != null) {
          result = valueAndVersion[0];
          if (clientEvent != null) {
            clientEvent.setVersionTag((VersionTag) valueAndVersion[1]);
          }
          if (!preferCD && result instanceof CachedDeserializable) {
            CachedDeserializable cd = (CachedDeserializable) result;
            // fix for bug 43023
            if (!disableCopyOnRead && isCopyOnRead()) {
              result = cd.getDeserializedWritableCopy(null, null);
            } else {
              result = cd.getDeserializedForReading();
            }

          } else if (!disableCopyOnRead) {
            result = conditionalCopy(result);
          }
          // what was a miss is now a hit
          if (isCreate) {
            RegionEntry regionEntry = basicGetEntry(keyInfo.getKey());
            updateStatsForGet(regionEntry, true);
          }
          return result;
        }
        // if value == null, try our own search/load
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        // TODO check a CancelCriterion here?
        return null;
      } catch (ExecutionException e) {
        // unexpected since there is no background thread
        // NOTE: this was creating InternalGemFireError and initCause with itself
        throw new InternalGemFireError(
            LocalizedStrings.LocalRegion_UNEXPECTED_EXCEPTION.toLocalizedString(), e);
      }
    }
    // didn't find a future, do one more probe for the entry to catch a race
    // condition where the future was just removed by another thread
    try {
      boolean partitioned = this.getDataPolicy().withPartitioning();
      if (!partitioned) {
        localValue = getDeserializedValue(null, keyInfo, isCreate, disableCopyOnRead, preferCD,
            clientEvent, false, false);

        // stats have now been updated
        if (localValue != null && !Token.isInvalid(localValue)) {
          result = localValue;
          return result;
        }
        isCreate = localValue == null;
        result = findObjectInSystem(keyInfo, isCreate, null, generateCallbacks, localValue,
            disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);

      } else {

        // For PRs we don't want to deserialize the value and we can't use findObjectInSystem
        // because it can invoke code that is transactional.
        result =
            getSharedDataView().findObject(keyInfo, this, isCreate, generateCallbacks, localValue,
                disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones);
      }

      if (result == null && localValue != null) {
        if (localValue != Token.TOMBSTONE || returnTombstones) {
          result = localValue;
        }
      }
      // findObjectInSystem does not call conditionalCopy
    } finally {
      if (result != null) {
        VersionTag tag = clientEvent == null ? null : clientEvent.getVersionTag();
        thisFuture.set(new Object[] {result, tag});
      } else {
        thisFuture.set(null);
      }
      this.getFutures.remove(keyInfo.getKey());
    }
    if (!disableCopyOnRead) {
      result = conditionalCopy(result);
    }
    return result;
  }

  /**
   * Returns true if get should give a copy; false if a reference.
   *
   * @since GemFire 4.0
   */
  @Override
  public boolean isCopyOnRead() {
    return this.compressor == null && this.cache.isCopyOnRead()
        && !this.isUsedForPartitionedRegionAdmin && !this.isUsedForMetaRegion && !getOffHeap()
        && !isSecret();
  }

  /**
   * Makes a copy, if copy-on-get is enabled, of the specified object.
   *
   * @since GemFire 4.0
   */
  Object conditionalCopy(Object o) {
    if (isCopyOnRead() && !Token.isInvalid(o)) {
      return CopyHelper.copy(o);
    } else {
      return o;
    }
  }

  private final String fullPath;

  @Override
  public String getFullPath() {
    return this.fullPath;
  }

  @Override
  public Region getParentRegion() {
    return this.parentRegion;
  }

  @Override
  public Region getSubregion(String path) {
    checkReadiness();
    return getSubregion(path, false);
  }

  @Override
  public void invalidateRegion(Object aCallbackArgument) throws TimeoutException {
    getDataView().checkSupportsRegionInvalidate();
    checkReadiness();
    checkForLimitedOrNoAccess();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_INVALIDATE,
        aCallbackArgument, false, getMyId(), generateEventID());

    basicInvalidateRegion(event);
  }

  @Override
  public Object put(Object key, Object value, Object aCallbackArgument)
      throws TimeoutException, CacheWriterException {
    long startPut = CachePerfStats.getStatTime();
    @Released
    EntryEventImpl event = newUpdateEntryEvent(key, value, aCallbackArgument);
    try {
      return validatedPut(event, startPut);
    } finally {
      event.release();
    }
  }

  Object validatedPut(EntryEventImpl event, long startPut)
      throws TimeoutException, CacheWriterException {

    if (event.getEventId() == null && generateEventID()) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    Object oldValue = null;
    if (basicPut(event, false, // ifNew
        false, // ifOld
        null, // expectedOldValue
        false // requireOldValue
    )) {
      if (!event.isOldValueOffHeap()) {
        // don't copy it to heap just to return from put.
        // TODO: come up with a better way to do this.
        oldValue = event.getOldValue();
      }
      if (!getDataView().isDeferredStats()) {
        getCachePerfStats().endPut(startPut, false);
      }
    }
    return handleNotAvailable(oldValue);
  }

  @Retained
  EntryEventImpl newUpdateEntryEvent(Object key, Object value, Object aCallbackArgument) {

    validateArguments(key, value, aCallbackArgument);
    if (value == null) {
      throw new NullPointerException(
          LocalizedStrings.LocalRegion_VALUE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    checkReadiness();
    checkForLimitedOrNoAccess();
    discoverJTA();

    // This used to call the constructor which took the old value. It
    // was modified to call the other EntryEventImpl constructor so that
    // an id will be generated by default. Null was passed in anyway.
    // generate EventID
    @Retained
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.UPDATE, key, value,
        aCallbackArgument, false, getMyId());
    boolean eventReturned = false;
    try {
      extractDeltaIntoEvent(value, event);
      eventReturned = true;
      return event;
    } finally {
      if (!eventReturned)
        event.release();
    }
  }

  private void extractDeltaIntoEvent(Object value, EntryEventImpl event) {
    // 1. Check for DS-level delta property.
    // 2. Default value for operation type is UPDATE, so no need to check that here.
    // 3. Check if it has server region proxy.
    // We do not have a handle to event in PutOpImpl to check if we have
    // delta bytes calculated already. So no need to calculate it here.
    // 4. Check if value is instanceof org.apache.geode.Delta
    // 5. Check if Region in PR with redundantCopies > 0. Set extractDelta.
    // 6. Check if Region has peers. Set extractDelta.
    // 7. Check if it has any delta proxies attached to it. Set extractDelta.
    // 8. If extractDelta is set, check if it has delta.
    // 9. If delta is found, extract it and set it into the event.
    // 10. If any exception is caught while invoking the delta callbacks, throw it back.
    // 11. Wrap any checked exception in InternalGemFireException before throwing it.
    try {
      // How costly is this if check?
      if (getSystem().getConfig().getDeltaPropagation() && value instanceof Delta) {
        boolean extractDelta = false;
        if (!this.hasServerProxy()) {
          if (this instanceof PartitionedRegion) {
            if (((PartitionedRegion) this).getRedundantCopies() > 0) {
              extractDelta = true;
            } else {
              InternalDistributedMember ids = (InternalDistributedMember) PartitionRegionHelper
                  .getPrimaryMemberForKey(this, event.getKey());
              if (ids != null) {
                extractDelta = !this.getSystem().getMemberId().equals(ids.getId())
                    || hasAdjunctRecipientsNeedingDelta(event);
              } else {
                extractDelta = true;
              }
            }
          } else if (this instanceof DistributedRegion
              && !((DistributedRegion) this).scope.isDistributedNoAck()
              && !((CacheDistributionAdvisee) this).getCacheDistributionAdvisor().adviseCacheOp()
                  .isEmpty()) {
            extractDelta = true;
          }
          if (!extractDelta && ClientHealthMonitor.getInstance() != null) {
            extractDelta = ClientHealthMonitor.getInstance().hasDeltaClients();
          }
        } else if (getSystem().isDeltaEnabledOnServer()) {
          // This is a client region
          extractDelta = true;
        }
        if (extractDelta && ((org.apache.geode.Delta) value).hasDelta()) {
          HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
          long start = DistributionStats.getStatTime();
          try {
            ((org.apache.geode.Delta) value).toDelta(hdos);
          } catch (RuntimeException re) {
            throw re;
          } catch (Exception e) {
            throw new DeltaSerializationException(
                LocalizedStrings.DistributionManager_CAUGHT_EXCEPTION_WHILE_SENDING_DELTA
                    .toLocalizedString(),
                e);
          }
          event.setDeltaBytes(hdos.toByteArray());
          this.getCachePerfStats().endDeltaPrepared(start);
        }
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new InternalGemFireException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean hasAdjunctRecipientsNeedingDelta(EntryEventImpl event) {
    PartitionedRegion partitionedRegion = (PartitionedRegion) this;
    BucketRegion bucketRegion;

    int bId = event.getKeyInfo().getBucketId();
    try {
      bucketRegion = partitionedRegion.dataStore.getInitializedBucketForId(event.getKey(), bId);
    } catch (ForceReattemptException ignore) {
      return true;
    }
    Set<InternalDistributedMember> recipients =
        bucketRegion.getCacheDistributionAdvisor().adviseUpdate(event);
    Set<Object> twoMessages = bucketRegion.getBucketAdvisor().adviseRequiresTwoMessages();
    CacheDistributionAdvisor cda = partitionedRegion.getCacheDistributionAdvisor();
    FilterRoutingInfo filterRouting = cda.adviseFilterRouting(event, recipients);
    Set<Object> adjunctRecipients =
        bucketRegion.getAdjunctReceivers(event, recipients, twoMessages, filterRouting);
    Set cacheServerMembers = cda.adviseCacheServers();
    return !Collections.disjoint(adjunctRecipients, cacheServerMembers);
  }

  @Override
  public Region.Entry getEntry(Object key) {
    validateKey(key);
    checkReadiness();
    checkForNoAccess();
    discoverJTA();
    return getDataView().getEntry(getKeyInfo(key), this, false);
  }

  /** internally we often need to get an entry whether it is a tombstone or not */
  public Region.Entry getEntry(Object key, boolean allowTombstones) {
    return getDataView().getEntry(getKeyInfo(key), this, allowTombstones);
  }

  /**
   * Just like getEntry but also updates the stats that get would have depending on a flag. See bug
   * 42410. Also skips discovering JTA
   *
   * @return the entry if it exists; otherwise null.
   */
  public Entry accessEntry(Object key, boolean updateStats) {
    validateKey(key);
    checkReadiness();
    checkForNoAccess();
    if (updateStats) {
      return getDataView().accessEntry(getKeyInfo(key), this);
    } else {
      return getDataView().getEntry(getKeyInfo(key), this, false);
    }
  }

  /** a fast estimate of total number of entries locally in the region */
  public long getEstimatedLocalSize() {
    if (!this.isDestroyed) {
      long size;
      // if region has not been initialized yet, then get the estimate from
      // disk region's recovery map if available
      RegionMap regionMap;
      if (!this.initialized && this.diskRegion != null
          && (regionMap = this.diskRegion.getRecoveredEntryMap()) != null
          && (size = regionMap.size()) > 0) {
        return size;
      }
      if ((regionMap = getRegionMap()) != null) {
        return regionMap.size();
      }
    }
    return 0;
  }

  /**
   * @param access true if caller wants last accessed time updated
   * @param allowTombstones whether an entry with a TOMBSTONE value can be returned
   */
  protected Region.Entry nonTXGetEntry(KeyInfo keyInfo, boolean access, boolean allowTombstones) {
    final Object key = keyInfo.getKey();
    RegionEntry re = this.entries.getEntry(key);
    boolean miss = re == null || re.isDestroyedOrRemoved();
    if (access) {
      updateStatsForGet(re, !miss);
    }
    if (re == null) {
      return null;
    }
    if (re.isTombstone()) {
      if (!allowTombstones) {
        return null;
      } // else return an entry (client GII / putAll results)
    } else if (miss) {
      return null;
    }

    return new NonTXEntry(re);
  }

  protected boolean isClosed() {
    return this.cache.isClosed();
  }

  /**
   * Returns true if this region is or has been closed or destroyed. Note that unlike
   * {@link #isDestroyed()} this method will not return true if the cache is closing but has not yet
   * started closing this region.
   */
  @Override
  public boolean isThisRegionBeingClosedOrDestroyed() {
    return this.isDestroyed;
  }

  /** returns true if this region has been destroyed */
  @Override
  public boolean isDestroyed() {
    if (isClosed()) {
      return true; // for bug 42328
    }

    boolean isTraceEnabled = logger.isTraceEnabled();

    if (this.isDestroyed) {
      if (isTraceEnabled) {
        logger.trace("isDestroyed: true, this.isDestroyed: {}", getFullPath());
      }
      return true;
    }

    if (isTraceEnabled) {
      logger.trace("isDestroyed: false : {}", getFullPath());
    }
    return false;
  }

  /** a variant of subregions() that does not perform a readiness check */
  @Override
  public Set basicSubregions(boolean recursive) {
    return new SubregionsSet(recursive);
  }

  @Override
  public Set subregions(boolean recursive) {
    checkReadiness();
    return new SubregionsSet(recursive);
  }

  @Override
  public Set entrySet(boolean recursive) {
    checkReadiness();
    checkForNoAccess();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return basicEntries(recursive);
  }

  /** Returns set of entries without performing validation checks. */
  public Set basicEntries(boolean recursive) {
    return new EntriesSet(this, recursive, IteratorType.ENTRIES, false);
  }

  /**
   * Flavor of keys that will not do repeatable read
   *
   * @since GemFire 5.5
   */
  public Set testHookKeys() {
    checkReadiness();
    checkForNoAccess();
    return new EntriesSet(this, false, IteratorType.KEYS, false /* allowTombstones */);
  }

  public Set keys() {
    checkReadiness();
    checkForNoAccess();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return new EntriesSet(this, false, IteratorType.KEYS, false);
  }

  /**
   * return a set of the keys in this region
   *
   * @param allowTombstones whether destroyed entries should be included
   * @return the keys
   */
  public Set keySet(boolean allowTombstones) {
    checkReadiness();
    checkForNoAccess();
    return new EntriesSet(this, false, IteratorType.KEYS, allowTombstones);
  }

  @Override
  public Collection values() {
    checkReadiness();
    checkForNoAccess();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return new EntriesSet(this, false, IteratorType.VALUES, false);
  }

  @Override
  public Object getUserAttribute() {
    return this.regionUserAttribute;
  }

  @Override
  public void setUserAttribute(Object value) {
    checkReadiness();
    this.regionUserAttribute = value;
  }

  @Override
  public boolean containsKey(Object key) {
    checkReadiness();
    checkForNoAccess();
    return getDataView().containsKey(getKeyInfo(key), this);
  }

  public boolean containsTombstone(Object key) {
    checkReadiness();
    checkForNoAccess();
    if (!this.getConcurrencyChecksEnabled()) {
      return false;
    } else {
      try {
        Entry entry = getDataView().getEntry(getKeyInfo(key), this, true);
        return entry != null && entry.getValue() == Token.TOMBSTONE;
      } catch (EntryDestroyedException ignore) {
        return true;
      }
    }
  }

  protected boolean nonTXContainsKey(KeyInfo keyInfo) {
    boolean contains = getRegionMap().containsKey(keyInfo.getKey());
    if (contains && this.imageState.isClient()) {
      // fix for bug #40871 - concurrent RI causes containsKey for destroyed entry
      // to return true
      RegionEntry regionEntry = this.entries.getEntry(keyInfo.getKey());
      if (regionEntry == null || regionEntry.isDestroyedOrRemoved()) {
        contains = false;
      }
    }
    return contains;
  }

  @Override
  public boolean containsValueForKey(Object key) {
    discoverJTA();
    return getDataView().containsValueForKey(getKeyInfo(key), this);
  }

  protected boolean nonTXContainsValueForKey(KeyInfo keyInfo) {
    checkReadiness();
    checkForNoAccess();
    if (this.diskRegion != null) {
      this.diskRegion.setClearCountReference();
    }
    try {
      RegionEntry entry = this.entries.getEntry(keyInfo.getKey());
      boolean result = entry != null;
      if (result) {
        ReferenceCountHelper.skipRefCountTracking();
        // no need to decompress since we only want to know if we have an existing value
        Object val = entry.getTransformedValue();
        if (val instanceof StoredObject) {
          OffHeapHelper.release(val);
          ReferenceCountHelper.unskipRefCountTracking();
          return true;
        }
        ReferenceCountHelper.unskipRefCountTracking();
        // No need to to check CachedDeserializable because of Bruce's fix in r30960 for bug 42162.
        // See bug 42732.
        // this works because INVALID and LOCAL_INVALID will never be faulted out of mem
        // If val is NOT_AVAILABLE that means we have a valid value on disk.
        result = !Token.isInvalidOrRemoved(val);
      }
      return result;
    } finally {
      if (this.diskRegion != null) {
        this.diskRegion.removeClearCountReference();
      }
    }
  }

  @Override
  public RegionAttributes getAttributes() {
    // to fix bug 35134 allow attribute access on closed regions
    return this;
  }

  @Override
  public String getName() {
    return this.regionName;
  }

  /**
   * Convenience method to get region name for logging/exception messages. if this region is an
   * instanceof bucket region, it returns the bucket region name
   *
   * @return name of the region or the owning partitioned region
   */
  @Override
  public String getDisplayName() {
    if (this.isUsedForPartitionedRegionBucket()) {
      return this.getPartitionedRegion().getName();
    }
    return this.regionName;
  }

  /**
   * Returns the number of entries in this region. Note that because of the concurrency properties
   * of the {@link RegionMap}, the number of entries is only an approximate. That is, other threads
   * may change the number of entries in this region while this method is being invoked.
   *
   * @see RegionMap#size
   */
  public int entryCount() {
    return getDataView().entryCount(this);
  }

  public int entryCount(Set<Integer> buckets) {
    return entryCount(buckets, false);
  }

  protected int entryCount(Set<Integer> buckets, boolean estimate) {
    assert buckets == null : "unexpected buckets " + buckets + " for region " + toString();

    return getDataView().entryCount(this);
  }

  /**
   * @return size after considering imageState
   */
  public int getRegionSize() {
    synchronized (getSizeGuard()) {
      int result = getRegionMap().size();
      // if this is a client with no tombstones then we subtract the number
      // of entries being affected by register-interest refresh
      if (this.imageState.isClient() && !this.getConcurrencyChecksEnabled()) {
        return result - this.imageState.getDestroyedEntriesCount();
      }
      return result - this.tombstoneCount.get();
    }
  }

  /**
   * Returns the {@code DiskRegion} that this region uses to access data on disk.
   *
   * @return {@code null} if disk regions are not being used
   *
   * @since GemFire 3.2
   */
  @Override
  public DiskRegion getDiskRegion() {
    return this.diskRegion;
  }

  @Override
  public DiskRegionView getDiskRegionView() {
    return getDiskRegion();
  }

  /**
   * Lets the customer do an explicit evict of a value to disk and removes the value from memory.
   */
  public void evictValue(Object key) {
    if (getDiskRegion() != null) {
      this.entries.evictValue(key);
    }
  }

  private boolean isOverflowEnabled() {
    EvictionAttributes ea = getAttributes().getEvictionAttributes();
    return ea != null && ea.getAction().isOverflowToDisk();
  }

  @Override
  public void writeToDisk() {
    if (this.diskRegion == null) {
      DataPolicy dp = getDataPolicy();
      if (dp.isEmpty()) {
        throw new IllegalStateException(
            LocalizedStrings.LocalRegion_CANNOT_WRITE_A_REGION_WITH_DATAPOLICY_0_TO_DISK
                .toLocalizedString(dp));
      } else if (!dp.withPersistence() && !isOverflowEnabled()) {
        throw new IllegalStateException(
            LocalizedStrings.LocalRegion_CANNOT_WRITE_A_REGION_THAT_IS_NOT_CONFIGURED_TO_ACCESS_DISKS
                .toLocalizedString());
      }
    } else {
      this.diskRegion.asynchForceFlush();
    }
  }

  /**
   * Used by tests to force everything out to disk.
   */
  public void forceFlush() {
    if (this.diskRegion != null) {
      this.diskRegion.flushForTesting();
    }
  }

  /**
   * This implementation only checks readiness and scope
   */
  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {
    checkReadiness();
    checkForLimitedOrNoAccess();
    Scope theScope = getAttributes().getScope();
    Assert.assertTrue(theScope == Scope.LOCAL);
    throw new IllegalStateException(
        LocalizedStrings.LocalRegion_ONLY_SUPPORTED_FOR_GLOBAL_SCOPE_NOT_LOCAL.toLocalizedString());
  }

  /**
   * This implementation only checks readiness and scope
   */
  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    checkReadiness();
    checkForLimitedOrNoAccess();
    Scope theScope = getAttributes().getScope();
    Assert.assertTrue(theScope == Scope.LOCAL);
    throw new IllegalStateException(
        LocalizedStrings.LocalRegion_ONLY_SUPPORTED_FOR_GLOBAL_SCOPE_NOT_LOCAL.toLocalizedString());
  }

  @Override
  public void invalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException {
    checkReadiness();
    checkForLimitedOrNoAccess();
    validatedInvalidate(key, aCallbackArgument);
  }

  /**
   * Destroys entry without performing validations. Call this after validating key, callback arg,
   * and runtime state.
   */
  void validatedInvalidate(Object key, Object aCallbackArgument)
      throws TimeoutException, EntryNotFoundException {
    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.INVALIDATE, key, null,
        aCallbackArgument, false, getMyId());
    try {
      if (generateEventID()) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      basicInvalidate(event);
    } finally {
      event.release();
    }
  }

  @Override
  public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    validateKey(key);
    checkReadiness();
    checkForNoAccess();
    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.LOCAL_DESTROY, key, null,
        aCallbackArgument, false, getMyId());
    if (generateEventID()) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    try {
      basicDestroy(event, false, null); // expectedOldValue
    } catch (CacheWriterException e) {
      // cache writer not called
      throw new Error(
          LocalizedStrings.LocalRegion_CACHE_WRITER_SHOULD_NOT_HAVE_BEEN_CALLED_FOR_LOCALDESTROY
              .toLocalizedString(),
          e);
    } catch (TimeoutException e) {
      // no distributed lock
      throw new Error(
          LocalizedStrings.LocalRegion_NO_DISTRIBUTED_LOCK_SHOULD_HAVE_BEEN_ATTEMPTED_FOR_LOCALDESTROY
              .toLocalizedString(),
          e);
    } finally {
      event.release();
    }
  }

  @Override
  public void localDestroyRegion(Object aCallbackArgument) {
    getDataView().checkSupportsRegionDestroy();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOCAL_DESTROY,
        aCallbackArgument, false, getMyId(), generateEventID()/* generate EventID */);
    try {
      basicDestroyRegion(event, false);
    } catch (CacheWriterException e) {
      // not possible with local operation, CacheWriter not called
      throw new Error(
          LocalizedStrings.LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
              .toLocalizedString(),
          e);
    } catch (TimeoutException e) {
      // not possible with local operation, no distributed locks possible
      throw new Error(
          LocalizedStrings.LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
              .toLocalizedString(),
          e);
    }
  }

  @Override
  public void close() {
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_CLOSE, null, false,
        getMyId(), generateEventID()/* generate EventID */);
    try {
      // NOTE: the 422dynamicRegions branch added the callbackEvents argument
      // to basicDestroyRegion and inhibited events on region.close. This
      // clashed with the new SystemMemberCacheListener functionality in
      // 5.0, causing unit tests to fail
      basicDestroyRegion(event, false, true, true);
    } catch (CacheWriterException e) {
      // not possible with local operation, CacheWriter not called
      throw new Error(
          LocalizedStrings.LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
              .toLocalizedString(),
          e);
    } catch (TimeoutException e) {
      // not possible with local operation, no distributed locks possible
      throw new Error(
          LocalizedStrings.LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_IN_LOCALDESTROYREGION
              .toLocalizedString(),
          e);
    }
  }

  @Override
  public void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException {
    validateKey(key);
    checkReadiness();
    checkForNoAccess();

    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.LOCAL_INVALIDATE, key,
        null/* newValue */, aCallbackArgument, false, getMyId());
    try {
      if (generateEventID()) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      event.setLocalInvalid(true);
      basicInvalidate(event);
    } finally {
      event.release();
    }
  }

  @Override
  public void localInvalidateRegion(Object aCallbackArgument) {
    getDataView().checkSupportsRegionInvalidate();
    checkReadiness();
    checkForNoAccess();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOCAL_INVALIDATE,
        aCallbackArgument, false, getMyId());
    basicInvalidateRegion(event);
  }

  /**
   * Do any extra initialization required. Region is already visible in parent's subregion map. This
   * method releases the initialization Latches, so subclasses should call this super method last
   * after performing additional initialization.
   *
   * @param imageTarget ignored, used by subclass for get initial image
   * @see DistributedRegion#initialize(InputStream, InternalDistributedMember,
   *      InternalRegionArguments)
   */
  @Override
  public void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException {
    if (!isInternalRegion()) {
      // Subclasses may have already called this method, but this is
      // acceptable because addResourceListener won't add it twice
      if (!this.isDestroyed) {
        this.cache.getInternalResourceManager().addResourceListener(ResourceType.MEMORY, this);
      }
    }

    // if not local, then recovery happens in InitialImageOperation
    if (this.scope.isLocal()) {
      createOQLIndexes(internalRegionArgs);
      if (this.diskRegion != null) {
        try {
          this.diskRegion.initializeOwner(this);
          this.diskRegion.finishInitializeOwner(this, GIIStatus.NO_GII);
          // This block was added so that early recovery could figure out that
          // this data needs to be recovered from disk. Local regions used to
          // not bother assigning a memberId but that is what the early
          // recovery
          // code uses to figure out that a region needs to be recovered.
          PersistentMemberID oldId = this.diskRegion.getMyInitializingID();
          if (oldId == null) {
            oldId = this.diskRegion.getMyPersistentID();
          }
          if (oldId == null) {
            PersistentMemberID newId = this.diskRegion.generatePersistentID();
            this.diskRegion.setInitializing(newId);
            this.diskRegion.setInitialized();
          }
        } catch (DiskAccessException dae) {
          releaseAfterRegionCreateEventLatch();
          this.handleDiskAccessException(dae, true);
          throw dae;
        }
      }
    }

    // make sure latches are released if they haven't been by now already
    releaseBeforeGetInitialImageLatch();
    if (snapshotInputStream != null && this.scope.isLocal()) {
      try {
        loadSnapshotDuringInitialization(snapshotInputStream);
      } catch (DiskAccessException dae) {
        releaseAfterRegionCreateEventLatch();
        this.handleDiskAccessException(dae);
        throw dae;
      }
    }

    releaseAfterGetInitialImageLatch();

    if (logger.isDebugEnabled()) {
      logger.debug("Calling addExpiryTasks for {}", this);
    }
    // these calls can throw RegionDestroyedException if this region is
    // destroyed
    // at this point
    try {
      addIdleExpiryTask();
      addTTLExpiryTask();
      if (isEntryExpiryPossible()) {
        rescheduleEntryExpiryTasks(); // called after gii to fix bug 35214
      }
      initialized();
    } catch (RegionDestroyedException ignore) {
      // whether it is this region or a parent region that is destroyed,
      // then so must we be
      Assert.assertTrue(isDestroyed());
      // just proceed, a destroyed region will be returned to caller
    }
  }

  void createOQLIndexes(InternalRegionArguments internalRegionArgs) {
    createOQLIndexes(internalRegionArgs, false);
  }

  void createOQLIndexes(InternalRegionArguments internalRegionArgs, boolean recoverFromDisk) {

    if (internalRegionArgs == null || internalRegionArgs.getIndexes() == null
        || internalRegionArgs.getIndexes().isEmpty()) {
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("LocalRegion.createOQLIndexes on region {}", this.getFullPath());
    }
    long start = getCachePerfStats().startIndexInitialization();
    List oqlIndexes = internalRegionArgs.getIndexes();

    if (this.indexManager == null) {
      this.indexManager = IndexUtils.getIndexManager(cache, this, true);
    }
    DiskRegion dr = this.getDiskRegion();
    boolean isOverflowToDisk = false;
    if (dr != null) {
      isOverflowToDisk = dr.isOverflowEnabled();
      if (recoverFromDisk && !isOverflowToDisk) {
        // Refer bug #44119
        // For disk regions, index creation should wait for async value creation to complete before
        // it starts its iteration
        // In case of disk overflow regions the waitForAsyncRecovery is done in populateOQLIndexes
        // method via getBestIterator()
        dr.waitForAsyncRecovery();
      }
    }
    Set<Index> indexes = new HashSet<Index>();
    Set<Index> prIndexes = new HashSet<>();
    int initLevel = 0;
    try {
      // Release the initialization latch for index creation.
      initLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
      for (Object o : oqlIndexes) {
        IndexCreationData icd = (IndexCreationData) o;
        try {
          if (icd.getPartitionedIndex() != null) {
            ExecutionContext externalContext = new ExecutionContext(null, this.cache);
            if (internalRegionArgs.getPartitionedRegion() != null) {
              externalContext.setBucketRegion(internalRegionArgs.getPartitionedRegion(),
                  (BucketRegion) this);
            }
            if (logger.isDebugEnabled()) {
              logger.debug("IndexManager Index creation process for {}", icd.getIndexName());
            }

            // load entries during initialization only for non overflow regions
            indexes.add(this.indexManager.createIndex(icd.getIndexName(), icd.getIndexType(),
                icd.getIndexExpression(), icd.getIndexFromClause(), icd.getIndexImportString(),
                externalContext, icd.getPartitionedIndex(), !isOverflowToDisk));
            prIndexes.add(icd.getPartitionedIndex());
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("QueryService Index creation process for {}" + icd.getIndexName());
            }
            DefaultQueryService qs = (DefaultQueryService) getGemFireCache().getLocalQueryService();
            String fromClause =
                icd.getIndexType() == IndexType.FUNCTIONAL || icd.getIndexType() == IndexType.HASH
                    ? icd.getIndexFromClause() : this.getFullPath();
            // load entries during initialization only for non overflow regions
            indexes.add(
                qs.createIndex(icd.getIndexName(), icd.getIndexType(), icd.getIndexExpression(),
                    fromClause, icd.getIndexImportString(), !isOverflowToDisk));
          }

        } catch (Exception ex) {
          logger.info("Failed to create index {} on region {} with exception: {}",
              icd.getIndexName(), this.getFullPath(), ex);

        }
      }
    } finally {
      // Reset the initialization lock.
      LocalRegion.setThreadInitLevelRequirement(initLevel);
    }
    // Load data into OQL indexes in case of disk recovery and disk overflow
    if (isOverflowToDisk) {
      if (recoverFromDisk) {
        populateOQLIndexes(indexes);
      } else {
        // Empty indexes are created for overflow regions but not populated at this stage
        // since this is not recovery.
        // Setting the populate flag to true so that the indexes can apply updates.
        this.indexManager.setPopulateFlagForIndexes(indexes);
      }
      // due to bug #52096, the pr index populate flags were not being set
      // we should revisit and clean up the index creation code paths
      this.indexManager.setPopulateFlagForIndexes(prIndexes);
    }
    getCachePerfStats().endIndexInitialization(start);
  }

  /**
   * Populate the indexes with region entries
   */
  private void populateOQLIndexes(Set<Index> indexes) {
    logger.info(LocalizedMessage.create(LocalizedStrings.GemFireCache_INDEX_LOADING));
    try {
      this.indexManager.populateIndexes(indexes);
    } catch (MultiIndexCreationException ex) {
      logger.info("Failed to update index on region {}: {}", this.getFullPath(), ex.getMessage());
    }
  }

  /**
   * The region is now fully initialized, as far as LocalRegion is concerned
   */
  protected void initialized() {
    // does nothing in LocalRegion at this time
  }

  protected void releaseLatches() {
    releaseBeforeGetInitialImageLatch();
    releaseAfterGetInitialImageLatch();
    releaseAfterRegionCreateEventLatch();
  }

  void releaseBeforeGetInitialImageLatch() {
    if (logger.isDebugEnabled()) {
      logger.debug("Releasing Initialization Latch (before initial image) for {}", getFullPath());
    }
    releaseLatch(this.getInitializationLatchBeforeGetInitialImage());
  }

  void releaseAfterGetInitialImageLatch() {
    if (logger.isDebugEnabled()) {
      logger.debug("Releasing Initialization Latch (after initial image) for {}", getFullPath());
    }
    releaseLatch(this.getInitializationLatchAfterGetInitialImage());
  }

  /**
   * Called after we have delivered our REGION_CREATE event.
   *
   * @since GemFire 5.0
   */
  private void releaseAfterRegionCreateEventLatch() {
    releaseLatch(this.afterRegionCreateEventLatch);
  }

  /**
   * Used to cause cache listener events to wait until the after region create event is delivered.
   *
   * @since GemFire 5.0
   */
  private void waitForRegionCreateEvent() {
    StoppableCountDownLatch latch = this.afterRegionCreateEventLatch;
    if (latch != null && latch.getCount() == 0) {
      return;
    }
    waitOnInitialization(latch);
  }

  private static void releaseLatch(StoppableCountDownLatch latch) {
    if (latch == null)
      return;
    latch.countDown();
  }

  /**
   * Removes entries and recursively destroys subregions.
   *
   * @param eventSet collects the events for all destroyed regions if null, then we're closing so
   *        don't send events to callbacks or destroy the disk region
   */
  private void recursiveDestroyRegion(Set eventSet, RegionEventImpl regionEvent, boolean cacheWrite)
      throws CacheWriterException, TimeoutException {
    final boolean isClose = regionEvent.getOperation().isClose();
    // do the cacheWriter beforeRegionDestroy first to fix bug 47736
    if (eventSet != null && cacheWrite) {
      try {
        cacheWriteBeforeRegionDestroy(regionEvent);
      } catch (CancelException e) {
        // I don't think this should ever happens: bulletproofing for bug 39454
        if (!this.cache.forcedDisconnect()) {
          logger.warn(
              LocalizedMessage.create(
                  LocalizedStrings.LocalRegion_RECURSIVEDESTROYREGION_PROBLEM_IN_CACHEWRITEBEFOREREGIONDESTROY),
              e);
        }
      }
    }

    getEventTracker().stop();
    if (logger.isTraceEnabled(LogMarker.RVV_VERBOSE) && getVersionVector() != null) {
      logger.trace(LogMarker.RVV_VERBOSE, "version vector for {} is {}", getName(),
          getVersionVector().fullToString());
    }
    cancelTTLExpiryTask();
    cancelIdleExpiryTask();
    cancelAllEntryExpiryTasks();
    if (!isInternalRegion()) {
      getCachePerfStats().incRegions(-1);
    }
    this.cache.getInternalResourceManager(false).removeResourceListener(this);
    if (getMembershipAttributes().hasRequiredRoles()) {
      if (!isInternalRegion()) {
        getCachePerfStats().incReliableRegions(-1);
      }
    }

    // Note we need to do this even if we don't have a listener
    // because of the SystemMemberCacheEventProcessor. Once we have
    // a way to check for existence of SystemMemberCacheEventProcessor listeners
    // then the add only needs to be done if hasListener || hasAdminListener
    if (eventSet != null) {
      eventSet.add(regionEvent);
    }

    try {
      // call recursiveDestroyRegion on each subregion and remove it
      // from this subregion map
      Collection values = this.subregions.values();
      for (Iterator itr = values.iterator(); itr.hasNext();) {
        // element is a LocalRegion
        Object element = itr.next();
        LocalRegion region;
        try {
          LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
          try {
            // converts to a LocalRegion
            region = toRegion(element);
          } finally {
            LocalRegion.setThreadInitLevelRequirement(LocalRegion.AFTER_INITIAL_IMAGE);
          }
        } catch (CancelException ignore) {
          // ignore, keep going through the motions though
          region = (LocalRegion) element;
        } catch (RegionDestroyedException ignore) {
          // SharedRegionData was destroyed
          continue;
        }

        // if the region is destroyed, then it is a race condition with
        // failed initialization removing it from the parent subregion map
        if (region.isDestroyed) {
          continue;
        }
        // BEGIN operating on subregion of this region (rgn)
        if (eventSet != null) {
          regionEvent = (RegionEventImpl) regionEvent.clone();
          regionEvent.region = region;
        }

        try {
          region.recursiveDestroyRegion(eventSet, regionEvent, cacheWrite);
          if (!region.isInternalRegion()) {
            InternalDistributedSystem system = region.cache.getInternalDistributedSystem();
            system.handleResourceEvent(ResourceEvent.REGION_REMOVE, region);
          }
        } catch (CancelException e) {
          // I don't think this should ever happen: bulletproofing for bug 39454
          if (!this.cache.forcedDisconnect()) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.LocalRegion_RECURSIVEDESTROYREGION_RECURSION_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0,
                region.getFullPath()), e);
          }
        }
        itr.remove(); // remove from this subregion map;
        // END operating on subregion of this region
      } // for

      try {
        if (this.indexManager != null) {
          try {
            if (this instanceof BucketRegion) {
              this.indexManager.removeBucketIndexes(getPartitionedRegion());
            }
            this.indexManager.destroy();
          } catch (QueryException e) {
            throw new IndexMaintenanceException(e);
          }
        }
      } catch (CancelException e) {
        // I don't think this should ever happens: bulletproofing for bug 39454
        if (!this.cache.forcedDisconnect()) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.LocalRegion_BASICDESTROYREGION_INDEX_REMOVAL_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0,
              getFullPath()), e);
        }
      }
    } finally {
      // mark this region as destroyed.
      if (regionEvent.isReinitializing()) {
        this.reinitialized_old = true;
      }
      this.cache.setRegionByPath(getFullPath(), null);

      getEventTracker().stop();

      if (this.diskRegion != null) {
        this.diskRegion.prepareForClose(this);
      }

      this.isDestroyed = true;
      // after isDestroyed is set to true call removeResourceListener to fix bug 49555
      this.cache.getInternalResourceManager(false).removeResourceListener(this);
      closeEntries();
      if (logger.isDebugEnabled()) {
        logger.debug("recursiveDestroyRegion: Region Destroyed: {}", getFullPath());
      }

      // if eventSet is null then we need to close the listener as well
      // otherwise, the listener will be closed after the destroy event
      try {
        postDestroyRegion(!isClose, regionEvent);
      } catch (CancelException e) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.LocalRegion_RECURSIVEDESTROYREGION_POSTDESTROYREGION_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0,
            getFullPath()), e);
      }

      // Destroy cqs created aganist this Region in a server cache.
      // fix for bug #47061
      if (getServerProxy() == null) {
        closeCqs();
      }

      detachPool();

      if (eventSet != null) {
        closeCallbacksExceptListener();
      } else {
        closeAllCallbacks();
      }
      if (this.getConcurrencyChecksEnabled() && this.getDataPolicy().withReplication()
          && !this.cache.isClosed()) {
        this.cache.getTombstoneService().unscheduleTombstones(this);
      }
      if (this.hasOwnStats) {
        this.cachePerfStats.close();
      }
    }
  }

  public void closeEntries() {
    this.entries.close(null);
  }

  public Set<VersionSource> clearEntries(RegionVersionVector rvv) {
    return this.entries.clear(rvv, null);
  }

  @Override
  public void checkReadiness() {
    checkRegionDestroyed(true);
  }

  /**
   * This method should be called when the caller cannot locate an entry and that condition is
   * unexpected. This will first double check the cache and region state before throwing an
   * EntryNotFoundException. EntryNotFoundException should be a last resort exception.
   *
   * @param entryKey the missing entry's key.
   */
  @Override
  public void checkEntryNotFound(Object entryKey) {
    checkReadiness();
    // Localized string for partitioned region is generic enough for general use
    throw new EntryNotFoundException(
        LocalizedStrings.PartitionedRegion_ENTRY_NOT_FOUND_FOR_KEY_0.toLocalizedString(entryKey));
  }

  /**
   *
   * Search for the value in a server (if one exists), then try a loader.
   *
   * If we find a value, we put it in the cache.
   *
   * @param preferCD return the CacheDeserializable, if that's what the value is.
   * @param requestingClient the client making the request, if any
   * @param clientEvent the client's event, if any. If not null, we set the version tag
   * @return the deserialized value
   */
  protected Object findObjectInSystem(KeyInfo keyInfo, boolean isCreate, TXStateInterface tx,
      boolean generateCallbacks, Object localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws CacheLoaderException, TimeoutException {

    final Object key = keyInfo.getKey();
    final Object aCallbackArgument = keyInfo.getCallbackArg();
    Object value = null;
    boolean fromServer = false;
    VersionTagHolder holder = null;

    /*
     * First lets try the server
     */
    ServerRegionProxy mySRP = getServerProxy();
    if (mySRP != null) {
      holder = new VersionTagHolder();
      value = mySRP.get(key, aCallbackArgument, holder);
      fromServer = value != null;
    }

    /*
     * If we didn't get anything from the server, try the loader
     */
    if (!fromServer || value == Token.TOMBSTONE) {
      // copy into local var to prevent race condition
      CacheLoader loader = basicGetLoader();
      if (loader != null) {
        fromServer = false;
        CachePerfStats stats = getCachePerfStats();
        long statStart = stats.startLoad();
        try {
          value = callCacheLoader(loader, key, aCallbackArgument, preferCD);
        } finally {
          stats.endLoad(statStart);
        }
      }
    }

    // don't allow tombstones into a client cache if it doesn't
    // have concurrency checks enabled
    if (fromServer && value == Token.TOMBSTONE && !this.getConcurrencyChecksEnabled()) {
      value = null;
    }

    /*
     * If we got a value back, let's put it in the cache.
     */
    RegionEntry re = null;
    if (value != null && !isMemoryThresholdReachedForLoad()) {

      long startPut = CachePerfStats.getStatTime();
      validateKey(key);
      Operation op;
      if (isCreate) {
        op = Operation.LOCAL_LOAD_CREATE;
      } else {
        op = Operation.LOCAL_LOAD_UPDATE;
      }

      @Released
      EntryEventImpl event = EntryEventImpl.create(this, op, key, value, aCallbackArgument, false,
          getMyId(), generateCallbacks);
      try {

        // bug #47716 - do not put an invalid entry into the cache if there's
        // already one there with the same version
        if (fromServer) {
          if (alreadyInvalid(key, event)) {
            return null;
          }
          event.setFromServer(fromServer);
          event.setVersionTag(holder.getVersionTag());
          if (clientEvent != null) {
            clientEvent.setVersionTag(holder.getVersionTag());
          }
        }

        // set the event id so that we can progagate
        // the value to the server
        if (!fromServer) {
          event.setNewEventId(this.cache.getDistributedSystem());
        }
        try {
          try {
            re = basicPutEntry(event, 0L);
            if (!fromServer && clientEvent != null) {
              clientEvent.setVersionTag(event.getVersionTag());
              clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
            }
            if (fromServer && event.getRawNewValue() == Token.TOMBSTONE) {
              return null; // tombstones are destroyed entries
            }
          } catch (ConcurrentCacheModificationException ignore) {
            // this means the value attempted to overwrite a newer modification and was rejected
            if (logger.isDebugEnabled()) {
              logger.debug("caught concurrent modification attempt when applying {}", event);
            }
            notifyBridgeClients(event);
          }
          if (!getDataView().isDeferredStats()) {
            getCachePerfStats().endPut(startPut, event.isOriginRemote());
          }
        } catch (CacheWriterException cwe) {
          if (logger.isDebugEnabled()) {
            logger.debug("findObjectInSystem: writer exception putting entry {}", event, cwe);
          }
        }
      } finally {
        event.release();
      }
    }
    if (isCreate) {
      recordMiss(re, key);
    }
    return value;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected Object callCacheLoader(CacheLoader loader, final Object key,
      final Object aCallbackArgument, boolean preferCD) {
    LoaderHelper loaderHelper = this.loaderHelperFactory.createLoaderHelper(key, aCallbackArgument,
        false /* netSearchAllowed */, true /* netloadAllowed */, null /* searcher */);
    Object result = loader.load(loaderHelper);
    result = this.getCache().convertPdxInstanceIfNeeded(result, preferCD);
    return result;
  }

  protected boolean isMemoryThresholdReachedForLoad() {
    return isMemoryThresholdReached();
  }

  /**
   * Returns true if the cache already has this key as an invalid entry with a version >= the one in
   * the given event. This is used in cache-miss processing to avoid overwriting the entry when it
   * is not necessary, so that we avoid invoking cache listeners.
   *
   * @return whether the entry is already invalid
   */
  private boolean alreadyInvalid(Object key, EntryEventImpl event) {
    @Unretained(ENTRY_EVENT_NEW_VALUE)
    Object newValue = event.getRawNewValue();
    if (newValue == null || Token.isInvalid(newValue)) {
      RegionEntry entry = this.entries.getEntry(key);
      if (entry != null) {
        synchronized (entry) {
          if (entry.isInvalid()) {
            VersionStamp stamp = entry.getVersionStamp();
            if (stamp == null || event.getVersionTag() == null) {
              return true;
            }
            if (stamp.getEntryVersion() >= event.getVersionTag().getEntryVersion()) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * @return true if cacheWrite was performed
   * @see DistributedRegion#cacheWriteBeforeDestroy(EntryEventImpl, Object)
   */
  @Override
  public boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    boolean result = false;
    // copy into local var to prevent race condition
    CacheWriter writer = basicGetWriter();
    if (writer != null && event.getOperation() != Operation.REMOVE
        && !event.inhibitAllNotifications()) {
      final long start = getCachePerfStats().startCacheWriterCall();
      event.setReadOldValueFromDisk(true);
      try {
        writer.beforeDestroy(event);
      } finally {
        event.setReadOldValueFromDisk(false);
        getCachePerfStats().endCacheWriterCall(start);
      }
      result = true;
    }
    serverDestroy(event, expectedOldValue);
    return result;
  }

  /** @return true if this was a client region; false if not */
  @Override
  public boolean bridgeWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    if (hasServerProxy()) {
      serverDestroy(event, expectedOldValue);
      return true;
    } else {
      return false;
    }
  }

  /**
   * @since GemFire 5.7
   */
  void serverRegionDestroy(RegionEventImpl regionEvent) {
    if (regionEvent.getOperation().isDistributed()) {
      ServerRegionProxy mySRP = getServerProxy();
      if (mySRP != null) {
        EventID eventId = regionEvent.getEventId();
        Object callbackArg = regionEvent.getRawCallbackArgument();
        mySRP.destroyRegion(eventId, callbackArg);
      }
    }
  }

  /**
   * @since GemFire 5.7
   */
  private void serverRegionClear(RegionEventImpl regionEvent) {
    if (regionEvent.getOperation().isDistributed()) {
      ServerRegionProxy mySRP = getServerProxy();
      if (mySRP != null) {
        EventID eventId = regionEvent.getEventId();
        Object callbackArg = regionEvent.getRawCallbackArgument();
        mySRP.clear(eventId, callbackArg);
      }
    }
  }

  /**
   * @since GemFire 5.7
   */
  void serverInvalidate(EntryEventImpl event) {
    if (event.getOperation().isDistributed() && !event.isOriginRemote()) {
      ServerRegionProxy mySRP = getServerProxy();
      if (mySRP != null) {
        mySRP.invalidate(event);
      }
    }
  }

  /**
   * @since GemFire 5.7
   */
  void serverPut(EntryEventImpl event, boolean requireOldValue, Object expectedOldValue) {
    if (event.getOperation().isDistributed() && !event.isFromServer()) {
      ServerRegionProxy mySRP = getServerProxy();
      if (mySRP != null) {
        if (event.isBulkOpInProgress()) {
          // this is a put all, ignore this!
          return;
        }
        Operation op = event.getOperation();
        // TODO: is the newEntry flag needed?
        Object key = event.getKey();
        Object value = event.getRawNewValue();

        // serverPut is called by cacheWriteBeforePut so the new value will not yet be off-heap
        Object callbackArg = event.getRawCallbackArgument();
        boolean isCreate = event.isCreate();
        Object result = mySRP.put(key, value, event.getDeltaBytes(), event, op, requireOldValue,
            expectedOldValue, callbackArg, isCreate);

        // bug #42296, serverProxy returns null when cache is closing
        getCancelCriterion().checkCancelInProgress(null);
        // if concurrent map operations failed we don't want the region map
        // to apply the operation and need to throw an exception
        if (op.guaranteesOldValue()) {
          if (op != Operation.REPLACE || requireOldValue) {
            event.setConcurrentMapOldValue(result);
          }
          if (op == Operation.PUT_IF_ABSENT) {
            if (result != null) {
              // customers don't see this exception
              throw new EntryNotFoundException("entry existed for putIfAbsent");
            }
          } else if (op == Operation.REPLACE) {
            if (requireOldValue && result == null) {
              throw new EntryNotFoundException("entry not found for replace");
            } else if (!requireOldValue) {
              if (!(Boolean) result) {
                // customers don't see this exception
                throw new EntryNotFoundException("entry found with wrong value");
              }
            }
          }
        }
      }
    }
  }

  /**
   * Destroy an entry on the server given its event.
   *
   * @since GemFire 5.7
   */
  void serverDestroy(EntryEventImpl event, Object expectedOldValue) {
    if (event.getOperation().isDistributed()) {
      ServerRegionProxy mySRP = getServerProxy();
      if (mySRP != null) {
        // send to server
        Object key = event.getKey();
        Object callbackArg = event.getRawCallbackArgument();
        Object result =
            mySRP.destroy(key, expectedOldValue, event.getOperation(), event, callbackArg);
        if (result instanceof EntryNotFoundException) {
          throw (EntryNotFoundException) result;
        }
      }
    }
  }

  /**
   * @return true if cacheWrite was performed
   * @see DistributedRegion#cacheWriteBeforeRegionDestroy(RegionEventImpl)
   */
  boolean cacheWriteBeforeRegionDestroy(RegionEventImpl event)
      throws CacheWriterException, TimeoutException {
    boolean result = false;
    // copy into local var to prevent race condition
    CacheWriter writer = basicGetWriter();
    if (writer != null) {
      final long start = getCachePerfStats().startCacheWriterCall();
      try {
        writer.beforeRegionDestroy(event);
      } finally {
        getCachePerfStats().endCacheWriterCall(start);
      }
      result = true;
    }
    serverRegionDestroy(event);
    return result;
  }

  private void cacheWriteBeforeRegionClear(RegionEventImpl event)
      throws CacheWriterException, TimeoutException {
    // copy into local var to prevent race condition
    CacheWriter writer = basicGetWriter();
    if (writer != null) {
      final long start = getCachePerfStats().startCacheWriterCall();
      try {
        writer.beforeRegionClear(event);
      } finally {
        getCachePerfStats().endCacheWriterCall(start);
      }
    }
    serverRegionClear(event);
  }

  @Override
  public void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients,
      CacheWriter localWriter, boolean requireOldValue, Object expectedOldValue)
      throws CacheWriterException, TimeoutException {
    Assert.assertTrue(netWriteRecipients == null);
    Operation operation = event.getOperation();
    boolean isPutIfAbsentOrReplace =
        operation == Operation.PUT_IF_ABSENT || operation == Operation.REPLACE;
    if (!isPutIfAbsentOrReplace && localWriter != null && !event.inhibitAllNotifications()) {
      final long start = getCachePerfStats().startCacheWriterCall();
      final boolean newEntry = event.getOperation().isCreate();
      event.setReadOldValueFromDisk(true);
      try {
        if (!newEntry) {
          localWriter.beforeUpdate(event);
        } else {
          localWriter.beforeCreate(event);
        }
      } finally {
        event.setReadOldValueFromDisk(false);
        getCachePerfStats().endCacheWriterCall(start);
      }
    }
    serverPut(event, requireOldValue, expectedOldValue);
  }

  protected void validateArguments(Object key, Object value, Object aCallbackArgument) {
    validateKey(key);
    validateValue(value);
  }

  void validateKey(Object key) {
    if (key == null) {
      throw new NullPointerException(
          LocalizedStrings.LocalRegion_KEY_CANNOT_BE_NULL.toLocalizedString());
    }

    // check validity of key against keyConstraint
    if (this.keyConstraint != null) {
      if (!this.keyConstraint.isInstance(key))
        throw new ClassCastException(
            LocalizedStrings.LocalRegion_KEY_0_DOES_NOT_SATISFY_KEYCONSTRAINT_1
                .toLocalizedString(key.getClass().getName(), this.keyConstraint.getName()));
    }

    // We don't need to check that the key is Serializable. Instead,
    // we let the lower-level (data) serialization mechanism take care
    // of this for us. See bug 32394.
  }

  /**
   * @since GemFire 5.0.2
   */
  private final boolean doExpensiveValidations =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DO_EXPENSIVE_VALIDATIONS");

  /**
   * the number of tombstone entries in the RegionMap
   */
  private final AtomicInteger tombstoneCount = new AtomicInteger();

  /** a boolean for issuing a client/server configuration mismatch message */
  private boolean concurrencyMessageIssued;

  /**
   * Starting in 3.5, we don't check to see if the value is {@code Serializable}. We instead rely on
   * the actual serialization (which happens in-thread with the put) to tell us if there are any
   * problems.
   */
  private void validateValue(Object value) {
    // check validity of value against valueConstraint
    if (this.valueConstraint != null) {
      if (value != null) {
        if (value instanceof CachedDeserializable) {
          if (this.doExpensiveValidations) {
            value = ((CachedDeserializable) value).getDeserializedValue(null, null);
          } else {
            return;
          }
        }
        if (!this.valueConstraint.isInstance(value)) {
          String valueClassName = value.getClass().getName();
          // check for a REST object, which has a @type field denoting its class
          if (value instanceof PdxInstance) {
            PdxInstance pdx = (PdxInstance) value;
            if (pdx.getClassName().equals(JSONFormatter.JSON_CLASSNAME)) {
              Object type = pdx.getField("@type");
              if (type != null && type instanceof String) {
                valueClassName = (String) type;
              } else {
                return;
              }
            }
            if (valueClassName.equals(this.valueConstraint.getName())) {
              return;
            }
          }
          throw new ClassCastException(
              LocalizedStrings.LocalRegion_VALUE_0_DOES_NOT_SATISFY_VALUECONSTRAINT_1
                  .toLocalizedString(valueClassName, this.valueConstraint.getName()));
        }
      }
    }
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    // return this.cache.getCachePerfStats();
    return this.cachePerfStats;
  }

  public CachePerfStats getRegionPerfStats() {
    return this.cachePerfStats;
  }

  /** regions track the number of tombstones their map holds for size calculations */
  public void incTombstoneCount(int delta) {
    this.tombstoneCount.addAndGet(delta);
    this.cachePerfStats.incTombstoneCount(delta);

    // Fix for 45204 - don't include the tombstones in
    // any of our entry count stats.
    this.cachePerfStats.incEntryCount(-delta);
  }

  public int getTombstoneCount() {
    return this.tombstoneCount.get();
  }

  @Override
  public void scheduleTombstone(RegionEntry entry, VersionTag destroyedVersion) {
    scheduleTombstone(entry, destroyedVersion, false);
  }

  private void scheduleTombstone(RegionEntry entry, VersionTag destroyedVersion,
      boolean reschedule) {
    if (destroyedVersion == null) {
      throw new NullPointerException("destroyed version tag cannot be null");
    }

    if (!reschedule) {
      incTombstoneCount(1);
    }

    if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT_VERBOSE)) {
      logger.trace(LogMarker.TOMBSTONE_COUNT_VERBOSE,
          "{} tombstone for {} version={} count is {} entryMap size is {}",
          reschedule ? "rescheduling" : "scheduling", entry.getKey(),
          entry.getVersionStamp().asVersionTag(), this.tombstoneCount.get(),
          this.entries.size()/* , new Exception("stack trace") */);
    }
    getGemFireCache().getTombstoneService().scheduleTombstone(this, entry, destroyedVersion);
  }

  @Override
  public void rescheduleTombstone(RegionEntry entry, VersionTag version) {
    scheduleTombstone(entry, version, true);
  }

  @Override
  public void unscheduleTombstone(RegionEntry entry) {
    unscheduleTombstone(entry, true);
  }

  private void unscheduleTombstone(RegionEntry entry, boolean validate) {
    incTombstoneCount(-1);
    if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
      logger.trace(LogMarker.TOMBSTONE_VERBOSE,
          "unscheduling tombstone for {} count is {} entryMap size is {}", entry.getKey(),
          this.tombstoneCount.get(), this.entries.size()/* , new Exception("stack trace") */);
    }
    if (logger.isTraceEnabled(LogMarker.TOMBSTONE_COUNT_VERBOSE) && validate) {
      if (this.entries instanceof AbstractRegionMap) {
        ((AbstractRegionMap) this.entries).verifyTombstoneCount(this.tombstoneCount);
      }
    }
    // we don't have to remove the entry from the sweeper since the version has
    // changed. It would be costly to iterate over the tombstone list for
    // every tombstone exhumed while holding the entry's lock
    // this.cache.getTombstoneService().unscheduleTombstone(entry);
  }

  /**
   * remove any tombstones from the given member that are <= the given version
   *
   * @param eventID event identifier for the GC operation
   * @param clientRouting routing info (if null a routing is computed)
   */
  public void expireTombstones(Map<VersionSource, Long> regionGCVersions, EventID eventID,
      FilterInfo clientRouting) {
    if (!this.getConcurrencyChecksEnabled()) {
      return;
    }
    Set<Object> keys = null;
    if (!this.versionVector.containsTombstoneGCVersions(regionGCVersions)) {
      keys = this.cache.getTombstoneService().gcTombstones(this, regionGCVersions,
          needsTombstoneGCKeysForClients(eventID, clientRouting));
      if (keys == null) {
        // deltaGII prevented tombstone GC
        return;
      }
    }
    if (eventID != null) { // bug #50683 - old members might not send an eventID
      notifyClientsOfTombstoneGC(regionGCVersions, keys, eventID, clientRouting);
    }
  }

  public void expireTombstoneKeys(Set<Object> tombstoneKeys) {
    if (this.getConcurrencyChecksEnabled()) {
      this.cache.getTombstoneService().gcTombstoneKeys(this, tombstoneKeys);
    }
  }

  protected boolean needsTombstoneGCKeysForClients(EventID eventID, FilterInfo clientRouting) {
    return false;
  }

  /**
   * pass tombstone garbage-collection info to clients
   *
   * @param eventID the ID of the event (see bug #50683)
   * @param routing routing info (routing is computed if this is null)
   */
  protected void notifyClientsOfTombstoneGC(Map<VersionSource, Long> regionGCVersions,
      Set<Object> keysRemoved, EventID eventID, FilterInfo routing) {
    if (CacheClientNotifier.getInstance() != null) {
      // Only route the event to clients interested in the partitioned region.
      // We do this by constructing a region-level event and then use it to
      // have the filter profile ferret out all of the clients that have interest
      // in this region
      FilterProfile fp = getFilterProfile();
      if (fp != null || routing != null) { // null check - fix for bug #45614
        RegionEventImpl regionEvent =
            new RegionEventImpl(this, Operation.REGION_DESTROY, null, true, getMyId());
        regionEvent.setEventID(eventID);
        FilterInfo clientRouting = routing;
        if (clientRouting == null) {
          clientRouting = fp.getLocalFilterRouting(regionEvent);
        }
        regionEvent.setLocalFilterInfo(clientRouting);
        ClientUpdateMessage clientMessage =
            ClientTombstoneMessage.gc(this, regionGCVersions, eventID);
        CacheClientNotifier.notifyClients(regionEvent, clientMessage);
      }
    }
  }

  /** local regions do not perform versioning */
  protected boolean shouldGenerateVersionTag(RegionEntry entry, EntryEventImpl event) {
    if (this.getDataPolicy().withPersistence()) {
      return true;
    } else {
      return this.getConcurrencyChecksEnabled()
          && (entry.getVersionStamp().hasValidVersion() || this.getDataPolicy().withReplication());
    }
  }

  protected void enableConcurrencyChecks() {
    this.setConcurrencyChecksEnabled(true);
    if (this.getDataPolicy().withStorage()) {
      RegionEntryFactory versionedEntryFactory = this.entries.getEntryFactory().makeVersioned();
      Assert.assertTrue(this.entries.isEmpty(),
          "RegionMap should be empty but was of size:" + this.entries.size());
      this.entries.setEntryFactory(versionedEntryFactory);
    }
  }

  protected boolean getEnableConcurrencyChecks() {
    return this.getConcurrencyChecksEnabled();
  }

  /**
   * validate attributes of subregion being created, sent to parent
   *
   * @throws IllegalArgumentException if attrs is null
   * @throws IllegalStateException if attributes are invalid
   */
  private void validateSubregionAttributes(RegionAttributes attrs) {
    if (attrs == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.LocalRegion_REGION_ATTRIBUTES_MUST_NOT_BE_NULL.toLocalizedString());
    }
    if (this.scope == Scope.LOCAL && attrs.getScope() != Scope.LOCAL) {
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_A_REGION_WITH_SCOPELOCAL_CAN_ONLY_HAVE_SUBREGIONS_WITH_SCOPELOCAL
              .toLocalizedString());
    }
  }

  /**
   * Returns the value of the entry with the given key as it is stored in the VM. This means that if
   * the value is invalid, the invalid token will be returned. If the value is a
   * {@link CachedDeserializable}received from another VM, that object will be returned. If the
   * value does not reside in the VM because it has been overflowed to disk, {@code null} will be
   * returned. This method is intended for testing.testing purposes only.
   *
   * @throws EntryNotFoundException No entry with {@code key} exists
   *
   * @see RegionMap#getEntry
   *
   * @since GemFire 3.2
   */
  @Override
  public Object getValueInVM(Object key) throws EntryNotFoundException {
    return basicGetValueInVM(key, true);
  }

  public Object getValueInVM(EntryEventImpl event) throws EntryNotFoundException {
    return basicGetValueInVM(event.getKey(), true);
  }

  /**
   * @param rememberRead true if read should be remembered in a transaction
   * @since GemFire 5.5
   */
  private Object basicGetValueInVM(Object key, boolean rememberRead) throws EntryNotFoundException {
    return getDataView().getValueInVM(getKeyInfo(key), this, rememberRead);
  }

  @Retained
  Object nonTXbasicGetValueInVM(KeyInfo keyInfo) {
    RegionEntry regionEntry = this.entries.getEntry(keyInfo.getKey());
    if (regionEntry == null) {
      checkEntryNotFound(keyInfo.getKey());
    }
    // OFFHEAP returned to callers
    Object value = regionEntry.getValueInVM(this);
    if (Token.isRemoved(value)) {
      checkEntryNotFound(keyInfo.getKey());
    }
    if (value == Token.NOT_AVAILABLE) {
      return null;
    }
    return value;
  }

  /**
   * This is a test hook method used to find out what keys the current tx has read or written.
   *
   * @return an unmodifiable set of keys that have been read or written by the transaction on this
   *         thread.
   * @throws IllegalStateException if not tx in progress
   * @since GemFire 5.5
   */
  public Set testHookTXKeys() {
    if (!isTX()) {
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_TX_NOT_IN_PROGRESS.toLocalizedString());
    }
    TXStateProxyImpl tx = (TXStateProxyImpl) getTXState();
    if (!tx.isRealDealLocal()) {
      return Collections.emptySet();
    }
    TXRegionState txr = txReadRegion();
    if (txr == null) {
      return Collections.emptySet();
    } else {
      return txr.getEntryKeys();
    }
  }

  /**
   * Returns the value of the entry with the given key as it is stored on disk. While the value may
   * be read from disk, it is <b>not</b> stored into the entry in the VM. This method is intended
   * for testing purposes only.
   *
   * @throws EntryNotFoundException No entry with {@code key} exists
   * @throws IllegalStateException If this region does not write to disk
   *
   * @see RegionEntry#getValueOnDisk
   *
   * @since GemFire 3.2
   */
  @Override
  public Object getValueOnDisk(Object key) throws EntryNotFoundException {
    // Ok for this to ignore tx state
    RegionEntry re = this.entries.getEntry(key);
    if (re == null) {
      throw new EntryNotFoundException(key.toString());
    }
    return re.getValueOnDisk(this);
  }

  /**
   * Gets the value from VM, if present, otherwise from disk without fault in.
   */
  @Override
  public Object getValueInVMOrDiskWithoutFaultIn(Object key) throws EntryNotFoundException {
    RegionEntry re = this.entries.getEntry(key);
    if (re == null) {
      throw new EntryNotFoundException(key.toString());
    }
    return re.getValueInVMOrDiskWithoutFaultIn(this);
  }

  /**
   * Returns the value of the entry with the given key as it is stored present in the buffer or
   * disk. While the value may be read from disk or buffer, it is <b>not</b> stored into the entry
   * in the VM. This is different from getValueonDisk in that it checks for a value both in asynch
   * buffers ( subject to asynch mode enabled) as well as Disk
   *
   * @throws EntryNotFoundException No entry with {@code key} exists
   * @throws IllegalStateException If this region does not write to disk
   *
   * @see RegionEntry#getValueOnDisk
   *
   * @since GemFire 5.1
   */
  public Object getValueOnDiskOrBuffer(Object key) throws EntryNotFoundException {
    // Ok for this to ignore tx state
    RegionEntry re = this.entries.getEntry(key);
    if (re == null) {
      throw new EntryNotFoundException(key.toString());
    }
    return re.getValueOnDiskOrBuffer(this);
  }

  /**
   * Does a get that attempts to not fault values in from disk or make the entry the most recent in
   * the LRU.
   *
   * @param adamant fault in and affect LRU as a last resort
   * @param allowTombstone also return Token.TOMBSTONE if the entry is deleted
   * @param serializedFormOkay if the serialized form can be returned
   */
  Object getNoLRU(Object key, boolean adamant, boolean allowTombstone, boolean serializedFormOkay) {
    Object value = null;
    try {
      value = getValueInVM(key); // OFFHEAP deserialize
      if (value == null) {
        // must be on disk
        // fault it in w/o putting it back in the region
        value = getValueOnDiskOrBuffer(key);
        if (value == null) {
          // try memory one more time in case it was already faulted back in
          value = getValueInVM(key); // OFFHEAP deserialize
          if (value == null) {
            if (adamant) {
              value = get(key);
            }
          } else {
            if (!serializedFormOkay && value instanceof CachedDeserializable) {
              value =
                  ((CachedDeserializable) value).getDeserializedValue(this, getRegionEntry(key));
            }
          }
        }
      } else {
        if (!serializedFormOkay && value instanceof CachedDeserializable) {
          value = ((CachedDeserializable) value).getDeserializedValue(this, getRegionEntry(key));
        }
      }
    } catch (EntryNotFoundException ignore) {
      // just return null;
    }
    if (value == Token.TOMBSTONE && !allowTombstone) {
      value = null;
    }
    return value;
  }

  /**
   * Bump this number any time an incompatible change is made to the snapshot format.
   */
  private static final byte SNAPSHOT_VERSION = 1;

  private static final byte SNAPSHOT_VALUE_OBJ = 23;

  private static final byte SNAPSHOT_VALUE_INVALID = 24;

  private static final byte SNAPSHOT_VALUE_LOCAL_INVALID = 25;

  @Override
  public void saveSnapshot(OutputStream outputStream) throws IOException {
    if (isProxy()) {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_REGIONS_WITH_DATAPOLICY_0_DO_NOT_SUPPORT_SAVESNAPSHOT
              .toLocalizedString(getDataPolicy()));
    }
    checkForNoAccess();
    DataOutputStream out = new DataOutputStream(outputStream);
    try {
      out.writeByte(SNAPSHOT_VERSION);
      for (Object entryObject : entrySet(false)) {
        Entry entry = (Entry) entryObject;
        try {
          Object key = entry.getKey();
          Object value = entry.getValue();
          if (value == Token.TOMBSTONE) {
            continue;
          }
          DataSerializer.writeObject(key, out);
          if (value == null) {
            // fix for bug 33311
            NonTXEntry lre = (NonTXEntry) entry;
            RegionEntry re = lre.getRegionEntry();
            // OFFHEAP: incrc, copy info heap cd for serialization, decrc
            value = re.getValue(this);
            if (value == Token.INVALID) {
              out.writeByte(SNAPSHOT_VALUE_INVALID);
            } else if (value == Token.LOCAL_INVALID) {
              out.writeByte(SNAPSHOT_VALUE_LOCAL_INVALID);
            } else {
              out.writeByte(SNAPSHOT_VALUE_OBJ);
              DataSerializer.writeObject(value, out);
            }
          } else {
            out.writeByte(SNAPSHOT_VALUE_OBJ);
            DataSerializer.writeObject(value, out);
          }
        } catch (EntryDestroyedException ignore) {
          // continue to next entry
        }
      }
      // write NULL terminator
      DataSerializer.writeObject(null, out);
    } finally {
      out.close();
    }
  }

  @Override
  public void loadSnapshot(InputStream inputStream)
      throws CacheWriterException, TimeoutException, ClassNotFoundException, IOException {
    if (isProxy()) {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_REGIONS_WITH_DATAPOLICY_0_DO_NOT_SUPPORT_LOADSNAPSHOT
              .toLocalizedString(getDataPolicy()));
    }
    if (inputStream == null) {
      throw new NullPointerException(
          LocalizedStrings.LocalRegion_INPUTSTREAM_MUST_NOT_BE_NULL.toLocalizedString());
    }
    checkReadiness();
    checkForLimitedOrNoAccess();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOAD_SNAPSHOT, null, false,
        getMyId(), generateEventID()/* generate EventID */);
    reinitialize(inputStream, event);
  }

  @Override
  public void registerInterest(Object key) {
    registerInterest(key, false);
  }

  @Override
  public void registerInterest(Object key, boolean isDurable) {
    registerInterest(key, isDurable, true);
  }

  @Override
  public void registerInterest(Object key, boolean isDurable, boolean receiveValues) {
    registerInterest(key, InterestResultPolicy.DEFAULT, isDurable, receiveValues);
  }

  public void startRegisterInterest() {
    getImageState().writeLockRI();
    try {
      this.cache.registerInterestStarted();
      this.riCnt++;
    } finally {
      getImageState().writeUnlockRI();
    }
  }

  public void finishRegisterInterest() {
    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "testing.slow-interest-recovery")) {
      if (logger.isDebugEnabled()) {
        logger.debug("slowing interest recovery...");
      }
      try {
        Thread.sleep(20000);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        return;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("done slowing interest recovery");
      }
    }

    boolean gotLock = false;
    try {
      getImageState().writeLockRI();
      gotLock = true;
      this.riCnt--;
      Assert.assertTrue(this.riCnt >= 0, "register interest count can not be < 0 ");
      if (this.riCnt == 0) {
        // remove any destroyed entries from the region and clear the hashset
        destroyEntriesAndClearDestroyedKeysSet();
      }
    } finally {
      this.cache.registerInterestCompleted();
      if (gotLock) {
        getImageState().writeUnlockRI();
      }
    }
  }

  // TODO: this is distressingly similar to code in the client.internal package
  private void processSingleInterest(Object key, int interestType,
      InterestResultPolicy interestResultPolicy, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    final ServerRegionProxy proxy = getServerProxy();
    if (proxy == null) {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_REGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
    if (isDurable && !proxy.getPool().isDurableClient()) {
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_DURABLE_FLAG_ONLY_APPLICABLE_FOR_DURABLE_CLIENTS
              .toLocalizedString());
    }
    if (!proxy.getPool().getSubscriptionEnabled()) {
      String msg = "Interest registration requires a pool whose queue is enabled.";
      throw new SubscriptionNotEnabledException(msg);
    }

    if (getAttributes().getDataPolicy().withReplication() // fix for bug 36185
        && !getAttributes().getScope().isLocal()) { // fix for bug 37692
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_REGISTRATION_NOT_SUPPORTED_ON_REPLICATED_REGIONS
              .toLocalizedString());
    }

    if (key == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.LocalRegion_INTEREST_KEY_MUST_NOT_BE_NULL.toLocalizedString());
    }

    // Sequence of events, on a single entry:
    // 1. Client puts value (a).
    // 2. Server updates with value (b). Client never gets the update,
    // because it isn't interested in that key.
    // 3. Client registers interest.
    // At this point, there is an entry in the local cache, but it is
    // inconsistent with the server.
    //
    // Because of this, we must _always_ destroy and refetch affected values
    // during registerInterest.

    startRegisterInterest();
    try {
      this.clearKeysOfInterest(key, interestType, interestResultPolicy);
      // Checking for the Dunit test(testRegisterInterst_Destroy_Concurrent) flag
      if (PoolImpl.BEFORE_REGISTER_CALLBACK_FLAG) {
        ClientServerObserver bo = ClientServerObserverHolder.getInstance();
        bo.beforeInterestRegistration();
      } // Test Code Ends

      final byte regionDataPolicy = getAttributes().getDataPolicy().ordinal;
      List serverKeys;

      switch (interestType) {
        case InterestType.FILTER_CLASS:
          serverKeys = proxy.registerInterest(key, interestType, interestResultPolicy, isDurable,
              receiveUpdatesAsInvalidates, regionDataPolicy);
          break;

        case InterestType.KEY:
          if (key instanceof String && key.equals("ALL_KEYS")) {
            logger.warn(
                "Usage of registerInterest('ALL_KEYS') has been deprecated.  Please use registerInterestForAllKeys()");
            serverKeys = proxy.registerInterest(".*", InterestType.REGULAR_EXPRESSION,
                interestResultPolicy, isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
          } else {
            if (key instanceof List) {
              logger.warn(
                  "Usage of registerInterest(List) has been deprecated. Please use registerInterestForKeys(Iterable)");
              serverKeys = proxy.registerInterestList((List) key, interestResultPolicy, isDurable,
                  receiveUpdatesAsInvalidates, regionDataPolicy);
            } else {
              serverKeys = proxy.registerInterest(key, InterestType.KEY, interestResultPolicy,
                  isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
            }
          }
          break;

        case InterestType.OQL_QUERY:
          serverKeys = proxy.registerInterest(key, InterestType.OQL_QUERY, interestResultPolicy,
              isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
          break;

        case InterestType.REGULAR_EXPRESSION: {
          String regex = (String) key;
          // compile regex throws java.util.regex.PatternSyntaxException if invalid
          // we do this before sending to the server because it's more efficient
          // and the client is not receiving exception messages properly
          Pattern.compile(regex); // TODO: result of Pattern.compile is ignored
          serverKeys = proxy.registerInterest(regex, InterestType.REGULAR_EXPRESSION,
              interestResultPolicy, isDurable, receiveUpdatesAsInvalidates, regionDataPolicy);
          break;

        }
        default:
          throw new InternalGemFireError(
              LocalizedStrings.LocalRegion_UNKNOWN_INTEREST_TYPE.toLocalizedString());
      }

      boolean finishedRefresh = false;
      try {
        refreshEntriesFromServerKeys(null, serverKeys, interestResultPolicy);
        finishedRefresh = true;
      } finally {
        if (!finishedRefresh) {
          // unregister before throwing the exception caused by the refresh
          switch (interestType) {
            case InterestType.FILTER_CLASS:
              proxy.unregisterInterest(key, interestType, false, false);
              break;

            case InterestType.KEY:
              if (key instanceof String && key.equals("ALL_KEYS")) {
                proxy.unregisterInterest(".*", InterestType.REGULAR_EXPRESSION, false, false);
              } else if (key instanceof List) {
                proxy.unregisterInterestList((List) key, false, false);
              } else {
                proxy.unregisterInterest(key, InterestType.KEY, false, false);
              }
              break;

            case InterestType.OQL_QUERY:
              proxy.unregisterInterest(key, InterestType.OQL_QUERY, false, false);
              break;

            case InterestType.REGULAR_EXPRESSION: {
              proxy.unregisterInterest(key, InterestType.REGULAR_EXPRESSION, false, false);
              break;

            }
            default:
              throw new InternalGemFireError(
                  LocalizedStrings.LocalRegion_UNKNOWN_INTEREST_TYPE.toLocalizedString());
          }
        }
      }
    } finally {
      finishRegisterInterest();
    }
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy) {
    registerInterest(key, policy, false);
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable) {
    registerInterest(key, policy, isDurable, true);
  }

  @Override
  public void registerInterest(Object key, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    processSingleInterest(key, InterestType.KEY, policy, isDurable, !receiveValues);
  }

  @Override
  public void registerInterestRegex(String regex) {
    registerInterestRegex(regex, false);
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable) {
    registerInterestRegex(regex, InterestResultPolicy.DEFAULT, isDurable, true);
  }

  @Override
  public void registerInterestRegex(String regex, boolean isDurable, boolean receiveValues) {
    registerInterestRegex(regex, InterestResultPolicy.DEFAULT, isDurable, receiveValues);
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy) {
    registerInterestRegex(regex, policy, false);
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable) {
    registerInterestRegex(regex, policy, isDurable, true);
  }

  @Override
  public void registerInterestRegex(String regex, InterestResultPolicy policy, boolean isDurable,
      boolean receiveValues) {
    processSingleInterest(regex, InterestType.REGULAR_EXPRESSION, policy, isDurable,
        !receiveValues);
  }

  private void registerInterestFilter(String className, boolean isDurable) {
    registerInterestFilter(className, isDurable, true);
  }

  private void registerInterestFilter(String className, boolean isDurable, boolean receiveValues) {
    processSingleInterest(className, InterestType.FILTER_CLASS, InterestResultPolicy.DEFAULT,
        isDurable, !receiveValues);
  }

  private void registerInterestOQL(String query, boolean isDurable) {
    registerInterestOQL(query, isDurable, true);
  }

  private void registerInterestOQL(String query, boolean isDurable, boolean receiveValues) {
    processSingleInterest(query, InterestType.OQL_QUERY, InterestResultPolicy.DEFAULT, isDurable,
        !receiveValues);
  }

  @Override
  public void unregisterInterest(Object key) {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      // Keep support for "ALL_KEYS" in 4.2.x
      if (key instanceof String && key.equals("ALL_KEYS")) {
        proxy.unregisterInterest(".*", InterestType.REGULAR_EXPRESSION, false, false);
      } else if (key instanceof List) {
        proxy.unregisterInterestList((List) key, false, false);
      } else {
        proxy.unregisterInterest(key, InterestType.KEY, false, false);
      }
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public void unregisterInterestRegex(String regex) {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      proxy.unregisterInterest(regex, InterestType.REGULAR_EXPRESSION, false, false);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  public void unregisterInterestFilter(String className) {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      proxy.unregisterInterest(className, InterestType.FILTER_CLASS, false, false);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  public void unregisterInterestOQL(String query) {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      proxy.unregisterInterest(query, InterestType.OQL_QUERY, false, false);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public List getInterestList() {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.getInterestList(InterestType.KEY);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_UNREGISTRATION_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  /**
   * finds the keys in this region using the given interestType and argument. Currently only
   * InterestType.REGULAR_EXPRESSION and InterestType.KEY are supported
   *
   * @param interestType an InterestType value
   * @param interestArg the associated argument (regex string, key or key list, etc)
   * @param allowTombstones whether to return destroyed entries
   * @return a set of the keys matching the given criterion
   */
  public Set getKeysWithInterest(int interestType, Object interestArg, boolean allowTombstones) {
    Set ret;
    if (interestType == InterestType.REGULAR_EXPRESSION) {
      if (interestArg == null || ".*".equals(interestArg)) {
        ret = new HashSet(keySet(allowTombstones));
      } else {
        ret = new HashSet();
        // Handle the regex pattern
        if (!(interestArg instanceof String)) {
          throw new IllegalArgumentException(
              LocalizedStrings.AbstractRegion_REGULAR_EXPRESSION_ARGUMENT_WAS_NOT_A_STRING
                  .toLocalizedString());
        }

        Pattern keyPattern = Pattern.compile((String) interestArg);
        for (Object entryKey : this.keySet(allowTombstones)) {
          if (!(entryKey instanceof String)) {
            // key is not a String, cannot apply regex to this entry
            continue;
          }
          if (!keyPattern.matcher((String) entryKey).matches()) {
            // key does not match the regex, this entry should not be returned.
            continue;
          }
          ret.add(entryKey);
        }
      }

    } else if (interestType == InterestType.KEY) {
      if (interestArg instanceof List) {
        ret = new HashSet(); // TODO optimize initial size
        List keyList = (List) interestArg;
        for (Iterator it = keyList.iterator(); it.hasNext();) {
          Object entryKey = it.next();
          if (this.containsKey(entryKey) || allowTombstones && this.containsTombstone(entryKey)) {
            ret.add(entryKey);
          }
        }
      } else {
        ret = new HashSet();
        if (this.containsKey(interestArg)
            || (allowTombstones && this.containsTombstone(interestArg))) {
          ret.add(interestArg);
        }
      }

    } else if (interestType == InterestType.FILTER_CLASS) {
      throw new UnsupportedOperationException(
          LocalizedStrings.AbstractRegion_INTERESTTYPEFILTER_CLASS_NOT_YET_SUPPORTED
              .toLocalizedString());

    } else if (interestType == InterestType.OQL_QUERY) {
      throw new UnsupportedOperationException(
          LocalizedStrings.AbstractRegion_INTERESTTYPEOQL_QUERY_NOT_YET_SUPPORTED
              .toLocalizedString());

    } else {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_UNSUPPORTED_INTEREST_TYPE_0
          .toLocalizedString(interestType));
    }
    return ret;
  }

  @Override
  public List<String> getInterestListRegex() {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.getInterestList(InterestType.REGULAR_EXPRESSION);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_INTEREST_LIST_RETRIEVAL_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public Set keySetOnServer() {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.keySet();
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_SERVER_KEYSET_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public boolean containsKeyOnServer(Object key) {
    checkReadiness();
    checkForNoAccess();
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.containsKey(key);
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_SERVER_KEYSET_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public int sizeOnServer() {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.size();
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_SERVER_SIZE_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  @Override
  public boolean isEmptyOnServer() {
    ServerRegionProxy proxy = getServerProxy();
    if (proxy != null) {
      return proxy.size() == 0;
    } else {
      throw new UnsupportedOperationException(
          LocalizedStrings.LocalRegion_SERVER_ISEMPTY_REQUIRES_A_POOL.toLocalizedString());
    }
  }

  /**
   * WARNING: this method is overridden in subclasses.
   */
  protected void localDestroyNoCallbacks(Object key) {
    if (logger.isDebugEnabled()) {
      logger.debug("localDestroyNoCallbacks key={}", key);
    }
    checkReadiness();
    validateKey(key);
    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.LOCAL_DESTROY, key, false,
        getMyId(), false /* generateCallbacks */, true);
    try {
      basicDestroy(event, false, null); // expectedOldValue
    } catch (CacheWriterException e) {
      // cache writer not called
      throw new Error(
          LocalizedStrings.LocalRegion_CACHE_WRITER_SHOULD_NOT_HAVE_BEEN_CALLED_FOR_LOCALDESTROY
              .toLocalizedString(),
          e);
    } catch (TimeoutException e) {
      // no distributed lock
      throw new Error(
          LocalizedStrings.LocalRegion_NO_DISTRIBUTED_LOCK_SHOULD_HAVE_BEEN_ATTEMPTED_FOR_LOCALDESTROY
              .toLocalizedString(),
          e);
    } catch (EntryNotFoundException ignore) {
      // not a problem
    } finally {
      event.release();
    }
  }

  /**
   * Do localDestroy on a list of keys, if they exist
   *
   * @param keys the list of arrays of keys to invalidate
   * @see #registerInterest(Object)
   */
  private void clearViaList(List keys) {
    for (Object entryObject : entrySet(false)) {
      Entry entry = (Entry) entryObject;
      try {
        Object entryKey = entry.getKey();
        boolean match = false;
        for (Object key : keys) {
          if (entryKey.equals(key)) {
            match = true;
            break;
          }
        } // for
        if (!match) {
          continue;
        }
        localDestroyNoCallbacks(entryKey);
      } catch (EntryDestroyedException ignore) {
        // ignore to fix bug 35534
      }
    }
  }

  /**
   * do a localDestroy on all matching keys
   *
   * @param key the regular expression to match on
   * @see #registerInterestRegex(String)
   */
  private void clearViaRegEx(String key) {
    // TODO: if (key.equals(".*)) then cmnClearRegionNoCallbacks
    Pattern keyPattern = Pattern.compile(key);
    for (Iterator it = this.entrySet(false).iterator(); it.hasNext();) {
      Region.Entry entry = (Region.Entry) it.next();
      try {
        Object entryKey = entry.getKey();
        if (!(entryKey instanceof String)) {
          continue;
        }
        if (!keyPattern.matcher((String) entryKey).matches()) {
          // key does not match the regex, this entry should not be returned.
          continue;
        }
        localDestroyNoCallbacks(entryKey);
      } catch (EntryDestroyedException ignore) {
        // ignore to fix bug 35534
      }
    }
  }

  /**
   * do a localDestroy on all matching keys
   *
   * @param key the regular expression to match on
   */
  private void clearViaFilterClass(String key) {
    InterestFilter filter;
    try {
      Class filterClass = ClassLoadUtil.classFromName(key);
      filter = (InterestFilter) filterClass.newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(
          LocalizedStrings.LocalRegion_CLASS_0_NOT_FOUND_IN_CLASSPATH.toLocalizedString(key), cnfe);
    } catch (Exception e) {
      throw new RuntimeException(
          LocalizedStrings.LocalRegion_CLASS_0_COULD_NOT_BE_INSTANTIATED.toLocalizedString(key), e);
    }

    for (Object entryObject : entrySet(false)) {
      Entry entry = (Entry) entryObject;
      try {
        Object entryKey = entry.getKey();
        if (!(entryKey instanceof String)) {
          continue;
        }
        InterestEvent e = new InterestEvent(entryKey, entry.getValue(), true);
        if (!filter.notifyOnRegister(e)) {
          // the filter does not want to know about this entry, so skip it.
          continue;
        }
        localDestroyNoCallbacks(entryKey);
      } catch (EntryDestroyedException ignore) {
        // ignore to fix bug 35534
      }
    }
  }

  /**
   * Do a localDestroy of all matching keys
   */
  private void clearViaQuery(String query) {
    throw new InternalGemFireError(
        LocalizedStrings.LocalRegion_NOT_YET_SUPPORTED.toLocalizedString());
  }

  /**
   * Refresh local entries based on server's list of keys
   */
  public void refreshEntriesFromServerKeys(Connection con, List serverKeys,
      InterestResultPolicy interestResultPolicy) {
    if (serverKeys == null) {
      return;
    }
    ServerRegionProxy proxy = getServerProxy();
    if (logger.isDebugEnabled()) {
      logKeys(serverKeys, interestResultPolicy);
    }

    if (interestResultPolicy == InterestResultPolicy.NONE) {
      return; // done
    }

    if (logger.isDebugEnabled()) {
      logger.debug("refreshEntries region={}", getFullPath());
    }
    for (Object serverKey : serverKeys) {
      ArrayList keysList = (ArrayList) serverKey;
      // The chunk can contain null data if there are no entries on the server
      // corresponding to the requested keys
      if (keysList == null) {
        continue;
      }
      // int numberOfResults = keysList.size();
      if (EntryLogger.isEnabled()) {
        if (con != null) {
          Endpoint endpoint = con.getEndpoint();
          if (endpoint != null) {
            EntryLogger.setSource(endpoint.getMemberId(), "RIGII");
          }
        }
      }
      try {
        List list = new ArrayList(keysList);
        if (interestResultPolicy != InterestResultPolicy.KEYS_VALUES) {
          for (Object currentKey : keysList) {
            // Don't apply riResponse if the entry was destroyed when
            // ri is in progress
            if (currentKey == null || getImageState().hasDestroyedEntry(currentKey)) {
              list.remove(currentKey);
            }
          }
        }
        if (interestResultPolicy == InterestResultPolicy.KEYS) {
          // Attempt to create an invalid in without overwriting
          if (!isProxy()) {
            for (Object currentKey : list) {
              this.entries.initialImagePut(currentKey, 0, Token.LOCAL_INVALID, false, false, null,
                  null, false);
            }
          }
          // Size statistics don't take key into account, so we don't
          // need to modify the region's size.
        } else if (!list.isEmpty()) {
          Assert.assertTrue(interestResultPolicy == InterestResultPolicy.KEYS_VALUES);

          VersionedObjectList values = (VersionedObjectList) list.get(0);

          if (logger.isDebugEnabled()) {
            logger.debug("processing interest response: {}", values.size());
          }
          VersionedObjectList.Iterator listIt = values.iterator();
          while (listIt.hasNext()) {
            VersionedObjectList.Entry entry = listIt.next();
            Object currentKey = entry.getKey();
            if (currentKey == null || getImageState().hasDestroyedEntry(currentKey)) {
              continue;
            }
            Object val = entry.getObject();
            boolean isBytes = entry.isBytes();
            boolean isKeyOnServer = !entry.isKeyNotOnServer();
            boolean isTombstone = this.getConcurrencyChecksEnabled() && entry.isKeyNotOnServer()
                && entry.getVersionTag() != null;
            final VersionTag tag = entry.getVersionTag();
            if (val instanceof Throwable) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.LocalRegion_CAUGHT_THE_FOLLOWING_EXCEPTION_FOR_KEY_0_WHILE_PERFORMING_A_REMOTE_GETALL,
                  currentKey), (Throwable) val);
              localDestroyNoCallbacks(currentKey);
              continue;
            } else {
              if (logger.isDebugEnabled()) {
                logger.debug("refreshEntries key={} value={} version={}", currentKey, entry, tag);
              }
              if (tag == null) { // no version checks
                localDestroyNoCallbacks(currentKey);
              }
            }

            if (val instanceof byte[] && !isBytes) {
              val = CachedDeserializableFactory.create((byte[]) val, getCache());
            }

            if (isTombstone) {
              assert val == null : "server returned a value for a destroyed entry";
              val = Token.TOMBSTONE;
            }

            if (val != null || isTombstone) {
              // Sneakily drop in the value into our local cache,
              // but don't overwrite
              if (!isProxy()) {
                this.entries.initialImagePut(currentKey, 0, val, false, false, tag, null, false);
              }
            } else {
              RegionEntry regionEntry = this.entries.getEntry(currentKey);
              if (!isProxy() && isKeyOnServer) {
                this.entries.initialImagePut(currentKey, 0, Token.LOCAL_INVALID, false, false, tag,
                    null, false);
              } else {
                if (regionEntry != null) {
                  synchronized (regionEntry) {
                    if (regionEntry.isDestroyedOrRemovedButNotTombstone()) {
                      this.entries.removeEntry(currentKey, regionEntry, false);
                    }
                  }
                }
              }
              // In this case, if we didn't overwrite, we don't have a local
              // value, so no size change needs to be recorded.
            }
          }
        }
      } catch (DiskAccessException dae) {
        this.handleDiskAccessException(dae);
        throw dae;
      } finally {
        EntryLogger.clearSource();
      }
    } // for
  }

  private void logKeys(List serverKeys, InterestResultPolicy pol) {
    int totalKeys = 0;
    StringBuffer buffer = new StringBuffer();
    for (final Object serverKey : serverKeys) {
      List keysList = (List) serverKey;
      // The chunk can contain null data if there are no entries on the server
      // corresponding to the requested keys
      // TODO: is this still possible?
      if (keysList == null) {
        continue;
      }
      int numThisChunk = keysList.size();
      totalKeys += numThisChunk;
      for (Object key : keysList) {
        if (key != null) {
          if (key instanceof VersionedObjectList) {
            Set keys = ((VersionedObjectList) key).keySet();
            for (Object k : keys) {
              buffer.append("  ").append(k).append(getLineSeparator());
            }
          } else {
            buffer.append("  ").append(key).append(getLineSeparator());
          }
        }
      }
    } // for
    if (logger.isDebugEnabled()) {
      logger.debug("{} refreshEntriesFromServerKeys count={} policy={}{}{}", this, totalKeys, pol,
          getLineSeparator(), buffer);
    }
  }

  /**
   * Remove values in local cache before registering interest
   *
   * TODO: interestResultPolicy is never used
   *
   * @param key the interest key
   * @param interestType the interest type from {@link InterestType}
   * @param interestResultPolicy the policy from {@link InterestResultPolicy}
   */
  public void clearKeysOfInterest(Object key, int interestType,
      InterestResultPolicy interestResultPolicy) {
    switch (interestType) {
      case InterestType.FILTER_CLASS:
        clearViaFilterClass((String) key);
        break;
      case InterestType.KEY:
        if (key instanceof String && key.equals("ALL_KEYS"))
          clearViaRegEx(".*");
        else if (key instanceof List)
          clearViaList((List) key);
        else
          localDestroyNoCallbacks(key);
        break;
      case InterestType.OQL_QUERY:
        clearViaQuery((String) key);
        break;
      case InterestType.REGULAR_EXPRESSION:
        clearViaRegEx((String) key);
        break;
      default:
        throw new InternalGemFireError(
            LocalizedStrings.LocalRegion_UNKNOWN_INTEREST_TYPE.toLocalizedString());
    }
  }

  /**
   * Destroys and recreates this region. If this is triggered by loadSnapshot inputStream will be
   * supplied. If this is triggered by LossAction of reinitialize then inputStream will be null, and
   * the region will go through regular GetInitialImage if it is a mirrored replicate.
   * <p>
   * Acquires and releases the DestroyLock.
   *
   * @since GemFire 5.0
   */
  void reinitialize(InputStream inputStream, RegionEventImpl event)
      throws TimeoutException, IOException, ClassNotFoundException {
    acquireDestroyLock();
    try {
      reinitialize_destroy(event);
      recreate(inputStream, null);
    } finally {
      releaseDestroyLock();
    }
  }

  /** must be holding destroy lock */
  void reinitializeFromImageTarget(InternalDistributedMember imageTarget)
      throws TimeoutException, IOException, ClassNotFoundException {
    Assert.assertTrue(imageTarget != null);
    recreate(null, imageTarget);
  }

  /**
   * Returns true if this region was reinitialized, e.g. a snapshot was loaded, and this is the
   * recreated region
   */
  boolean reinitialized_new() {
    return this.reinitialized_new;
  }

  /** must be holding destroy lock */
  void reinitialize_destroy(RegionEventImpl event) throws CacheWriterException, TimeoutException {
    final boolean cacheWrite = !event.originRemote;
    // register this region as reinitializing
    this.cache.regionReinitializing(getFullPath());
    basicDestroyRegion(event, cacheWrite, false/* lock */, true);
  }

  /** must be holding destroy lock */
  private void recreate(InputStream inputStream, InternalDistributedMember imageTarget)
      throws TimeoutException, IOException, ClassNotFoundException {
    String thePath = getFullPath();
    Region newRegion = null;
    // recreate new region with snapshot data

    try {
      LocalRegion parent = this.parentRegion;
      // If specified diskDir in DEFAULT diskstore, we should not use null
      // as diskstore name any more
      if (this.diskStoreImpl != null
          && this.diskStoreImpl.getName().equals(DiskStoreFactory.DEFAULT_DISK_STORE_NAME)
          && this.diskStoreName == null && !useDefaultDiskStore()) {
        this.diskStoreName = this.diskStoreImpl.getName();
      }
      RegionAttributes attrs = this;
      boolean getDestroyLock = false;
      InternalRegionArguments internalRegionArguments = new InternalRegionArguments()
          .setDestroyLockFlag(getDestroyLock).setSnapshotInputStream(inputStream)
          .setImageTarget(imageTarget).setRecreateFlag(true);
      if (this instanceof BucketRegion) {
        BucketRegion me = (BucketRegion) this;
        internalRegionArguments.setPartitionedRegionBucketRedundancy(me.getRedundancyLevel());
      }

      if (parent == null) {
        newRegion = this.cache.createVMRegion(this.regionName, attrs, internalRegionArguments);
      } else {
        newRegion = parent.createSubregion(this.regionName, attrs, internalRegionArguments);
      }

      // note that createVMRegion and createSubregion now call regionReinitialized
    } catch (RegionExistsException e) {
      // shouldn't happen since we're holding the destroy lock
      throw new InternalGemFireError(
          LocalizedStrings.LocalRegion_GOT_REGIONEXISTSEXCEPTION_IN_REINITIALIZE_WHEN_HOLDING_DESTROY_LOCK
              .toLocalizedString(),
          e);
    } finally {
      if (newRegion == null) {
        // failed to create region
        this.cache.unregisterReinitializingRegion(thePath);
      }
    }
  }

  void loadSnapshotDuringInitialization(InputStream inputStream)
      throws IOException, ClassNotFoundException {
    DataInputStream in = new DataInputStream(inputStream);
    try {
      RegionMap map = getRegionMap();
      byte snapshotVersion = in.readByte();
      if (snapshotVersion != SNAPSHOT_VERSION) {
        throw new IllegalArgumentException(
            LocalizedStrings.LocalRegion_UNSUPPORTED_SNAPSHOT_VERSION_0_ONLY_VERSION_1_IS_SUPPORTED
                .toLocalizedString(new Object[] {snapshotVersion, SNAPSHOT_VERSION}));
      }
      for (;;) {
        Object key = DataSerializer.readObject(in);
        if (key == null) {
          break;
        }
        byte aByte = in.readByte();
        Object value;

        if (aByte == SNAPSHOT_VALUE_OBJ) {
          value = DataSerializer.readObject(in);
        } else if (aByte == SNAPSHOT_VALUE_INVALID || aByte == SNAPSHOT_VALUE_LOCAL_INVALID) {
          // Even though it was a distributed invalidate when the snapshot was created I think it is
          // correct to turn it into a local invalidate when we load the snapshot since we don't do
          // a distributed invalidate operation when loading.
          value = Token.LOCAL_INVALID;
        } else {
          throw new IllegalArgumentException(
              LocalizedStrings.LocalRegion_UNEXPECTED_SNAPSHOT_CODE_0_THIS_SNAPSHOT_WAS_PROBABLY_WRITTEN_BY_AN_EARLIER_INCOMPATIBLE_RELEASE
                  .toLocalizedString(aByte));
        }

        // If versioning is enabled, we will give the entry a "fake" version.
        VersionTag tag = null;
        if (this.getConcurrencyChecksEnabled()) {
          tag = VersionTag.create(getVersionMember());
        }
        map.initialImagePut(key, cacheTimeMillis(), value, false, false, tag, null, false);
      }
    } finally {
      in.close();
    }
    this.reinitialized_new = true;
  }

  /**
   * Blocks until initialization is complete.
   *
   * @param destroyedRegionOk true if it is okay to return a region that isDestroyed
   * @see DestroyRegionOperation
   */
  @Override
  public Region getSubregion(String path, boolean destroyedRegionOk) {
    if (destroyedRegionOk) {
      checkCacheClosed();
    } else if (isDestroyed()) {
      // Assume if the owner of the subregion is destroyed, so are all of its
      // subregions
      return null;
    }

    if (path == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.LocalRegion_PATH_SHOULD_NOT_BE_NULL.toLocalizedString());
    }
    if (path.isEmpty()) {
      waitOnInitialization(); // some internal methods rely on this
      return this;
    }
    if (path.charAt(0) == SEPARATOR_CHAR) {
      throw new IllegalArgumentException(
          LocalizedStrings.LocalRegion_PATH_SHOULD_NOT_START_WITH_A_SLASH.toLocalizedString());
    }

    // initialize the current region as this one
    LocalRegion region = this;

    // initialize the rest of the name to be regionName
    String name = path;

    // last: are we on the last part of the path?
    boolean last;
    do {
      // if the rest of the name is empty, then we're done, return current region
      if (name.isEmpty()) {
        // return region
        break;
      }

      // the index of the next separator
      int separatorIndex = name.indexOf(SEPARATOR_CHAR);

      // this is the last part if no separator
      last = separatorIndex < 0;

      // try to get next region
      String next = last ? name : name.substring(0, separatorIndex);
      region = region.basicGetSubregion(next);
      if (region == null) {
        // not found
        return null;
      }
      if (region.isDestroyed() && !destroyedRegionOk) {
        return null;
      }

      if (!last) {
        // if found but still more to do, get next rest of path
        name = name.substring(separatorIndex + 1);
      }
    } while (!last);

    region.waitOnInitialization();

    // if region has just been destroyed return null unless specified not to
    if (region.isDestroyed()) {
      if (!destroyedRegionOk) {
        return null;
      }
      return region;
    }

    return region;
  }

  /**
   * Called by a thread that is doing region initialization. Causes the initialization Latch to be
   * bypassed by this thread.
   *
   */
  public static int setThreadInitLevelRequirement(int level) {
    int oldLevel = threadInitLevelRequirement();
    if (level == AFTER_INITIAL_IMAGE) {
      // if setting to default, just reset
      initializationThread.remove();
    } else {
      initializationThread.set(level);
    }
    return oldLevel;
  }

  /**
   * Return the access level this thread has for regions with respect to how initialized they need
   * to be before this thread can have a reference to it. AFTER_INITIAL_IMAGE: Must be fully
   * initialized (the default) BEFORE_INITIAL_IMAGE: Must have had first latch opened ANY_INIT:
   * Thread uses region as soon as possible
   */
  public static int threadInitLevelRequirement() {
    Integer initLevel = initializationThread.get();
    if (initLevel == null) {
      return AFTER_INITIAL_IMAGE;
    }
    return initLevel;
  }

  @Override
  public boolean checkForInitialization() {
    if (this.initialized) {
      return true;
    }
    switch (threadInitLevelRequirement()) {
      case AFTER_INITIAL_IMAGE:
        return checkForInitialization(this.getInitializationLatchAfterGetInitialImage());
      case BEFORE_INITIAL_IMAGE:
        return checkForInitialization(this.getInitializationLatchBeforeGetInitialImage());
      case ANY_INIT:
        return true;
      default:
        throw new InternalGemFireError(
            LocalizedStrings.LocalRegion_UNEXPECTED_THREADINITLEVELREQUIREMENT.toLocalizedString());
    }
  }

  private boolean checkForInitialization(StoppableCountDownLatch latch) {
    return latch.getCount() == 0;
  }

  /** wait on the initialization Latch based on thread requirements */
  @Override
  public void waitOnInitialization() {
    if (this.initialized) {
      return;
    }
    switch (threadInitLevelRequirement()) {
      case AFTER_INITIAL_IMAGE:
        waitOnInitialization(this.getInitializationLatchAfterGetInitialImage());
        break;
      case BEFORE_INITIAL_IMAGE:
        waitOnInitialization(this.getInitializationLatchBeforeGetInitialImage());
        break;
      case ANY_INIT:
        return;
      default:
        throw new InternalGemFireError(
            LocalizedStrings.LocalRegion_UNEXPECTED_THREADINITLEVELREQUIREMENT.toLocalizedString());
    }
  }

  @Override
  public void waitOnInitialization(StoppableCountDownLatch latch) {
    if (latch == null) {
      return; // latch resource has been freed
    }
    while (true) {
      this.cache.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
        this.cache.getCancelCriterion().checkCancelInProgress(e);
        // continue waiting
      } finally {
        if (interrupted) {
          // set interrupted flag if was interrupted
          Thread.currentThread().interrupt();
        }
      }
    } // while
  }

  /**
   * Wait until data is ready in this region
   */
  public void waitForData() {
    if (this.initialized) {
      return;
    }
    waitOnInitialization(this.getInitializationLatchAfterGetInitialImage());
  }

  /** return null if not found */
  @Override
  public RegionEntry basicGetEntry(Object key) {
    // ok to ignore tx state; all callers are non-transactional
    RegionEntry regionEntry = this.entries.getEntry(key);
    if (regionEntry != null && regionEntry.isRemoved()) {
      regionEntry = null;
    }
    return regionEntry;
  }

  /**
   * Return true if invalidation occurred; false if it did not, for example if it was already
   * invalidated
   *
   * @see DistributedRegion#basicInvalidate(EntryEventImpl)
   */
  public void basicInvalidate(EntryEventImpl event) throws EntryNotFoundException {
    basicInvalidate(event, isInitialized()/* for bug 35214 */);
  }

  /**
   * Used by disk regions when recovering data from backup. Currently this "put" is done at a very
   * low level to keep it from generating events or pushing updates to others.
   */
  @Override
  public DiskEntry initializeRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    Assert.assertTrue(this.diskRegion != null);
    // region operation so it is ok to ignore tx state
    RegionEntry regionEntry = this.entries.initRecoveredEntry(key, re);
    if (regionEntry == null) {
      throw new InternalGemFireError(
          LocalizedStrings.LocalRegion_ENTRY_ALREADY_EXISTED_0.toLocalizedString(key));
    }
    return (DiskEntry) regionEntry;
  }

  /**
   * Used by disk regions when recovering data from backup and initializedRecoveredEntry has already
   * been called for the given key. Currently this "put" is done at a very low level to keep it from
   * generating events or pushing updates to others.
   */
  @Override
  public DiskEntry updateRecoveredEntry(Object key, DiskEntry.RecoveredEntry re) {
    Assert.assertTrue(this.diskRegion != null);
    // region operation so it is ok to ignore tx state
    RegionEntry regionEntry = this.entries.updateRecoveredEntry(key, re);
    return (DiskEntry) regionEntry;
  }

  @Override
  public void copyRecoveredEntries(RegionMap rm) {
    this.entries.copyRecoveredEntries(rm);
  }

  @Override
  public void recordRecoveredGCVersion(VersionSource member, long gcVersion) {
    // TODO - RVV - I'm not sure about this recordGCVersion method. It seems like it's not doing the
    // right thing if the current member is the member we just recovered. We need to update the RVV
    // in memory
    this.versionVector.recordGCVersion(member, gcVersion);

    // We also need to update the RVV that represents what we have persisted on disk
    DiskRegion region = this.getDiskRegion();
    if (region != null) {
      region.recordRecoveredGCVersion(member, gcVersion);
    }
  }

  @Override
  public void recordRecoveredVersonHolder(VersionSource member, RegionVersionHolder versionHolder,
      boolean latestOplog) {
    if (this.getConcurrencyChecksEnabled()) {
      // We need to update the RVV in memory
      this.versionVector.initRecoveredVersion(member, versionHolder, latestOplog);
      DiskRegion region = this.getDiskRegion();
      // We also need to update the RVV that represents what we have persisted on disk
      if (region != null) {
        region.recordRecoveredVersonHolder(member, versionHolder, latestOplog);
      }
    }
  }

  @Override
  public void recordRecoveredVersionTag(VersionTag tag) {
    if (this.getConcurrencyChecksEnabled()) {
      this.versionVector.recordVersion(tag.getMemberID(), tag.getRegionVersion());
      DiskRegion region = this.getDiskRegion();
      // We also need to update the RVV that represents what we have persisted on disk
      if (region != null) {
        region.recordRecoveredVersionTag(tag);
      }
    }
  }

  @Override
  public void setRVVTrusted(boolean rvvTrusted) {
    if (this.getConcurrencyChecksEnabled()) {
      DiskRegion region = this.getDiskRegion();
      // Update whether or not the RVV we have recovered is trusted (accurately represents what we
      // have on disk).
      if (region != null) {
        region.setRVVTrusted(rvvTrusted);
      }
    }
  }

  /**
   * Get the best iterator for the region entries.
   */
  public Iterator<RegionEntry> getBestIterator(boolean includeValues) {
    if (this instanceof DistributedRegion) {
      return ((DistributedRegion) this).getBestIterator(includeValues);
    }

    return this.entries.regionEntries().iterator();
  }

  /**
   * Fix up our RVV by iterating over the entries in the region and making sure they are applied to
   * the RVV.
   *
   * If we failed to do a GII, we may have applied the RVV from a remote member. That RVV may not
   * have seen some of the events in our local RVV. Those entries were supposed to be replaced with
   * the results of the GII. However, if we failed the GII, those entries may still be in the cache,
   * but are no longer reflected in the local RVV. This method iterates over those keys and makes
   * sure their versions are applied to the local RVV.
   *
   * TODO - this method should probably rebuild the RVV from scratch, instead of starting with the
   * existing RVV. By starting with the existing RVV, we may claim to have entries that we actually
   * don't have. Unfortunately, we can't really rebuild the RVV from scratch because we will end up
   * with huge exception lists.
   *
   * However, if we are in the state of recovering from disk with an untrusted RVV, we must be newer
   * than any other surviving members. So they shouldn't have any entries in their cache that match
   * entries that we failed to receive through the GII but are reflected in our current RVV. So it
   * should be safe to start with the current RVV.
   *
   */
  void repairRVV() {
    RegionVersionVector rvv = this.getVersionVector();

    if (rvv == null) {
      // No need to do anything.
      return;
    }

    Iterator<RegionEntry> it = getBestIterator(false);
    VersionSource<?> myId = getVersionMember();

    // Iterate over the all of the entries
    while (it.hasNext()) {
      RegionEntry mapEntry = it.next();
      VersionStamp<?> stamp = mapEntry.getVersionStamp();
      VersionSource<?> id = stamp.getMemberID();
      if (id == null) {
        id = myId;
      }

      // Make sure the version is applied to the regions RVV
      rvv.recordVersion(id, stamp.getRegionVersion());
    }
  }

  /**
   * Return true if invalidation occurred; false if it did not, for example if it was already
   * invalidated
   */
  private void basicInvalidate(final EntryEventImpl event, boolean invokeCallbacks)
      throws EntryNotFoundException {
    final boolean forceNewEntryInClientCache = this.serverRegionProxy != null
        && getConcurrencyChecksEnabled();
    basicInvalidate(event, invokeCallbacks, forceNewEntryInClientCache);
  }

  /**
   * basicInvalidate is overridden in HARegion to abort expiry of Events which have key as Long , if
   * it is not able to destroy from availableIDs
   *
   * @param forceNewEntry true if we are a mirror and still in the initialization phase. Called from
   *        InvalidateOperation.InvalidateMessage
   */
  void basicInvalidate(final EntryEventImpl event, boolean invokeCallbacks,
      final boolean forceNewEntry) throws EntryNotFoundException {
    if (!event.isOriginRemote() && !event.isDistributed() && getScope().isDistributed()
        && getDataPolicy().withReplication() && invokeCallbacks) {
      // catches case where being called by (distributed) invalidateRegion
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_CANNOT_DO_A_LOCAL_INVALIDATE_ON_A_REPLICATED_REGION
              .toLocalizedString());
    }

    if (hasSeenEvent(event)) {
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "LR.basicInvalidate: this cache has already seen this event {}", event);
      }
      if (this.getConcurrencyChecksEnabled() && event.getVersionTag() != null
          && !event.getVersionTag().isRecorded()) {
        getVersionVector().recordVersion((InternalDistributedMember) event.getDistributedMember(),
            event.getVersionTag());
      }
      return;
    }

    discoverJTA();
    getDataView().invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
  }

  void basicInvalidatePart2(RegionEntry regionEntry, EntryEventImpl event,
      boolean conflictWithClear, boolean invokeCallbacks) {
    updateStatsForInvalidate();

    if (invokeCallbacks) {
      try {
        regionEntry.dispatchListenerEvents(event);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        this.stopper.checkCancelInProgress(null);
      }
    } else {
      event.callbacksInvoked(true);
    }
  }

  /**
   * Update stats
   */
  private void updateStatsForInvalidate() {
    getCachePerfStats().incInvalidates();
  }

  void basicInvalidatePart3(RegionEntry re, EntryEventImpl event, boolean invokeCallbacks) {
    // No op. overridden by sub classes.
    // Dispatching listener events moved to basic*Part2.
  }

  /**
   * invoke callbacks for an invalidation
   */
  @Override
  public void invokeInvalidateCallbacks(final EnumListenerEvent eventType,
      final EntryEventImpl event, final boolean callDispatchListenerEvent) {
    // Notify bridge clients (if this is a CacheServer)
    event.setEventType(eventType);
    notifyBridgeClients(event);
    if (callDispatchListenerEvent) {
      dispatchListenerEvent(eventType, event);
    }
  }

  /**
   * @param key the key of the entry to invalidate
   * @param newValue the new value of the entry
   * @param didDestroy true if tx destroyed this entry at some point
   * @param event filled in if operation performed
   * @param txEntryState for passing up versionTag - only on near side
   * @param versionTag tag generated by txCoordinator - only on far side
   * @param tailKey tail (shadow) key generated by txCoordinator for WAN - only on farside
   */
  @Override
  public void txApplyInvalidate(Object key, Object newValue, boolean didDestroy,
      TransactionId transactionId, TXRmtEvent event, boolean localOp, EventID eventId,
      Object aCallbackArgument, List<EntryEventImpl> pendingCallbacks,
      FilterRoutingInfo filterRoutingInfo, ClientProxyMembershipID bridgeContext,
      TXEntryState txEntryState, VersionTag versionTag, long tailKey) {

    this.entries.txApplyInvalidate(key, newValue, didDestroy, transactionId, event, localOp,
        eventId, aCallbackArgument, pendingCallbacks, filterRoutingInfo, bridgeContext,
        txEntryState, versionTag, tailKey);
  }

  /**
   * Called by lower levels, while still holding the write sync lock, and the low level has
   * completed its part of the basic destroy
   */
  void txApplyInvalidatePart2(RegionEntry regionEntry, Object key, boolean didDestroy,
      boolean didInvalidate) {
    if (this.testCallable != null) {
      this.testCallable.call(this, Operation.INVALIDATE, regionEntry);
    }
    if (didInvalidate) {
      updateStatsForInvalidate();
      // Bug 40842: clearing index of the old value performed in AbstractRegionMap
    }
    if (didDestroy) {
      if (this.entryUserAttributes != null) {
        this.entryUserAttributes.remove(key);
      }
    }
  }

  /**
   * Called by AbstractRegionMap txApplyPut when it was told a destroy was also done
   * by the transaction.
   */
  @Override
  public void txApplyPutHandleDidDestroy(Object key) {
    if (this.entryUserAttributes != null) {
      this.entryUserAttributes.remove(key);
    }
  }

  /**
   * Allows null as new value to accomodate create with a null value. Assumes all key, value, and
   * callback validations have been performed.
   *
   * @param event the event object for this operation, with the exception that the oldValue
   *        parameter is not yet filled in. The oldValue will be filled in by this operation.
   *
   * @param ifNew true if this operation must not overwrite an existing key
   * @param ifOld true if this operation must not create a new key
   * @param expectedOldValue only succeed if old value is equal to this value. If null, then doesn't
   *        matter what old value is. If INVALID token, must be INVALID.
   * @param requireOldValue true if the oldValue should be set in event even if ifNew and entry
   *        exists
   * @return false if ifNew is true and there is an existing key or if ifOld is true and
   *         expectedOldValue does not match the current value in the cache. Otherwise return true.
   */
  @Override
  public boolean basicPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue)
      throws TimeoutException, CacheWriterException {
    return getDataView().putEntry(event, ifNew, ifOld, expectedOldValue, requireOldValue, 0L,
        false);
  }

  /**
   * @param putOp describes the operation that did the put
   * @param key the key of the entry to put
   * @param newValue the new value of the entry
   * @param didDestroy true if tx destroyed this entry at some point
   * @param event filled in if operation performed
   * @param aCallbackArgument argument passed in by user
   * @param txEntryState for passing up versionTag - only on near side
   * @param versionTag tag generated by txCoordinator - only on far side
   * @param tailKey tail (shadow) key generated by txCoordinator for WAN - only on farside
   */
  @Override
  public void txApplyPut(Operation putOp, Object key, Object newValue, boolean didDestroy,
      TransactionId transactionId, TXRmtEvent event, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey) {
    long startPut = CachePerfStats.getStatTime();
    this.entries.txApplyPut(putOp, key, newValue, didDestroy, transactionId, event, eventId,
        aCallbackArgument, pendingCallbacks, filterRoutingInfo, bridgeContext, txEntryState,
        versionTag, tailKey);
    updateStatsForPut(startPut);
    // Fix for 47507 - make sure we throw an exception if we skip the TX put because
    // the region is cleared (due to a destroy).
    checkReadiness();
  }

  /**
   * update stats
   */
  private void updateStatsForPut(long startPut) {
    getCachePerfStats().endPut(startPut, false);
  }

  @Override
  public void txApplyPutPart2(RegionEntry regionEntry, Object key, long lastModified,
      boolean isCreate, boolean didDestroy, boolean clearConflict) {
    if (this.testCallable != null) {
      Operation op = isCreate ? Operation.CREATE : Operation.UPDATE;
      this.testCallable.call(this, op, regionEntry);
    }
    if (isCreate) {
      updateStatsForCreate();
    }
    if (!isProxy() && !clearConflict) {
      if (this.indexManager != null) {
        try {
          this.indexManager.updateIndexes(regionEntry,
              isCreate ? IndexManager.ADD_ENTRY : IndexManager.UPDATE_ENTRY,
              isCreate ? IndexProtocol.OTHER_OP : IndexProtocol.AFTER_UPDATE_OP);
        } catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        }
      }
    }
    if (didDestroy) {
      if (this.entryUserAttributes != null) {
        this.entryUserAttributes.remove(key);
      }
    }
    if (this.statisticsEnabled && !clearConflict) {
      addExpiryTaskIfAbsent(regionEntry);
    }
    setLastModifiedTime(lastModified);
  }

  public boolean basicBridgeCreate(final Object key, final byte[] value, boolean isObject,
      Object callbackArg, final ClientProxyMembershipID client, boolean fromClient,
      EntryEventImpl clientEvent, boolean throwEntryExists)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    EventID eventId = clientEvent.getEventId();
    Object theCallbackArg = callbackArg;

    long startPut = CachePerfStats.getStatTime();
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        theCallbackArg = new GatewaySenderEventCallbackArgument(theCallbackArg);
      }
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.CREATE, key, value,
        theCallbackArg, false /* origin remote */, client.getDistributedMember(),
        true /* generateCallbacks */, eventId);

    try {
      event.setContext(client);

      // if this is a replayed operation or WAN event we may already have a version tag
      event.setVersionTag(clientEvent.getVersionTag());
      // carry over the possibleDuplicate flag from clientEvent
      event.setPossibleDuplicate(clientEvent.isPossibleDuplicate());

      // Fix for 42448 - Only make create with null a local invalidate for
      // normal regions. Otherwise, it will become a distributed invalidate.
      if (getDataPolicy() == DataPolicy.NORMAL) {
        event.setLocalInvalid(true);
      }

      // Set the new value to the input byte[] if it isn't null
      if (value != null) {
        // If the byte[] represents an object, then store it serialized
        // in a CachedDeserializable; otherwise store it directly as a byte[]
        if (isObject) {
          // The value represents an object
          event.setSerializedNewValue(value);
        } else {
          // The value does not represent an object
          event.setNewValue(value);
        }
      }

      // cannot overwrite an existing key
      boolean ifNew = true;
      // can create a new key
      boolean ifOld = false;
      // use now
      long lastModified = 0L;
      // not okay to overwrite the DESTROYED token
      boolean overwriteDestroyed = false;

      boolean success = basicUpdate(event, ifNew, ifOld, lastModified, overwriteDestroyed);
      clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());

      if (success) {
        clientEvent.setVersionTag(event.getVersionTag());
        getCachePerfStats().endPut(startPut, event.isOriginRemote());
      } else {
        this.stopper.checkCancelInProgress(null);
        if (throwEntryExists) {
          throw new EntryExistsException("" + key, event.getOldValue());
        }
      }
      return success;
    } finally {
      event.release();
    }
  }

  public boolean basicBridgePut(Object key, Object value, byte[] deltaBytes, boolean isObject,
      Object callbackArg, ClientProxyMembershipID memberId, boolean fromClient,
      EntryEventImpl clientEvent) throws TimeoutException, CacheWriterException {

    EventID eventID = clientEvent.getEventId();
    Object theCallbackArg = callbackArg;
    long startPut = CachePerfStats.getStatTime();

    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        theCallbackArg = new GatewaySenderEventCallbackArgument(theCallbackArg);
      }
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.UPDATE, key,
        null /* new value */, theCallbackArg, false /* origin remote */,
        memberId.getDistributedMember(), true /* generateCallbacks */, eventID);

    try {
      event.setContext(memberId);
      event.setDeltaBytes(deltaBytes);

      // if this is a replayed operation we may already have a version tag
      event.setVersionTag(clientEvent.getVersionTag());
      // carry over the possibleDuplicate flag from clientEvent
      event.setPossibleDuplicate(clientEvent.isPossibleDuplicate());

      // Set the new value to the input byte[]. If the byte[] represents an object, then store it
      // serialized in a CachedDeserializable; otherwise store it directly as a byte[].
      if (isObject && value instanceof byte[]) {
        event.setSerializedNewValue((byte[]) value);
      } else {
        event.setNewValue(value);
      }

      boolean success = false;

      try {
        boolean ifNew = false; // can overwrite an existing key
        boolean ifOld = false; // can create a new key
        long lastModified = 0L; // use now
        boolean overwriteDestroyed = false; // not okay to overwrite the DESTROYED token
        success = basicUpdate(event, ifNew, ifOld, lastModified, overwriteDestroyed);

      } catch (ConcurrentCacheModificationException ignore) {
        // thrown by WAN conflicts
        event.isConcurrencyConflict(true);
      }

      clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());

      if (success) {
        clientEvent.setVersionTag(event.getVersionTag());
        getCachePerfStats().endPut(startPut, event.isOriginRemote());
      } else {
        this.stopper.checkCancelInProgress(null);
      }

      return success;
    } finally {
      event.release();
    }
  }

  /**
   * issue a config message if the server and client have different concurrency checking
   * expectations
   */
  private void concurrencyConfigurationCheck(VersionTag tag) {
    if (!this.concurrencyMessageIssued && tag == null && this.getConcurrencyChecksEnabled()) {
      this.concurrencyMessageIssued = true;
      logger.info(LocalizedMessage.create(
          LocalizedStrings.LocalRegion_SERVER_HAS_CONCURRENCY_CHECKS_ENABLED_0_BUT_CLIENT_HAS_1_FOR_REGION_2,
          new Object[] {!this.getConcurrencyChecksEnabled(), this.getConcurrencyChecksEnabled(),
              this}));
    }
  }

  /**
   * Perform an update in a bridge client. See CacheClientUpdater.handleUpdate() The op is from the
   * bridge server and should not be distributed back to it.
   */
  public void basicBridgeClientUpdate(DistributedMember serverId, Object key, Object value,
      byte[] deltaBytes, boolean isObject, Object callbackArgument, boolean isCreate,
      boolean processedMarker, EntryEventImpl event, EventID eventID)
      throws TimeoutException, CacheWriterException {

    if (isCacheContentProxy()) {
      return;
    }

    concurrencyConfigurationCheck(event.getVersionTag());
    long startPut = CachePerfStats.getStatTime();

    // Generate EventID as it is possible that client is a cache server
    // in hierarchical cache
    if (generateEventID() && !this.cache.getCacheServers().isEmpty()) {
      event.setNewEventId(cache.getDistributedSystem());
    } else {
      event.setEventId(eventID);
    }
    event.setDeltaBytes(deltaBytes);

    // Set the new value to the input byte[] if it isn't null
    if (value != null) {
      // If the byte[] represents an object, then store it
      // serialized in a CachedDeserializable; otherwise store it directly
      // as a byte[].
      if (isObject && value instanceof byte[]) {
        // The value represents an object
        event.setSerializedNewValue((byte[]) value);
      } else {
        // The value does not represent an object
        event.setNewValue(value);
      }
    }

    // If the marker has been processed, process this put event normally;
    // otherwise, this event occurred in the past and has been stored for a
    // durable client. In this case, just invoke the put callbacks.
    if (processedMarker) {
      boolean ifNew = false; // can overwrite an existing key
      boolean ifOld = false; // can create a new key
      long lastModified = 0L; // use now
      boolean overwriteDestroyed = true; // okay to overwrite the DESTROYED token
      if (basicUpdate(event, ifNew, ifOld, lastModified, overwriteDestroyed)) {
        getCachePerfStats().endPut(startPut, event.isOriginRemote());
      }
    } else {
      if (isInitialized()) {
        invokePutCallbacks(
            isCreate ? EnumListenerEvent.AFTER_CREATE : EnumListenerEvent.AFTER_UPDATE, event, true,
            true);
      }
    }
  }

  /**
   * Perform an invalidate in a bridge client. The op is from the bridge server and should not be
   * distributed back to it.
   */
  public void basicBridgeClientInvalidate(DistributedMember serverId, Object key,
      Object callbackArgument, boolean processedMarker, EventID eventID, VersionTag versionTag)
      throws EntryNotFoundException {

    if (!isCacheContentProxy()) {
      concurrencyConfigurationCheck(versionTag);

      // Create an event and put the entry
      @Released
      EntryEventImpl event =
          EntryEventImpl.create(this, Operation.INVALIDATE, key, null /* newValue */,
              callbackArgument /* callbackArg */, true /* originRemote */, serverId);
      try {

        event.setVersionTag(versionTag);
        event.setFromServer(true);
        if (generateEventID() && !this.cache.getCacheServers().isEmpty()) {
          event.setNewEventId(cache.getDistributedSystem());
        } else {
          event.setEventId(eventID);
        }

        // If the marker has been processed, process this invalidate event
        // normally; otherwise, this event occurred in the past and has been
        // stored for a durable client. In this case, just invoke the invalidate
        // callbacks.
        if (processedMarker) {
          // changed to force new entry creation for consistency
          final boolean forceNewEntry = this.getConcurrencyChecksEnabled();
          basicInvalidate(event, true, forceNewEntry);
          if (event.isConcurrencyConflict()) {
            // bug #45520 - we must throw this for the CacheClientUpdater
            throw new ConcurrentCacheModificationException();
          }
        } else {
          if (isInitialized()) {
            invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE, event, true);
          }
        }
      } finally {
        event.release();
      }
    }
  }

  /**
   * Perform a destroy in a bridge client. The op is from the bridge server and should not be
   * distributed back to it.
   */
  public void basicBridgeClientDestroy(DistributedMember serverId, Object key,
      Object callbackArgument, boolean processedMarker, EventID eventID, VersionTag versionTag)
      throws EntryNotFoundException {

    if (!isCacheContentProxy()) {
      concurrencyConfigurationCheck(versionTag);

      // Create an event and destroy the entry
      @Released
      EntryEventImpl event =
          EntryEventImpl.create(this, Operation.DESTROY, key, null /* newValue */,
              callbackArgument /* callbackArg */, true /* originRemote */, serverId);
      try {
        event.setFromServer(true);
        event.setVersionTag(versionTag);

        if (generateEventID() && !this.cache.getCacheServers().isEmpty()) {
          event.setNewEventId(cache.getDistributedSystem());
        } else {
          event.setEventId(eventID);
        }

        // If the marker has been processed, process this destroy event normally;
        // otherwise, this event occurred in the past and has been stored for a
        // durable client. In this case, just invoke the destroy callbacks.
        if (logger.isDebugEnabled()) {
          logger.debug("basicBridgeClientDestroy(processedMarker={})", processedMarker);
        }

        if (processedMarker) {
          basicDestroy(event, false, null);
          if (event.isConcurrencyConflict()) {
            // bug #45520 - we must throw an exception for CacheClientUpdater
            throw new ConcurrentCacheModificationException();
          }
        } else {
          if (isInitialized()) {
            invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event, true, true);
          }
        }
      } finally {
        event.release();
      }
    }
  }

  /**
   * Clear the region from a server request.
   *
   * @param callbackArgument The callback argument. This is currently null since
   *        {@link java.util.Map#clear} supports no parameters.
   * @param processedMarker Whether the marker has been processed (for durable clients)
   */
  public void basicBridgeClientClear(Object callbackArgument, boolean processedMarker) {
    checkReadiness();
    checkForNoAccess();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOCAL_CLEAR,
        callbackArgument, true, getMyId(), generateEventID()/* generate EventID */);
    // If the marker has been processed, process this clear event normally;
    // otherwise, this event occurred in the past and has been stored for a
    // durable client. In this case, just invoke the clear callbacks.
    if (processedMarker) {
      basicLocalClear(event);
    } else {
      if (isInitialized()) {
        dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, event);
      }
    }
  }

  public void basicBridgeDestroy(Object key, Object callbackArg, ClientProxyMembershipID memberId,
      boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {

    Object theCallbackArg = callbackArg;
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        theCallbackArg = new GatewaySenderEventCallbackArgument(theCallbackArg);
      }
    }

    // Create an event and put the entry
    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.DESTROY, key, null,
        theCallbackArg, false, memberId.getDistributedMember(), true, clientEvent.getEventId());

    try {
      event.setContext(memberId);
      // if this is a replayed or WAN operation we may already have a version tag
      event.setVersionTag(clientEvent.getVersionTag());
      try {
        basicDestroy(event, true, null);
      } catch (ConcurrentCacheModificationException ignore) {
        // thrown by WAN conflicts
        event.isConcurrencyConflict(true);
      } finally {
        clientEvent.setVersionTag(event.getVersionTag());
        clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
        clientEvent.setIsRedestroyedEntry(event.getIsRedestroyedEntry());
      }
    } finally {
      event.release();
    }
  }

  // TODO: fromClient is always true
  public void basicBridgeInvalidate(Object key, Object callbackArg,
      ClientProxyMembershipID memberId, boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {

    Object theCallbackArg = callbackArg;
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        theCallbackArg = new GatewaySenderEventCallbackArgument(theCallbackArg);
      }
    }

    // Create an event and put the entry
    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.INVALIDATE, key, null,
        theCallbackArg, false, memberId.getDistributedMember(), true, clientEvent.getEventId());

    try {
      event.setContext(memberId);

      // if this is a replayed operation we may already have a version tag
      event.setVersionTag(clientEvent.getVersionTag());

      try {
        basicInvalidate(event);
      } finally {
        clientEvent.setVersionTag(event.getVersionTag());
        clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
      }
    } finally {
      event.release();
    }
  }

  // TODO: fromClient is always false and never used
  // TODO: callbackArg is never used
  public void basicBridgeUpdateVersionStamp(Object key, Object callbackArg,
      ClientProxyMembershipID memberId, boolean fromClient, EntryEventImpl clientEvent) {

    // Create an event and update version stamp of the entry
    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.UPDATE_VERSION_STAMP, key, null,
        null, false, memberId.getDistributedMember(), false, clientEvent.getEventId());

    event.setContext(memberId);

    // if this is a replayed operation we may already have a version tag
    event.setVersionTag(clientEvent.getVersionTag());

    try {
      basicUpdateEntryVersion(event);
    } finally {
      clientEvent.setVersionTag(event.getVersionTag());
      clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
      event.release();
    }
  }

  void basicUpdateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {
    if (hasSeenEvent(event)) {
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "LR.basicDestroy: this cache has already seen this event {}", event);
      }
      if (this.getConcurrencyChecksEnabled() && event.getVersionTag() != null
          && !event.getVersionTag().isRecorded()) {
        getVersionVector().recordVersion((InternalDistributedMember) event.getDistributedMember(),
            event.getVersionTag());
      }
      return;
    }

    getDataView().updateEntryVersion(event);
  }

  /**
   * Allows null as new value to accommodate create with a null value.
   *
   * @param event the event object for this operation, with the exception that the oldValue
   *        parameter is not yet filled in. The oldValue will be filled in by this operation.
   *
   * @param ifNew true if this operation must not overwrite an existing key
   * @param ifOld true if this operation must not create a new entry
   * @param lastModified the lastModified time to set with the value; if 0L, then the lastModified
   *        time will be set to now.
   * @param overwriteDestroyed true if okay to overwrite the DESTROYED token: when this is true has
   *        the following effect: even when ifNew is true will write over DESTROYED token when
   *        overwriteDestroyed is false and ifNew or ifOld is true then if the put doesn't occur
   *        because there is a DESTROYED token present then the entry flag blockedDestroyed is set.
   * @return false if ifNew is true and there is an existing key, or ifOld is true and there is no
   *         existing entry; otherwise return true.
   */
  boolean basicUpdate(final EntryEventImpl event, final boolean ifNew, final boolean ifOld,
      final long lastModified, final boolean overwriteDestroyed)
      throws TimeoutException, CacheWriterException {

    // check validity of key against keyConstraint
    if (this.keyConstraint != null) {
      if (!this.keyConstraint.isInstance(event.getKey())) {
        throw new ClassCastException(
            LocalizedStrings.LocalRegion_KEY_0_DOES_NOT_SATISFY_KEYCONSTRAINT_1.toLocalizedString(
                event.getKey().getClass().getName(), this.keyConstraint.getName()));
      }
    }

    validateValue(event.basicGetNewValue());

    return getDataView().putEntry(event, ifNew, ifOld, null, false, lastModified,
        overwriteDestroyed);
  }

  /**
   * Subclasses should reimplement if needed
   */
  @Override
  public boolean virtualPut(final EntryEventImpl event, final boolean ifNew, final boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, final long lastModified,
      final boolean overwriteDestroyed) throws TimeoutException, CacheWriterException {

    if (!MemoryThresholds.isLowMemoryExceptionDisabled()) {
      checkIfAboveThreshold(event);
    }

    Operation originalOp = event.getOperation();
    RegionEntry oldEntry;

    try {
      oldEntry = this.entries.basicPut(event, lastModified, ifNew, ifOld, expectedOldValue,
          requireOldValue, overwriteDestroyed);

    } catch (ConcurrentCacheModificationException ignore) {
      // this can happen in a client cache when another thread
      // managed to slip in its version info to the region entry before this
      // thread got around to doing so
      if (logger.isDebugEnabled()) {
        logger.debug("caught concurrent modification attempt when applying {}", event);
      }
      notifyBridgeClients(event);
      notifyGatewaySender(event.getOperation().isUpdate() ? EnumListenerEvent.AFTER_UPDATE
          : EnumListenerEvent.AFTER_CREATE, event);
      return false;
    }

    // for EMPTY clients, see if a concurrent map operation had an entry on the server
    ServerRegionProxy mySRP = getServerProxy();

    if (mySRP != null && this.getDataPolicy() == DataPolicy.EMPTY) {
      if (originalOp == Operation.PUT_IF_ABSENT) {
        return !event.hasOldValue();
      }
      if (originalOp == Operation.REPLACE && !requireOldValue) {
        // LocalRegion.serverPut throws an EntryNotFoundException if the operation failed
        return true;
      }
    }

    return oldEntry != null;
  }

  /**
   * check to see if a LowMemoryException should be thrown for this event
   */
  @Override
  public void checkIfAboveThreshold(final EntryEventImpl entryEvent) throws LowMemoryException {
    if (entryEvent == null) {
      checkIfAboveThreshold("UNKNOWN");
      return;
    }
    // Threshold check is performed elsewhere for putAll when there is a server proxy
    boolean alreadyCheckedThreshold = hasServerProxy() && entryEvent.getOperation().isPutAll();
    if (!alreadyCheckedThreshold && !entryEvent.isOriginRemote()) {
      checkIfAboveThreshold(entryEvent.getKey());
    }
  }

  /**
   * Checks to see if the event should be rejected because of sick state either due to exceeding
   * local critical threshold or a remote member exceeding critical threshold
   *
   * @param key the key for the operation
   * @throws LowMemoryException if the target member for this operation is sick
   */
  private void checkIfAboveThreshold(final Object key) throws LowMemoryException {
    MemoryThresholdInfo info = getAtomicThresholdInfo();
    if (info.isMemoryThresholdReached()) {
      Set<DistributedMember> membersThatReachedThreshold = info.getMembersThatReachedThreshold();
      // #45603: trigger a background eviction since we're above the the critical
      // threshold
      InternalResourceManager.getInternalResourceManager(this.cache).getHeapMonitor()
          .updateStateAndSendEvent();
      throw new LowMemoryException(
          LocalizedStrings.ResourceManager_LOW_MEMORY_IN_0_FOR_PUT_1_MEMBER_2.toLocalizedString(
              getFullPath(), key, membersThatReachedThreshold),
          membersThatReachedThreshold);
    }
  }

  @Override
  public MemoryThresholdInfo getAtomicThresholdInfo() {
    if (!isMemoryThresholdReached()) {
      return MemoryThresholdInfo.getNotReached();
    }
    return new MemoryThresholdInfo(isMemoryThresholdReached(),
        Collections.singleton(this.cache.getMyId()));
  }

  /**
   * Allows null as new value to accommodate create with a null value.
   *
   * @param event the event object for this operation, with the exception that the oldValue
   *        parameter is not yet filled in. The oldValue will be filled in by this operation.
   * @param lastModified the lastModified time to set with the value; if 0L then the lastModified
   *        time will be set to now.
   * @return null if put not done; otherwise the put entry
   */
  protected RegionEntry basicPutEntry(final EntryEventImpl event, final long lastModified)
      throws TimeoutException, CacheWriterException {

    discoverJTA();
    TXStateInterface tx = getTXState();

    // Note we are doing a load or netsearch result so it seems like we should set ifNew to true.
    // The entry should not yet exist. However since the non-tx code sets ifNew to false this code
    // will also.
    final boolean ifNew = false;

    if (isTX()) {
      tx.txPutEntry(event, ifNew, false, false, null);
      return null;
    } else {
      if (DistTXState.internalBeforeNonTXBasicPut != null) {
        DistTXState.internalBeforeNonTXBasicPut.run();
      }

      return this.entries.basicPut(event, lastModified, ifNew, false, null, false, false);
    }
  }

  @Override
  public long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
      long lastModified, boolean clearConflict) {

    final boolean isNewKey = event.getOperation().isCreate();

    // Invoke callbacks only if we are not creating a tombstone
    final boolean invokeCallbacks = event.basicGetNewValue() != Token.TOMBSTONE;

    if (isNewKey) {
      updateStatsForCreate();
    }

    final boolean lruRecentUse = event.isNetSearch() || event.isLoad(); // fix for bug 31102

    // the event may have a version timestamp that we need to use, so get the
    // event time to store in the entry
    long lastModifiedTime = event.getEventTime(lastModified);
    updateStatsForPut(entry, lastModifiedTime, lruRecentUse);

    if (!isProxy()) {
      if (!clearConflict && this.indexManager != null) {
        try {
          if (!entry.isInvalid()) {
            this.indexManager.updateIndexes(entry,
                isNewKey ? IndexManager.ADD_ENTRY : IndexManager.UPDATE_ENTRY,
                isNewKey ? IndexProtocol.OTHER_OP : IndexProtocol.AFTER_UPDATE_OP);
          }
        } catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        } finally {
          IndexManager.setIndexBufferTime(lastModifiedTime, cacheTimeMillis());
        }
      }
    }

    if (invokeCallbacks) {
      boolean doCallback = false;
      if (isInitialized) {
        // fix for #46662: skip wan notification during import newwan moves notification to here
        // from invokePutCallbacks
        if (event.isGenerateCallbacks()) {
          doCallback = true;
        }
      } else if (this.isUsedForPartitionedRegionBucket) {
        // invokePutCallbacks in BucketRegion will be more discriminating
        doCallback = true;
      }

      if (doCallback) {
        if (event.isBulkOpInProgress() && this.isUsedForPartitionedRegionBucket) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "For bulk operation on bucket region, not to notify gateway sender earlier.");
          }
        } else {
          notifyGatewaySender(event.getOperation().isUpdate() ? EnumListenerEvent.AFTER_UPDATE
              : EnumListenerEvent.AFTER_CREATE, event);
        }

        // Notify listeners
        if (!event.isBulkOpInProgress()) {
          try {
            entry.dispatchListenerEvents(event);
          } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            this.stopper.checkCancelInProgress(null);
          }
        }
      }
    }
    return lastModifiedTime;
  }

  /**
   * To lower latency, PRs generate the local filter routing in DistributedCacheOperation after
   * message distribution and before waiting for responses.
   *
   * Warning: Even if you comment out bucket condition in following method, getLocalRoutingInfo()
   * does NOT process CQs for bucket regions internally. See
   * {@link FilterProfile#getFilterRoutingInfoPart2(FilterRoutingInfo, CacheEvent)} .
   */
  protected void generateLocalFilterRouting(InternalCacheEvent event) {
    boolean isEntryEvent = event.getOperation().isEntry();
    EntryEventImpl entryEvent = isEntryEvent ? (EntryEventImpl) event : null;

    FilterProfile filterProfile = this.getFilterProfile();
    FilterInfo routing = event.getLocalFilterInfo();

    if (filterProfile != null && routing == null) {
      boolean lockForCQ = false;
      Object regionEntryObject = null;

      if (isEntryEvent && entryEvent.getRegionEntry() != null) {
        // bug #45520 we should either have the lock on the region entry
        // or the event was elided and CQ processing won't be done on it
        regionEntryObject = entryEvent.getRegionEntry();
        if (!entryEvent.isConcurrencyConflict()) {
          Assert.assertTrue(regionEntryObject != null);
          lockForCQ = true;
        }
      }
      if (isEntryEvent) {
        if (logger.isDebugEnabled()) {
          logger.debug("getting local client routing.");
        }
      }
      if (lockForCQ) {
        synchronized (regionEntryObject) {
          routing = filterProfile.getLocalFilterRouting(event);
        }
      } else {
        routing = filterProfile.getLocalFilterRouting(event);
      }
      event.setLocalFilterInfo(routing);
    }

    // bug #45520 - do not send CQ events to clients out of order
    if (routing != null && event.getOperation().isEntry()
        && ((EntryEventImpl) event).isConcurrencyConflict()) {
      if (logger.isDebugEnabled()) {
        logger.debug("clearing CQ routing for event that's in conflict");
      }
      routing.clearCQRouting();
    }
  }

  /**
   * This notifies all WAN sites about updated timestamp on local site.
   */
  @Override
  public void notifyTimestampsToGateways(EntryEventImpl event) {
    // Create updateTimeStampEvent from event.
    VersionTagHolder updateTimeStampEvent = new VersionTagHolder(event.getVersionTag());
    updateTimeStampEvent.setOperation(Operation.UPDATE_VERSION_STAMP);
    updateTimeStampEvent.setKeyInfo(event.getKeyInfo());
    updateTimeStampEvent.setGenerateCallbacks(false);
    updateTimeStampEvent.distributedMember = event.getDistributedMember();
    updateTimeStampEvent.setNewEventId(getSystem());

    if (event.getRegion() instanceof BucketRegion) {
      BucketRegion bucketRegion = (BucketRegion) event.getRegion();
      PartitionedRegion partitionedRegion = bucketRegion.getPartitionedRegion();
      updateTimeStampEvent.setRegion(partitionedRegion);

      // increment the tailKey for the event
      if (partitionedRegion.isParallelWanEnabled()) {
        bucketRegion.handleWANEvent(updateTimeStampEvent);
      }

      if (partitionedRegion.isInitialized()) {
        partitionedRegion.notifyGatewaySender(EnumListenerEvent.TIMESTAMP_UPDATE,
            updateTimeStampEvent);
      }
    } else {
      updateTimeStampEvent.setRegion(event.getRegion());
      notifyGatewaySender(EnumListenerEvent.TIMESTAMP_UPDATE, updateTimeStampEvent);
    }
  }

  /**
   * Update CachePerfStats
   */
  private void updateStatsForCreate() {
    getCachePerfStats().incCreates();
  }

  @Override
  public void basicPutPart3(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
      long lastModified, boolean invokeCallbacks, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) {

    // We used to dispatch listener events here which is moved to part2 to be in RE lock #45520.
    if (invokeCallbacks) {
      if (event.isBulkOpInProgress()) {
        event.getPutAllOperation().addEntry(event);
      }
    }
  }

  @Override
  public void invokePutCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways) {

    // disallow callbacks on import
    if (!event.isGenerateCallbacks()) {
      return;
    }

    // Notify bridge clients (if this is a BridgeServer)
    Operation op = event.getOperation();

    // The spec for ConcurrentMap support requires that operations be mapped
    // to non-CM counterparts
    if (op == Operation.PUT_IF_ABSENT) {
      event.setOperation(Operation.CREATE);
    } else if (op == Operation.REPLACE) {
      event.setOperation(Operation.UPDATE);
    }

    event.setEventType(eventType);
    notifyBridgeClients(event);

    if (notifyGateways) {
      notifyGatewaySender(eventType, event);
    }
    if (callDispatchListenerEvent) {
      dispatchListenerEvent(eventType, event);
    }
  }

  /**
   * retrieve a deep copy of the Region's event state. This is used for getInitialImage. The result
   * is installed in the receiver of the image.
   */
  public Map<? extends DataSerializable, ? extends DataSerializable> getEventState() {
    return getEventTracker().getState();
  }

  /**
   * Record the event state encapsulated in the given Map.
   * <p>
   * This is intended for state transfer during GII.
   *
   * @param provider the member that provided this state
   * @param state a Map obtained from getEventState()
   */
  protected void recordEventState(InternalDistributedMember provider, Map state) {
    getEventTracker().recordState(provider, state);
  }

  /**
   * generate version tag if it does not exist and set it into the event.
   */
  @Override
  public void generateAndSetVersionTag(InternalCacheEvent event, RegionEntry entry) {
    if (entry != null && event.getOperation().isEntry()) {
      EntryEventImpl entryEvent = (EntryEventImpl) event;
      if (!entryEvent.isOriginRemote() && shouldGenerateVersionTag(entry, entryEvent)) {
        boolean eventHasDelta = getSystem().getConfig().getDeltaPropagation()
            && !this.scope.isDistributedNoAck() && entryEvent.getDeltaBytes() != null;
        VersionTag v = entry.generateVersionTag(null, eventHasDelta, this, entryEvent);
        if (logger.isDebugEnabled() && v != null) {
          logger.debug("generated version tag {} for {}", v, entryEvent.getKey());
        }
      }
    }
  }

  /**
   * record the event's sequenceId in Region's event state to prevent replay.
   */
  @Override
  public void recordEvent(InternalCacheEvent event) {
    getEventTracker().recordEvent(event);
  }

  /**
   * has the Region's event state seen this event?
   *
   * @return true if the Region's event state has seen the event
   */
  @Override
  public boolean hasSeenEvent(EntryEventImpl event) {
    return getEventTracker().hasSeenEvent(event);
  }

  /**
   * tries to find the version tag for a event
   *
   * @return the version tag, if known. Null if not
   */
  @Override
  public VersionTag findVersionTagForEvent(EventID eventId) {
    return getEventTracker().findVersionTagForSequence(eventId);
  }

  /**
   * tries to find the version tag for a replayed client event
   *
   * @return the version tag, if known. Null if not
   */
  public VersionTag findVersionTagForClientBulkOp(EventID eventId) {
    return getEventTracker().findVersionTagForBulkOp(eventId);
  }

  /**
   * has the Region's event state seen this event? Most checks should use the method that takes an
   * Event, not an ID, but with transactions we do not have an event at the time the check needs to
   * be made. Consequently, this method may cause events to be recorded that would otherwise be
   * ignored.
   *
   * @param eventID the identifier of the event
   * @return true if the Region's event state has seen the event
   */
  @Override
  public boolean hasSeenEvent(EventID eventID) {
    return getEventTracker().hasSeenEvent(eventID);
  }

  /**
   * A routine to provide synchronization running based on <memberShipID, threadID> of the
   * requesting client for the region's event state
   *
   * @param task - a Runnable to wrap the processing of the bulk op
   * @param eventId - the base event ID of the bulk op
   *
   * @since GemFire 5.7
   */
  @Override
  public void syncBulkOp(Runnable task, EventID eventId) {
    getEventTracker().syncBulkOp(task, eventId, isTX());
  }

  public void recordBulkOpStart(ThreadIdentifier membershipID, EventID eventID) {
    if (!isTX()) {
      getEventTracker().recordBulkOpStart(eventID, membershipID);
    }
  }

  protected void notifyBridgeClients(CacheEvent event) {
    int numBS = getCache().getCacheServers().size();

    // #Bugfix 37518: In case of localOperations no need to notify clients.
    if (event.getOperation().isLocal() || numBS == 0) {
      return;
    }
    // Return if the inhibit all notifications flag is set
    if (event instanceof EntryEventImpl) {
      if (((EntryEventImpl) event).inhibitAllNotifications()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Notification inhibited for key {}", event);
        }
        return;
      }
    }

    if (shouldNotifyBridgeClients()) {
      if (numBS > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: notifying {} bridge servers of event: {}", this.getName(), numBS,
              event);
        }
      }

      Operation op = event.getOperation();
      if (event.getOperation().isEntry()) {
        EntryEventImpl e = (EntryEventImpl) event;
        if (e.getEventType() == null) {
          if (op.isCreate()) {
            e.setEventType(EnumListenerEvent.AFTER_CREATE);
          } else if (op.isUpdate()) {
            e.setEventType(EnumListenerEvent.AFTER_UPDATE);
          } else if (op.isDestroy()) {
            e.setEventType(EnumListenerEvent.AFTER_DESTROY);
          } else if (op.isInvalidate()) {
            e.setEventType(EnumListenerEvent.AFTER_INVALIDATE);
          } else {
            throw new IllegalStateException("event is missing client notification eventType: " + e);
          }
        }
      }

      InternalCacheEvent ice = (InternalCacheEvent) event;
      if (!this.isUsedForPartitionedRegionBucket()) {
        generateLocalFilterRouting(ice);
      }

      CacheClientNotifier.notifyClients((InternalCacheEvent) event);
    }
  }

  /**
   * Returns true if this region notifies multiple serial gateways.
   */
  public boolean notifiesMultipleSerialGateways() {
    if (isPdxTypesRegion()) {
      return false;
    }
    Set<String> allGatewaySenderIds = getAllGatewaySenderIds();
    if (!allGatewaySenderIds.isEmpty()) {
      List<Integer> allRemoteDSIds = getRemoteDsIds(allGatewaySenderIds);
      if (allRemoteDSIds != null) {
        int serialGatewayCount = 0;
        for (GatewaySender sender : getCache().getAllGatewaySenders()) {
          if (allGatewaySenderIds.contains(sender.getId())) {
            if (!sender.isParallel()) {
              serialGatewayCount++;
              if (serialGatewayCount > 1) {
                return true;
              }
            }
          }
        }
      }
    }
    return false;
  }

  protected void notifyGatewaySender(EnumListenerEvent operation, EntryEventImpl event) {
    if (isPdxTypesRegion()) {
      return;
    }

    // Return if the inhibit all notifications flag is set
    if (event.inhibitAllNotifications()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Notification inhibited for key {}", event);
      }
      return;
    }

    checkSameSenderIdsAvailableOnAllNodes();
    Set<String> allGatewaySenderIds;
    if (event.getOperation() == Operation.UPDATE_VERSION_STAMP) {
      allGatewaySenderIds = getGatewaySenderIds();
    } else {
      allGatewaySenderIds = getAllGatewaySenderIds();
    }

    List<Integer> allRemoteDSIds = getRemoteDsIds(allGatewaySenderIds);
    if (allRemoteDSIds != null) {
      for (GatewaySender sender : getCache().getAllGatewaySenders()) {
        if (allGatewaySenderIds.contains(sender.getId())) {
          // TODO: This is a BUG. Why return and not continue?
          if (!this.getDataPolicy().withStorage() && sender.isParallel()) {
            return;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Notifying the GatewaySender : {}", sender.getId());
          }
          ((AbstractGatewaySender) sender).distribute(operation, event, allRemoteDSIds);
        }
      }
    }
  }

  public void checkSameSenderIdsAvailableOnAllNodes() {
    // do nothing
  }

  /**
   * @param cacheWrite if true, then we're just cleaning up the local cache and calling listeners,
   * @see DistributedRegion#basicDestroyRegion(RegionEventImpl, boolean, boolean, boolean)
   */
  void basicDestroyRegion(RegionEventImpl event, boolean cacheWrite)
      throws CacheWriterException, TimeoutException {
    basicDestroyRegion(event, cacheWrite, true, true);
  }

  void basicDestroyRegion(RegionEventImpl event, boolean cacheWrite, boolean lock,
      boolean callbackEvents) throws CacheWriterException, TimeoutException {
    preDestroyChecks();

    final TXStateProxy tx = this.cache.getTXMgr().pauseTransaction();
    try {
      boolean acquiredLock = false;
      if (lock) {
        try {
          acquireDestroyLock();
          acquiredLock = true;
        } catch (CancelException ignore) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "basicDestroyRegion: acquireDestroyLock failed due to cache closure, region = {}",
                getFullPath());
          }
        }
      }

      try {
        // maintain destroy lock and TXStateInterface
        // I moved checkRegionDestroyed up out of the following
        // try block because it does not seem correct to deliver
        // a destroy event to the clients of the region was already
        // destroyed on the server.
        checkRegionDestroyed(false);
        boolean cancelledByCacheWriterException = false; // see bug 47736
        HashSet eventSet = null;

        try { // ensure that destroy events are dispatched
          if (this instanceof PartitionedRegion
              && !((PartitionedRegion) this).getParallelGatewaySenderIds().isEmpty()) {
            ((PartitionedRegion) this).destroyParallelGatewaySenderRegion(event.getOperation(),
                cacheWrite, lock, callbackEvents);
          }

          if (this.parentRegion != null) {
            // "Bubble up" the cache statistics to parent if this regions are more recent
            this.parentRegion.updateStats();
          }

          try {
            eventSet = callbackEvents ? new HashSet() : null;
            this.destroyedSubregionSerialNumbers = collectSubregionSerialNumbers();
            recursiveDestroyRegion(eventSet, event, cacheWrite);
          } catch (CancelException e) {
            // This should be properly caught and ignored; if we see this there is
            // a serious problem.
            if (!this.cache.forcedDisconnect()) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.LocalRegion_RECURSIVEDESTROYREGION_RECURSION_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0,
                  getFullPath()), e);
            }
          } catch (CacheWriterException cwe) {
            cancelledByCacheWriterException = true;
            throw cwe;
          }

          // at this point all subregions are destroyed and this region has been marked as destroyed
          // and postDestroyRegion has been called for each region. The only detail left is
          // unhooking this region from the parent subregion map, and sending listener events
          Assert.assertTrue(this.isDestroyed);

          // Added for M&M : At this point we can safely call ResourceEvent to remove the region
          // artifacts From Management Layer
          if (!isInternalRegion()) {
            InternalDistributedSystem system = this.cache.getInternalDistributedSystem();
            system.handleResourceEvent(ResourceEvent.REGION_REMOVE, this);
          }

          try {
            LocalRegion parent = this.parentRegion;
            if (parent == null) {
              this.cache.removeRoot(this);
            } else {
              parent.subregions.remove(this.regionName, this);
            }
          } catch (CancelException e) {
            // I don't think this should ever happens: bulletproofing for bug 39454
            if (!this.cache.forcedDisconnect()) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.LocalRegion_BASICDESTROYREGION_PARENT_REMOVAL_FAILED_DUE_TO_CACHE_CLOSURE_REGION_0,
                  getFullPath()), e);
            }
          }

        } finally {
          // ensure that destroy events are dispatched
          if (!cancelledByCacheWriterException) {
            // We only need to notify bridgeClients of the top level region destroy
            // which it will take and do a localRegionDestroy.
            // So we pass it event and NOT eventSet
            event.setEventType(EnumListenerEvent.AFTER_REGION_DESTROY);
            notifyBridgeClients(event);
          }

          // call sendPendingRegionDestroyEvents even if cancelledByCacheWriterException
          // since some of the destroys happened.
          if (eventSet != null && callbackEvents) {
            try {
              sendPendingRegionDestroyEvents(eventSet);
            } catch (CancelException ignore) {
              // ignore, we're mute.
            }
          }
        }

      } finally {
        if (acquiredLock) {
          try {
            releaseDestroyLock();
          } catch (CancelException ignore) {
            // ignore
          }
        }
      }

    } finally {
      this.cache.getTXMgr().unpauseTransaction(tx);
    }
  }

  protected void preDestroyChecks() {
    // do nothing
  }

  protected void distributeDestroyRegion(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    // do nothing
  }

  public static final float DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE = 80.0f;

  /**
   * Called after this region has been completely created
   *
   * @since GemFire 5.0
   *
   * @see DistributedRegion#postDestroyRegion(boolean, RegionEventImpl)
   */
  @Override
  public void postCreateRegion() {
    if (getEvictionAttributes().getAlgorithm().isLRUHeap()) {
      final LogWriter logWriter = this.cache.getLogger();
      float evictionPercentage = DEFAULT_HEAPLRU_EVICTION_HEAP_PERCENTAGE;

      // This is new to 6.5. If a heap lru region is created
      // we make sure that the eviction percentage is enabled.
      InternalResourceManager rm = this.cache.getInternalResourceManager();
      if (!getOffHeap()) {
        if (!rm.getHeapMonitor().hasEvictionThreshold()) {
          float criticalPercentage = rm.getCriticalHeapPercentage();
          if (criticalPercentage > 0.0f) {
            if (criticalPercentage >= 10.f) {
              evictionPercentage = criticalPercentage - 5.0f;
            } else {
              evictionPercentage = criticalPercentage;
            }
          }
          rm.setEvictionHeapPercentage(evictionPercentage);
          if (logWriter.fineEnabled()) {
            logWriter
                .fine("Enabled heap eviction at " + evictionPercentage + " percent for LRU region");
          }
        }

      } else {
        if (!rm.getOffHeapMonitor().hasEvictionThreshold()) {
          float criticalPercentage = rm.getCriticalOffHeapPercentage();
          if (criticalPercentage > 0.0f) {
            if (criticalPercentage >= 10.f) {
              evictionPercentage = criticalPercentage - 5.0f;
            } else {
              evictionPercentage = criticalPercentage;
            }
          }
          rm.setEvictionOffHeapPercentage(evictionPercentage);
          if (logWriter.fineEnabled()) {
            logWriter.fine(
                "Enabled off-heap eviction at " + evictionPercentage + " percent for LRU region");
          }
        }
      }
    }

    if (!isInternalRegion()) {
      getCachePerfStats().incRegions(1);
      if (getMembershipAttributes().hasRequiredRoles()) {
        getCachePerfStats().incReliableRegions(1);
      }
    }

    if (hasListener()) {
      RegionEventImpl event =
          new RegionEventImpl(this, Operation.REGION_CREATE, null, false, getMyId());
      dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CREATE, event);
    }
    releaseAfterRegionCreateEventLatch();
    SystemMemberCacheEventProcessor.send(getCache(), this, Operation.REGION_CREATE);
    initializingRegion.remove();
  }

  /**
   * This method is invoked after isDestroyed has been set to true
   */
  protected void postDestroyRegion(boolean destroyDiskRegion, RegionEventImpl event) {
    if (this.diskRegion != null) {
      if (destroyDiskRegion) {
        this.diskRegion.endDestroy(this);
      } else {
        this.diskRegion.close(this);
      }
    }
    if (this.versionVector != null) {
      try {
        this.cache.getDistributionManager().removeMembershipListener(this.versionVector);
      } catch (CancelException ignore) {
        // ignore: cache close will remove the membership listener
      }
    }
  }

  /**
   * @param cacheWrite true if cacheWrite should be performed or false if cacheWrite should not be
   *        performed
   * @see DistributedRegion#basicDestroy(EntryEventImpl, boolean, Object)
   */
  @Override
  public void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
      Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException, TimeoutException {

    if (!event.isOriginRemote()) {
      checkIfReplicatedAndLocalDestroy(event);
    }

    if (hasSeenEvent(event)) {
      assert getJTAEnlistedTX() == null;
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "LR.basicDestroy: this cache has already seen this event {}", event);
      }
      if (this.getConcurrencyChecksEnabled() && event.getVersionTag() != null
          && !event.getVersionTag().isRecorded()) {
        getVersionVector().recordVersion((InternalDistributedMember) event.getDistributedMember(),
            event.getVersionTag());
      }
      // Bug 49449: When client retried and returned with hasSeenEvent for both LR and DR, the
      // server should still
      // notifyGatewayHubs even the event could be duplicated in gateway queues1
      notifyGatewaySender(EnumListenerEvent.AFTER_DESTROY, event);
      return;
    }

    discoverJTA();
    getDataView().destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  protected final boolean restoreSetOperationTransactionBehavior =
      SystemPropertyHelper.restoreSetOperationTransactionBehavior();

  /**
   * Do the expensive work of discovering an existing JTA transaction Only needs to be called at
   * Region.Entry entry points e.g. Region.put, Region.invalidate, etc.
   *
   * @since GemFire tx
   */
  void discoverJTA() {
    if (!isSecret() && !isUsedForPartitionedRegionAdmin() && !isUsedForMetaRegion()) {
      // prevent internal regions from participating in a TX
      getJTAEnlistedTX();
    }
  }

  private boolean isTransactionPaused() {
    TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    return txMgr.isTransactionPaused();
  }

  private boolean isJTAPaused() {
    TXManagerImpl txMgr = (TXManagerImpl) getCache().getCacheTransactionManager();
    return txMgr.isJTAPaused();
  }

  /**
   * @return true if a transaction is in process
   * @since GemFire tx
   */
  public boolean isTX() {
    return getTXState() != null;
  }

  /**
   * @param expectedOldValue if this is non-null, only destroy if key exists and old value is equal
   *        to expectedOldValue
   * @return true if a the destroy was done; false if it was not needed
   */
  @Override
  public boolean mapDestroy(final EntryEventImpl event, final boolean cacheWrite,
      final boolean isEviction, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {

    final boolean inGII = lockGII();
    try { // make sure unlockGII is called for bug 40001
      return mapDestroy(event, cacheWrite, isEviction, expectedOldValue, inGII, false);
    } finally {
      if (inGII) {
        unlockGII();
      }
    }
  }

  private boolean mapDestroy(final EntryEventImpl event, final boolean cacheWrite,
      final boolean isEviction, Object expectedOldValue, boolean needTokensForGII,
      boolean removeRecoveredEntry) {

    // When register interest is in progress ,
    // We should not remove the key from the
    // region and instead replace the value
    // in the map with a DESTROYED token
    final boolean inRI = !needTokensForGII && !event.isFromRILocalDestroy() && lockRIReadLock();

    // at this point riCnt is guaranteed to be correct and we know for sure
    // whether a RI is in progress and that riCnt will not change during this
    // destroy operation
    try {
      final boolean needRIDestroyToken = inRI && this.riCnt > 0;
      final boolean inTokenMode = needTokensForGII || needRIDestroyToken;

      // the following will call basicDestroyPart2 at the correct moment
      return this.entries.destroy(event, inTokenMode, needRIDestroyToken, cacheWrite, isEviction,
          expectedOldValue, removeRecoveredEntry);

    } catch (ConcurrentCacheModificationException ignore) {
      // this can happen in a client/server cache when another thread
      // managed to slip in its version info to the region entry before this
      // thread got around to doing so
      if (logger.isDebugEnabled()) {
        logger.debug("caught concurrent modification attempt when applying {}", event);
      }
      // Notify clients only if its NOT a gateway event.
      if (event.getVersionTag() != null && !event.getVersionTag().isGatewayTag()) {
        notifyBridgeClients(event);
        notifyGatewaySender(EnumListenerEvent.AFTER_DESTROY, event);
      }
      return true; // event was elided

    } catch (DiskAccessException dae) {
      handleDiskAccessException(dae);
      throw dae;

    } finally {
      if (inRI) {
        unlockRIReadLock();
      }
    }
  }

  /**
   * Return true if dae was caused by a RegionDestroyedException. This was added for bug 39603.
   */
  static boolean causedByRDE(DiskAccessException diskAccessException) {
    boolean result = false;
    if (diskAccessException != null) {
      Throwable cause = diskAccessException.getCause();
      while (cause != null) {
        if (cause instanceof RegionDestroyedException) {
          result = true;
          break;
        }
        cause = cause.getCause();
      }
    }
    return result;
  }

  @Override
  public void handleDiskAccessException(DiskAccessException dae) {
    handleDiskAccessException(dae, false);
  }

  /**
   * @param dae DiskAccessException encountered by the thread
   * @param duringInitialization indicates that this exception occurred during region
   *        initialization. Instead of closing the cache here, we rely on the region initialization
   *        to clean things up.
   * @see DistributedRegion#initialize(InputStream, InternalDistributedMember,
   *      InternalRegionArguments)
   * @see LocalRegion#initialize(InputStream, InternalDistributedMember, InternalRegionArguments)
   * @see InitialImageOperation#processChunk
   */
  public void handleDiskAccessException(DiskAccessException dae, boolean duringInitialization) {

    if (duringInitialization && !(dae instanceof ConflictingPersistentDataException)) {
      return;
    }

    if (causedByRDE(dae)) {
      return;
    }

    // log the error
    StringId sid =
        LocalizedStrings.LocalRegion_A_DISKACCESSEXCEPTION_HAS_OCCURRED_WHILE_WRITING_TO_THE_DISK_FOR_REGION_0_THE_CACHE_WILL_BE_CLOSED;
    logger.error(LocalizedMessage.create(sid, this.fullPath), dae);

    // forward the error to the disk store
    getDiskStore().handleDiskAccessException(dae);
  }

  void expireDestroy(final EntryEventImpl event, final boolean cacheWrite) {
    basicDestroy(event, cacheWrite, null);
  }

  void expireInvalidate(final EntryEventImpl event) {
    basicInvalidate(event);
  }

  /**
   * Creates an event for EVICT_DESTROY operations. It is intended that this method be overridden to
   * allow for special handling of Partitioned Regions.
   *
   * @param key - the key that this event is related to
   * @return an event for EVICT_DESTROY
   */
  @Retained
  protected EntryEventImpl generateEvictDestroyEvent(final Object key) {
    @Retained
    EntryEventImpl event = EntryEventImpl.create(this, Operation.EVICT_DESTROY, key,
        null/* newValue */, null, false, getMyId());

    // Fix for bug#36963
    if (generateEventID()) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    return event;
  }

  /**
   * @return true if the evict destroy was done; false if it was not needed
   */
  boolean evictDestroy(EvictableEntry entry) {
    checkReadiness();

    @Released
    final EntryEventImpl event = generateEvictDestroyEvent(entry.getKey());

    try {
      return mapDestroy(event, false, // cacheWrite
          true, // isEviction
          null); // expectedOldValue
    } catch (CacheWriterException error) {
      throw new Error(
          LocalizedStrings.LocalRegion_CACHE_WRITER_SHOULD_NOT_HAVE_BEEN_CALLED_FOR_EVICTDESTROY
              .toLocalizedString(),
          error);
    } catch (TimeoutException anotherError) {
      throw new Error(
          LocalizedStrings.LocalRegion_NO_DISTRIBUTED_LOCK_SHOULD_HAVE_BEEN_ATTEMPTED_FOR_EVICTDESTROY
              .toLocalizedString(),
          anotherError);
    } catch (EntryNotFoundException yetAnotherError) {
      throw new Error(
          LocalizedStrings.LocalRegion_ENTRYNOTFOUNDEXCEPTION_SHOULD_BE_MASKED_FOR_EVICTDESTROY
              .toLocalizedString(),
          yetAnotherError);
    } finally {
      event.release();
    }
  }

  /**
   * Called by lower levels {@link AbstractRegionMap} while holding the entry synchronization
   * <bold>and</bold> while the entry remains in the map. Once the entry is removed from the map,
   * then other operations synchronize on a new entry, allow for ordering problems between
   * {@link #create(Object, Object, Object)} and {@link #destroy(Object, Object)} operations.
   *
   * @param entry the Region entry being destroyed
   * @param event the event describing the destroy operation
   * @since GemFire 5.1
   */
  @Override
  public void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event) {
    // do nothing
  }

  /**
   * Called by lower levels, while still holding the write sync lock, and the low level has
   * completed its part of the basic destroy
   */
  @Override
  public void basicDestroyPart2(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean conflictWithClear, boolean duringRI, boolean invokeCallbacks) {
    if (!(this instanceof HARegion)) {
      if (logger.isTraceEnabled()) {
        logger.trace("basicDestroyPart2(inTokenMode={},conflictWithClear={},duringRI={}) event={}",
            inTokenMode, conflictWithClear, duringRI, event);
      }
    }
    VersionTag v = event.getVersionTag();

    /*
     * destroys that are not part of the cleaning out of keys prior to a register-interest are
     * marked with Tombstones instead of Destroyed tokens so that they are not reaped after the RI
     * completes. RI does not create Tombstones because it would flood the TombstoneService with
     * unnecessary work.
     */
    if (inTokenMode && !(this.getConcurrencyChecksEnabled() || event.isFromRILocalDestroy())) {
      if (re.isDestroyed()) {
        getImageState().addDestroyedEntry(event.getKey());
        if (!(this instanceof HARegion)) {
          if (logger.isTraceEnabled()) {
            logger.trace("basicDestroy: {}--> Token.DESTROYED", event.getKey());
          }
        }
      }
    } else {
      if (this.getConcurrencyChecksEnabled() && !(this instanceof HARegion)) {
        if (logger.isDebugEnabled()) {
          logger.debug("basicDestroyPart2: {}, version={}", event.getKey(), v);
        }
      }
    }

    /*
     * this is too late to do index maintenance with a CompactRangeIndex because we need to have the
     * old value still intact. At this point the old value has already be replaced with a destroyed
     * token.
     */

    if (event.isBulkOpInProgress() && this.isUsedForPartitionedRegionBucket) {
      if (logger.isDebugEnabled()) {
        logger.debug("For bulk operation on bucket region, not to notify gateway sender earlier.");
      }
    } else {
      notifyGatewaySender(EnumListenerEvent.AFTER_DESTROY, event);
    }

    // invoke callbacks if initialized and told to do so, or if this is a bucket in a partitioned
    // region
    if (invokeCallbacks && !event.isBulkOpInProgress()) {
      if (isInitialized() && (!inTokenMode || duringRI) || this.isUsedForPartitionedRegionBucket) {
        try {
          re.dispatchListenerEvents(event);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
          this.stopper.checkCancelInProgress(null);
        }
      } else {
        event.callbacksInvoked(true);
      }
    }
  }

  /**
   * distribution and callback notification are done in part2 inside entry lock for maintaining the
   * order of events.
   */
  @Override
  public void basicDestroyPart3(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean duringRI, boolean invokeCallbacks, Object expectedOldValue) {

    if (invokeCallbacks) {
      if (event.isBulkOpInProgress()) {
        event.getRemoveAllOperation().addEntry(event);
      }
    }
    if (!inTokenMode || duringRI) {
      updateStatsForDestroy();
    }
    if (this.entryUserAttributes != null) {
      this.entryUserAttributes.remove(event.getKey());
    }
  }

  /**
   * Update stats
   */
  private void updateStatsForDestroy() {
    getCachePerfStats().incDestroys();
  }

  void txClearRegion() {
    TXStateInterface tx = getJTAEnlistedTX();
    if (tx != null) {
      tx.rmRegion(this);
    }
  }

  @Override
  public void invokeDestroyCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways) {

    // The spec for ConcurrentMap support requires that operations be mapped
    // to non-CM counterparts
    if (event.getOperation() == Operation.REMOVE) {
      event.setOperation(Operation.DESTROY);
    }
    event.setEventType(eventType);
    notifyBridgeClients(event);
    if (notifyGateways) {
      notifyGatewaySender(eventType, event);
    }
    if (callDispatchListenerEvent && !event.getIsRedestroyedEntry()) {
      dispatchListenerEvent(eventType, event);
    }
  }

  @Override
  public void invokeTXCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent) {

    // The spec for ConcurrentMap support requires that operations be mapped
    // to non-CM counterparts

    Operation operation = event.getOperation();

    if (logger.isDebugEnabled()) {
      logger.debug("invokeTXCallbacks for event {}", event);
    }

    if (operation == Operation.REMOVE) {
      event.setOperation(Operation.DESTROY);
    } else if (operation == Operation.PUT_IF_ABSENT) {
      event.setOperation(Operation.CREATE);
    } else if (operation == Operation.REPLACE) {
      event.setOperation(Operation.UPDATE);
    }
    event.setEventType(eventType);
    notifyBridgeClients(event);
    notifyGatewaySender(eventType, event);
    if (callDispatchListenerEvent) {
      if (event.getInvokePRCallbacks() || !(event.getRegion() instanceof PartitionedRegion)
          && !event.getRegion().isUsedForPartitionedRegionBucket()) {
        dispatchListenerEvent(eventType, event);
      }
    }
  }

  /**
   * @param key the key of the entry to destroy
   * @param rmtOrigin true if transaction being applied had a remote origin
   * @param event filled in if operation performed
   * @param needTokensForGII true if caller has determined we are in destroy token mode and will
   *        keep us in that mode while this call is executing.
   * @param isOriginRemote whether the event originated in a peer or in this vm
   * @param txEntryState for passing up versionTag - only on near side
   * @param versionTag tag generated by txCoordinator - only on far side
   * @param tailKey tail (shadow) key generated by txCoordinator for WAN - only on farside
   */
  @Override
  public void txApplyDestroy(Object key, TransactionId rmtOrigin, TXRmtEvent event,
      boolean needTokensForGII, Operation op, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, boolean isOriginRemote, TXEntryState txEntryState,
      VersionTag versionTag, long tailKey) {

    final boolean inRI = !needTokensForGII && lockRIReadLock();
    final boolean needRIDestroyToken = inRI && this.riCnt > 0;

    try {
      final boolean inTokenMode = needTokensForGII || needRIDestroyToken;
      this.entries.txApplyDestroy(key, rmtOrigin, event, inTokenMode, needRIDestroyToken, op,
          eventId, aCallbackArgument, pendingCallbacks, filterRoutingInfo, bridgeContext,
          isOriginRemote, txEntryState, versionTag, tailKey);
    } finally {
      if (inRI) {
        unlockRIReadLock();
      }
    }
  }

  /**
   * Called by lower levels, while still holding the write sync lock, and the low level has
   * completed its part of the basic destroy
   */
  void txApplyDestroyPart2(RegionEntry re, Object key, boolean inTokenMode, boolean clearConflict,
      boolean alreadyDestroyedOrRemoved) {
    if (this.testCallable != null) {
      this.testCallable.call(this, Operation.DESTROY, re);
    }
    if (inTokenMode) {
      getImageState().addDestroyedEntry(key);
    } else if (!alreadyDestroyedOrRemoved) {
      updateStatsForDestroy();
    }
    if (this.entryUserAttributes != null) {
      this.entryUserAttributes.remove(key);
    }
  }

  void basicInvalidateRegion(RegionEventImpl event) {
    final TXStateProxy tx = this.cache.getTXMgr().pauseTransaction();
    try {
      this.setRegionInvalid(true);
      getImageState().setRegionInvalidated(true);
      invalidateAllEntries(event);

      Set allSubregions = subregions(true);
      for (Object allSubregion : allSubregions) {
        LocalRegion region = (LocalRegion) allSubregion;
        region.setRegionInvalid(true);
        try {
          region.getImageState().setRegionInvalidated(true);
          region.invalidateAllEntries(event);

          if (!region.isInitialized()) {
            continue; // don't invoke callbacks if not initialized yet
          }

          if (region.hasListener()) {
            RegionEventImpl event2 = (RegionEventImpl) event.clone();
            event2.region = region;
            region.dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_INVALIDATE, event2);
          }
        } catch (RegionDestroyedException ignore) {
          // ignore subregions that have been destroyed to fix bug 33276
        }
      }

      if (!isInitialized()) {
        return;
      }

      event.setEventType(EnumListenerEvent.AFTER_REGION_INVALIDATE);
      notifyBridgeClients(event);

      boolean hasListener = hasListener();
      if (logger.isDebugEnabled()) {
        logger.debug("basicInvalidateRegion: hasListener = {}", hasListener);
      }
      if (hasListener) {
        dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_INVALIDATE, event);
      }

    } finally {
      this.cache.getTXMgr().unpauseTransaction(tx);
    }
  }

  /**
   * Determines whether the receiver is unexpired with regard to the given timeToLive and idleTime
   * attributes, which may different from this entry's actual attributes. Used for validation of
   * objects during netSearch(), which must validate remote entries against local timeout
   * attributes.
   */
  boolean isExpiredWithRegardTo(Object key, int ttl, int idleTime) {
    if (!getAttributes().getStatisticsEnabled()) {
      return false;
    }

    long expTime;
    try {
      expTime = new NetSearchExpirationCalculator(this, key, ttl, idleTime).getExpirationTime();
    } catch (EntryNotFoundException ignore) {
      return true;
    }

    return expTime != 0 && expTime <= cacheTimeMillis();
  }

  @Override
  public void dispatchListenerEvent(EnumListenerEvent op, InternalCacheEvent event) {
    // Return if the inhibit all notifications flag is set
    boolean isEntryEvent = event instanceof EntryEventImpl;
    if (isEntryEvent) {
      if (((EntryEventImpl) event).inhibitAllNotifications()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Notification inhibited for key {}", event);
        }
        return;
      }
    }

    if (shouldDispatchListenerEvent()) {
      if (logger.isTraceEnabled()) {
        logger.trace("dispatchListenerEvent event={}", event);
      }

      final long start = getCachePerfStats().startCacheListenerCall();

      boolean isOriginRemote = false;
      boolean isOriginRemoteSetOnEvent = false;

      try {
        if (isEntryEvent) {
          if (((EntryEventImpl) event).isSingleHop()) {
            isOriginRemote = event.isOriginRemote();
            ((EntryEventImpl) event).setOriginRemote(true);
            isOriginRemoteSetOnEvent = true;
          }
          RegionEntry regionEntry = ((EntryEventImpl) event).getRegionEntry();
          if (regionEntry != null) {
            ((EntryEventImpl) event).getRegionEntry().setCacheListenerInvocationInProgress(true);
          }
        }

        if (this.cache.getEventThreadPool() == null) {
          dispatchEvent(this, event, op);
        } else {
          final EventDispatcher eventDispatcher = new EventDispatcher(event, op);
          try {
            this.cache.getEventThreadPool().execute(eventDispatcher);
          } catch (RejectedExecutionException ignore) {
            eventDispatcher.release();
            dispatchEvent(this, event, op);
          }
        }

      } finally {
        getCachePerfStats().endCacheListenerCall(start);
        if (isOriginRemoteSetOnEvent) {
          ((EntryEventImpl) event).setOriginRemote(isOriginRemote);
        }
        if (isEntryEvent) {
          RegionEntry regionEntry = ((EntryEventImpl) event).getRegionEntry();
          if (regionEntry != null) {
            regionEntry.setCacheListenerInvocationInProgress(false);
          }
        }
      }
    }
  }

  /** @return true if initialization is complete */
  @Override
  public boolean isInitialized() {
    if (this.initialized) {
      return true;
    }

    StoppableCountDownLatch latch = this.getInitializationLatchAfterGetInitialImage();
    if (latch == null) {
      return true;
    }

    long count = latch.getCount();
    if (count == 0) {
      this.initialized = true;
      return true;
    }

    return false;
  }

  /**
   * @return true if event state has been transfered to this region from another cache
   */
  boolean isEventTrackerInitialized() {
    return getEventTracker().isInitialized();
  }

  public void acquireDestroyLock() {
    LocalRegion root = getRoot();
    boolean acquired = false;
    do {
      this.cache.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        root.destroyLock.acquire();
        acquired = true;
      } catch (InterruptedException ie) {
        interrupted = true;
        this.cache.getCancelCriterion().checkCancelInProgress(ie);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } while (!acquired);
    if (logger.isDebugEnabled()) {
      logger.debug("Acquired Destroy Lock: {}", root);
    }
  }

  public void releaseDestroyLock() {
    LocalRegion root = getRoot();
    if (logger.isDebugEnabled()) {
      logger.debug("Releasing Destroy Lock: {}", root.getName());
    }
    root.destroyLock.release();
  }

  /**
   * Cleans up any resources that may have been allocated for this region during its initialization.
   */
  @Override
  public void cleanupFailedInitialization() {
    // mark as destroyed fixes 49555.
    this.isDestroyed = true;

    // after isDestroyed is set to true call removeResourceListener to fix bug 49555
    this.cache.getInternalResourceManager(false).removeResourceListener(this);

    // fixes bug 41333
    closeEntries();

    this.destroyedSubregionSerialNumbers = collectSubregionSerialNumbers();

    try {
      getEventTracker().stop();

      if (this.diskRegion != null) {
        // This was needed to fix bug 30937
        try {
          this.diskRegion.cleanupFailedInitialization(this);
        } catch (IllegalStateException ignore) {
          // just ignore this exception since whoever called us is going
          // to report the exception that caused initialization to fail.
        }
      }

      // Clean up region in RegionListeners
      this.cache.invokeCleanupFailedInitialization(this);
    } finally {
      // make sure any waiters on initializing Latch are released
      this.releaseLatches();
    }
  }

  LocalRegion getRoot() {
    LocalRegion region = this;
    while (region.parentRegion != null) {
      region = region.parentRegion;
    }
    return region;
  }

  private void initializationFailed(LocalRegion subregion) {
    // bugfix for bug#34883
    synchronized (this.subregionsLock) {
      this.subregions.remove(subregion.getName());
    }
    subregion.cleanupFailedInitialization();
  }

  /**
   * PRECONDITIONS: Synchronized on updateMonitor for this key in order to guarantee write-through
   * to map entry, and key must be in map
   *
   * @param lastModified time, may be 0 in which case uses now instead
   *
   * @return the actual lastModifiedTime used.
   */
  @Override
  public long updateStatsForPut(RegionEntry entry, long lastModified, boolean lruRecentUse) {
    long lastAccessed = cacheTimeMillis();
    if (lruRecentUse) {
      // fix for bug 31102
      entry.setRecentlyUsed(this);
    }
    if (lastModified == 0L) {
      lastModified = lastAccessed;
    }

    entry.updateStatsForPut(lastModified, lastAccessed);

    if (this.statisticsEnabled && !isProxy()) {
      // do not reschedule if there is already a task in the queue.
      // this prevents bloat in the TimerTask since cancelled tasks
      // do not actually get removed from the TimerQueue.
      // When the already existing task gets fired it checks to see
      // if it is premature and if so reschedules a task at that time.
      addExpiryTaskIfAbsent(entry);
    }

    // propagate to region
    setLastModifiedTime(lastModified);
    return lastModified;
  }

  /**
   * Returns a region in the subregion map first, then looks in the reinitializing region registry.
   *
   * @return the region or null if not found, may be destroyed
   */
  private LocalRegion basicGetSubregion(String name) {
    LocalRegion region = toRegion(this.subregions.get(name));

    // don't wait for reinitialization if the init_level for this thread is
    // ANY_INIT: We don't want CreateRegion messages to wait on a future
    // because it would cause a deadlock. If the region is ready for a
    // CreateRegion message, it would have been in the subregions map.

    if (region == null && threadInitLevelRequirement() != ANY_INIT) {
      String thePath = getFullPath() + SEPARATOR + name;
      if (logger.isDebugEnabled()) {
        logger.debug("Trying reinitializing region, fullPath={}", thePath);
      }
      region = (LocalRegion) this.cache.getReinitializingRegion(thePath);
      if (logger.isDebugEnabled()) {
        logger.debug("Reinitialized region is {}", region);
      }
    }

    return region;
  }

  /**
   * Make a LocalRegion from an element in the subregion map Sent to parent region.
   *
   * @return This method may return null or a destroyed region if the region was just destroyed
   */
  private LocalRegion toRegion(Object element) {
    LocalRegion region = (LocalRegion) element;
    if (region != null) {
      // do not return until done initializing (unless this is an initializing thread)
      region.waitOnInitialization();
    }
    return region;
  }

  /**
   * Update the API statistics appropriately for returning this value from get.
   *
   * @param regionEntry the entry whose value was accessed
   */
  public void updateStatsForGet(final RegionEntry regionEntry, final boolean hit) {
    if (!this.statisticsEnabled) {
      return;
    }

    final long now = cacheTimeMillis();
    if (regionEntry != null) {
      regionEntry.updateStatsForGet(hit, now);
      if (isEntryIdleExpiryPossible()) {
        addExpiryTaskIfAbsent(regionEntry);
      }
    }

    // update region stats
    setLastAccessedTime(now, hit);
  }

  private void sendPendingRegionDestroyEvents(Set regionEvents) {
    for (Object regionEvent : regionEvents) {
      RegionEventImpl regionEventImpl = (RegionEventImpl) regionEvent;
      regionEventImpl.region.dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_DESTROY,
          regionEventImpl);
      if (!this.cache.forcedDisconnect()) {
        SystemMemberCacheEventProcessor.send(getCache(), regionEventImpl.getRegion(),
            regionEventImpl.getOperation());
      }
    }
  }

  /** The listener is not closed until after the afterRegionDestroy event */
  protected void closeCallbacksExceptListener() {
    closeCacheCallback(getCacheLoader());
    closeCacheCallback(getCacheWriter());
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      evictionController.close();
    }
  }

  /** This is only done when the cache is closed. */
  private void closeAllCallbacks() {
    closeCallbacksExceptListener();
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners != null) {
      for (final CacheListener listener : listeners) {
        closeCacheCallback(listener);
      }
    }
  }

  /**
   * Release the client connection pool if we have one
   *
   * @since GemFire 5.7
   */
  private void detachPool() {
    ServerRegionProxy serverRegionProxy = getServerProxy();
    if (serverRegionProxy != null) {
      InternalCache internalCache = getCache();
      String poolName = this.getPoolName();
      PoolImpl pool = (PoolImpl) PoolManager.find(this.getPoolName());
      if (poolName != null && pool != null) {
        serverRegionProxy
            .detach(internalCache.keepDurableSubscriptionsAlive() || pool.getKeepAlive());
      } else {
        serverRegionProxy.detach(internalCache.keepDurableSubscriptionsAlive());
      }
    }
  }

  /**
   * Closes the cqs created based on this region (Cache Client/writer/loader).
   */
  private void closeCqs() {
    CqService cqService = getCache().getCqService();
    if (cqService != null) {
      try {
        cqService.closeCqs(getFullPath());
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.warn(
            LocalizedMessage.create(
                LocalizedStrings.LocalRegion_EXCEPTION_OCCURRED_WHILE_CLOSING_CQS_ON_REGION_DESTORY),
            t);
      }
    }
  }

  /**
   * Called when the cache is closed. Behaves just like a Region.close except the operation is
   * CACHE_CLOSE
   */
  @Override
  public void handleCacheClose(Operation operation) {
    RegionEventImpl event =
        new RegionEventImpl(this, operation, null, false, getMyId(), generateEventID());
    if (!this.isDestroyed) { // don't destroy if already destroyed
      try {
        basicDestroyRegion(event, false, true, true);
      } catch (CancelException ignore) {
        // If the region was destroyed we see this because the cache is closing.
        // Since we are trying to close the cache don't get upset if
        // a region was destroyed out from under us
        if (logger.isDebugEnabled()) {
          logger.debug("handleCacheClose: Encountered cache closure while closing region {}",
              getFullPath());
        }
      } catch (RegionDestroyedException ignore) {
        // Since we are trying to close the cache don't get upset if
        // a region was destroyed out from under us
      } catch (CacheWriterException e) {
        // not possible with local operation, CacheWriter not called
        throw new Error(LocalizedStrings.LocalRegion_CACHEWRITEREXCEPTION_SHOULD_NOT_BE_THROWN_HERE
            .toLocalizedString(), e);
      } catch (TimeoutException e) {
        // not possible with local operation, no distributed locks possible
        InternalDistributedSystem ids = getCache().getInternalDistributedSystem();
        if (!ids.isDisconnecting()) {
          throw new InternalGemFireError(
              LocalizedStrings.LocalRegion_TIMEOUTEXCEPTION_SHOULD_NOT_BE_THROWN_HERE
                  .toLocalizedString(),
              e);
        }
      }
    }
  }

  private void checkCacheClosed() {
    if (this.cache.isClosed()) {
      throw this.cache.getCacheClosedException(null, null);
    }
  }

  private void checkRegionDestroyed(boolean checkCancel) {
    if (checkCancel) {
      this.cache.getCancelCriterion().checkCancelInProgress(null);
    }

    if (this.isDestroyed) {
      RegionDestroyedException regionDestroyedException;
      if (this.reinitialized_old) {
        regionDestroyedException = new RegionReinitializedException(toString(), getFullPath());
      } else if (this.cache.isCacheAtShutdownAll()) {
        throw cache.getCacheClosedException("Cache is being closed by ShutdownAll");
      } else {
        regionDestroyedException = new RegionDestroyedException(toString(), getFullPath());
      }

      // Race condition could cause the cache to be destroyed after the
      // cache close check above, so we need to re-check before throwing.
      if (checkCancel) {
        this.cache.getCancelCriterion().checkCancelInProgress(null);
      }

      throw regionDestroyedException;
    }

    if (this.isDestroyedForParallelWAN) {
      throw new RegionDestroyedException(
          LocalizedStrings.LocalRegion_REGION_IS_BEING_DESTROYED_WAITING_FOR_PARALLEL_QUEUE_TO_DRAIN
              .toLocalizedString(),
          getFullPath());
    }
  }

  /**
   * For each region entry in this region call the callback
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public void foreachRegionEntry(RegionEntryCallback callback) {
    for (RegionEntry regionEntry : this.entries.regionEntriesInVM()) {
      callback.handleRegionEntry(regionEntry);
    }
  }

  /**
   * Used by {@link #foreachRegionEntry}.
   *
   * @since GemFire prPersistSprint2
   */
  public interface RegionEntryCallback {
    void handleRegionEntry(RegionEntry regionEntry);
  }

  protected void checkIfReplicatedAndLocalDestroy(EntryEventImpl event) {
    // disallow local invalidation for replicated regions
    if (getScope().isDistributed() && getDataPolicy().withReplication() && !event.isDistributed()
        && !isUsedForSerialGatewaySenderQueue()) {
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_NOT_ALLOWED_TO_DO_A_LOCAL_DESTROY_ON_A_REPLICATED_REGION
              .toLocalizedString());
    }
  }

  /**
   * Return the number of subregions, including this region. Used for recursive size calculation in
   * SubregionsSet.size
   */
  private int allSubregionsSize() {
    int size = 1; /* 1 for this region */
    for (Object regionObject : this.subregions.values()) {
      LocalRegion region = (LocalRegion) regionObject;
      if (region != null && region.isInitialized() && !region.isDestroyed()) {
        size += region.allSubregionsSize();
      }
    }
    return size;
  }

  /**
   * Return the number of entries including in subregions. Used for recursive size calculation in
   * EntriesSet.size. This does not include tombstone entries stored in the region.
   */
  int allEntriesSize() {
    int size = entryCount();
    for (Object regionObject : this.subregions.values()) {
      LocalRegion region = toRegion(regionObject);
      if (region != null && !region.isDestroyed()) {
        size += region.allEntriesSize();
      }
    }
    return size;
  }

  /**
   * @param rgnEvent the RegionEvent for region invalidation
   */
  protected void invalidateAllEntries(RegionEvent rgnEvent) {
    Operation operation = Operation.LOCAL_INVALIDATE;
    if (rgnEvent.getOperation().isDistributed()) {
      operation = Operation.INVALIDATE;
    }

    // if this is a local invalidation, then set local invalid flag on event
    // so LOCAL_INVALID tokens is used (even though each individual entry
    // invalidation is not distributed).

    // region operation so it is ok to ignore tx state
    for (Object keyObject : keySet()) {
      try {

        // EventID will not be generated by this constructor
        @Released
        EntryEventImpl event = EntryEventImpl.create(this, operation, keyObject, null, null,
            rgnEvent.isOriginRemote(), rgnEvent.getDistributedMember());

        try {
          event.setLocalInvalid(!rgnEvent.getOperation().isDistributed());
          basicInvalidate(event, false);
        } finally {
          event.release();
        }

      } catch (EntryNotFoundException ignore) {
        // ignore
      }
    }
  }

  boolean hasListener() {
    CacheListener[] listeners = fetchCacheListenersField();
    return listeners != null && listeners.length > 0;
  }

  private final DiskStoreImpl diskStoreImpl;

  @Override
  public DiskStoreImpl getDiskStore() {
    return this.diskStoreImpl;
  }

  /**
   * Return true if all disk attributes are defaults. DWA.isSynchronous can be true or false.
   */
  private boolean useDefaultDiskStore() {
    assert getDiskStoreName() == null;
    if (!Arrays.equals(getDiskDirs(), DefaultDiskDirs.getDefaultDiskDirs())) {
      return false;
    }
    if (!Arrays.equals(getDiskDirSizes(), DiskStoreFactory.DEFAULT_DISK_DIR_SIZES)) {
      return false;
    }

    DiskWriteAttributesFactory attributesFactory = new DiskWriteAttributesFactory();
    attributesFactory.setSynchronous(false);
    if (attributesFactory.create().equals(getDiskWriteAttributes())) {
      return true;
    }
    attributesFactory.setSynchronous(true);

    return attributesFactory.create().equals(getDiskWriteAttributes());
  }

  /**
   * Returns true if this region's config indicates that it will use a disk store. Added for bug
   * 42055.
   */
  protected boolean usesDiskStore(RegionAttributes regionAttributes) {
    return !isProxy() && (getAttributes().getDataPolicy().withPersistence() || isOverflowEnabled());
  }

  protected DiskStoreImpl findDiskStore(RegionAttributes regionAttributes,
      InternalRegionArguments internalRegionArgs) {
    // validate that persistent type registry is persistent
    if (getAttributes().getDataPolicy().withPersistence()) {
      getCache().getPdxRegistry().creatingPersistentRegion();
    }

    if (usesDiskStore(regionAttributes)) {
      if (getDiskStoreName() != null) {
        DiskStoreImpl diskStore =
            (DiskStoreImpl) getGemFireCache().findDiskStore(getDiskStoreName());
        if (diskStore == null) {
          throw new IllegalStateException(LocalizedStrings.CacheCreation_DISKSTORE_NOTFOUND_0
              .toLocalizedString(getDiskStoreName()));
        }
        return diskStore;

      } else if (useDefaultDiskStore()) {
        return getGemFireCache().getOrCreateDefaultDiskStore();

      } else {
        // backwards compat mode
        DiskStoreFactory diskStoreFactory = getGemFireCache().createDiskStoreFactory();
        diskStoreFactory.setDiskDirsAndSizes(getDiskDirs(), getDiskDirSizes());
        DiskWriteAttributes dwa = getDiskWriteAttributes();
        diskStoreFactory.setAutoCompact(dwa.isRollOplogs());
        diskStoreFactory.setMaxOplogSize(dwa.getMaxOplogSize());
        diskStoreFactory.setTimeInterval(dwa.getTimeInterval());

        if (dwa.getBytesThreshold() > 0) {
          diskStoreFactory.setQueueSize(1);
        } else {
          diskStoreFactory.setQueueSize(0);
        }

        DiskStoreFactoryImpl diskStoreFactoryImpl = (DiskStoreFactoryImpl) diskStoreFactory;
        return diskStoreFactoryImpl.createOwnedByRegion(getFullPath().replace('/', '_'),
            this instanceof PartitionedRegion, internalRegionArgs);
      }
    }

    return null;
  }

  /**
   * Creates a new {@code DiskRegion} for this region. We assume that the attributes and the name of
   * the region have been set.
   *
   * @return {@code null} is a disk region is not desired
   *
   * @since GemFire 3.2
   */
  protected DiskRegion createDiskRegion(InternalRegionArguments internalRegionArgs)
      throws DiskAccessException {

    if (internalRegionArgs.getDiskRegion() != null) {
      DiskRegion region = internalRegionArgs.getDiskRegion();
      region.createDataStorage();
      return region;
    }

    if (this.diskStoreImpl == null) {
      return null;
    }

    DiskRegionStats stats;

    if (this instanceof BucketRegion) {
      stats = internalRegionArgs.getPartitionedRegion().getDiskRegionStats();
    } else {
      stats = new DiskRegionStats(getCache().getDistributedSystem(), getFullPath());
    }

    EnumSet<DiskRegionFlag> diskFlags = EnumSet.noneOf(DiskRegionFlag.class);

    // Add flag if this region has versioning enabled
    if (this.getAttributes().getConcurrencyChecksEnabled()) {
      diskFlags.add(DiskRegionFlag.IS_WITH_VERSIONING);
    }

    // TODO: fix NO_PARTITITON typo
    return DiskRegion.create(this.diskStoreImpl, getFullPath(), false,
        getDataPolicy().withPersistence(), isOverflowEnabled(), isDiskSynchronous(), stats,
        getCancelCriterion(), this, getAttributes(), diskFlags, "NO_PARTITITON", -1,
        getCompressor(), getOffHeap());
  }

  /**
   * Returns the object sizer on this region or null if it has no sizer.
   *
   * @since GemFire 6.1.2.9
   */
  @Override
  public ObjectSizer getObjectSizer() {
    ObjectSizer result = null;
    EvictionAttributes ea = getEvictionAttributes();
    if (ea != null) {
      result = ea.getObjectSizer();
    }
    return result;
  }

  /**
   * Add the Region TTL expiry task to the scheduler
   */
  void addTTLExpiryTask() {
    synchronized (this.regionExpiryLock) {
      RegionTTLExpiryTask task = this.regionTTLExpiryTask;
      if (task != null) {
        task.cancel();
      }

      if (this.regionTimeToLive > 0) {
        this.regionTTLExpiryTask = (RegionTTLExpiryTask) this.cache.getExpirationScheduler()
            .addExpiryTask(new RegionTTLExpiryTask(this));
        if (this.regionTTLExpiryTask != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Initialized Region TTL Expiry Task {}", this.regionTTLExpiryTask);
          }
        }

      } else {
        this.regionTTLExpiryTask = null;
      }
    }
  }

  void addTTLExpiryTask(RegionTTLExpiryTask callingTask) {
    synchronized (this.regionExpiryLock) {
      if (this.regionTTLExpiryTask != null && this.regionTTLExpiryTask != callingTask) {
        return;
      }
      if (this.regionTimeToLive <= 0) {
        this.regionTTLExpiryTask = null;
        return;
      }

      RegionTTLExpiryTask task = new RegionTTLExpiryTask(this);
      if (logger.isDebugEnabled()) {
        logger.debug("Scheduling Region TTL Expiry Task {} which replaces {}", task,
            this.regionTTLExpiryTask);
      }
      this.regionTTLExpiryTask =
          (RegionTTLExpiryTask) this.cache.getExpirationScheduler().addExpiryTask(task);
    }
  }

  /**
   * Add the Region Idle expiry task to the scheduler
   */
  void addIdleExpiryTask() {
    synchronized (this.regionExpiryLock) {
      RegionIdleExpiryTask task = this.regionIdleExpiryTask;
      if (task != null) {
        task.cancel();
      }

      if (this.regionIdleTimeout > 0) {
        this.regionIdleExpiryTask = (RegionIdleExpiryTask) this.cache.getExpirationScheduler()
            .addExpiryTask(new RegionIdleExpiryTask(this));
        if (this.regionIdleExpiryTask != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Initialized Region Idle Expiry Task {}", this.regionIdleExpiryTask);
          }
        }

      } else {
        this.regionIdleExpiryTask = null;
      }
    }
  }

  void addIdleExpiryTask(RegionIdleExpiryTask callingTask) {
    synchronized (this.regionExpiryLock) {
      if (this.regionIdleExpiryTask != null && this.regionIdleExpiryTask != callingTask) {
        return;
      }
      if (this.regionIdleTimeout <= 0) {
        this.regionIdleExpiryTask = null;
        return;
      }

      RegionIdleExpiryTask task = new RegionIdleExpiryTask(this);
      if (logger.isDebugEnabled()) {
        logger.debug("Scheduling Region Idle Expiry Task {} which replaces {}", task,
            this.regionIdleExpiryTask);
      }
      this.regionIdleExpiryTask =
          (RegionIdleExpiryTask) this.cache.getExpirationScheduler().addExpiryTask(task);
    }
  }

  /**
   * Added to fix bug 31204
   */
  protected boolean isEntryIdleExpiryPossible() {
    return this.entryIdleTimeout > 0 || this.customEntryIdleTimeout != null;
  }

  private void cancelTTLExpiryTask() {
    RegionTTLExpiryTask task;
    synchronized (this.regionExpiryLock) {
      task = this.regionTTLExpiryTask;
      if (task != null) {
        this.regionTTLExpiryTask = null;
      }
    }
    if (task != null) {
      task.cancel();
    }
  }

  private void cancelIdleExpiryTask() {
    RegionIdleExpiryTask task;
    synchronized (this.regionExpiryLock) {
      task = this.regionIdleExpiryTask;
      if (task != null) {
        this.regionIdleExpiryTask = null;
      }
    }
    if (task != null) {
      task.cancel();
    }
  }

  @Override
  protected void regionTimeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    addTTLExpiryTask();
  }

  @Override
  protected void regionIdleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
    addIdleExpiryTask();
  }

  @Override
  protected void timeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    int oldTimeout = oldTimeToLive.getTimeout();
    if (this.customEntryTimeToLive != null) {
      rescheduleEntryExpiryTasks();
    }
    if (this.entryTimeToLive > 0 && (oldTimeout == 0 || this.entryTimeToLive < oldTimeout)) {
      rescheduleEntryExpiryTasks();
    }
    // else it's safe to let them get rescheduled lazily, as the old expiration time will cause the
    // tasks to fire sooner than the new ones.
  }

  @Override
  protected void idleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
    int oldTimeout = oldIdleTimeout.getTimeout();
    if (this.customEntryIdleTimeout != null) {
      rescheduleEntryExpiryTasks();
    }
    if (this.entryIdleTimeout > 0 && (oldTimeout == 0 || this.entryIdleTimeout < oldTimeout)) {
      rescheduleEntryExpiryTasks();
    }
    // else it's safe to let them get rescheduled lazily, as the old expiration time will cause the
    // tasks to fire sooner than the new ones.
  }

  void rescheduleEntryExpiryTasks() {
    if (isProxy()) {
      return;
    }
    if (!isInitialized()) {
      return; // don't schedule expiration until region is initialized (bug
    }
    if (!isEntryExpiryPossible()) {
      return;
    }
    // OK to ignore transaction since Expiry only done non-tran
    Iterator<RegionEntry> it = this.entries.regionEntries().iterator();
    if (it.hasNext()) {
      ExpiryTask.doWithNowSet(this, () -> {
        while (it.hasNext()) {
          addExpiryTask(it.next());
        }
      });
    }
  }

  @Override
  public void addExpiryTaskIfAbsent(RegionEntry entry) {
    addExpiryTask(entry, true);
  }

  void addExpiryTask(RegionEntry re) {
    addExpiryTask(re, false);
  }

  /**
   * Used to create a cheap Region.Entry that can be passed to the CustomExpiry callback
   */
  private static class ExpiryRegionEntry implements Region.Entry {
    private final LocalRegion region;
    private final RegionEntry regionEntry;

    ExpiryRegionEntry(LocalRegion region, RegionEntry regionEntry) {
      this.region = region;
      this.regionEntry = regionEntry;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (this.regionEntry == null ? 0 : this.regionEntry.hashCode());
      result = prime * result + (this.region == null ? 0 : this.region.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ExpiryRegionEntry other = (ExpiryRegionEntry) obj;
      if (this.regionEntry == null) {
        if (other.regionEntry != null) {
          return false;
        }
      } else if (!this.regionEntry.equals(other.regionEntry)) {
        return false;
      }
      if (this.region == null) {
        if (other.region != null) {
          return false;
        }
      } else if (!this.region.equals(other.region))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "region=" + this.region.getFullPath() + ", key=" + getKey() + " value=" + getValue();
    }

    @Override
    public Region getRegion() {
      return this.region;
    }

    /**
     * Returns the entry's RegionEntry if it "checks" out. The check is to see if the region entry
     * still exists.
     *
     * @throws EntryNotFoundException if the RegionEntry has been removed.
     */
    private RegionEntry getCheckedRegionEntry() throws EntryNotFoundException {
      if (this.regionEntry.isDestroyedOrRemoved()) {
        throw new EntryNotFoundException(
            "Entry for key " + this.regionEntry.getKey() + " no longer exists");
      }
      return this.regionEntry;
    }

    @Override
    public Object getValue() {
      Object value =
          this.region.getDeserialized(getCheckedRegionEntry(), false, false, false, false);
      if (value == null) {
        throw new EntryDestroyedException(getKey().toString());
      } else if (Token.isInvalid(value)) {
        return null;
      }
      return value;
    }

    @Override
    public boolean isLocal() {
      return true; // we only create expiry tasks for local entries
    }

    @Override
    public CacheStatistics getStatistics() {
      LocalRegion lr = this.region;
      if (!lr.statisticsEnabled) {
        throw new StatisticsDisabledException(
            LocalizedStrings.LocalRegion_STATISTICS_DISABLED_FOR_REGION_0
                .toLocalizedString(lr.getFullPath()));
      }
      return new CacheStatisticsImpl(getCheckedRegionEntry(), lr);
    }

    @Override
    public Object getUserAttribute() {
      Map userAttr = this.region.entryUserAttributes;
      if (userAttr == null) {
        return null;
      }
      return userAttr.get(getKey());
    }

    @Override
    public Object setUserAttribute(Object userAttribute) {
      LocalRegion lr = this.region;
      if (lr.entryUserAttributes == null) {
        lr.entryUserAttributes = new ConcurrentHashMap();
      }
      return lr.entryUserAttributes.put(getKey(), userAttribute);
    }

    @Override
    public boolean isDestroyed() {
      return this.regionEntry.isDestroyedOrRemoved();
    }

    @Override
    public Object setValue(Object value) {
      return this.region.put(getKey(), value);
    }

    @Override
    public Object getKey() {
      return this.regionEntry.getKey();
    }
  }

  /**
   * If custom expiration returns non-null expiration attributes then create a CustomEntryExpiryTask
   * for this region and the given entry and return it. Otherwise if the region is configured for
   * expiration then create an EntryExpiryTask for this region and the given entry and return it.
   * Null is returned if the expiration attributes indicate that expiration is disabled.
   */
  private EntryExpiryTask createExpiryTask(RegionEntry regionEntry) {
    if (regionEntry == null || regionEntry.isDestroyedOrRemoved()) {
      return null;
    }
    if (this.customEntryIdleTimeout != null || this.customEntryTimeToLive != null) {
      ExpiryRegionEntry expiryRegionEntry = new ExpiryRegionEntry(this, regionEntry);
      ExpirationAttributes ttlAttributes = null;
      ExpirationAttributes idleAttributes = null;
      final RegionAttributes<?, ?> regionAttributes = this.getAttributes();

      final CustomExpiry<?, ?> customTTL = regionAttributes.getCustomEntryTimeToLive();
      if (customTTL != null) {
        try {
          ttlAttributes = customTTL.getExpiry(expiryRegionEntry);
          if (ttlAttributes != null) {
            this.checkEntryTimeoutAction("timeToLive", ttlAttributes.getAction());
          }
        } catch (RegionDestroyedException ignore) {
          // Ignore - #42273
        } catch (EntryNotFoundException ignore) {
          // Ignore - #51933
        } catch (EntryDestroyedException ignore) {
          // Ignore - #51933
        } catch (Exception e) {
          logger.fatal(
              LocalizedMessage.create(
                  LocalizedStrings.EntryExpiryTask_ERROR_CALCULATING_EXPIRATION_0, e.getMessage()),
              e);
        }
      }
      if (ttlAttributes == null) {
        ttlAttributes = regionAttributes.getEntryTimeToLive();
      }

      CustomExpiry<?, ?> customIdle = regionAttributes.getCustomEntryIdleTimeout();
      if (customIdle != null) {
        try {
          idleAttributes = customIdle.getExpiry(expiryRegionEntry);
          if (idleAttributes != null) {
            this.checkEntryTimeoutAction("idleTimeout", idleAttributes.getAction());
          }
        } catch (RegionDestroyedException ignore) {
          // Ignore - #42273
        } catch (EntryNotFoundException ignore) {
          // Ignore - #51933
        } catch (EntryDestroyedException ignore) {
          // Ignore - #51933
        } catch (Exception e) {
          logger.fatal(
              LocalizedMessage.create(
                  LocalizedStrings.EntryExpiryTask_ERROR_CALCULATING_EXPIRATION_0, e.getMessage()),
              e);
        }
      }
      if (idleAttributes == null) {
        idleAttributes = regionAttributes.getEntryIdleTimeout();
      }

      final boolean ttlDisabled = ttlAttributes == null || ttlAttributes.getTimeout() == 0;
      final boolean idleDisabled = idleAttributes == null || idleAttributes.getTimeout() == 0;

      if (ttlDisabled && idleDisabled) {
        return null;
      } else if ((ttlDisabled || ttlAttributes.equals(regionAttributes.getEntryTimeToLive()))
          && (idleDisabled || idleAttributes.equals(regionAttributes.getEntryIdleTimeout()))) {
        // no need for custom since we can just use the region's expiration attributes.
        return new EntryExpiryTask(this, regionEntry);
      } else {
        return new CustomEntryExpiryTask(this, regionEntry, ttlAttributes, idleAttributes);
      }

    } else if (isEntryExpiryPossible()) {
      return new EntryExpiryTask(this, regionEntry);
    } else {
      return null;
    }
  }

  @Override
  public EntryExpiryTask getEntryExpiryTask(Object key) {
    RegionEntry re = this.getRegionEntry(key);
    if (re == null) {
      throw new EntryNotFoundException("Entry for key " + key + " does not exist.");
    }
    return this.entryExpiryTasks.get(re);
  }

  /**
   * Used by unit tests to get access to the RegionIdleExpiryTask of this region. Returns null if no
   * task exists.
   */
  @Override
  public RegionIdleExpiryTask getRegionIdleExpiryTask() {
    return this.regionIdleExpiryTask;
  }

  /**
   * Used by unit tests to get access to the RegionTTLExpiryTask of this region. Returns null if no
   * task exists.
   */
  @Override
  public RegionTTLExpiryTask getRegionTTLExpiryTask() {
    return this.regionTTLExpiryTask;
  }

  private void addExpiryTask(RegionEntry regionEntry, boolean ifAbsent) {
    if (isProxy()) {
      return;
    }
    if (!isInitialized()) {
      // don't schedule expiration until region is initialized (#35214)
      return;
    }
    if (isEntryExpiryPossible()) {
      EntryExpiryTask newTask = null;
      EntryExpiryTask oldTask;
      if (ifAbsent) {
        oldTask = this.entryExpiryTasks.get(regionEntry);
        if (oldTask != null) {
          boolean keepOldTask = true;
          if (this.customEntryIdleTimeout != null || this.customEntryTimeToLive != null) {
            newTask = createExpiryTask(regionEntry);
            if (newTask == null) {
              return;
            }
            // to fix bug 44418 see if the new tasks expiration would be earlier than
            // the scheduled task.
            long newTaskTime = newTask.getExpirationTime();
            try {
              if (newTaskTime != 0 && newTaskTime < oldTask.getExpirationTime()) {
                // it is so get rid of the old task and schedule the new one.
                keepOldTask = false;
              }
            } catch (EntryNotFoundException ignore) {
              keepOldTask = false;
            }
          }
          if (keepOldTask) {
            // if an oldTask is present leave it be
            if (logger.isTraceEnabled()) {
              logger.trace("Expiry Task not added because one already present. Key={}",
                  regionEntry.getKey());
            }
            return;
          }
        }
      }
      if (newTask == null) {
        newTask = createExpiryTask(regionEntry);
        if (newTask == null) {
          return;
        }
      }
      oldTask = this.entryExpiryTasks.put(regionEntry, newTask);
      ExpirationScheduler scheduler = this.cache.getExpirationScheduler();
      if (oldTask != null) {
        if (oldTask.cancel()) {
          scheduler.incCancels();
        }
      }
      if (!scheduler.addEntryExpiryTask(newTask)) {
        this.entryExpiryTasks.remove(regionEntry);
      } else {
        if (ExpiryTask.expiryTaskListener != null) {
          ExpiryTask.expiryTaskListener.afterSchedule(newTask);
        }
      }
    } else {
      if (logger.isTraceEnabled()) {
        logger.trace("addExpiryTask(key) ignored");
      }
    }
  }

  @Override
  public void cancelExpiryTask(RegionEntry regionEntry) {
    cancelExpiryTask(regionEntry, null);
  }

  void cancelExpiryTask(RegionEntry regionEntry, ExpiryTask expiryTask) {
    if (expiryTask != null) {
      this.entryExpiryTasks.remove(regionEntry, expiryTask);
      if (expiryTask.cancel()) {
        this.cache.getExpirationScheduler().incCancels();
      }
    } else {
      EntryExpiryTask oldTask = this.entryExpiryTasks.remove(regionEntry);
      if (oldTask != null) {
        if (oldTask.cancel()) {
          this.cache.getExpirationScheduler().incCancels();
        }
      }
    }
  }

  private void cancelAllEntryExpiryTasks() {
    // This method gets called during LocalRegion construction
    // in which case the final entryExpiryTasks field can still be null
    if (this.entryExpiryTasks == null)
      return;
    if (this.entryExpiryTasks.isEmpty())
      return;
    boolean doPurge = false;
    for (EntryExpiryTask task : this.entryExpiryTasks.values()) {
      // no need to call incCancels since we will call forcePurge
      task.cancel();
      doPurge = true;
    }
    if (doPurge) {
      // do a force to not leave any refs to this region
      this.cache.getExpirationScheduler().forcePurge();
    }
  }

  /**
   * get the ImageState for this region
   */
  @Override
  public ImageState getImageState() {
    return this.imageState;
  }

  /**
   * Callers of this method should always follow the call with: if (lockGII()) { try { } finally {
   * unlockGII(); } }
   *
   * @return true if lock obtained and unlock needs to be called
   */
  @Override
  public boolean lockGII() {
    ImageState imageState = getImageState();
    if (imageState.isReplicate() && !isInitialized()) {
      imageState.lockGII();
      // recheck initialized while holding lock
      if (isInitialized()) {
        // we didn't need to lock after all so clear and return false
        imageState.unlockGII();
      } else {
        return true;
      }
    }
    return false;
  }

  @Override
  public void unlockGII() {
    ImageState imageState = getImageState();
    assert imageState.isReplicate();
    imageState.unlockGII();
  }

  /**
   * Callers of this method should always follow the call with: if (lockRIReadLock()) { try { }
   * finally { unlockRIReadLock(); } }
   *
   * @return true if lock obtained and unlock needs to be called
   */
  private boolean lockRIReadLock() {
    if (getImageState().isClient()) {
      getImageState().readLockRI();
      return true;
    } else {
      return false;
    }
  }

  private void unlockRIReadLock() {
    assert getImageState().isClient();
    getImageState().readUnlockRI();
  }

  /** doesn't throw RegionDestroyedException, used by CacheDistributionAdvisor */
  LocalRegion basicGetParentRegion() {
    return this.parentRegion;
  }

  @Override
  public Object basicGetEntryUserAttribute(Object entryKey) {
    Map userAttr = this.entryUserAttributes;
    if (userAttr == null) {
      return null;
    }
    return userAttr.get(entryKey);
  }

  public TXStateProxy getTXState() {
    if (this.supportsTX) {
      return TXManagerImpl.getCurrentTXState();
    } else {
      return null;
    }
  }

  @Override
  public TXId getTXId() {
    final TXStateInterface tx = getTXState();
    if (tx == null) {
      return null;
    }
    return (TXId) tx.getTransactionId();
  }

  private TXRegionState txReadRegion() {
    final TXStateInterface txState = getTXState();
    if (txState != null) {
      return txState.txReadRegion(this);
    } else {
      return null;
    }
  }

  @Override
  public TXEntryState createReadEntry(TXRegionState txRegionState, KeyInfo keyInfo,
      boolean createIfAbsent) {
    TXEntryState result = null;
    final RegionEntry regionEntry = this.basicGetTXEntry(keyInfo);
    if (regionEntry != null) {
      boolean needsLRUCleanup = false;
      try {
        synchronized (regionEntry) {
          if (!regionEntry.isRemoved()) {
            Object value = regionEntry.getValueInVM(this);
            if (value == Token.NOT_AVAILABLE || regionEntry.isEvicted()) {
              // Entry value is on disk
              // Handle the case where we fault in a evicted disk entry
              needsLRUCleanup = txLRUStart();
              // Fault in the value from disk
              value = regionEntry.getValue(this);
            }

            /*
             * The tx will need the raw value for identity comparison. Please see
             * TXEntryState#checkForConflict(LocalRegion,Object)
             */
            Object id = regionEntry.getTransformedValue();

            result = txRegionState.createReadEntry(this, keyInfo.getKey(), regionEntry, id, value);
          }
        }
      } catch (DiskAccessException dae) {
        handleDiskAccessException(dae);
        needsLRUCleanup = false;
        throw dae;
      } finally {
        if (needsLRUCleanup) {
          // do this after releasing sync
          txLRUEnd();
        }
      }
    }
    if (result == null && createIfAbsent) {
      result = txRegionState.createReadEntry(this, keyInfo.getKey(), null, null, null);
    }
    return result;
  }

  private TXStateInterface getJTAEnlistedTX() {
    if (this.ignoreJTA) {
      // fixes bug 45541
      return null;
    }
    TXStateInterface txState = getTXState();
    if (txState != null) {
      return txState;
    } else {
      try {
        if (!this.ignoreJTA && this.cache.getJTATransactionManager() != null) {

          Transaction jtaTransaction = this.cache.getJTATransactionManager().getTransaction();
          if (jtaTransaction == null
              || jtaTransaction.getStatus() == Status.STATUS_NO_TRANSACTION) {
            return null;
          }
          if (isTransactionPaused() || isJTAPaused()) {
            // Do not bootstrap JTA again, if the transaction has been paused.
            return null;
          }
          txState = this.cache.getTXMgr().beginJTA();
          jtaTransaction.registerSynchronization(txState);
          return txState;
        } else {
          return null;
        }
      } catch (SystemException se) {
        // this can be thrown when the system is shutting down (see bug #39728)
        this.stopper.checkCancelInProgress(se);
        jtaEnlistmentFailureCleanup(txState, se);
        return null;
      } catch (RollbackException | IllegalStateException re) {
        jtaEnlistmentFailureCleanup(txState, re);
        return null;
      }
    }
  }

  private void jtaEnlistmentFailureCleanup(TXStateInterface txState, Exception reason) {
    if (this.cache == null) {
      return;
    }

    this.cache.getTXMgr().setTXState(null);
    if (txState != null) {
      txState.rollback();
    }

    String jtaTransName = null;
    try {
      jtaTransName = this.cache.getJTATransactionManager().getTransaction().toString();
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable ignore) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }

    throw new FailedSynchronizationException(
        LocalizedStrings.LocalRegion_FAILED_ENLISTEMENT_WITH_TRANSACTION_0
            .toLocalizedString(jtaTransName),
        reason);
  }

  @Override
  public boolean txLRUStart() {
    return this.entries.disableLruUpdateCallback();
  }

  @Override
  public void txLRUEnd() {
    this.entries.enableLruUpdateCallback();
    try {
      this.entries.lruUpdateCallback();
    } catch (DiskAccessException dae) {
      handleDiskAccessException(dae);
      throw dae;
    }
  }

  @Override
  public void txDecRefCount(RegionEntry regionEntry) {
    this.entries.decTxRefCount(regionEntry);
  }

  /** Does not throw RegionDestroyedException even if destroyed */
  List debugGetSubregionNames() {
    List names = new ArrayList();
    names.addAll(this.subregions.keySet());
    return names;
  }

  @Override
  public void incRecentlyUsed() {
    // nothing
    this.entries.incRecentlyUsed();
  }

  // package-private to avoid synthetic accessor
  static void dispatchEvent(LocalRegion region, InternalCacheEvent event,
      EnumListenerEvent operation) {

    CacheListener[] listeners = region.fetchCacheListenersField();
    if (event.getOperation().isCreate()) {
      if (logger.isDebugEnabled()) {
        logger.debug("invoking listeners: {}", Arrays.toString(listeners));
      }
    }
    if (listeners == null || listeners.length == 0) {
      return;
    }
    if (operation != EnumListenerEvent.AFTER_REGION_CREATE) {
      try {
        region.waitForRegionCreateEvent();
      } catch (CancelException ignore) {
        // ignore and keep going
        if (logger.isTraceEnabled()) {
          logger.trace("Dispatching events after cache closure for region {}",
              region.getFullPath());
        }
      }
    }

    if (!event.isGenerateCallbacks()) {
      return;
    }

    for (CacheListener listener : listeners) {
      if (listener != null) {
        try {
          operation.dispatchEvent(event, listener);
        } catch (CancelException ignore) {
          // ignore for bug 37105
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(LocalizedMessage
              .create(LocalizedStrings.LocalRegion_EXCEPTION_OCCURRED_IN_CACHELISTENER), t);
        }
      }
    }
  }

  /*****************************************************************************
   * INNER CLASSES
   ****************************************************************************/

  class EventDispatcher implements Runnable {
    /**
     * released by the release method
     */
    @Retained
    InternalCacheEvent event;

    EnumListenerEvent op;

    EventDispatcher(InternalCacheEvent event, EnumListenerEvent op) {
      if (LocalRegion.this.offHeap && event instanceof EntryEventImpl) {
        // Make a copy that has its own off-heap refcount so fix bug 48837
        event = new EntryEventImpl((EntryEventImpl) event);
      }
      this.event = event;
      this.op = op;
    }

    @Override
    public void run() {
      try {
        dispatchEvent(LocalRegion.this, this.event, this.op);
      } finally {
        this.release();
      }
    }

    public void release() {
      if (LocalRegion.this.offHeap && this.event instanceof EntryEventImpl) {
        ((Releasable) this.event).release();
      }
    }
  }

  /** Set view of subregions */
  private class SubregionsSet extends AbstractSet {
    final boolean recursive;

    SubregionsSet(boolean recursive) {
      this.recursive = recursive;
    }

    @Override
    public Iterator iterator() {
      // iterates breadth-first (if recursive)
      return new Iterator() {
        Iterator currentIterator = LocalRegion.this.subregions.values().iterator();

        List queue; // FIFO queue of iterators

        Object nextElement = null;

        @Override
        public void remove() {
          throw new UnsupportedOperationException(
              LocalizedStrings.LocalRegion_THIS_ITERATOR_DOES_NOT_SUPPORT_MODIFICATION
                  .toLocalizedString());
        }

        @Override
        public boolean hasNext() {
          if (this.nextElement != null) {
            return true;
          } else {
            Object element = next(true);
            if (element != null) {
              this.nextElement = element;
              return true;
            } else {
              return false;
            }
          }
        }

        private boolean doHasNext() {
          return this.currentIterator != null && this.currentIterator.hasNext();
        }

        @Override
        public Object next() {
          return next(false);
        }

        /**
         * @param nullOK if true, return null instead of throwing NoSuchElementException
         * @return the next element
         */
        private Object next(boolean nullOK) {
          if (this.nextElement != null) {
            Object next = this.nextElement;
            this.nextElement = null;
            return next;
          }

          LocalRegion region;
          do {
            region = null;
            if (!doHasNext()) {
              if (this.queue == null || this.queue.isEmpty()) {
                if (nullOK) {
                  return null;
                } else {
                  throw new NoSuchElementException();
                }
              } else {
                this.currentIterator = (Iterator) queue.remove(0);
                continue;
              }
            }
            region = (LocalRegion) currentIterator.next();
          } while (region == null || !region.isInitialized() || region.isDestroyed());

          if (recursive) {
            Iterator nextIterator = region.subregions.values().iterator();
            if (nextIterator.hasNext()) {
              if (this.queue == null) {
                this.queue = new ArrayList();
              }
              this.queue.add(nextIterator);
            }
          }
          if (!doHasNext()) {
            if (this.queue == null || this.queue.isEmpty()) {
              this.currentIterator = null;
            } else {
              this.currentIterator = (Iterator) this.queue.remove(0);
            }
          }
          return region;
        }
      };
    }

    @Override
    public int size() {
      if (this.recursive) {
        return allSubregionsSize() - 1 /* don't count this region */;
      } else {
        return LocalRegion.this.subregions.size();
      }
    }

    @Override
    public Object[] toArray() {
      List temp = new ArrayList(this.size());
      // do NOT use addAll or this results in stack overflow - must use iterator()
      for (Iterator iter = iterator(); iter.hasNext();) {
        temp.add(iter.next());
      }
      return temp.toArray();
    }

    @Override
    public Object[] toArray(Object[] array) {
      List temp = new ArrayList(this.size());
      // do NOT use addAll or this results in stack overflow - must use iterator()
      for (Iterator iter = iterator(); iter.hasNext();) {
        temp.add(iter.next());
      }
      return temp.toArray(array);
    }
  }

  public class NonTXEntry implements Region.Entry {

    private final Object key;

    private boolean entryIsDestroyed = false;

    @Override
    public boolean isLocal() {
      return true;
    }

    /**
     * Create an Entry given a key. The returned Entry may or may not be destroyed
     */
    public NonTXEntry(RegionEntry regionEntry) {
      if (regionEntry == null) {
        throw new IllegalArgumentException(
            LocalizedStrings.LocalRegion_REGIONENTRY_SHOULD_NOT_BE_NULL.toLocalizedString());
      }
      // for a soplog region, since the entry may not be in memory,
      // we will have to fetch it from soplog, if the entry is in
      // memory this is a quick lookup, so rather than RegionEntry
      // we keep reference to key
      this.key = regionEntry.getKey();
    }

    /** Internal method for getting the underlying RegionEntry */
    public RegionEntry getRegionEntry() {
      RegionEntry regionEntry = LocalRegion.this.getRegionMap().getEntry(this.key);
      if (regionEntry == null) {
        throw new EntryDestroyedException(this.key.toString());
      }
      return regionEntry;
    }

    private RegionEntry basicGetEntry() {
      RegionEntry re = LocalRegion.this.basicGetEntry(this.key);
      if (re == null) {
        throw new EntryDestroyedException(this.key.toString());
      }
      return re;
    }

    @Override
    public boolean isDestroyed() {
      if (this.entryIsDestroyed) {
        return true;
      }
      if (LocalRegion.this.isDestroyed || LocalRegion.this.basicGetEntry(this.key) == null) {
        this.entryIsDestroyed = true;
      }
      return this.entryIsDestroyed;
    }

    @Override
    public Object getKey() {
      return basicGetEntry().getKey();
    }

    @Override
    public Object getValue() {
      return getValue(false);
    }

    public Object getValue(boolean ignoreCopyOnRead) {
      Object value = getDeserialized(this.basicGetEntry(), false, ignoreCopyOnRead, false, false);
      if (value == null) {
        throw new EntryDestroyedException(getKey().toString());
      } else if (Token.isInvalid(value)) {
        return null;
      }

      return value;
    }

    /**
     * To get the value from region in serialized form
     *
     * @return {@link VMCachedDeserializable}
     */
    public Object getRawValue() {
      Object value = basicGetEntry().getValue((RegionEntryContext) getRegion());
      if (value == null) {
        throw new EntryDestroyedException(this.getRegionEntry().getKey().toString());
      } else if (Token.isInvalid(value)) {
        return null;
      }

      return value;
    }

    @Override
    public Region getRegion() {
      basicGetEntry();
      return LocalRegion.this;
    }

    @Override
    public CacheStatistics getStatistics() {
      // prefer entry destroyed exception over statistics disabled exception
      basicGetEntry();
      if (!LocalRegion.this.statisticsEnabled) {
        throw new StatisticsDisabledException(
            LocalizedStrings.LocalRegion_STATISTICS_DISABLED_FOR_REGION_0
                .toLocalizedString(getFullPath()));
      }
      return new CacheStatisticsImpl(this.basicGetEntry(), LocalRegion.this);
    }

    @Override
    public Object getUserAttribute() {
      this.basicGetEntry();
      Map userAttributes = LocalRegion.this.entryUserAttributes;
      if (userAttributes == null) {
        return null;
      }
      return userAttributes.get(basicGetEntry().getKey());
    }

    @Override
    public Object setUserAttribute(Object userAttribute) {
      if (LocalRegion.this.entryUserAttributes == null) {
        LocalRegion.this.entryUserAttributes = new ConcurrentHashMap();
      }
      return LocalRegion.this.entryUserAttributes.put(basicGetEntry().getKey(), userAttribute);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof LocalRegion.NonTXEntry)) {
        return false;
      }
      LocalRegion.NonTXEntry entry = (LocalRegion.NonTXEntry) obj;
      return this.basicGetEntry().equals(entry.getRegionEntry())
          && this.getRegion() == entry.getRegion();
    }

    @Override
    public int hashCode() {
      return basicGetEntry().hashCode() ^ getRegion().hashCode();
    }

    @Override
    public String toString() {
      return new StringBuilder("NonTXEntry@")
          .append(Integer.toHexString(System.identityHashCode(this))).append(' ')
          .append(this.getRegionEntry()).toString();
    }

    /**
     * @since GemFire 5.0
     */
    @Override
    public Object setValue(Object value) {
      return put(getKey(), value);
    }
  }

  /**
   * For internal use only.
   */
  @Override
  public RegionMap getRegionMap() {
    // OK to ignore tx state
    return this.entries;
  }

  /**
   * (description copied from entryCount() Returns the number of entries in this region. Note that
   * because of the concurrency properties of the {@link RegionMap}, the number of entries is only
   * an approximate. That is, other threads may change the number of entries in this region while
   * this method is being invoked.
   *
   * @see LocalRegion#entryCount()
   */
  @Override
  public int size() {
    checkReadiness();
    checkForNoAccess();
    discoverJTA();
    boolean isClient = this.imageState.isClient();
    if (isClient) {
      // bug #40871 - test sees wrong size for region during RI
      lockRIReadLock();
    }
    try {
      return entryCount();
    } finally {
      if (isClient) {
        unlockRIReadLock();
      }
    }
  }

  /**
   * returns an estimate of the number of entries in this region. This method should be preferred
   * over size() for hdfs regions where an accurate size is not needed. This method is not supported
   * on a client
   *
   * @return the estimated size of this region
   */
  public int sizeEstimate() {
    boolean isClient = this.imageState.isClient();
    if (isClient) {
      throw new UnsupportedOperationException("Method not supported on a client");
    }
    return entryCount(null, true);
  }

  /**
   * This method returns true if Region is Empty.
   */
  @Override
  public boolean isEmpty() {
    // checkForNoAccess(); // size does this check
    return this.size() <= 0;
  }

  /**
   * Returns true if the value is present in the Map
   */
  @Override
  public boolean containsValue(final Object value) {
    if (value == null) {
      throw new NullPointerException(
          LocalizedStrings.LocalRegion_VALUE_FOR_CONTAINSVALUEVALUE_CANNOT_BE_NULL
              .toLocalizedString());
    }
    checkReadiness();
    checkForNoAccess();
    boolean result = false;
    for (Object entry : new EntriesSet(this, false, IteratorType.VALUES, false)) {
      if (entry != null) {
        if (value.equals(entry)) {
          result = true;
          break;
        }
      }
    }
    return result;
  }

  /**
   * Returns a set of the entries present in the Map. This set is Not Modifiable. If changes are
   * made to this set, they will be not reflected in the map
   */
  @Override
  public Set entrySet() {
    // entries(false) takes care of open transactions
    return entrySet(false);
  }

  /**
   * Returns a set of the keys present in the Map. This set is Not Modifiable. If changes are made
   * to this set, they will be not reflected in the map
   */
  @Override
  public Set keySet() {
    // keys() takes care of open transactions
    return keys();
  }

  /**
   * removes the object from the Map and returns the object removed. The object is returned only if
   * present in the localMap. If the value is present in another Node, null is returned
   */
  @Override
  public Object remove(Object key) {
    // no validations needed here since destroy does it for us
    Object value = null;
    try {
      value = destroy(key);
    } catch (EntryNotFoundException ignore) {
      // No need to log this exception; caller can test for null;
    }
    return value;
  }

  // TODO: fromClient is always true
  public void basicBridgeDestroyRegion(Object callbackArg, final ClientProxyMembershipID client,
      boolean fromClient, EventID eventId)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    RegionEventImpl event = new ClientRegionEventImpl(this, Operation.REGION_DESTROY, callbackArg,
        false, client.getDistributedMember(), client, eventId);

    basicDestroyRegion(event, true);
  }

  public void basicBridgeClear(Object callbackArg, final ClientProxyMembershipID client,
      boolean fromClient, EventID eventId)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    RegionEventImpl event = new ClientRegionEventImpl(this, Operation.REGION_CLEAR, callbackArg,
        false, client.getDistributedMember(), client, eventId);

    basicClear(event, true);
  }

  @Override
  void basicClear(RegionEventImpl regionEvent) {
    getDataView().checkSupportsRegionClear();
    basicClear(regionEvent, true);
  }

  void basicClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    cmnClearRegion(regionEvent, cacheWrite, true);
  }

  // TODO: what does cmn refer to?
  void cmnClearRegion(RegionEventImpl regionEvent, boolean cacheWrite, boolean useRVV) {
    RegionVersionVector rvv = null;
    if (useRVV && this.getDataPolicy().withReplication() && this.getConcurrencyChecksEnabled()) {
      rvv = this.versionVector.getCloneForTransmission();
    }
    clearRegionLocally(regionEvent, cacheWrite, rvv);
  }

  /**
   * Common code used by both clear and localClear. On the lines of destroyRegion, this method will
   * be invoked for clearing the local cache.The cmnClearRegion will be overridden in the derived
   * class DistributedRegion too. For clear operation , no CacheWriter will be invoked . It will
   * only have afterClear callback. Also like destroyRegion & invalidateRegion , the clear operation
   * will not take distributedLock. The clear operation will also clear the local tranxnl entries .
   * The clear operation will have immediate committed state.
   */
  void clearRegionLocally(RegionEventImpl regionEvent, boolean cacheWrite,
      RegionVersionVector vector) {
    final boolean isRvvDebugEnabled = logger.isTraceEnabled(LogMarker.RVV_VERBOSE);

    RegionVersionVector rvv = vector;
    if (this.serverRegionProxy != null) {
      // clients and local regions do not maintain a full RVV. can't use it with clear()
      rvv = null;
    }
    if (rvv != null && this.getDataPolicy().withStorage()) {
      if (isRvvDebugEnabled) {
        logger.trace(LogMarker.RVV_VERBOSE,
            "waiting for my version vector to dominate{}mine={}{} other={}", getLineSeparator(),
            getLineSeparator(), this.versionVector.fullToString(), rvv);
      }
      boolean result = this.versionVector.waitToDominate(rvv, this);
      if (!result) {
        if (isRvvDebugEnabled) {
          logger.trace(LogMarker.RVV_VERBOSE, "incrementing clearTimeouts for {} rvv={}", getName(),
              this.versionVector.fullToString());
        }
        getCachePerfStats().incClearTimeouts();
      }
    }

    // If the initial image operation is still in progress then we need will have to do the clear
    // operation at the end of the GII.For this we try to acquire the lock of GII the boolean
    // returned is true that means lock was obtained which also means that GII is still in progress.
    boolean isGIIinProgress = lockGII();
    if (isGIIinProgress) {
      // Set a flag which will indicate that the Clear was invoked.
      // Also we should try & abort the GII
      try {
        getImageState().setClearRegionFlag(true /* Clear region */, rvv);
      } finally {
        unlockGII();
      }
    }

    if (cacheWrite && !isGIIinProgress) {
      this.cacheWriteBeforeRegionClear(regionEvent);
    }

    RegionVersionVector myVector = getVersionVector();
    if (myVector != null) {
      if (isRvvDebugEnabled) {
        logger.trace(LogMarker.RVV_VERBOSE, "processing version information for {}", regionEvent);
      }
      if (!regionEvent.isOriginRemote() && !regionEvent.getOperation().isLocal()) {
        // generate a new version for the operation
        VersionTag tag = VersionTag.create(getVersionMember());
        tag.setVersionTimeStamp(cacheTimeMillis());
        tag.setRegionVersion(myVector.getNextVersionWhileLocked());
        if (isRvvDebugEnabled) {
          logger.trace(LogMarker.RVV_VERBOSE, "generated version tag for clear: {}", tag);
        }
        regionEvent.setVersionTag(tag);
      } else {
        VersionTag tag = regionEvent.getVersionTag();
        if (tag != null) {
          if (isRvvDebugEnabled) {
            logger.trace(LogMarker.RVV_VERBOSE, "recording version tag for clear: {}", tag);
          }
          // clear() events always have the ID in the tag
          myVector.recordVersion(tag.getMemberID(), tag);
        }
      }
    }

    // Clear the expiration task for all the entries. It is possible that
    // after clearing it some new entries may get added before issuing clear
    // on the map , but that should be OK, as the expiration thread will
    // silently move ahead if the entry to be expired no longer existed
    this.cancelAllEntryExpiryTasks();
    if (this.entryUserAttributes != null) {
      this.entryUserAttributes.clear();
    }

    // if all current content has been removed then the version vector
    // does not need to retain any exceptions and the GC versions can
    // be set to the current vector versions
    if (rvv == null && myVector != null) {
      myVector.removeOldVersions();
    }

    /*
     * First we need to clear the TX state for the current region for the thread. The operation will
     * not take global lock similar to regionInvalidator regionDestroy behaviour.
     */

    // clear the disk region if present
    if (this.diskRegion != null) {
      // persist current rvv and rvvgc which contained version for clear() itself
      if (this.getDataPolicy().withPersistence()) {
        // null means not to change dr.rvvTrust
        if (isRvvDebugEnabled) {
          logger.trace(LogMarker.RVV_VERBOSE, "Clear: Saved current rvv: {}",
              this.diskRegion.getRegionVersionVector());
        }
        this.diskRegion.writeRVV(this, null);
        this.diskRegion.writeRVVGC(this);
      }

      // clear the entries in disk
      this.diskRegion.clear(this, rvv);
    }
    // this will be done in diskRegion.clear if it is not null else it has to be
    // done here
    else {
      // Now remove the tx entries for this region
      txClearRegion();
      // Now clear the map of committed entries
      Set<VersionSource> remainingIDs = clearEntries(rvv);
      if (!this.getDataPolicy().withPersistence()) {
        // persistent regions do not reap IDs
        if (myVector != null) {
          myVector.removeOldMembers(remainingIDs);
        }
      }
    }

    if (!isProxy()) {
      // Now we need to recreate all the indexes.
      // If the indexManager is null we don't have to worry
      // for any other thread creating index at that instant
      // because the region has already been cleared
      // of entries.

      // TODO made indexManager variable is made volatile. Is it necessary?
      if (this.indexManager != null) {
        try {
          this.indexManager.rerunIndexCreationQuery();
        } catch (QueryException qe) {
          // Create an anonymous inner class of CacheRuntimeException so
          // that a RuntimeException is thrown
          // TODO: never throw an annonymous class (and outer-class is not serializable)
          throw new CacheRuntimeException(
              LocalizedStrings.LocalRegion_EXCEPTION_OCCURRED_WHILE_RE_CREATING_INDEX_DATA_ON_CLEARED_REGION
                  .toLocalizedString(),
              qe) {
            private static final long serialVersionUID = 0L;
          };
        }
      }
    }

    if (ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      CacheObserverHolder.getInstance().afterRegionClear(regionEvent);
    }

    if (isGIIinProgress) {
      return;
    }
    regionEvent.setEventType(EnumListenerEvent.AFTER_REGION_CLEAR);

    // Issue a callback to afterClear if the region is initialized
    boolean hasListener = hasListener();
    if (hasListener) {
      dispatchListenerEvent(EnumListenerEvent.AFTER_REGION_CLEAR, regionEvent);
    }
  }

  @Override
  void basicLocalClear(RegionEventImpl rEvent) {
    getDataView().checkSupportsRegionClear();
    cmnClearRegion(rEvent, false/* cacheWrite */, false/* useRVV */);
  }

  public void handleInterestEvent(InterestRegistrationEvent event) {
    throw new UnsupportedOperationException(
        LocalizedStrings.LocalRegion_REGION_INTEREST_REGISTRATION_IS_ONLY_SUPPORTED_FOR_PARTITIONEDREGIONS
            .toLocalizedString());
  }

  // TODO: refactor basicGetAll
  @Override
  Map basicGetAll(Collection keys, Object callback) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();

    if (isDebugEnabled) {
      logger.debug("Processing getAll request for: {}", keys);
    }
    discoverJTA();
    Map allResults = new HashMap();
    if (hasServerProxy()) {
      // Some of our implementation expects a list of keys so make sure it is a list
      List keysList;
      if (keys instanceof List) {
        keysList = (List) keys;
      } else {
        keysList = new ArrayList(keys);
      }

      // Gather any local values
      // We only need to do this if this region has local storage
      if (getTXState() == null && hasStorage()) {
        if (keysList == keys) {
          // Create a copy of the collection of keys
          // to keep the original collection intact
          keysList = new ArrayList(keys);
        }
        for (Iterator iterator = keysList.iterator(); iterator.hasNext();) {
          Object key = iterator.next();
          Object value;
          Region.Entry entry = accessEntry(key, true);
          if (entry != null && (value = entry.getValue()) != null) {
            allResults.put(key, value);
            iterator.remove();
          }
        }
        if (isDebugEnabled) {
          logger.debug("Added local results for getAll request: {}", allResults);
        }
      }

      // Send the rest of the keys to the server (if necessary)
      if (!keysList.isEmpty()) {
        VersionedObjectList remoteResults = getServerProxy().getAll(keysList, callback);
        if (isDebugEnabled) {
          logger.debug("remote getAll results are {}", remoteResults);
        }

        // Add remote results to local cache and all results if successful
        for (VersionedObjectList.Iterator it = remoteResults.iterator(); it.hasNext();) {
          VersionedObjectList.Entry entry = it.next();
          Object key = entry.getKey();
          boolean notOnServer = entry.isKeyNotOnServer();
          // in 8.0 we added transfer of tombstones with RI/getAll results for bug #40791
          boolean createTombstone = false;
          if (notOnServer) {
            createTombstone = entry.getVersionTag() != null && this.getConcurrencyChecksEnabled();
            allResults.put(key, null);
            if (isDebugEnabled) {
              logger.debug("Added remote result for missing key: {}", key);
            }
            if (!createTombstone) {
              continue;
            }
          }

          Object value;
          if (createTombstone) {
            // the value is null in this case, so use TOKEN_TOMBSTONE
            value = Token.TOMBSTONE;
          } else {
            value = entry.getObject();
          }

          if (value instanceof Throwable) {
            continue;
          }

          // The following basicPutEntry needs to be done
          // even if we do not have storage so that the
          // correct events will be delivered to any callbacks we have.
          long startPut = CachePerfStats.getStatTime();
          validateKey(key);

          @Released
          EntryEventImpl event = EntryEventImpl.create(this, Operation.LOCAL_LOAD_CREATE, key,
              value, callback, false, getMyId(), true);

          try {
            event.setFromServer(true);
            event.setVersionTag(entry.getVersionTag());

            if (!alreadyInvalid(key, event)) {
              // bug #47716 - don't update if it's already here & invalid
              TXStateProxy txState = this.cache.getTXMgr().pauseTransaction();
              try {
                basicPutEntry(event, 0L);
              } catch (ConcurrentCacheModificationException e) {
                if (isDebugEnabled) {
                  logger.debug(
                      "getAll result for {} not stored in cache due to concurrent modification",
                      key, e);
                }
              } finally {
                this.cache.getTXMgr().unpauseTransaction(txState);
              }
              getCachePerfStats().endPut(startPut, event.isOriginRemote());
            }

            if (!createTombstone) {
              allResults.put(key, value);
              if (isTraceEnabled) {
                logger.trace("Added remote result for getAll request: {}, {}", key, value);
              }
            }
          } finally {
            event.release();
          }
        }
      }

    } else {
      // This implementation for a P2P VM is a stop-gap to provide the
      // functionality. It needs to be rewritten more efficiently.
      for (Object key : keys) {
        try {
          allResults.put(key, get(key, callback));
        } catch (Exception e) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.LocalRegion_THE_FOLLOWING_EXCEPTION_OCCURRED_ATTEMPTING_TO_GET_KEY_0,
              key), e);
        }
      }
    }
    return allResults;
  }

  /**
   * Return false if it will never store entry ekys and values locally; otherwise return true.
   */
  protected boolean hasStorage() {
    return this.getDataPolicy().withStorage();
  }

  private void verifyPutAllMap(Map map) {
    Map.Entry mapEntry;
    Collection theEntries = map.entrySet();
    for (Object theEntry : theEntries) {
      mapEntry = (Map.Entry) theEntry;
      Object key = mapEntry.getKey();
      if (mapEntry.getValue() == null || key == null) {
        throw new NullPointerException("Any key or value in putAll should not be null");
      }
      if (!MemoryThresholds.isLowMemoryExceptionDisabled()) {
        checkIfAboveThreshold(key);
      }
      // Threshold check should not be performed again
    }
  }

  private void verifyRemoveAllKeys(Collection<Object> keys) {
    for (Object key : keys) {
      if (key == null) {
        throw new NullPointerException("Any key in removeAll must not be null");
      }
    }
  }

  /**
   * Called on a bridge server when it has a received a putAll command from a client.
   *
   * @param map a map of key->value for the entries we are putting
   * @param retryVersions a map of key->version tag. If any of the entries are the result of a
   *        retried client event, we need to make sure we send the original version tag along with
   *        the event.
   * @param callbackArg callback argument from client
   */
  public VersionedObjectList basicBridgePutAll(Map map, Map<Object, VersionTag> retryVersions,
      ClientProxyMembershipID memberId, EventID eventId, boolean skipCallbacks, Object callbackArg)
      throws TimeoutException, CacheWriterException {

    long startPut = CachePerfStats.getStatTime();
    if (isGatewaySenderEnabled()) {
      callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.PUTALL_CREATE, null,
        null /* new value */, callbackArg, false /* origin remote */,
        memberId.getDistributedMember(), !skipCallbacks /* generateCallbacks */, eventId);

    try {
      event.setContext(memberId);
      DistributedPutAllOperation putAllOp = new DistributedPutAllOperation(event, map.size(), true);
      try {
        VersionedObjectList result = basicPutAll(map, putAllOp, retryVersions);
        getCachePerfStats().endPutAll(startPut);
        return result;
      } finally {
        putAllOp.freeOffHeapResources();
      }
    } finally {
      event.release();
    }
  }

  /**
   * Called on a bridge server when it has a received a removeAll command from a client.
   *
   * @param keys a collection of the keys we are putting
   * @param retryVersions a collection of version tags. If the client is retrying a key then that
   *        keys slot will be non-null in this collection. Note that keys and retryVersions are
   *        parallel lists.
   * @param callbackArg callback argument from client
   */
  public VersionedObjectList basicBridgeRemoveAll(List<Object> keys,
      ArrayList<VersionTag> retryVersions, ClientProxyMembershipID memberId, EventID eventId,
      Object callbackArg) throws TimeoutException, CacheWriterException {

    long startOp = CachePerfStats.getStatTime();
    if (isGatewaySenderEnabled()) {
      callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.REMOVEALL_DESTROY, null,
        null /* new value */, callbackArg, false /* origin remote */,
        memberId.getDistributedMember(), true /* generateCallbacks */, eventId);

    try {
      event.setContext(memberId);
      DistributedRemoveAllOperation removeAllOp =
          new DistributedRemoveAllOperation(event, keys.size(), true);
      try {
        VersionedObjectList result = basicRemoveAll(keys, removeAllOp, retryVersions);
        getCachePerfStats().endRemoveAll(startOp);
        return result;
      } finally {
        removeAllOp.freeOffHeapResources();
      }
    } finally {
      event.release();
    }
  }

  // TODO: return value is never used
  public VersionedObjectList basicImportPutAll(Map map, boolean skipCallbacks) {
    long startPut = CachePerfStats.getStatTime();

    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.PUTALL_CREATE, null, null, null,
        true, getMyId(), !skipCallbacks);

    try {
      DistributedPutAllOperation putAllOp =
          new DistributedPutAllOperation(event, map.size(), false);
      try {
        VersionedObjectList result = basicPutAll(map, putAllOp, null);
        getCachePerfStats().endPutAll(startPut);
        return result;
      } finally {
        putAllOp.freeOffHeapResources();
      }
    } finally {
      event.release();
    }
  }

  @Override
  public void putAll(Map map, Object aCallbackArgument) {
    long startPut = CachePerfStats.getStatTime();
    final DistributedPutAllOperation putAllOp = newPutAllOperation(map, aCallbackArgument);
    if (putAllOp != null) {
      try {
        basicPutAll(map, putAllOp, null);
      } finally {
        putAllOp.getBaseEvent().release();
        putAllOp.freeOffHeapResources();
      }
    }

    getCachePerfStats().endPutAll(startPut);
  }

  @Override
  public void putAll(Map map) {
    putAll(map, null);
  }

  @Override
  public void removeAll(Collection keys) {
    removeAll(keys, null);
  }

  @Override
  public void removeAll(Collection keys, Object aCallbackArgument) {
    long startOp = CachePerfStats.getStatTime();
    DistributedRemoveAllOperation operation = newRemoveAllOperation(keys, aCallbackArgument);
    if (operation != null) {
      try {
        basicRemoveAll(keys, operation, null);
      } finally {
        operation.getBaseEvent().release();
        operation.freeOffHeapResources();
      }
    }
    getCachePerfStats().endRemoveAll(startOp);
  }

  /**
   * Returns true if a one-hop (RemoteOperationMessage) should be used when applying the change to
   * the system.
   */
  public boolean requiresOneHopForMissingEntry(EntryEventImpl event) {
    return false;
  }

  // TODO: refactor basicPutAll
  public VersionedObjectList basicPutAll(final Map<?, ?> map,
      final DistributedPutAllOperation putAllOp, final Map<Object, VersionTag> retryVersions) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    final EntryEventImpl event = putAllOp.getBaseEvent();
    EventID eventId = event.getEventId();
    if (eventId == null && generateEventID()) {
      // We need to "reserve" the eventIds for the entries in map here
      event.reserveNewEventId(this.cache.getDistributedSystem(), map.size());
      eventId = event.getEventId();
    }

    verifyPutAllMap(map);
    VersionedObjectList proxyResult = null;
    boolean partialResult = false;
    RuntimeException runtimeException = null;
    if (hasServerProxy()) {
      // send message to bridge server
      if (isTX()) {
        TXStateProxyImpl txState = (TXStateProxyImpl) this.cache.getTxManager().getTXState();
        txState.getRealDeal(null, this);
      }
      try {
        proxyResult = getServerProxy().putAll(map, eventId, !event.isGenerateCallbacks(),
            event.getCallbackArgument());
        if (isDebugEnabled) {
          logger.debug("PutAll received response from server: {}", proxyResult);
        }
      } catch (PutAllPartialResultException e) {
        // adjust the map to only add succeeded entries, then apply the adjustedMap
        proxyResult = e.getSucceededKeysAndVersions();
        partialResult = true;
        if (isDebugEnabled) {
          logger.debug(
              "putAll in client encountered a PutAllPartialResultException:{}{}. Adjusted keys are: {}",
              e.getMessage(), getLineSeparator(), proxyResult.getKeys());
        }
        Throwable txException = e.getFailure();
        while (txException != null) {
          if (txException instanceof TransactionException) {
            runtimeException = (RuntimeException) txException;
            break;
          }
          txException = txException.getCause();
        }
        if (runtimeException == null) {
          // for cache close
          runtimeException = getCancelCriterion().generateCancelledException(e.getFailure());
          if (runtimeException == null) {
            runtimeException = new ServerOperationException(
                LocalizedStrings.Region_PutAll_Applied_PartialKeys_At_Server_0
                    .toLocalizedString(getFullPath()),
                e.getFailure());
          }
        }
      }
    }

    final VersionedObjectList succeeded =
        new VersionedObjectList(map.size(), true, this.getConcurrencyChecksEnabled());

    // if this is a transactional putAll, we will not have version information as it is only
    // generated at commit
    // so treat transactional putAll as if the server is not versioned
    final boolean serverIsVersioned = proxyResult != null && proxyResult.regionIsVersioned()
        && !isTX() && this.getDataPolicy() != DataPolicy.EMPTY;
    if (!serverIsVersioned && !partialResult) {
      // we don't need server information if it isn't versioned or if the region is empty
      proxyResult = null;
    }

    lockRVVForBulkOp();
    try {
      try {
        int size = proxyResult == null ? map.size() : proxyResult.size();

        if (isDebugEnabled) {
          logger.debug("size of put result is {} maps is {} proxyResult is {}", size, map,
              proxyResult);
        }

        final PutAllPartialResult partialKeys = new PutAllPartialResult(size);
        final Iterator iterator;
        final boolean isVersionedResults;
        if (proxyResult != null) {
          iterator = proxyResult.iterator();
          isVersionedResults = true;
        } else {
          iterator = map.entrySet().iterator();
          isVersionedResults = false;
        }

        // TODO: refactor this mess
        Runnable task = new Runnable() {
          @Override
          public void run() {
            int offset = 0;
            VersionTagHolder tagHolder = new VersionTagHolder();
            while (iterator.hasNext()) {
              stopper.checkCancelInProgress(null);
              Map.Entry mapEntry = (Map.Entry) iterator.next();
              Object key = mapEntry.getKey();
              VersionTag versionTag = null;
              tagHolder.setVersionTag(null);
              final Object value;
              boolean overwritten = false;
              if (isVersionedResults) {
                versionTag = ((VersionedObjectList.Entry) mapEntry).getVersionTag();
                value = map.get(key);
                if (isDebugEnabled) {
                  logger.debug("putAll key {} -> {} version={}", key, value, versionTag);
                }
                if (versionTag == null && serverIsVersioned && getConcurrencyChecksEnabled()
                    && getDataPolicy().withStorage()) {
                  // server was unable to determine the version for this operation.
                  // I'm not sure this can still happen as described below on a pr.
                  // But it can happen on the server if NORMAL or PRELOADED. See bug 51644.
                  // This can happen in a PR with redundancy if there is a bucket
                  // failure or migration during the operation. We destroy the
                  // entry since we don't know what its state should be (but the server should)
                  if (isDebugEnabled) {
                    logger.debug("server returned no version information for {}", key);
                  }
                  localDestroyNoCallbacks(key);
                  // to be consistent we need to fetch the current entry
                  get(key, event.getCallbackArgument(), false, null);
                  overwritten = true;
                }
              } else {
                value = mapEntry.getValue();
                if (isDebugEnabled) {
                  logger.debug("putAll {} -> {}", key, value);
                }
              }
              try {
                if (serverIsVersioned) {
                  if (isDebugEnabled) {
                    logger.debug("associating version tag with {} version={}", key, versionTag);
                  }
                  // If we have received a version tag from a server, add it to the event
                  tagHolder.setVersionTag(versionTag);
                  tagHolder.setFromServer(true);
                } else if (retryVersions != null && retryVersions.containsKey(key)) {
                  // If this is a retried event, and we have a version tag for the retry,
                  // add it to the event.
                  tagHolder.setVersionTag(retryVersions.get(key));
                }

                if (!overwritten) {
                  basicEntryPutAll(key, value, putAllOp, offset, tagHolder);
                }
                // now we must check again since the cache may have closed during
                // distribution (causing this process to not receive and queue the
                // event for clients
                stopper.checkCancelInProgress(null);
                succeeded.addKeyAndVersion(key, tagHolder.getVersionTag());
              } catch (Exception ex) {
                if (isDebugEnabled) {
                  logger.debug("PutAll operation encountered exception for key {}", key, ex);
                }
                partialKeys.saveFailedKey(key, ex);
              }
              offset++;
            }
          }
        };

        syncBulkOp(task, eventId);
        if (partialKeys.hasFailure()) {
          // Bug 51725: Now succeeded contains an order key list, may be missing the version tags.
          // Save reference of succeeded into partialKeys. The succeeded may be modified by
          // postPutAll() to fill in the version tags.
          partialKeys.setSucceededKeysAndVersions(succeeded);
          logger
              .info(LocalizedMessage.create(LocalizedStrings.Region_PutAll_Applied_PartialKeys_0_1,
                  new Object[] {getFullPath(), partialKeys}));
          if (isDebugEnabled) {
            logger.debug(partialKeys.detailString());
          }
          if (runtimeException == null) {
            // if received exception from server first, ignore local exception
            if (putAllOp.isBridgeOperation()) {
              if (partialKeys.getFailure() instanceof CancelException) {
                runtimeException = (RuntimeException) partialKeys.getFailure();
              } else if (partialKeys.getFailure() instanceof LowMemoryException) {
                // fix for #43589
                throw partialKeys.getFailure();
              } else {
                runtimeException = new PutAllPartialResultException(partialKeys);
                if (isDebugEnabled) {
                  logger.debug("basicPutAll:" + partialKeys.detailString());
                }
              }
            } else {
              throw partialKeys.getFailure();
            }
          }
        }
      } catch (LowMemoryException lme) {
        throw lme;
      } catch (RuntimeException ex) {
        runtimeException = ex;
      } catch (Exception ex) {
        runtimeException = new RuntimeException(ex);
      } finally {
        putAllOp.getBaseEvent().release();
        putAllOp.freeOffHeapResources();
      }
      getDataView().postPutAll(putAllOp, succeeded, this);
    } finally {
      unlockRVVForBulkOp();
    }
    if (runtimeException != null) {
      throw runtimeException;
    }
    return succeeded;
  }

  VersionedObjectList basicRemoveAll(final Collection<Object> keys,
      final DistributedRemoveAllOperation removeAllOp, final List<VersionTag> retryVersions) {

    final boolean isDebugEnabled = logger.isDebugEnabled();
    final boolean isTraceEnabled = logger.isTraceEnabled();

    final EntryEventImpl event = removeAllOp.getBaseEvent();
    EventID eventId = event.getEventId();
    if (eventId == null && generateEventID()) {
      // We need to "reserve" the eventIds for the entries in map here
      event.reserveNewEventId(this.cache.getDistributedSystem(), keys.size());
      eventId = event.getEventId();
    }

    verifyRemoveAllKeys(keys);
    VersionedObjectList proxyResult = null;
    boolean partialResult = false;
    RuntimeException runtimeException = null;

    if (hasServerProxy()) {
      // send message to bridge server
      if (isTX()) {
        TXStateProxyImpl txState = (TXStateProxyImpl) this.cache.getTxManager().getTXState();
        txState.getRealDeal(null, this);
      }
      try {
        proxyResult = getServerProxy().removeAll(keys, eventId, event.getCallbackArgument());
        if (isDebugEnabled) {
          logger.debug("removeAll received response from server: {}", proxyResult);
        }
      } catch (PutAllPartialResultException e) {
        // adjust the map to only add succeeded entries, then apply the adjustedMap
        proxyResult = e.getSucceededKeysAndVersions();
        partialResult = true;
        if (isDebugEnabled) {
          logger.debug(
              "removeAll in client encountered a BulkOpPartialResultException: {}{}. Adjusted keys are: {}",
              e.getMessage(), getLineSeparator(), proxyResult.getKeys());
        }
        Throwable txException = e.getFailure();
        while (txException != null) {
          if (txException instanceof TransactionException) {
            runtimeException = (RuntimeException) txException;
            break;
          }
          txException = txException.getCause();
        }
        if (runtimeException == null) {
          runtimeException = new ServerOperationException(
              LocalizedStrings.Region_RemoveAll_Applied_PartialKeys_At_Server_0
                  .toLocalizedString(getFullPath()),
              e.getFailure());
        }
      }
    }

    final VersionedObjectList succeeded =
        new VersionedObjectList(keys.size(), true, this.getConcurrencyChecksEnabled());

    // If this is a transactional removeAll, we will not have version information as it is only
    // generated at commit
    // so treat transactional removeAll as if the server is not versioned.
    // If we have no storage then act as if the server is not versioned.
    final boolean serverIsVersioned = proxyResult != null && proxyResult.regionIsVersioned()
        && !isTX() && getDataPolicy().withStorage();
    if (!serverIsVersioned && !partialResult) {
      // since the server is not versioned and we do not have a partial result
      // get rid of the proxyResult info returned by the server.
      proxyResult = null;
    }

    lockRVVForBulkOp();
    try {
      try {
        int size = proxyResult == null ? keys.size() : proxyResult.size();

        if (isInternalRegion()) {
          if (isTraceEnabled) {
            logger.trace("size of removeAll result is {} keys are {} proxyResult is {}", size, keys,
                proxyResult);
          } else {
            if (isTraceEnabled) {
              logger.trace("size of removeAll result is {} keys are {} proxyResult is {}", size,
                  keys, proxyResult);
            }
          }
        } else {
          if (isTraceEnabled) {
            logger.trace("size of removeAll result is {} keys are {} proxyResult is {}", size, keys,
                proxyResult);
          }
        }

        final PutAllPartialResult partialKeys = new PutAllPartialResult(size);
        final Iterator iterator;
        final boolean isVersionedResults;
        if (proxyResult != null) {
          iterator = proxyResult.iterator();
          isVersionedResults = true;
        } else {
          iterator = keys.iterator();
          isVersionedResults = false;
        }

        // TODO: refactor this mess
        Runnable task = new Runnable() {
          @Override
          public void run() {
            int offset = 0;
            VersionTagHolder tagHolder = new VersionTagHolder();
            while (iterator.hasNext()) {
              stopper.checkCancelInProgress(null);
              tagHolder.setVersionTag(null);
              Object key;
              VersionTag versionTag = null;
              if (isVersionedResults) {
                Map.Entry mapEntry = (Map.Entry) iterator.next();
                key = mapEntry.getKey();
                versionTag = ((VersionedObjectList.Entry) mapEntry).getVersionTag();
                if (isDebugEnabled) {
                  logger.debug("removeAll key {} version={}", key, versionTag);
                }
                if (versionTag == null) {
                  if (isDebugEnabled) {
                    logger.debug(
                        "removeAll found invalid version tag, which means the entry is not found at server for key={}.",
                        key);
                  }
                  succeeded.addKeyAndVersion(key, null);
                  continue;
                }
                // No need for special handling here in removeAll.
                // We can just remove this key from the client with versionTag set to null.
              } else {
                key = iterator.next();
                if (isTraceEnabled) {
                  logger.trace("removeAll {}", key);
                }
              }

              try {
                if (serverIsVersioned) {
                  if (isDebugEnabled) {
                    logger.debug("associating version tag with {} version={}", key, versionTag);
                  }
                  // If we have received a version tag from a server, add it to the event
                  tagHolder.setVersionTag(versionTag);
                  tagHolder.setFromServer(true);
                } else if (retryVersions != null) {
                  VersionTag versionTag1 = retryVersions.get(offset);
                  if (versionTag1 != null) {
                    // If this is a retried event, and we have a version tag for the retry,
                    // add it to the event.
                    tagHolder.setVersionTag(versionTag1);
                  }
                }

                basicEntryRemoveAll(key, removeAllOp, offset, tagHolder);
                // now we must check again since the cache may have closed during
                // distribution causing this process to not receive and queue the
                // event for clients
                stopper.checkCancelInProgress(null);
                succeeded.addKeyAndVersion(key, tagHolder.getVersionTag());
              } catch (Exception ex) {
                partialKeys.saveFailedKey(key, ex);
              }
              offset++;
            }
          }
        };

        syncBulkOp(task, eventId);
        if (partialKeys.hasFailure()) {
          // Bug 51725: Now succeeded contains an order key list, may be missing the version tags.
          // Save reference of succeeded into partialKeys. The succeeded may be modified by
          // postRemoveAll() to fill in the version tags.
          partialKeys.setSucceededKeysAndVersions(succeeded);
          logger.info(
              LocalizedMessage.create(LocalizedStrings.Region_RemoveAll_Applied_PartialKeys_0_1,
                  new Object[] {getFullPath(), partialKeys}));
          if (isDebugEnabled) {
            logger.debug(partialKeys.detailString());
          }
          if (runtimeException == null) {
            // if received exception from server first, ignore local exception
            if (removeAllOp.isBridgeOperation()) {
              if (partialKeys.getFailure() instanceof CancelException) {
                runtimeException = (RuntimeException) partialKeys.getFailure();
              } else if (partialKeys.getFailure() instanceof LowMemoryException) {
                throw partialKeys.getFailure(); // fix for #43589
              } else {
                runtimeException = new PutAllPartialResultException(partialKeys);
                if (isDebugEnabled) {
                  logger.debug("basicRemoveAll: {}", partialKeys.detailString());
                }
              }
            } else {
              throw partialKeys.getFailure();
            }
          }
        }
      } catch (LowMemoryException lme) {
        throw lme;
      } catch (RuntimeException ex) {
        runtimeException = ex;
      } catch (Exception ex) {
        runtimeException = new RuntimeException(ex);
      } finally {
        removeAllOp.getBaseEvent().release();
        removeAllOp.freeOffHeapResources();
      }
      getDataView().postRemoveAll(removeAllOp, succeeded, this);
    } finally {
      unlockRVVForBulkOp();
    }
    if (runtimeException != null) {
      throw runtimeException;
    }
    return succeeded;
  }

  /**
   * bug #46924 - putAll can be partially applied when a clear() occurs, leaving the cache in an
   * inconsistent state. Set the RVV to "cache op in progress" so clear() will block until the
   * putAll completes. This won't work for non-replicate regions though since they uses one-hop
   * during basicPutPart2 to get a valid version tag.
   */
  private void lockRVVForBulkOp() {
    ARMLockTestHook testHook = getRegionMap().getARMLockTestHook();
    if (testHook != null) {
      testHook.beforeBulkLock(this);
    }

    if (this.versionVector != null && this.getDataPolicy().withReplication()) {
      this.versionVector.lockForCacheModification(this);
    }

    if (testHook != null) {
      testHook.afterBulkLock(this);
    }
  }

  private void unlockRVVForBulkOp() {
    ARMLockTestHook testHook = getRegionMap().getARMLockTestHook();
    if (testHook != null) {
      testHook.beforeBulkRelease(this);
    }

    if (this.versionVector != null && this.getDataPolicy().withReplication()) {
      this.versionVector.releaseCacheModificationLock(this);
    }

    if (testHook != null) {
      testHook.afterBulkRelease(this);
    }
  }

  public DistributedPutAllOperation newPutAllOperation(Map<?, ?> map, Object callbackArg) {
    if (map == null) {
      throw new NullPointerException(
          LocalizedStrings.AbstractRegion_MAP_CANNOT_BE_NULL.toLocalizedString());
    }
    if (map.isEmpty()) {
      return null;
    }

    checkReadiness();
    checkForLimitedOrNoAccess();
    discoverJTA();

    // Create a dummy event for the PutAll operation. Always create a
    // PutAll operation, even if there is no distribution, so that individual
    // events can be tracked and handed off to callbacks in postPutAll
    // No need for release since disallowOffHeapValues called.
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.PUTALL_CREATE, null, null,
        callbackArg, true, getMyId());

    event.disallowOffHeapValues();
    return new DistributedPutAllOperation(event, map.size(), false);
  }

  private DistributedRemoveAllOperation newRemoveAllOperation(Collection<?> keys,
      Object callbackArg) {
    if (keys == null) {
      throw new NullPointerException("The keys Collection passed to removeAll was null.");
    }
    if (keys.isEmpty()) {
      return null;
    }

    checkReadiness();
    checkForLimitedOrNoAccess();
    discoverJTA();

    // Create a dummy event for the removeAll operation. Always create a
    // removeAll operation, even if there is no distribution, so that individual
    // events can be tracked and handed off to callbacks in postRemoveAll
    // No need for release since disallowOffHeapValues called.
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.REMOVEALL_DESTROY, null,
        null, callbackArg, false, getMyId());
    event.disallowOffHeapValues();
    return new DistributedRemoveAllOperation(event, keys.size(), false);
  }

  /**
   * This performs the putAll operation for a specific key and value
   *
   * @param key the cache key
   * @param value the cache value
   * @param putallOp the DistributedPutAllOperation associated with the event
   * @param tagHolder holder for version tag
   * @throws TimeoutException if the operation times out
   * @throws CacheWriterException if a cache writer objects to the update
   */
  private void basicEntryPutAll(Object key, Object value, DistributedPutAllOperation putallOp,
      int offset, EntryEventImpl tagHolder) throws TimeoutException, CacheWriterException {

    assert putallOp != null;

    checkReadiness();
    if (value == null) {
      throw new NullPointerException(
          LocalizedStrings.LocalRegion_VALUE_CANNOT_BE_NULL.toLocalizedString());
    }

    validateArguments(key, value, null);

    // event is marked as a PUTALL_CREATE but if the entry exists it
    // will be changed to a PUTALL_UPDATE later on.

    @Released
    EntryEventImpl event =
        EntryEventImpl.createPutAllEvent(putallOp, this, Operation.PUTALL_CREATE, key, value);

    try {
      if (tagHolder != null) {
        event.setVersionTag(tagHolder.getVersionTag());
        event.setFromServer(tagHolder.isFromServer());
      }
      if (generateEventID()) {
        event.setEventId(new EventID(putallOp.getBaseEvent().getEventId(), offset));
      }

      // TODO: could be called once for the entire putAll instead of calling it for every key
      discoverJTA();

      /*
       * If this is tx, do putEntry, unless it is a local region?
       */
      performPutAllEntry(event);
      if (tagHolder != null) {
        tagHolder.setVersionTag(event.getVersionTag());
        tagHolder.isConcurrencyConflict(event.isConcurrencyConflict());
      }
    } finally {
      event.release();
    }
  }

  private void basicEntryRemoveAll(Object key, DistributedRemoveAllOperation op, int offset,
      EntryEventImpl tagHolder) throws TimeoutException, CacheWriterException {

    assert op != null;

    checkReadiness();
    validateKey(key);

    @Released
    EntryEventImpl event = EntryEventImpl.createRemoveAllEvent(op, this, key);

    try {
      if (tagHolder != null) {
        event.setVersionTag(tagHolder.getVersionTag());
        event.setFromServer(tagHolder.isFromServer());
      }
      if (generateEventID()) {
        event.setEventId(new EventID(op.getBaseEvent().getEventId(), offset));
      }

      // TODO: could be called once for the entire removeAll instead of calling it for every key
      discoverJTA();

      /*
       * If this is tx, do destroyEntry, unless it is a local region?
       */
      try {
        performRemoveAllEntry(event);
      } catch (EntryNotFoundException ignore) {
        if (event.getVersionTag() == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("RemoveAll encoutered EntryNotFoundException: event={}", event);
          }
        }
      }
      if (tagHolder != null) {
        tagHolder.setVersionTag(event.getVersionTag());
        tagHolder.isConcurrencyConflict(event.isConcurrencyConflict());
      }
    } finally {
      event.release();
    }
  }

  public void performPutAllEntry(EntryEventImpl event) {
    getDataView().putEntry(event, false, false, null, false, 0L, false);
  }

  public void performRemoveAllEntry(EntryEventImpl event) {
    basicDestroy(event, true, null);
  }

  @Override
  public void postPutAllFireEvents(DistributedPutAllOperation putAllOp,
      VersionedObjectList successfulPuts) {

    if (!this.getDataPolicy().withStorage() && this.getConcurrencyChecksEnabled()
        && putAllOp.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulPuts list for transmission back to the client
      successfulPuts.clear();
      putAllOp.fillVersionedObjectList(successfulPuts);
    }

    Set successfulKeys = new HashSet(successfulPuts.size());
    for (Object key : successfulPuts.getKeys()) {
      successfulKeys.add(key);
    }

    for (Iterator it = putAllOp.eventIterator(); it.hasNext();) {
      @Unretained
      EntryEventImpl event = (EntryEventImpl) it.next();

      if (successfulKeys.contains(event.getKey())) {
        EnumListenerEvent op = event.getOperation().isCreate() ? EnumListenerEvent.AFTER_CREATE
            : EnumListenerEvent.AFTER_UPDATE;
        invokePutCallbacks(op, event, !event.callbacksInvoked() && !event.isPossibleDuplicate(),
            this.isUsedForPartitionedRegionBucket
        /*
         * If this is replicated region, use "false". We must notify gateways inside RegionEntry
         * lock, NOT here, to preserve the order of events sent by gateways for same key. If this is
         * bucket region, use "true", because the event order is guaranteed
         */);
      }
    }
  }

  @Override
  public void postRemoveAllFireEvents(DistributedRemoveAllOperation removeAllOp,
      VersionedObjectList successfulOps) {

    if (!this.getDataPolicy().withStorage() && this.getConcurrencyChecksEnabled()
        && removeAllOp.getBaseEvent().isBridgeEvent()) {
      // if there is no local storage we need to transfer version information
      // to the successfulOps list for transmission back to the client
      successfulOps.clear();
      removeAllOp.fillVersionedObjectList(successfulOps);
    }

    Set successfulKeys = new HashSet(successfulOps.size());
    for (Object key : successfulOps.getKeys()) {
      successfulKeys.add(key);
    }

    for (Iterator it = removeAllOp.eventIterator(); it.hasNext();) {
      @Unretained
      EntryEventImpl event = (EntryEventImpl) it.next();

      if (successfulKeys.contains(event.getKey())) {
        invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, event,
            !event.callbacksInvoked() && !event.isPossibleDuplicate(),
            this.isUsedForPartitionedRegionBucket
        /*
         * If this is replicated region, use "false". We must notify gateways inside RegionEntry
         * lock, NOT here, to preserve the order of events sent by gateways for same key. If this is
         * bucket region, use "true", because the event order is guaranteed
         */);
      }
    }
  }

  @Override
  public long postPutAllSend(DistributedPutAllOperation putAllOp,
      VersionedObjectList successfulPuts) {
    /* No-op for local region of course */
    return -1;
  }

  @Override
  public long postRemoveAllSend(DistributedRemoveAllOperation op,
      VersionedObjectList successfulOps) {
    /* No-op for local region of course */
    return -1;
  }

  /**
   * DistributedRegion overrides isCurrentlyLockGrantor
   *
   * @see DistributedRegion#isCurrentlyLockGrantor()
   */
  @Override
  protected boolean isCurrentlyLockGrantor() {
    return false;
  }

  /**
   * Handle a local region destroy or a region close that was done on this region in a remote vm.
   * Currently the only thing needed is to have the advisor
   *
   * @param sender the id of the member that did the remote operation
   * @param topSerial the remote serialNumber for the top region (maybe root)
   * @param subregionSerialNumbers map of remote subregions to serialNumbers
   * @param regionDestroyed true if the region was destroyed on the remote host (as opposed to
   *        closed)
   * @since GemFire 5.0
   */
  void handleRemoteLocalRegionDestroyOrClose(InternalDistributedMember sender, int topSerial,
      Map subregionSerialNumbers, boolean regionDestroyed) {

    // go through initialization latches
    final int oldLevel = setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
      basicHandleRemoteLocalRegionDestroyOrClose(sender, topSerial, subregionSerialNumbers, false,
          regionDestroyed);
    } finally {
      setThreadInitLevelRequirement(oldLevel);
    }
  }

  /**
   * Does the core work for handleRemoteLocalRegionDestroyOrClose.
   *
   * @param sender the id of the member that did the remote operation
   * @param topSerial the remote serialNumber for the top region (maybe root)
   * @param subregionSerialNumbers remote map of subregions to serialNumbers
   * @since GemFire 5.0
   */
  private void basicHandleRemoteLocalRegionDestroyOrClose(InternalDistributedMember sender,
      int topSerial, Map subregionSerialNumbers, boolean subregion, boolean regionDestroyed) {

    // use topSerial unless this region is in subregionSerialNumbers map
    int serialForThisRegion = topSerial;

    if (subregion) {
      // OPTIMIZE: we don't know if this rgn is a subregion or the top region
      Integer serialNumber = (Integer) subregionSerialNumbers.get(getFullPath());
      if (serialNumber == null) {
        // sender didn't have this subregion
        return;
      } else {
        // non-null means this is a subregion under the destroyed region
        serialForThisRegion = serialNumber;
      }
    }

    // remove sender's serialForThisRegion from the advisor
    removeSenderFromAdvisor(sender, serialForThisRegion, regionDestroyed);

    // process subregions...
    for (Object regionObject : this.subregions.values()) {
      LocalRegion region = toRegion(regionObject);
      if (region != null && !region.isDestroyed()) {
        // recursively call basicHandleRemoteLocalRegionDestroyOrClose for subregions
        region.basicHandleRemoteLocalRegionDestroyOrClose(sender, topSerial, subregionSerialNumbers,
            true, regionDestroyed);
      }
    }
  }

  /**
   * Remove the specified sender from this regions advisor.
   *
   * @since GemFire 5.0
   */
  protected void removeSenderFromAdvisor(InternalDistributedMember sender, int serial,
      boolean regionDestroyed) {
    // nothing needs to be done here since LocalRegion does not have an advisor.
  }

  @Override
  public boolean isUsedForPartitionedRegionAdmin() {
    return this.isUsedForPartitionedRegionAdmin;
  }

  /**
   * This method determines whether this region should synchronize with peer replicated regions when
   * the given member has crashed.
   *
   * @param id the crashed member
   * @return true if synchronization should be attempted
   */
  public boolean shouldSyncForCrashedMember(InternalDistributedMember id) {
    return this.getConcurrencyChecksEnabled() && this.getDataPolicy().withReplication()
        && !this.isUsedForPartitionedRegionAdmin && !this.isUsedForMetaRegion
        && !this.isUsedForSerialGatewaySenderQueue;
  }

  /**
   * forces the diskRegion to switch the oplog
   *
   * @since GemFire 5.1
   */
  @Override
  public void forceRolling() throws DiskAccessException {
    if (this.diskRegion != null) {
      this.diskRegion.forceRolling();
    }
  }

  /**
   * TODO: return value is never used
   *
   * @deprecated as of prPersistSprint1 use forceCompaction instead
   */
  @Deprecated
  public boolean notifyToRoll() {
    return forceCompaction();
  }

  /**
   * filterProfile holds CQ and registerInterest information for clients having this region
   */
  FilterProfile filterProfile;

  /**
   *
   * @return int array containing the IDs of the oplogs which will potentially get rolled else null
   *         if no oplogs were available at the time of signal or region is not having disk
   *         persistence. Pls note that the actual number of oplogs rolled may be more than what is
   *         indicated
   * @since GemFire prPersistSprint1
   */
  @Override
  public boolean forceCompaction() {
    DiskRegion region = getDiskRegion();
    if (region != null) {
      if (region.isCompactionPossible()) {
        return region.forceCompaction();
      } else {
        throw new IllegalStateException(
            "To call notifyToCompact you must configure the region with <disk-write-attributes allow-force-compaction=true/>");
      }
    } else {
      return false;
    }
  }

  @Override
  public File[] getDiskDirs() {
    if (getDiskStore() != null) {
      return getDiskStore().getDiskDirs();
    } else {
      return this.diskDirs;
    }
  }

  @Override
  public int[] getDiskDirSizes() {
    if (getDiskStore() != null) {
      return getDiskStore().getDiskDirSizes();
    } else {
      return this.diskSizes;
    }
  }

  /**
   * @return Returns the isUsedForPartitionedRegionBucket.
   */
  @Override
  public boolean isUsedForPartitionedRegionBucket() {
    return this.isUsedForPartitionedRegionBucket;
  }

  public boolean isUsedForSerialGatewaySenderQueue() {
    return this.isUsedForSerialGatewaySenderQueue;
  }

  public boolean isUsedForParallelGatewaySenderQueue() {
    return this.isUsedForParallelGatewaySenderQueue;
  }

  public void removeCacheServiceProfile(String profileID) {
    cacheServiceProfileUpdateLock.lock();
    try {
      this.cacheServiceProfiles.remove(profileID);
    } finally {
      cacheServiceProfileUpdateLock.unlock();
    }

  }

  public AbstractGatewaySender getSerialGatewaySender() {
    return this.serialGatewaySender;
  }

  boolean isParallelWanEnabled() {
    Set<String> regionGatewaySenderIds = this.getAllGatewaySenderIds();
    if (regionGatewaySenderIds.isEmpty()) {
      return false;
    }
    Set<GatewaySender> cacheGatewaySenders = getCache().getAllGatewaySenders();
    for (GatewaySender sender : cacheGatewaySenders) {
      if (regionGatewaySenderIds.contains(sender.getId()) && sender.isParallel()) {
        return true;
      }
    }
    return false;
  }

  /**
   * A convenience method to get the PartitionedRegion for a Bucket
   *
   * @return If this is an instance of {@link BucketRegion}, returns the {@link PartitionedRegion}
   *         otherwise throws an IllegalArgumentException
   */
  @Override
  public PartitionedRegion getPartitionedRegion() {
    if (!this.isUsedForPartitionedRegionBucket) {
      throw new IllegalArgumentException();
    }
    return ((Bucket) this).getPartitionedRegion();
  }

  /**
   * @return Returns the isUsedForMetaRegion.
   */
  @Override
  public boolean isUsedForMetaRegion() {
    return this.isUsedForMetaRegion;
  }

  @Override
  public boolean isMetaRegionWithTransactions() {
    return this.isMetaRegionWithTransactions;
  }

  /**
   * @return true if this is not a user visible region
   */
  @Override
  public boolean isInternalRegion() {
    return isSecret() || isUsedForMetaRegion() || isUsedForPartitionedRegionAdmin()
        || isUsedForPartitionedRegionBucket();
  }

  public Map<String, CacheServiceProfile> getCacheServiceProfiles() {
    return this.cacheServiceProfiles.getSnapshot();
  }

  public void addCacheServiceProfile(CacheServiceProfile profile) {
    cacheServiceProfileUpdateLock.lock();
    try {
      this.cacheServiceProfiles.put(profile.getId(), profile);
    } finally {
      cacheServiceProfileUpdateLock.unlock();
    }
  }

  @Override
  public LoaderHelper createLoaderHelper(Object key, Object callbackArgument,
      boolean netSearchAllowed, boolean netLoadAllowed, SearchLoadAndWriteProcessor searcher) {
    return new LoaderHelperImpl(this, key, callbackArgument, netSearchAllowed, netLoadAllowed,
        searcher);
  }

  /** visitor over the CacheProfiles to check if the region has a CacheLoader */
  private static final DistributionAdvisor.ProfileVisitor<Void> netLoaderVisitor =
      new DistributionAdvisor.ProfileVisitor<Void>() {
        @Override
        public boolean visit(DistributionAdvisor advisor, Profile profile, int profileIndex,
            int numProfiles, Void aggregate) {
          assert profile instanceof CacheProfile;
          final CacheProfile prof = (CacheProfile) profile;

          // if region in cache is not yet initialized, exclude
          if (prof.regionInitialized) { // fix for bug 41102
            // cut the visit short if we find a CacheLoader
            return !prof.hasCacheLoader;
          }
          // continue the visit
          return true;
        }
      };

  /** visitor over the CacheProfiles to check if the region has a CacheWriter */
  private static final DistributionAdvisor.ProfileVisitor<Void> netWriterVisitor =
      new DistributionAdvisor.ProfileVisitor<Void>() {
        @Override
        public boolean visit(DistributionAdvisor advisor, Profile profile, int profileIndex,
            int numProfiles, Void aggregate) {
          assert profile instanceof CacheProfile;
          final CacheProfile prof = (CacheProfile) profile;

          // if region in cache is in recovery
          if (!prof.inRecovery) {
            // cut the visit short if we find a CacheWriter
            return !prof.hasCacheWriter;
          }
          // continue the visit
          return true;
        }
      };

  /**
   * Return true if some other member of the distributed system, not including self, has a
   * CacheLoader defined on the region.
   */
  boolean hasNetLoader(CacheDistributionAdvisor distAdvisor) {
    return !distAdvisor.accept(netLoaderVisitor, null);
  }

  /**
   * Used to indicate that this region is used for internal purposes
   */
  @Override
  public boolean isSecret() {
    return false;
  }

  /**
   * whether concurrency checks should be disabled for this region
   */
  @Override
  public boolean supportsConcurrencyChecks() {
    return !isSecret() || this.getDataPolicy().withPersistence();
  }

  /**
   * Used to prevent notification of bridge clients, typically used for internal "meta" regions and
   * if the cache doesn't have any bridge servers
   *
   * @return true only if it's cache has bridge servers and this is nt a meta region
   */
  @Override
  public boolean shouldNotifyBridgeClients() {
    return !this.cache.getCacheServers().isEmpty() && !this.isUsedForPartitionedRegionAdmin
        && !this.isUsedForPartitionedRegionBucket && !this.isUsedForMetaRegion;
  }

  /**
   * Check if the region has has a Listener or not
   *
   * @return true only if this region has a Listener
   */
  @Override
  public boolean shouldDispatchListenerEvent() {
    return hasListener();
  }

  /**
   * Called by ccn when a client goes away
   *
   * @since GemFire 5.7
   */
  @Override
  public void cleanupForClient(CacheClientNotifier clientNotifier, ClientProxyMembershipID client) {
    if (this.cache.isClosed() || this.isDestroyed) {
      return;
    }

    this.filterProfile.cleanupForClient(clientNotifier, client);

    for (Object regionObject : new SubregionsSet(false)) {
      LocalRegion region = (LocalRegion) regionObject;
      region.cleanupForClient(clientNotifier, client);
    }
  }

  /**
   * Returns the CQ/interest profile for this region
   */
  @Override
  public FilterProfile getFilterProfile() {
    return this.filterProfile;
  }

  /**
   * Returns a map of subregions that were destroyed when this region was destroyed. Map contains
   * subregion full paths to SerialNumbers. Return is defined as HashMap because
   * DestroyRegionOperation will provide the map to DataSerializer.writeHashMap which requires
   * HashMap. Returns {@link #destroyedSubregionSerialNumbers}.
   *
   * @return HashMap of subregions to SerialNumbers
   * @throws IllegalStateException if this region has not been destroyed
   */
  protected HashMap getDestroyedSubregionSerialNumbers() {
    if (!this.isDestroyed) {
      throw new IllegalStateException(
          LocalizedStrings.LocalRegion_REGION_0_MUST_BE_DESTROYED_BEFORE_CALLING_GETDESTROYEDSUBREGIONSERIALNUMBERS
              .toLocalizedString(getFullPath()));
    }
    return this.destroyedSubregionSerialNumbers;
  }

  /**
   * Returns a map of subregion full paths to SerialNumbers. Caller must have acquired the
   * destroyLock if a stable view is desired. Key is String, value is Integer.
   *
   * @return HashMap of subregions to SerialNumbers
   */
  private HashMap collectSubregionSerialNumbers() {
    HashMap map = new HashMap();
    addSubregionSerialNumbers(map);
    return map;
  }

  /**
   * Iterates over all subregions to put the full path and serial number into the provided map.
   *
   * @param map the map to put the full path and serial number into for each subregion
   */
  private void addSubregionSerialNumbers(Map map) {
    // iterate over all subregions to gather serialNumbers and recurse
    for (Object entryObject : this.subregions.entrySet()) {
      Map.Entry entry = (Map.Entry) entryObject;
      LocalRegion subregion = (LocalRegion) entry.getValue();
      map.put(subregion.getFullPath(), subregion.getSerialNumber());

      // recursively call down into each subregion tree
      subregion.addSubregionSerialNumbers(map);
    }
  }

  @Override
  public SelectResults query(String predicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {

    if (predicate == null) {
      throw new IllegalArgumentException(
          "The input query predicate is null. A null predicate is not allowed.");
    }
    predicate = predicate.trim();
    SelectResults results;
    if (hasServerProxy()) {

      // Trim whitespace
      String queryString = constructRegionQueryString(predicate.trim());

      try {
        results = getServerProxy().query(queryString, null);
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
          cause = e;
        }
        throw new QueryInvocationTargetException(e.getMessage(), cause);
      }
    } else {
      // TODO: params size is always zero so this whole block is wasted
      Object[] params = new Object[0];
      QueryService qs = getGemFireCache().getLocalQueryService();
      String queryStr = constructRegionQueryString(predicate.trim());
      DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
      if (query.getRegionsInQuery(params).size() != 1) {
        throw new QueryInvalidException(
            "Prevent multiple region query from being executed through region.query()");
      }
      results = (SelectResults) query.execute(params);
    }
    return results;
  }

  private String constructRegionQueryString(final String predicate) throws QueryInvalidException {
    // send it as is to the server
    boolean matches = false;
    for (Pattern queryPattern : QUERY_PATTERNS) {
      if (queryPattern.matcher(predicate).matches()) {
        if (!predicate.contains(getName())) {
          throw new QueryInvalidException(
              "Should not execute region.query with a different region in the from clause: "
                  + getName() + " was not present in:" + predicate);
        }
        matches = true;
        break;
      }
    }
    final String queryString;// Compare the query patterns to the 'predicate'. If one matches,
    if (matches) {
      queryString = predicate;
    } else {
      queryString = "select * from " + getFullPath() + " this where " + predicate;
    }
    return queryString;
  }

  /**
   * Execute the provided named function in all locations that contain the given keys. So function
   * can be executed on just one fabric node, executed in parallel on a subset of nodes in parallel
   * across all the nodes.
   *
   * @since GemFire 5.8Beta
   */
  public ResultCollector executeFunction(final DistributedRegionFunctionExecutor execution,
      final Function function, final Object args, final ResultCollector rc, final Set filter,
      final ServerToClientFunctionResultSender sender) {

    if (function.optimizeForWrite()
        && !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      MemoryThresholdInfo info = getAtomicThresholdInfo();
      if (info.isMemoryThresholdReached()) {
        Set<DistributedMember> members = info.getMembersThatReachedThreshold();
        throw new LowMemoryException(
            LocalizedStrings.ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1
                .toLocalizedString(function.getId(), members),
            members);
      }
    }
    final LocalResultCollector<?, ?> resultCollector =
        execution.getLocalResultCollector(function, rc);
    final DistributionManager dm = getDistributionManager();
    execution.setExecutionNodes(Collections.singleton(getMyId()));

    final DistributedRegionFunctionResultSender resultSender =
        new DistributedRegionFunctionResultSender(dm, resultCollector, function, sender);
    final RegionFunctionContextImpl context = new RegionFunctionContextImpl(cache, function.getId(),
        LocalRegion.this, args, filter, null, null, resultSender, execution.isReExecute());
    execution.executeFunctionOnLocalNode(function, context, resultSender, dm, isTX());
    return resultCollector;
  }

  @Override
  public void onEvent(MemoryEvent event) {
    if (logger.isDebugEnabled()) {
      logger.debug("Region:{} received a Memory event.{}", this, event);
    }
    setMemoryThresholdFlag(event);
  }

  protected void setMemoryThresholdFlag(MemoryEvent event) {
    assert getScope().isLocal();
    if (event.isLocal()) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()
          && (event.getType() == ResourceType.HEAP_MEMORY
              || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
        // start rejecting operations
        setMemoryThresholdReached(true);
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()
          && (event.getType() == ResourceType.HEAP_MEMORY
              || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
        setMemoryThresholdReached(false);
      }
    }
  }

  void updateSizeOnClearRegion(int sizeBeforeClear) {
    // Only needed by BucketRegion
  }

  /**
   * Calculate and return the size of a value for updating the bucket size. Zero is always returned
   * for non-bucket regions.
   */
  @Override
  public int calculateValueSize(Object value) {
    // Only needed by BucketRegion
    return 0;
  }

  @Override
  public int calculateRegionEntryValueSize(RegionEntry regionEntry) {
    // Only needed by BucketRegion
    return 0;
  }

  @Override
  public void updateSizeOnPut(Object key, int oldSize, int newSize) {
    // Only needed by BucketRegion
  }

  @Override
  public void updateSizeOnCreate(Object key, int newSize) {
    // Only needed by BucketRegion
  }

  @Override
  public void updateSizeOnRemove(Object key, int oldSize) {
    // Only needed by BucketRegion
  }

  // TODO: return value is never used
  @Override
  public int updateSizeOnEvict(Object key, int oldSize) {
    // Only needed by BucketRegion
    return 0;
  }

  @Override
  public void updateSizeOnFaultIn(Object key, int newSize, int bytesOnDisk) {
    // Only needed by BucketRegion
  }

  @Override
  public void initializeStats(long numEntriesInVM, long numOverflowOnDisk,
      long numOverflowBytesOnDisk) {
    getDiskRegion().getStats().incNumEntriesInVM(numEntriesInVM);
    getDiskRegion().getStats().incNumOverflowOnDisk(numOverflowOnDisk);
  }

  /**
   * This method is meant to be overridden by DistributedRegion and PartitionedRegions to cleanup
   * CRITICAL state
   */
  public void removeCriticalMember(DistributedMember member) {
    // should not be called for LocalRegion
    Assert.assertTrue(false);
  }

  /**
   * Initialize the set of remote members whose memory state is critical. This is called when
   * registering using
   * {@link InternalResourceManager#addResourceListener(ResourceType, ResourceListener)}. It should
   * only be called once and very early in this region's lifetime.
   *
   * @param localMemoryIsCritical true if the local memory is in a critical state
   * @param criticalMembers set of members whose memory is in a critical state
   * @see ResourceManager#setCriticalHeapPercentage(float) and
   *      ResourceManager#setCriticalOffHeapPercentage(float)
   * @since GemFire 6.0
   */
  public void initialCriticalMembers(boolean localMemoryIsCritical,
      Set<InternalDistributedMember> criticalMembers) {
    assert getScope().isLocal();
    if (localMemoryIsCritical) {
      setMemoryThresholdReached(true);
    }
  }

  @Override
  public void destroyRecoveredEntry(Object key) {
    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.LOCAL_DESTROY, key, null, null,
        false, getMyId(), false);
    try {
      event.inhibitCacheListenerNotification(true);
      mapDestroy(event, true, false, null, false, true);
    } finally {
      event.release();
    }
  }

  @Override
  public boolean lruLimitExceeded() {
    return this.entries.lruLimitExceeded(getDiskRegionView());
  }

  @Override
  public DiskEntry getDiskEntry(Object key) {
    // should return tombstone as an valid entry
    RegionEntry regionEntry = this.entries.getEntry(key);
    if (regionEntry != null && regionEntry.isRemoved() && !regionEntry.isTombstone()) {
      regionEntry = null;
    }
    return (DiskEntry) regionEntry;
  }

  /**
   * Fetch the Region which stores the given key The resulting Region will be used for a read
   * operation e.g. Region.get
   *
   * @param entryKey key to evaluate to determine the returned region
   * @return region that stores the key
   */
  @Override
  public LocalRegion getDataRegionForRead(KeyInfo entryKey) {
    return this;
  }

  /**
   * Fetch the Region which stores the given key. The resulting Region will be used for a write
   * operation e.g. Region.put
   *
   * @param entryKey key to evaluate to determine the returned region
   * @return region that stores the key
   */
  @Override
  public LocalRegion getDataRegionForWrite(KeyInfo entryKey) {
    return this;
  }

  /**
   * @return a set of keys, intended for use by the various Region set operations such as
   *         {@link EntriesSet}
   */
  Set getRegionKeysForIteration() {
    return getRegionMap().keySet();
  }

  public InternalDataView getSharedDataView() {
    return this.sharedDataView;
  }

  /**
   * Used to bootstrap txState.
   *
   * @return localMember for local and distributedRegions, member with primary bucket for
   *         partitionedRegions
   */
  @Override
  public DistributedMember getOwnerForKey(KeyInfo key) {
    return getMyId();
  }

  /**
   * @return the wrapped {@link KeyInfo}
   */
  @Override
  public KeyInfo getKeyInfo(Object key) {
    return new KeyInfo(key, null, null);
  }

  public KeyInfo getKeyInfo(Object key, Object callbackArg) {
    return getKeyInfo(key, null, callbackArg);
  }

  @Override
  public KeyInfo getKeyInfo(Object key, Object value, Object callbackArg) {
    return new KeyInfo(key, null, callbackArg);
  }

  /**
   * @see #basicGetEntry(Object)
   */
  protected RegionEntry basicGetTXEntry(KeyInfo keyInfo) {
    return basicGetEntry(keyInfo.getKey());
  }

  @Override
  public void senderCreated() {
    distributeUpdatedProfileOnSenderCreation();
  }

  void distributeUpdatedProfileOnSenderCreation() {
    // No op
  }

  static class RegionPerfStats extends CachePerfStats {
    CachePerfStats cachePerfStats;

    RegionPerfStats(InternalCache cache, CachePerfStats superStats, String regionName) {
      super(cache.getDistributedSystem(), regionName);
      this.cachePerfStats = superStats;
    }

    @Override
    public void incReliableQueuedOps(int inc) {
      this.stats.incInt(reliableQueuedOpsId, inc);
      this.cachePerfStats.incReliableQueuedOps(inc);
    }

    @Override
    public void incReliableQueueSize(int inc) {
      this.stats.incInt(reliableQueueSizeId, inc);
      this.cachePerfStats.incReliableQueueSize(inc);
    }

    @Override
    public void incReliableQueueMax(int inc) {
      this.stats.incInt(reliableQueueMaxId, inc);
      this.cachePerfStats.incReliableQueueMax(inc);
    }

    @Override
    public void incReliableRegions(int inc) {
      this.stats.incInt(reliableRegionsId, inc);
      this.cachePerfStats.incReliableRegions(inc);
    }

    @Override
    public void incReliableRegionsMissing(int inc) {
      this.stats.incInt(reliableRegionsMissingId, inc);
      this.cachePerfStats.incReliableRegionsMissing(inc);
    }

    @Override
    public void incReliableRegionsQueuing(int inc) {
      this.stats.incInt(reliableRegionsQueuingId, inc);
      this.cachePerfStats.incReliableRegionsQueuing(inc);
    }

    @Override
    public void incReliableRegionsMissingFullAccess(int inc) {
      this.stats.incInt(reliableRegionsMissingFullAccessId, inc);
      this.cachePerfStats.incReliableRegionsMissingFullAccess(inc);
    }

    @Override
    public void incReliableRegionsMissingLimitedAccess(int inc) {
      this.stats.incInt(reliableRegionsMissingLimitedAccessId, inc);
      this.cachePerfStats.incReliableRegionsMissingLimitedAccess(inc);
    }

    @Override
    public void incReliableRegionsMissingNoAccess(int inc) {
      this.stats.incInt(reliableRegionsMissingNoAccessId, inc);
      this.cachePerfStats.incReliableRegionsMissingNoAccess(inc);
    }

    @Override
    public void incQueuedEvents(int inc) {
      this.stats.incLong(eventsQueuedId, inc);
      this.cachePerfStats.incQueuedEvents(inc);
    }

    /**
     * @return the timestamp that marks the start of the operation
     */
    @Override
    public long startLoad() {
      this.stats.incInt(loadsInProgressId, 1);
      return this.cachePerfStats.startLoad();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endLoad(long start) {
      // note that load times are used in health checks and
      // should not be disabled by enableClockStats==false

      // don't use getStatTime so always enabled
      long ts = NanoTimer.getTime();
      this.stats.incLong(loadTimeId, ts - start);
      this.stats.incInt(loadsInProgressId, -1);
      this.stats.incInt(loadsCompletedId, 1);

      // need to think about timings
      this.cachePerfStats.endLoad(start);
    }

    /**
     * @return the timestamp that marks the start of the operation
     */
    @Override
    public long startNetload() {
      this.stats.incInt(netloadsInProgressId, 1);
      this.cachePerfStats.startNetload();
      return getStatTime();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endNetload(long start) {
      if (enableClockStats) {
        this.stats.incLong(netloadTimeId, getStatTime() - start);
      }
      this.stats.incInt(netloadsInProgressId, -1);
      this.stats.incInt(netloadsCompletedId, 1);
      this.cachePerfStats.endNetload(start);
    }

    /**
     * @return the timestamp that marks the start of the operation
     */
    @Override
    public long startNetsearch() {
      this.stats.incInt(netsearchesInProgressId, 1);
      return this.cachePerfStats.startNetsearch();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endNetsearch(long start) {
      // note that netsearch is used in health checks and timings should
      // not be disabled by enableClockStats==false

      // don't use getStatTime so always enabled
      long ts = NanoTimer.getTime();
      this.stats.incLong(netsearchTimeId, ts - start);
      this.stats.incInt(netsearchesInProgressId, -1);
      this.stats.incInt(netsearchesCompletedId, 1);
      this.cachePerfStats.endNetsearch(start);
    }

    /**
     * @return the timestamp that marks the start of the operation
     */
    @Override
    public long startCacheWriterCall() {
      this.stats.incInt(cacheWriterCallsInProgressId, 1);
      this.cachePerfStats.startCacheWriterCall();
      return getStatTime();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endCacheWriterCall(long start) {
      if (enableClockStats) {
        this.stats.incLong(cacheWriterCallTimeId, getStatTime() - start);
      }
      this.stats.incInt(cacheWriterCallsInProgressId, -1);
      this.stats.incInt(cacheWriterCallsCompletedId, 1);
      this.cachePerfStats.endCacheWriterCall(start);
    }

    /**
     * @return the timestamp that marks the start of the operation
     * @since GemFire 3.5
     */
    @Override
    public long startCacheListenerCall() {
      this.stats.incInt(cacheListenerCallsInProgressId, 1);
      this.cachePerfStats.startCacheListenerCall();
      return getStatTime();
    }

    /**
     * @param start the timestamp taken when the operation started
     * @since GemFire 3.5
     */
    @Override
    public void endCacheListenerCall(long start) {
      if (enableClockStats) {
        this.stats.incLong(cacheListenerCallTimeId, getStatTime() - start);
      }
      this.stats.incInt(cacheListenerCallsInProgressId, -1);
      this.stats.incInt(cacheListenerCallsCompletedId, 1);
      this.cachePerfStats.endCacheListenerCall(start);
    }

    /**
     * @return the timestamp that marks the start of the operation
     */
    @Override
    public long startGetInitialImage() {
      this.stats.incInt(getInitialImagesInProgressId, 1);
      this.cachePerfStats.startGetInitialImage();
      return getStatTime();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endGetInitialImage(long start) {
      if (enableClockStats) {
        this.stats.incLong(getInitialImageTimeId, getStatTime() - start);
      }
      this.stats.incInt(getInitialImagesInProgressId, -1);
      this.stats.incInt(getInitialImagesCompletedId, 1);
      this.cachePerfStats.endGetInitialImage(start);
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endNoGIIDone(long start) {
      if (enableClockStats) {
        this.stats.incLong(getInitialImageTimeId, getStatTime() - start);
      }
      this.stats.incInt(getInitialImagesInProgressId, -1);
      this.cachePerfStats.endNoGIIDone(start);
    }

    @Override
    public void incGetInitialImageKeysReceived() {
      this.stats.incInt(getInitialImageKeysReceivedId, 1);
      this.cachePerfStats.incGetInitialImageKeysReceived();
    }

    @Override
    public long startIndexUpdate() {
      this.stats.incInt(indexUpdateInProgressId, 1);
      this.cachePerfStats.startIndexUpdate();
      return getStatTime();
    }

    @Override
    public void endIndexUpdate(long start) {
      long ts = getStatTime();
      this.stats.incLong(indexUpdateTimeId, ts - start);
      this.stats.incInt(indexUpdateInProgressId, -1);
      this.stats.incInt(indexUpdateCompletedId, 1);
      this.cachePerfStats.endIndexUpdate(start);
    }

    @Override
    public void incRegions(int inc) {
      this.stats.incInt(regionsId, inc);
      this.cachePerfStats.incRegions(inc);

    }

    @Override
    public void incPartitionedRegions(int inc) {
      this.stats.incInt(partitionedRegionsId, inc);
      this.cachePerfStats.incPartitionedRegions(inc);
    }

    @Override
    public void incDestroys() {
      this.stats.incInt(destroysId, 1);
      this.cachePerfStats.incDestroys();
    }

    @Override
    public void incCreates() {
      this.stats.incInt(createsId, 1);
      this.cachePerfStats.incCreates();
    }

    @Override
    public void incInvalidates() {
      this.stats.incInt(invalidatesId, 1);
      this.cachePerfStats.incInvalidates();
    }

    @Override
    public void incTombstoneCount(int amount) {
      this.stats.incInt(tombstoneCountId, amount);
      this.cachePerfStats.incTombstoneCount(amount);
    }

    @Override
    public void incTombstoneGCCount() {
      this.stats.incInt(tombstoneGCCountId, 1);
      this.cachePerfStats.incTombstoneGCCount();
    }

    @Override
    public void incClearTimeouts() {
      this.stats.incInt(clearTimeoutsId, 1);
      this.cachePerfStats.incClearTimeouts();
    }

    @Override
    public void incConflatedEventsCount() {
      this.stats.incLong(conflatedEventsId, 1);
      this.cachePerfStats.incConflatedEventsCount();
    }

    /**
     * @param start the timestamp taken when the operation started
     */
    @Override
    public void endGet(long start, boolean miss) {
      if (enableClockStats) {
        this.stats.incLong(getTimeId, getStatTime() - start);
      }
      this.stats.incInt(getsId, 1);
      if (miss) {
        this.stats.incInt(missesId, 1);
      }
      this.cachePerfStats.endGet(start, miss);
    }

    /**
     * @param start the timestamp taken when the operation started
     * @param isUpdate true if the put was an update (origin remote)
     */
    @Override
    public long endPut(long start, boolean isUpdate) {
      long total = 0;
      if (isUpdate) {
        this.stats.incInt(updatesId, 1);
        if (enableClockStats) {
          total = getStatTime() - start;
          this.stats.incLong(updateTimeId, total);
        }
      } else {
        this.stats.incInt(putsId, 1);
        if (enableClockStats) {
          total = getStatTime() - start;
          this.stats.incLong(putTimeId, total);
        }
      }
      this.cachePerfStats.endPut(start, isUpdate);
      return total;
    }

    @Override
    public void endPutAll(long start) {
      this.stats.incInt(putallsId, 1);
      if (enableClockStats) {
        this.stats.incLong(putallTimeId, getStatTime() - start);
      }
      this.cachePerfStats.endPutAll(start);
    }

    @Override
    public void endQueryExecution(long executionTime) {
      this.stats.incInt(queryExecutionsId, 1);
      if (enableClockStats) {
        this.stats.incLong(queryExecutionTimeId, executionTime);
      }
      this.cachePerfStats.endQueryExecution(executionTime);
    }

    @Override
    public void endQueryResultsHashCollisionProbe(long start) {
      if (enableClockStats) {
        this.stats.incLong(queryResultsHashCollisionProbeTimeId, getStatTime() - start);
      }
      this.cachePerfStats.endQueryResultsHashCollisionProbe(start);
    }

    @Override
    public void incQueryResultsHashCollisions() {
      this.stats.incInt(queryResultsHashCollisionsId, 1);
      this.cachePerfStats.incQueryResultsHashCollisions();
    }

    @Override
    public void incTxConflictCheckTime(long delta) {
      this.stats.incLong(txConflictCheckTimeId, delta);
      this.cachePerfStats.incTxConflictCheckTime(delta);
    }

    @Override
    public void txSuccess(long opTime, long txLifeTime, int txChanges) {
      this.stats.incInt(txCommitsId, 1);
      this.stats.incInt(txCommitChangesId, txChanges);
      this.stats.incLong(txCommitTimeId, opTime);
      this.stats.incLong(txSuccessLifeTimeId, txLifeTime);
      this.cachePerfStats.txSuccess(opTime, txLifeTime, txChanges);
    }

    @Override
    public void txFailure(long opTime, long txLifeTime, int txChanges) {
      this.stats.incInt(txFailuresId, 1);
      this.stats.incInt(txFailureChangesId, txChanges);
      this.stats.incLong(txFailureTimeId, opTime);
      this.stats.incLong(txFailedLifeTimeId, txLifeTime);
      this.cachePerfStats.txFailure(opTime, txLifeTime, txChanges);
    }

    @Override
    public void txRollback(long opTime, long txLifeTime, int txChanges) {
      this.stats.incInt(txRollbacksId, 1);
      this.stats.incInt(txRollbackChangesId, txChanges);
      this.stats.incLong(txRollbackTimeId, opTime);
      this.stats.incLong(txRollbackLifeTimeId, txLifeTime);
      this.cachePerfStats.txRollback(opTime, txLifeTime, txChanges);
    }

    @Override
    public void incEventQueueSize(int items) {
      this.stats.incInt(eventQueueSizeId, items);
      this.cachePerfStats.incEventQueueSize(items);
    }

    @Override
    public void incEventQueueThrottleCount(int items) {
      this.stats.incInt(eventQueueThrottleCountId, items);
      this.cachePerfStats.incEventQueueThrottleCount(items);
    }

    @Override
    protected void incEventQueueThrottleTime(long nanos) {
      this.stats.incLong(eventQueueThrottleTimeId, nanos);
      this.cachePerfStats.incEventQueueThrottleTime(nanos);
    }

    @Override
    protected void incEventThreads(int items) {
      this.stats.incInt(eventThreadsId, items);
      this.cachePerfStats.incEventThreads(items);
    }

    @Override
    public void incEntryCount(int delta) {
      this.stats.incLong(entryCountId, delta);
      this.cachePerfStats.incEntryCount(delta);
    }

    @Override
    public void incRetries() {
      this.stats.incInt(retriesId, 1);
      this.cachePerfStats.incRetries();
    }

    @Override
    public void incDiskTasksWaiting() {
      this.stats.incInt(diskTasksWaitingId, 1);
      this.cachePerfStats.incDiskTasksWaiting();
    }

    @Override
    public void decDiskTasksWaiting() {
      this.stats.incInt(diskTasksWaitingId, -1);
      this.cachePerfStats.decDiskTasksWaiting();
    }

    @Override
    public void decDiskTasksWaiting(int count) {
      this.stats.incInt(diskTasksWaitingId, -count);
      this.cachePerfStats.decDiskTasksWaiting(count);
    }

    @Override
    public void incEvictorJobsStarted() {
      this.stats.incInt(evictorJobsStartedId, 1);
      this.cachePerfStats.incEvictorJobsStarted();
    }

    @Override
    public void incEvictorJobsCompleted() {
      this.stats.incInt(evictorJobsCompletedId, 1);
      this.cachePerfStats.incEvictorJobsCompleted();
    }

    @Override
    public void incEvictorQueueSize(int delta) {
      this.stats.incInt(evictorQueueSizeId, delta);
      this.cachePerfStats.incEvictorQueueSize(delta);
    }

    @Override
    public void incEvictWorkTime(long delta) {
      this.stats.incLong(evictWorkTimeId, delta);
      this.cachePerfStats.incEvictWorkTime(delta);
    }

    @Override
    public void incClearCount() {
      this.stats.incInt(clearsId, 1);
      this.cachePerfStats.incClearCount();
    }

    @Override
    public void incPRQueryRetries() {
      this.stats.incLong(partitionedRegionQueryRetriesId, 1);
      this.cachePerfStats.incPRQueryRetries();
    }

    @Override
    public void incMetaDataRefreshCount() {
      this.stats.incLong(metaDataRefreshCountId, 1);
      this.cachePerfStats.incMetaDataRefreshCount();
    }

    @Override
    public void endImport(long entryCount, long start) {
      this.stats.incLong(importedEntriesCountId, entryCount);
      if (enableClockStats) {
        this.stats.incLong(importTimeId, getStatTime() - start);
      }
      this.cachePerfStats.endImport(entryCount, start);
    }

    @Override
    public void endExport(long entryCount, long start) {
      this.stats.incLong(exportedEntriesCountId, entryCount);
      if (enableClockStats) {
        this.stats.incLong(exportTimeId, getStatTime() - start);
      }
      this.cachePerfStats.endExport(entryCount, start);
    }

    @Override
    public long startCompression() {
      this.stats.incLong(compressionCompressionsId, 1);
      this.cachePerfStats.stats.incLong(compressionCompressionsId, 1);
      return getStatTime();
    }

    @Override
    public void endCompression(long startTime, long startSize, long endSize) {
      if (enableClockStats) {
        long time = getStatTime() - startTime;
        this.stats.incLong(compressionCompressTimeId, time);
        this.cachePerfStats.stats.incLong(compressionCompressTimeId, time);
      }

      this.stats.incLong(compressionPreCompressedBytesId, startSize);
      this.stats.incLong(compressionPostCompressedBytesId, endSize);

      this.cachePerfStats.stats.incLong(compressionPreCompressedBytesId, startSize);
      this.cachePerfStats.stats.incLong(compressionPostCompressedBytesId, endSize);
    }

    @Override
    public long startDecompression() {
      this.stats.incLong(compressionDecompressionsId, 1);
      this.cachePerfStats.stats.incLong(compressionDecompressionsId, 1);
      return getStatTime();
    }

    @Override
    public void endDecompression(long startTime) {
      if (enableClockStats) {
        long time = getStatTime() - startTime;
        this.stats.incLong(compressionDecompressTimeId, time);
        this.cachePerfStats.stats.incLong(compressionDecompressTimeId, time);
      }
    }
  }

  /** test hook - dump the backing map for this region */
  public void dumpBackingMap() {
    synchronized (this.entries) {
      if (this.entries instanceof AbstractRegionMap) {
        ((AbstractRegionMap) this.entries).verifyTombstoneCount(this.tombstoneCount);
      }
      logger.debug("Dumping region of size {} tombstones: {}: {}", size(), getTombstoneCount(),
          this.toString());
      if (this.entries instanceof AbstractRegionMap) {
        ((AbstractRegionMap) this.entries).dumpMap();
      }
    }
  }

  private void checkIfConcurrentMapOpsAllowed() {
    // This check allows NORMAL with local scope to fix bug 44856
    if (this.serverRegionProxy == null
        && (this.getDataPolicy() == DataPolicy.NORMAL && this.scope.isDistributed()
            || this.getDataPolicy() == DataPolicy.EMPTY)) {
      // the functional spec says these data policies do not support concurrent map
      // operations
      throw new UnsupportedOperationException();
    }
  }

  public boolean canStoreDataLocally() {
    return this.getDataPolicy().withStorage();
  }

  /**
   * If the specified key is not already associated with a value, associate it with the given value.
   * This is equivalent to
   *
   * <pre>
   * if (!region.containsKey(key))
   *   return region.put(key, value);
   * else
   *   return region.get(key);
   * </pre>
   *
   * Except that the action is performed atomically.
   *
   * <i>Note that if this method returns null then there is no way to determine definitely whether
   * this operation succeeded and modified the region, or if the entry is in an invalidated state
   * and no modification occurred.</i>
   *
   * If this method does not modify the region then no listeners or other callbacks are executed. If
   * a modification does occur, then the behavior with respect to callbacks is the same as
   * {@link Region#create(Object, Object)}.
   *
   * @param key key with which the specified value is to be associated.
   * @param value the value for the new entry, which may be null meaning the new entry starts as if
   *        it had been locally invalidated.
   * @return previous value associated with specified key, or <tt>null</tt> if there was no mapping
   *         for key. A <tt>null</tt> return can also indicate that the entry in the region was
   *         previously in an invalidated state.
   *
   * @throws ClassCastException if key does not satisfy the keyConstraint
   * @throws IllegalArgumentException if the key or value is not serializable and this is a
   *         distributed region
   * @throws TimeoutException if timed out getting distributed lock for {@code Scope.GLOBAL}
   * @throws NullPointerException if key is <tt>null</tt>
   * @throws PartitionedRegionStorageException if the operation could not be completed.
   */
  public Object putIfAbsent(Object key, Object value, Object callbackArgument) {
    long startPut = CachePerfStats.getStatTime();

    checkIfConcurrentMapOpsAllowed();
    validateArguments(key, value, callbackArgument);

    // TODO ConcurrentMap.putIfAbsent() treats null as an invalidation operation
    // BUT we need to return the old value, which Invalidate isn't currently doing

    checkReadiness();
    checkForLimitedOrNoAccess();
    discoverJTA();

    // This used to call the constructor which took the old value. It
    // was modified to call the other EntryEventImpl constructor so that
    // an id will be generated by default. Null was passed in anyway.
    // generate EventID

    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.PUT_IF_ABSENT, key, value,
        callbackArgument, false, getMyId());

    try {
      if (generateEventID()) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      final Object oldValue = null;
      final boolean ifNew = true;
      final boolean ifOld = false;
      final boolean requireOldValue = true;
      if (!basicPut(event, ifNew, ifOld, oldValue, requireOldValue)) {
        return event.getOldValue();
      } else {
        if (!getDataView().isDeferredStats()) {
          getCachePerfStats().endPut(startPut, false);
        }
        return null;
      }
    } catch (EntryNotFoundException ignore) {
      return event.getOldValue();
    } finally {
      event.release();
    }
  }

  @Override
  public Object putIfAbsent(Object key, Object value) {
    return putIfAbsent(key, value, null);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return remove(key, value, null);
  }

  /**
   * Same as {@link #remove(Object, Object)} except a callback argument is supplied to be passed on
   * to <tt>CacheListener</tt>s and/or <tt>CacheWriter</tt>s.
   */
  public boolean remove(Object key, Object value, Object callbackArg) {
    checkIfConcurrentMapOpsAllowed();
    validateKey(key);
    checkReadiness();
    checkForLimitedOrNoAccess();

    if (value == null) {
      value = Token.INVALID;
    }

    @Released
    EntryEventImpl event =
        EntryEventImpl.create(this, Operation.REMOVE, key, null, callbackArg, false, getMyId());

    try {
      if (generateEventID() && event.getEventId() == null) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      discoverJTA();
      getDataView().destroyExistingEntry(event, true, value);

    } catch (EntryNotFoundException ignore) {
      return false;
    } catch (RegionDestroyedException rde) {
      if (!rde.getRegionFullPath().equals(getFullPath())) {
        // Handle when a bucket is destroyed
        throw new RegionDestroyedException(toString(), getFullPath(), rde);
      } else {
        throw rde;
      }
    } finally {
      event.release();
    }
    return true;
  }

  @Override
  public boolean replace(Object key, Object oldValue, Object newValue) {
    return replace(key, oldValue, newValue, null);
  }

  /**
   * Same as {@link #replace(Object, Object, Object)} except a callback argument is supplied to be
   * passed on to <tt>CacheListener</tt>s and/or <tt>CacheWriter</tt>s.
   */
  public boolean replace(Object key, Object expectedOldValue, Object newValue, Object callbackArg) {

    checkIfConcurrentMapOpsAllowed();
    if (newValue == null) {
      throw new NullPointerException();
    }

    long startPut = CachePerfStats.getStatTime();
    validateArguments(key, newValue, callbackArg);
    checkReadiness();
    checkForLimitedOrNoAccess();

    @Released
    EntryEventImpl event = EntryEventImpl.create(this, Operation.REPLACE, key, newValue,
        callbackArg, false, getMyId());

    try {
      if (generateEventID()) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }

      discoverJTA();

      // In general, expectedOldValue null is used when there is no particular
      // old value expected (it can be anything). Here, however, if null
      // is passed as expectedOldValue, then it specifically means that the
      // oldValue must actually be null (i.e. INVALID). So here we
      // change an expectedOldValue of null to the invalid token
      if (expectedOldValue == null) {
        expectedOldValue = Token.INVALID;
      }

      if (!basicPut(event, false, true, expectedOldValue, false)) {
        return false;
      } else {
        if (!getDataView().isDeferredStats()) {
          getCachePerfStats().endPut(startPut, false);
        }
        return true;
      }

    } catch (EntryNotFoundException ignore) {
      // put failed on server
      return false;
    } finally {
      event.release();
    }
  }

  @Override
  public Object replace(Object key, Object value) {
    return replaceWithCallbackArgument(key, value, null);
  }

  /**
   * Same as {@link #replace(Object, Object)} except a callback argument is supplied to be passed on
   * to <tt>CacheListener</tt>s and/or <tt>CacheWriter</tt>s.
   * <p>
   * TODO: callbackArg is always null but this method is for callbacks??
   */
  private Object replaceWithCallbackArgument(Object key, Object value, Object callbackArg) {
    long startPut = CachePerfStats.getStatTime();

    checkIfConcurrentMapOpsAllowed();

    if (value == null) {
      throw new NullPointerException();
    }

    validateArguments(key, value, callbackArg);
    checkReadiness();
    checkForLimitedOrNoAccess();

    @Released
    EntryEventImpl event =
        EntryEventImpl.create(this, Operation.REPLACE, key, value, callbackArg, false, getMyId());

    try {
      if (generateEventID()) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }

      discoverJTA();

      if (!basicPut(event, false, true, null, true)) {
        return null;
      } else {
        if (!getDataView().isDeferredStats()) {
          getCachePerfStats().endPut(startPut, false);
        }
        return event.getOldValue(); // may be null if was invalid
      }

    } catch (EntryNotFoundException ignore) {
      // put failed on server
      return null;
    } finally {
      event.release();
    }
  }

  // TODO: fromClient is always null
  public Object basicBridgePutIfAbsent(final Object key, Object value, boolean isObject,
      Object callbackArg, final ClientProxyMembershipID client, boolean fromClient,
      EntryEventImpl clientEvent)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    EventID eventId = clientEvent.getEventId();
    long startPut = CachePerfStats.getStatTime();
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.PUT_IF_ABSENT, key, null,
        callbackArg, false, client.getDistributedMember(), true, eventId);

    try {
      event.setContext(client);

      // if this is a replayed operation we may already have a version tag
      event.setVersionTag(clientEvent.getVersionTag());

      // Set the new value to the input byte[] if it isn't null
      if (value != null) {
        // If the byte[] represents an object, then store it serialized
        // in a CachedDeserializable; otherwise store it directly as a byte[]
        if (isObject) {
          // The value represents an object
          event.setSerializedNewValue((byte[]) value);
        } else {
          // The value does not represent an object
          event.setNewValue(value);
        }
      }

      validateArguments(key, event.basicGetNewValue(), callbackArg);

      // cannot overwrite an existing key
      boolean ifNew = true;
      // can create a new key
      boolean ifOld = false;
      // need the old value if the create fails
      boolean requireOldValue = true;

      boolean basicPut = basicPut(event, ifNew, ifOld, null, requireOldValue);

      getCachePerfStats().endPut(startPut, false);
      this.stopper.checkCancelInProgress(null);

      // to fix bug 42968 call getRawOldValue instead of getOldValue
      Object oldValue = event.getRawOldValueAsHeapObject();
      if (oldValue == Token.NOT_AVAILABLE) {
        oldValue = AbstractRegion.handleNotAvailable(oldValue);
      }

      if (basicPut) {
        clientEvent.setVersionTag(event.getVersionTag());
        clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
      } else if (oldValue == null) {
        // fix for 42189, putIfAbsent on server can return null if the
        // operation was not performed (oldValue in cache was null).
        // We return the INVALID token instead of null to distinguish
        // this case from successful operation
        return Token.INVALID;
      }

      return oldValue;
    } finally {
      event.release();
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  // TODO: fromClient is always true
  public boolean basicBridgeReplace(final Object key, Object expectedOldValue, Object value,
      boolean isObject, Object callbackArg, final ClientProxyMembershipID client,
      boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    EventID eventId = clientEvent.getEventId();
    long startPut = CachePerfStats.getStatTime();
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.REPLACE, key, null,
        callbackArg, false, client.getDistributedMember(), true, eventId);

    try {
      event.setContext(client);

      // Set the new value to the input byte[] if it isn't null
      if (value != null) {
        // If the byte[] represents an object, then store it serialized
        // in a CachedDeserializable; otherwise store it directly as a byte[]
        if (isObject) {
          // The value represents an object
          event.setSerializedNewValue((byte[]) value);
        } else {
          // The value does not represent an object
          event.setNewValue(value);
        }
      }

      validateArguments(key, event.basicGetNewValue(), callbackArg);

      // can overwrite an existing key
      boolean ifNew = false;
      // cannot create a new key
      boolean ifOld = true;
      boolean requireOldValue = false;

      boolean success = basicPut(event, ifNew, ifOld, expectedOldValue, requireOldValue);

      clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
      if (success) {
        clientEvent.setVersionTag(event.getVersionTag());
      }

      getCachePerfStats().endPut(startPut, false);
      this.stopper.checkCancelInProgress(null);

      return success;
    } finally {
      event.release();
    }
  }

  // TODO: fromClient is always true
  public Object basicBridgeReplace(final Object key, Object value, boolean isObject,
      Object callbackArg, final ClientProxyMembershipID client, boolean fromClient,
      EntryEventImpl clientEvent)
      throws TimeoutException, EntryExistsException, CacheWriterException {

    EventID eventId = clientEvent.getEventId();
    long startPut = CachePerfStats.getStatTime();
    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.REPLACE, key, null,
        callbackArg, false, client.getDistributedMember(), true, eventId);

    try {
      event.setContext(client);

      // Set the new value to the input byte[] if it isn't null
      if (value != null) {
        // If the byte[] represents an object, then store it serialized
        // in a CachedDeserializable; otherwise store it directly as a byte[]
        if (isObject) {
          // The value represents an object
          event.setSerializedNewValue((byte[]) value);
        } else {
          // The value does not represent an object
          event.setNewValue(value);
        }
      }

      validateArguments(key, event.basicGetNewValue(), callbackArg);

      // can overwrite an existing key
      boolean ifNew = false;
      // cannot create a new key
      boolean ifOld = true;
      boolean requireOldValue = true;

      boolean succeeded = basicPut(event, ifNew, ifOld, null, requireOldValue);

      getCachePerfStats().endPut(startPut, false);
      this.stopper.checkCancelInProgress(null);

      clientEvent.isConcurrencyConflict(event.isConcurrencyConflict());
      if (succeeded) {
        clientEvent.setVersionTag(event.getVersionTag());
        // to fix bug 42968 call getRawOldValue instead of getOldValue
        Object oldValue = event.getRawOldValueAsHeapObject();
        if (oldValue == Token.NOT_AVAILABLE) {
          oldValue = AbstractRegion.handleNotAvailable(oldValue);
        }
        if (oldValue == null) {
          // EntryEventImpl.setOldValue translates INVALID to null
          oldValue = Token.INVALID;
        }
        return oldValue;
      } else {
        return null;
      }
    } finally {
      event.release();
    }
  }

  // TODO: fromClient is always true
  public void basicBridgeRemove(Object key, Object expectedOldValue, Object callbackArg,
      ClientProxyMembershipID memberId, boolean fromClient, EntryEventImpl clientEvent)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {

    if (fromClient) {
      // If this region is also wan-enabled, then wrap that callback arg in a
      // GatewayEventCallbackArgument to store the event id.
      if (isGatewaySenderEnabled()) {
        callbackArg = new GatewaySenderEventCallbackArgument(callbackArg);
      }
    }

    // Create an event and put the entry
    @Released
    final EntryEventImpl event = EntryEventImpl.create(this, Operation.REMOVE, key, null,
        callbackArg, false, memberId.getDistributedMember(), true, clientEvent.getEventId());

    try {
      event.setContext(memberId);
      // we rely on exceptions to tell us that the operation didn't take
      // place. AbstractRegionMap performs the checks and throws the exception
      try {
        basicDestroy(event, true, expectedOldValue);
      } finally {
        clientEvent.setVersionTag(event.getVersionTag());
        clientEvent.setIsRedestroyedEntry(event.getIsRedestroyedEntry());
      }
    } finally {
      event.release();
    }
  }

  @Override
  public long getVersionForMember(VersionSource member) {
    throw new IllegalStateException("Operation only implemented for disk region");
  }

  /**
   * Return an IndexMap that is persisted to the disk store used by this region.
   *
   * This IndexMap should be used as the backing map for any regions that are using the Soplog
   * persistence.
   *
   * Calling this method may create a branch new index map on disk, or it may recover an index map
   * that was previously persisted, depending on whether the index previously existed.
   *
   * TODO: none of the parameters are ever used
   *
   * @param indexName the name of the index
   * @param indexedExpression the index expression
   * @param fromClause the from clause.
   *
   * @return The index map.
   *
   * @throws IllegalStateException if this region is not using soplog persistence
   *
   * @throws IllegalStateException if this index was previously persisted with a different
   *         expression or from clause.
   *
   */
  public IndexMap getIndexMap(String indexName, String indexedExpression, String fromClause) {
    return new IndexMapImpl();
  }

  @Override
  public void setInUseByTransaction(boolean value) {
    synchronized (this.regionExpiryLock) {
      if (value) {
        this.txRefCount++;
      } else {
        this.txRefCount--;
        assert this.txRefCount >= 0;
        if (this.txRefCount == 0) {
          if (this.regionTTLExpiryTask == null && this.regionTimeToLive > 0) {
            this.addTTLExpiryTask();
          }
          if (this.regionIdleExpiryTask == null && this.regionIdleTimeout > 0) {
            this.addIdleExpiryTask();
          }
        }
      }
    }
  }

  /**
   * Return true if the region expiry task should be rescheduled
   */
  boolean expireRegion(RegionExpiryTask regionExpiryTask, boolean distributed, boolean destroy) {
    synchronized (this.regionExpiryLock) {
      if (regionExpiryTask instanceof RegionTTLExpiryTask) {
        if (regionExpiryTask != this.regionTTLExpiryTask) {
          // We must be an old task so defer to the currently scheduled one
          return false;
        } else {
          this.regionTTLExpiryTask = null;
        }
      } else {
        if (regionExpiryTask != this.regionIdleExpiryTask) {
          // We must be an old task so defer to the currently scheduled one
          return false;
        } else {
          this.regionIdleExpiryTask = null;
        }
      }
      if (this.txRefCount > 0) {
        return false;
      }
    }
    // release the sync before doing the operation to prevent deadlock caused by r48875
    Operation op = destroy
        ? distributed ? Operation.REGION_EXPIRE_DESTROY : Operation.REGION_EXPIRE_LOCAL_DESTROY
        : distributed ? Operation.REGION_EXPIRE_INVALIDATE
            : Operation.REGION_EXPIRE_LOCAL_INVALIDATE;
    RegionEventImpl event =
        new RegionEventImpl(this, op, null, false, getMyId(), generateEventID());
    if (destroy) {
      basicDestroyRegion(event, distributed);
    } else {
      basicInvalidateRegion(event);
    }
    return true;
  }

  public int testHookGetValuesInVM() {
    int result = 0;
    for (RegionEntry re : getRegionMap().regionEntries()) {
      if (re.getValueAsToken() == Token.NOT_A_TOKEN) {
        result++;
      }
    }
    return result;
  }

  public int testHookGetValuesOnDisk() {
    int result = 0;
    for (RegionEntry re : getRegionMap().regionEntries()) {
      if (re.getValueAsToken() == null) {
        result++;
      }
    }
    return result;
  }

  /**
   * Send a message to all other members that can have this same region entry and return the latest
   * last access time.
   */
  public long getLatestLastAccessTimeFromOthers(Object key) {
    // local regions have no other members so return 0.
    return 0L;
  }

  /**
   * Returns the number of LRU evictions done by this region.
   */
  @Override
  public long getTotalEvictions() {
    return this.entries.getEvictions();
  }

  public void incBucketEvictions() {
    // nothing needed by default
    // override this method on BucketRegion
  }

  @Override
  public long getEvictionCounter() {
    long result = 0L;
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      EvictionCounters es = evictionController.getCounters();
      if (es != null) {
        result = es.getCounter();
      }
    }
    return result;
  }

  public long getEvictionLimit() {
    long result = 0L;
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      EvictionCounters es = evictionController.getCounters();
      if (es != null) {
        result = es.getLimit();
      }
    }
    return result;
  }

  public long getEvictionDestroys() {
    long result = 0;
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      EvictionCounters es = evictionController.getCounters();
      if (es != null) {
        result = es.getDestroys();
      }
    }
    return result;
  }

  @Override
  public EvictionController getEvictionController() {
    return getRegionMap().getEvictionController();
  }

  @Override
  public void setEvictionMaximum(int maximum) {
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      evictionController.setLimit(maximum);
    }
  }

  @Override
  public Statistics getEvictionStatistics() {
    Statistics result = null;
    EvictionController evictionController = getEvictionController();
    if (evictionController != null) {
      EvictionCounters es = evictionController.getCounters();
      if (es != null) {
        result = es.getStatistics();
      }
    }
    return result;
  }

  @Override
  public EvictionController getExistingController(InternalRegionArguments internalArgs) {
    return null;
  }

  @Override
  public String getNameForStats() {
    return getFullPath();
  }

  @Override
  public Lock getClientMetaDataLock() {
    return clientMetaDataLock;
  }

  public boolean isMemoryThresholdReached() {
    return memoryThresholdReached.get();
  }

  protected void setMemoryThresholdReached(boolean reached) {
    this.memoryThresholdReached.set(reached);
  }

}
