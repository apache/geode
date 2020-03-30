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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAccessException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.RoleException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.compression.Compressor;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.log4j.api.LogWithToString;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Takes care of RegionAttributes, AttributesMutator, and some no-brainer method implementations.
 */
@SuppressWarnings("deprecation")
public abstract class AbstractRegion implements InternalRegion, AttributesMutator, CacheStatistics,
    DataSerializableFixedID, Extensible<Region<?, ?>>, EvictableRegion, LogWithToString {

  private static final Logger logger = LogService.getLogger();
  private final ReentrantReadWriteLock readWriteLockForCacheLoader = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock readWriteLockForCacheWriter = new ReentrantReadWriteLock();
  protected final ConcurrentHashMap<RegionEntry, EntryExpiryTask> entryExpiryTasks =
      new ConcurrentHashMap<>();
  /**
   * Identifies the static order in which this region was created in relation to other regions or
   * other instances of this region during the life of this JVM.
   */
  private final int serialNumber;

  /**
   * Used to synchronize WRITES to cacheListeners. Always do copy on write.
   */
  private final Object clSync = new Object();

  // Used to synchronize creation of IndexManager
  private final Object imSync = new Object();

  /**
   * NOTE: care must be taken to never modify the array of cacheListeners in place. Instead allocate
   * a new array and modify it. This field is volatile so that it can be read w/o getting clSync.
   */
  private volatile CacheListener[] cacheListeners;

  /**
   * All access to cacheLoader must be protected by readWriteLockForCacheLoader
   */
  private volatile CacheLoader cacheLoader;

  /**
   * All access to cacheWriter must be protected by readWriteLockForCacheWriter
   */
  private volatile CacheWriter cacheWriter;

  protected int entryIdleTimeout;

  private ExpirationAction entryIdleTimeoutExpirationAction;

  protected CustomExpiry customEntryIdleTimeout;

  protected int entryTimeToLive;

  ExpirationAction entryTimeToLiveExpirationAction;

  protected CustomExpiry customEntryTimeToLive;

  protected int initialCapacity;

  protected Class keyConstraint;

  protected Class valueConstraint;

  protected float loadFactor;

  private DataPolicy dataPolicy;

  protected int regionIdleTimeout;

  private ExpirationAction regionIdleTimeoutExpirationAction;

  protected int regionTimeToLive;

  private ExpirationAction regionTimeToLiveExpirationAction;

  @Immutable
  public static final Scope DEFAULT_SCOPE = Scope.DISTRIBUTED_NO_ACK;

  protected Scope scope = DEFAULT_SCOPE;

  protected boolean statisticsEnabled;

  protected boolean isLockGrantor;

  private boolean mcastEnabled;

  protected int concurrencyLevel;

  private volatile boolean concurrencyChecksEnabled;

  protected boolean earlyAck;

  private final boolean isPdxTypesRegion;

  protected Set<String> gatewaySenderIds;

  protected Set<String> asyncEventQueueIds;

  private Set<String> visibleAsyncEventQueueIds;

  /**
   * This set is always unmodifiable.
   */
  private Set<String> allGatewaySenderIds;

  protected boolean enableSubscriptionConflation;

  protected boolean publisher;

  protected boolean enableAsyncConflation;

  /**
   * True if this region uses off-heap memory; otherwise false (default)
   *
   * @since Geode 1.0
   */
  protected boolean offHeap;

  private boolean cloningEnable = false;

  private DiskWriteAttributes diskWriteAttributes;

  protected File[] diskDirs;
  protected int[] diskSizes;
  protected String diskStoreName;
  protected boolean isDiskSynchronous;
  private boolean indexMaintenanceSynchronous = false;

  protected volatile IndexManager indexManager = null;

  // The ThreadLocal is used to identify if the thread is an
  // index creation thread. This identification helps skip the synchronization
  // block
  // if the value is "REMOVED" token. This prevents the dead lock , in case the
  // lock
  // over the entry is held by any Index Update Thread.
  // This is used to fix Bug # 33336.
  private static final ThreadLocal<Boolean> isIndexCreator = new ThreadLocal<>();

  /** Attributes that define this Region as a PartitionedRegion */
  protected PartitionAttributes partitionAttributes;

  private final EvictionAttributesImpl evictionAttributes;

  /** The membership attributes defining required roles functionality */
  protected MembershipAttributes membershipAttributes;

  /** The subscription attributes defining required roles functionality */
  protected SubscriptionAttributes subscriptionAttributes;

  /** should this region ignore in-progress JTA transactions? */
  protected boolean ignoreJTA;

  private final AtomicLong lastAccessedTime;

  private final AtomicLong lastModifiedTime;

  private static final boolean trackHits =
      !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "ignoreHits");

  private static final boolean trackMisses =
      !Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "ignoreMisses");

  private final AtomicLong hitCount = new AtomicLong();

  private final AtomicLong missCount = new AtomicLong();

  protected String poolName;

  protected Compressor compressor;

  /**
   * @see #getExtensionPoint()
   * @since GemFire 8.1
   */
  private ExtensionPoint<Region<?, ?>> extensionPoint =
      new SimpleExtensionPoint<Region<?, ?>>(this, this);

  protected final InternalCache cache;

  private final PoolFinder poolFinder;

  private final StatisticsClock statisticsClock;

  /** Creates a new instance of AbstractRegion */
  protected AbstractRegion(InternalCache cache, RegionAttributes<?, ?> attrs, String regionName,
      InternalRegionArguments internalRegionArgs, PoolFinder poolFinder,
      StatisticsClock statisticsClock) {
    this.poolFinder = poolFinder;
    this.statisticsClock = statisticsClock;

    this.cache = cache;
    serialNumber = DistributionAdvisor.createSerialNumber();
    isPdxTypesRegion = PeerTypeRegistration.REGION_NAME.equals(regionName);
    lastAccessedTime = new AtomicLong(cacheTimeMillis());
    lastModifiedTime = new AtomicLong(lastAccessedTime.get());
    dataPolicy = attrs.getDataPolicy(); // do this one first
    scope = attrs.getScope();

    offHeap = attrs.getOffHeap();

    // fix bug #52033 by invoking setOffHeap now (localMaxMemory may now be the temporary
    // placeholder for off-heap until DistributedSystem is created
    // found non-null PartitionAttributes and offHeap is true so let's setOffHeap on PA now
    PartitionAttributes<?, ?> partitionAttributes1 = attrs.getPartitionAttributes();
    if (offHeap && partitionAttributes1 != null) {
      PartitionAttributesImpl impl = (PartitionAttributesImpl) partitionAttributes1;
      impl.setOffHeap(true);
    }

    evictionAttributes = new EvictionAttributesImpl(attrs.getEvictionAttributes());
    if (attrs.getPartitionAttributes() != null && evictionAttributes.getAlgorithm()
        .isLRUMemory() && attrs.getPartitionAttributes().getLocalMaxMemory() != 0
        && evictionAttributes
            .getMaximum() != attrs.getPartitionAttributes().getLocalMaxMemory()) {
      logger.warn(
          "For region {} with data policy PARTITION, memory LRU eviction attribute maximum has been reset from {}MB to local-max-memory {}MB",
          new Object[] {regionName, evictionAttributes.getMaximum(),
              attrs.getPartitionAttributes().getLocalMaxMemory()});
      evictionAttributes.setMaximum(attrs.getPartitionAttributes().getLocalMaxMemory());
    }

    storeCacheListenersField(attrs.getCacheListeners());
    assignCacheLoader(attrs.getCacheLoader());
    assignCacheWriter(attrs.getCacheWriter());
    regionTimeToLive = attrs.getRegionTimeToLive().getTimeout();
    regionTimeToLiveExpirationAction = attrs.getRegionTimeToLive().getAction();
    setRegionTimeToLiveAtts();
    regionIdleTimeout = attrs.getRegionIdleTimeout().getTimeout();
    regionIdleTimeoutExpirationAction = attrs.getRegionIdleTimeout().getAction();
    setRegionIdleTimeoutAttributes();
    entryTimeToLive = attrs.getEntryTimeToLive().getTimeout();
    entryTimeToLiveExpirationAction = attrs.getEntryTimeToLive().getAction();
    setEntryTimeToLiveAttributes();
    customEntryTimeToLive = attrs.getCustomEntryTimeToLive();
    entryIdleTimeout = attrs.getEntryIdleTimeout().getTimeout();
    entryIdleTimeoutExpirationAction = attrs.getEntryIdleTimeout().getAction();
    setEntryIdleTimeoutAttributes();
    customEntryIdleTimeout = attrs.getCustomEntryIdleTimeout();
    updateEntryExpiryPossible();
    statisticsEnabled = attrs.getStatisticsEnabled();
    ignoreJTA = attrs.getIgnoreJTA();
    isLockGrantor = attrs.isLockGrantor();
    keyConstraint = attrs.getKeyConstraint();
    valueConstraint = attrs.getValueConstraint();
    initialCapacity = attrs.getInitialCapacity();
    loadFactor = attrs.getLoadFactor();
    concurrencyLevel = attrs.getConcurrencyLevel();
    setConcurrencyChecksEnabled(
        attrs.getConcurrencyChecksEnabled() && supportsConcurrencyChecks());
    earlyAck = attrs.getEarlyAck();
    gatewaySenderIds = attrs.getGatewaySenderIds();
    asyncEventQueueIds = attrs.getAsyncEventQueueIds();
    initializeVisibleAsyncEventQueueIds(internalRegionArgs);
    setAllGatewaySenderIds();
    enableSubscriptionConflation = attrs.getEnableSubscriptionConflation();
    publisher = attrs.getPublisher();
    enableAsyncConflation = attrs.getEnableAsyncConflation();
    indexMaintenanceSynchronous = attrs.getIndexMaintenanceSynchronous();
    mcastEnabled = attrs.getMulticastEnabled();
    partitionAttributes = attrs.getPartitionAttributes();
    membershipAttributes = attrs.getMembershipAttributes();
    subscriptionAttributes = attrs.getSubscriptionAttributes();
    cloningEnable = attrs.getCloningEnabled();
    poolName = attrs.getPoolName();
    if (poolName != null) {
      PoolImpl cp = getPool();
      if (cp == null) {
        throw new IllegalStateException(
            String.format("The connection pool %s has not been created",
                poolName));
      }
      cp.attach();
      if (cp.getMultiuserAuthentication() && !getDataPolicy().isEmpty()) {
        throw new IllegalStateException(
            "Region must have empty data-policy " + "when multiuser-authentication is true.");
      }
    }

    diskStoreName = attrs.getDiskStoreName();
    isDiskSynchronous = attrs.isDiskSynchronous();
    if (diskStoreName == null) {
      diskWriteAttributes = attrs.getDiskWriteAttributes();
      isDiskSynchronous = diskWriteAttributes.isSynchronous(); // fixes bug 41313
      diskDirs = attrs.getDiskDirs();
      diskSizes = attrs.getDiskDirSizes();
    }

    compressor = attrs.getCompressor();
    // enable concurrency checks for persistent regions
    if (!attrs.getConcurrencyChecksEnabled() && attrs.getDataPolicy().withPersistence()
        && supportsConcurrencyChecks()) {
      throw new IllegalStateException(
          "Concurrency checks cannot be disabled for regions that use persistence");
    }
  }

  @VisibleForTesting
  AbstractRegion() {
    statisticsClock = disabledClock();
    cache = null;
    serialNumber = 0;
    isPdxTypesRegion = false;
    lastAccessedTime = new AtomicLong(0);
    lastModifiedTime = new AtomicLong(0);
    evictionAttributes = new EvictionAttributesImpl();
    poolFinder = (a) -> null;
  }

  /**
   * configure this region to ignore or not ignore in-progress JTA transactions. Setting this to
   * true will cause cache operations to no longer notice JTA transactions. The default setting is
   * false
   *
   * @deprecated in 5.0 and later releases, use the region attribute ignoreJTA to configure this
   */
  @Deprecated
  public void setIgnoreJTA(boolean ignore) {
    ignoreJTA = ignore;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void create(Object key, Object value)
      throws TimeoutException, EntryExistsException, CacheWriterException {
    create(key, value, null);
  }

  @Override
  public Object destroy(Object key)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {
    return destroy(key, null);
  }

  @Override
  public Object get(Object key) throws CacheLoaderException, TimeoutException {
    return get(key, null, true, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object put(Object key, Object value) throws TimeoutException, CacheWriterException {
    return put(key, value, null);
  }

  @Override
  public Object get(Object key, Object aCallbackArgument)
      throws CacheLoaderException, TimeoutException {
    return get(key, aCallbackArgument, true, null);
  }

  @Override
  public void localDestroyRegion() {
    localDestroyRegion(null);
  }

  /**
   * @param key the key to find
   * @param aCallbackArgument argument for callbacks
   * @param generateCallbacks whether callbacks should be invoked
   * @param clientEvent client-side event, if any (used to pass back version information)
   * @return the value associated with the key
   */
  abstract Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      EntryEventImpl clientEvent) throws TimeoutException, CacheLoaderException;

  @Override
  public void localDestroy(Object key) throws EntryNotFoundException {
    localDestroy(key, null);
  }

  @Override
  public void destroyRegion() throws CacheWriterException, TimeoutException {
    destroyRegion(null);
  }

  @Override
  public void invalidate(Object key) throws TimeoutException, EntryNotFoundException {
    invalidate(key, null);
  }

  @Override
  public void localInvalidate(Object key) throws EntryNotFoundException {
    localInvalidate(key, null);
  }

  @Override
  public void localInvalidateRegion() {
    localInvalidateRegion(null);
  }

  @Override
  public void invalidateRegion() throws TimeoutException {
    invalidateRegion(null);
  }

  abstract void basicClear(RegionEventImpl regionEvent);

  @Override
  public void clear() {
    checkReadiness();
    checkForLimitedOrNoAccess();
    RegionEventImpl regionEvent = new RegionEventImpl(this, Operation.REGION_CLEAR, null, false,
        getMyId(), generateEventID());
    basicClear(regionEvent);
  }

  abstract void basicLocalClear(RegionEventImpl rEvent);

  @Override
  public void localClear() {
    checkReadiness();
    checkForNoAccess();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOCAL_CLEAR, null, false,
        getMyId(), generateEventID()/* generate EventID */);
    basicLocalClear(event);
  }

  @Override
  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  @Override
  public Map getAll(Collection keys, Object aCallbackArgument) {
    if (keys == null) {
      throw new NullPointerException("The collection of keys for getAll cannot be null");
    }
    checkReadiness();
    checkForLimitedOrNoAccess();
    return keys.isEmpty() ? new HashMap() : basicGetAll(keys, aCallbackArgument);
  }

  abstract Map basicGetAll(Collection keys, Object callback);

  protected StringBuilder getStringBuilder() {
    StringBuilder buf = new StringBuilder();
    buf.append(getClass().getName());
    buf.append("[path='").append(getFullPath()).append("';scope=").append(getScope())
        .append("';dataPolicy=").append(getDataPolicy());
    if (getConcurrencyChecksEnabled()) {
      buf.append("; concurrencyChecksEnabled");
    }
    return buf;
  }

  @Override
  public String toString() {
    return getStringBuilder().append(']').toString();
  }

  @Override
  public CacheLoader getCacheLoader() {
    readWriteLockForCacheLoader.readLock().lock();
    try {
      return cacheLoader;
    } finally {
      readWriteLockForCacheLoader.readLock().unlock();
    }
  }

  @Override
  public CacheWriter getCacheWriter() {
    readWriteLockForCacheWriter.readLock().lock();
    try {
      return cacheWriter;
    } finally {
      readWriteLockForCacheWriter.readLock().unlock();
    }
  }

  /**
   * Return a cache loader if this region has one. Note if region's loader is used to implement
   * bridge then null is returned.
   *
   * @since GemFire 5.7
   */
  CacheLoader basicGetLoader() {
    return cacheLoader;
  }

  /**
   * Return a cache writer if this region has one. Note if region's writer is used to implement
   * bridge then null is returned.
   *
   * @since GemFire 5.7
   */
  @Override
  public CacheWriter basicGetWriter() {
    return cacheWriter;
  }

  @Override
  public Class getKeyConstraint() {
    return keyConstraint;
  }

  @Override
  public Class getValueConstraint() {
    return valueConstraint;
  }

  private volatile ExpirationAttributes regionTimeToLiveAtts;

  private void setRegionTimeToLiveAtts() {
    regionTimeToLiveAtts =
        new ExpirationAttributes(regionTimeToLive, regionTimeToLiveExpirationAction);
  }

  @Override
  public ExpirationAttributes getRegionTimeToLive() {
    return regionTimeToLiveAtts;
  }

  private volatile ExpirationAttributes regionIdleTimeoutAttributes;

  private void setRegionIdleTimeoutAttributes() {
    regionIdleTimeoutAttributes =
        new ExpirationAttributes(regionIdleTimeout, regionIdleTimeoutExpirationAction);
  }

  @Override
  public ExpirationAttributes getRegionIdleTimeout() {
    return regionIdleTimeoutAttributes;
  }

  private volatile ExpirationAttributes entryTimeToLiveAtts;

  void setEntryTimeToLiveAttributes() {
    entryTimeToLiveAtts =
        new ExpirationAttributes(entryTimeToLive, entryTimeToLiveExpirationAction);
  }

  @Override
  public ExpirationAttributes getEntryTimeToLive() {
    return entryTimeToLiveAtts;
  }

  @Override
  public CustomExpiry getCustomEntryTimeToLive() {
    return customEntryTimeToLive;
  }

  private volatile ExpirationAttributes entryIdleTimeoutAttributes;

  private void setEntryIdleTimeoutAttributes() {
    entryIdleTimeoutAttributes =
        new ExpirationAttributes(entryIdleTimeout, entryIdleTimeoutExpirationAction);
  }

  @Override
  public ExpirationAttributes getEntryIdleTimeout() {
    return entryIdleTimeoutAttributes;
  }

  @Override
  public CustomExpiry getCustomEntryIdleTimeout() {
    return customEntryIdleTimeout;
  }

  @Override
  public MirrorType getMirrorType() {
    if (getDataPolicy().isNormal() || getDataPolicy().isPreloaded()
        || getDataPolicy().isEmpty() || getDataPolicy().withPartitioning()) {
      return MirrorType.NONE;
    } else if (getDataPolicy().withReplication()) {
      return MirrorType.KEYS_VALUES;
    } else {
      throw new IllegalStateException(
          String.format("No mirror type corresponds to data policy %s",
              getDataPolicy()));
    }
  }

  @Override
  public String getPoolName() {
    return poolName;
  }

  @Override
  public DataPolicy getDataPolicy() {
    return dataPolicy;
  }

  @Override
  public Scope getScope() {
    return scope;
  }

  @Override
  public CacheListener getCacheListener() {
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners == null || listeners.length == 0) {
      return null;
    }
    if (listeners.length == 1) {
      return listeners[0];
    } else {
      throw new IllegalStateException(
          "More than one cache listener exists.");
    }
  }

  public boolean isPdxTypesRegion() {
    return isPdxTypesRegion;
  }

  @Override
  public Set<String> getGatewaySenderIds() {
    return gatewaySenderIds;
  }

  @Override
  public Set<String> getAsyncEventQueueIds() {
    return asyncEventQueueIds;
  }

  @Override
  public Set<String> getVisibleAsyncEventQueueIds() {
    return visibleAsyncEventQueueIds;
  }

  @Override
  public Set<String> getAllGatewaySenderIds() {
    return allGatewaySenderIds;
  }

  /**
   * Return the remote DS IDs that need to receive events for this region.
   *
   * @param allGatewaySenderIds the set of gateway sender IDs to consider
   */
  List<Integer> getRemoteDsIds(Set<String> allGatewaySenderIds) throws IllegalStateException {
    int sz = allGatewaySenderIds.size();
    Set<GatewaySender> allGatewaySenders = cache.getAllGatewaySenders();
    if ((sz > 0 || isPdxTypesRegion) && !allGatewaySenders.isEmpty()) {
      List<Integer> allRemoteDSIds = new ArrayList<>(sz);
      for (GatewaySender sender : allGatewaySenders) {
        // This is for all regions except pdx Region
        if (!isPdxTypesRegion) {
          // Make sure we are distributing to only those senders whose id
          // is available on this region
          if (allGatewaySenderIds.contains(sender.getId())) {
            allRemoteDSIds.add(sender.getRemoteDSId());
          }
        } else { // this else is for PDX region
          allRemoteDSIds.add(sender.getRemoteDSId());
        }
      }
      return allRemoteDSIds;
    }
    return null;
  }

  @Immutable
  private static final CacheListener[] EMPTY_LISTENERS = new CacheListener[0];

  @Override
  public CacheListener[] getCacheListeners() {
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners == null || listeners.length == 0) {
      return EMPTY_LISTENERS;
    } else {
      CacheListener[] result = new CacheListener[listeners.length];
      System.arraycopy(listeners, 0, result, 0, listeners.length);
      return result;
    }
  }

  /**
   * Sets the cacheListeners field.
   */
  private void storeCacheListenersField(CacheListener[] value) {
    synchronized (clSync) {
      if (value != null && value.length != 0) {
        CacheListener[] cacheListeners = new CacheListener[value.length];
        System.arraycopy(value, 0, cacheListeners, 0, cacheListeners.length);
        value = cacheListeners;
      } else {
        value = EMPTY_LISTENERS;
      }
      cacheListeners = value;
    }
  }

  /**
   * Fetches the value in the cacheListeners field. NOTE: callers should not modify the contents of
   * the returned array.
   */
  CacheListener[] fetchCacheListenersField() {
    return cacheListeners;
  }

  @Override
  public int getInitialCapacity() {
    return initialCapacity;
  }

  @Override
  public float getLoadFactor() {
    return loadFactor;
  }

  abstract boolean isCurrentlyLockGrantor();

  @Override
  public boolean isLockGrantor() {
    return isLockGrantor;
  }

  /**
   * RegionAttributes implementation. Returns true if multicast can be used by the cache for this
   * region
   */
  @Override
  public boolean getMulticastEnabled() {
    return mcastEnabled;
  }

  @Override
  public boolean getStatisticsEnabled() {
    return statisticsEnabled;
  }

  @Override
  public boolean getIgnoreJTA() {
    return ignoreJTA;
  }

  @Override
  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  @Override
  public boolean getConcurrencyChecksEnabled() {
    return concurrencyChecksEnabled;
  }

  public void setConcurrencyChecksEnabled(boolean concurrencyChecksEnabled) {
    this.concurrencyChecksEnabled = concurrencyChecksEnabled;
  }

  @Override
  public boolean getPersistBackup() {
    return getDataPolicy().withPersistence();
  }

  @Override
  public boolean getEarlyAck() {
    return earlyAck;
  }

  /*
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  @Override
  public boolean getPublisher() {
    return publisher;
  }

  @Override
  public boolean getEnableConflation() { // deprecated in 5.0
    return getEnableSubscriptionConflation();
  }

  @Override
  public boolean getEnableBridgeConflation() {// deprecated in 5.7
    return getEnableSubscriptionConflation();
  }

  @Override
  public boolean getEnableSubscriptionConflation() {
    return enableSubscriptionConflation;
  }

  @Override
  public boolean getEnableAsyncConflation() {
    return enableAsyncConflation;
  }

  /*
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  @Override
  public DiskWriteAttributes getDiskWriteAttributes() {
    return diskWriteAttributes;
  }

  @Override
  public abstract File[] getDiskDirs();

  @Override
  public String getDiskStoreName() {
    return diskStoreName;
  }

  @Override
  public boolean isDiskSynchronous() {
    return isDiskSynchronous;
  }

  @Override
  public boolean getIndexMaintenanceSynchronous() {
    return indexMaintenanceSynchronous;
  }

  @Override
  public PartitionAttributes getPartitionAttributes() {
    return partitionAttributes;
  }

  @Override
  public MembershipAttributes getMembershipAttributes() {
    return membershipAttributes;
  }

  @Override
  public SubscriptionAttributes getSubscriptionAttributes() {
    return subscriptionAttributes;
  }

  /**
   * Get IndexManger for region
   */
  @Override
  public IndexManager getIndexManager() {
    return indexManager;
  }

  /**
   * This method call is guarded by imSync lock created for each region. Set IndexManger for region.
   */
  @Override
  public void setIndexManager(IndexManager indexManager) {
    checkReadiness();
    this.indexManager = indexManager;
  }

  /**
   * Use ONLY imSync for IndexManager get and set.
   *
   * @return {@link IndexManager} lock.
   */
  @Override
  public Object getIMSync() {
    return imSync;
  }

  // The ThreadLocal is used to identify if the thread is an
  // index creation thread. This is used to fix Bug # 33336. The value
  // is set from IndexManager ,if the thread happens to be an IndexCreation
  // Thread.
  // Once the thread has created the Index , it will unset the value in the
  // ThreadLocal Object
  public void setFlagForIndexCreationThread(boolean value) {
    isIndexCreator.set(value ? Boolean.TRUE : null);
  }

  // The boolean is used in AbstractRegionEntry to skip the synchronized
  // block
  // in case the value of the entry is "REMOVED" token. This prevents dead lock
  // caused by the Bug # 33336
  @Override
  public boolean isIndexCreationThread() {
    Boolean value = isIndexCreator.get();
    return value != null ? value : false;
  }

  @Override
  public Region getRegion() {
    return this;
  }

  @Override
  public void addGatewaySenderId(String gatewaySenderId) {
    getGatewaySenderIds().add(gatewaySenderId);
    setAllGatewaySenderIds();
  }

  @Override
  public void removeGatewaySenderId(String gatewaySenderId) {
    getGatewaySenderIds().remove(gatewaySenderId);
    setAllGatewaySenderIds();
  }

  @Override
  public void addAsyncEventQueueId(String asyncEventQueueId) {
    addAsyncEventQueueId(asyncEventQueueId, false);
  }

  public void addAsyncEventQueueId(String asyncEventQueueId, boolean isInternal) {
    getAsyncEventQueueIds().add(asyncEventQueueId);
    if (!isInternal) {
      getVisibleAsyncEventQueueIds().add(asyncEventQueueId);
    }
    setAllGatewaySenderIds();
  }

  @Override
  public void removeAsyncEventQueueId(String asyncEventQueueId) {
    getAsyncEventQueueIds().remove(asyncEventQueueId);
    getVisibleAsyncEventQueueIds().remove(asyncEventQueueId);
    setAllGatewaySenderIds();
  }

  private void setAllGatewaySenderIds() {
    if (getGatewaySenderIds().isEmpty() && getAsyncEventQueueIds().isEmpty()) {
      allGatewaySenderIds = Collections.emptySet(); // fix for bug 45774
    }
    Set<String> tmp = new HashSet<>(getGatewaySenderIds());
    for (String asyncQueueId : getAsyncEventQueueIds()) {
      tmp.add(AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncQueueId));
    }
    allGatewaySenderIds = Collections.unmodifiableSet(tmp);
  }

  private void initializeVisibleAsyncEventQueueIds(InternalRegionArguments internalRegionArgs) {
    Set<String> visibleAsyncEventQueueIds = new CopyOnWriteArraySet<>(getAsyncEventQueueIds());
    // Remove all internal aeqIds from internal region args if necessary
    if (internalRegionArgs.getInternalAsyncEventQueueIds() != null) {
      visibleAsyncEventQueueIds.removeAll(internalRegionArgs.getInternalAsyncEventQueueIds());
    }
    this.visibleAsyncEventQueueIds = visibleAsyncEventQueueIds;
  }

  @Override
  public void addCacheListener(CacheListener aListener) {
    checkReadiness();
    if (aListener == null) {
      throw new IllegalArgumentException(
          "addCacheListener parameter was null");
    }
    CacheListener wcl = wrapRegionMembershipListener(aListener);
    boolean changed = false;
    synchronized (clSync) {
      CacheListener[] oldListeners = cacheListeners;
      if (oldListeners == null || oldListeners.length == 0) {
        cacheListeners = new CacheListener[] {wcl};
        changed = true;
      } else {
        List<CacheListener> listeners = Arrays.asList(oldListeners);
        if (!listeners.contains(aListener)) {
          cacheListeners =
              (CacheListener[]) ArrayUtils.insert(oldListeners, oldListeners.length, wcl);
        }
      }
    }
    if (changed) {
      // moved the following out of the sync for bug 34512
      cacheListenersChanged(true);
    }
  }

  /**
   * We wrap RegionMembershipListeners in a container when adding them at runtime, so that we can
   * properly initialize their membership set prior to delivering events to them.
   *
   * @param listener a cache listener to be added to the region
   */
  private CacheListener wrapRegionMembershipListener(CacheListener listener) {
    if (listener instanceof RegionMembershipListener) {
      return new WrappedRegionMembershipListener((RegionMembershipListener) listener);
    }
    return listener;
  }

  /**
   * Initialize any wrapped RegionMembershipListeners in the cache listener list
   */
  void initPostCreateRegionMembershipListeners(Set initialMembers) {
    synchronized (clSync) {
      DistributedMember[] members = null;
      CacheListener[] newListeners = null;
      for (int i = 0; i < cacheListeners.length; i++) {
        CacheListener cl = cacheListeners[i];
        if (cl instanceof WrappedRegionMembershipListener) {
          WrappedRegionMembershipListener wrml = (WrappedRegionMembershipListener) cl;
          if (!wrml.isInitialized()) {
            if (members == null) {
              members = (DistributedMember[]) initialMembers.toArray(new DistributedMember[0]);
            }
            wrml.initialMembers(this, members);
            if (newListeners == null) {
              newListeners = new CacheListener[cacheListeners.length];
              System.arraycopy(cacheListeners, 0, newListeners, 0, newListeners.length);
            }
            newListeners[i] = wrml.getWrappedListener();
          }
        }
      }
      if (newListeners != null) {
        cacheListeners = newListeners;
      }
    }
  }

  @Override
  public void initCacheListeners(CacheListener[] newListeners) {
    checkReadiness();
    CacheListener[] listenersToAdd = null;
    if (newListeners != null) {
      listenersToAdd = new CacheListener[newListeners.length];
      for (int i = 0; i < newListeners.length; i++) {
        listenersToAdd[i] = wrapRegionMembershipListener(newListeners[i]);
      }
    }
    CacheListener[] oldListeners;
    synchronized (clSync) {
      oldListeners = cacheListeners;
      if (listenersToAdd == null || listenersToAdd.length == 0) {
        cacheListeners = EMPTY_LISTENERS;
      } else { // we have some listeners to add
        if (Arrays.asList(listenersToAdd).contains(null)) {
          throw new IllegalArgumentException(
              "initCacheListeners parameter had a null element");
        }
        CacheListener[] newCacheListeners = new CacheListener[listenersToAdd.length];
        System.arraycopy(listenersToAdd, 0, newCacheListeners, 0, newCacheListeners.length);
        cacheListeners = newCacheListeners;
      }
    }
    // moved the following out of the sync for bug 34512
    if (listenersToAdd == null || listenersToAdd.length == 0) {
      if (oldListeners != null && oldListeners.length > 0) {
        for (CacheListener oldListener : oldListeners) {
          closeCacheCallback(oldListener);
        }
        cacheListenersChanged(false);
      }
    } else { // we had some listeners to add
      if (oldListeners != null && oldListeners.length > 0) {
        for (CacheListener oldListener : oldListeners) {
          closeCacheCallback(oldListener);
        }
      } else {
        cacheListenersChanged(true);
      }
    }
  }

  @Override
  public void removeCacheListener(CacheListener aListener) {
    checkReadiness();
    if (aListener == null) {
      throw new IllegalArgumentException(
          "removeCacheListener parameter was null");
    }
    boolean changed = false;
    synchronized (clSync) {
      CacheListener[] oldListeners = cacheListeners;
      if (oldListeners != null && oldListeners.length > 0) {
        List<CacheListener> newListeners = new ArrayList<>(Arrays.asList(oldListeners));
        if (newListeners.remove(aListener)) {
          if (newListeners.isEmpty()) {
            cacheListeners = EMPTY_LISTENERS;
          } else {
            CacheListener[] newCacheListeners = new CacheListener[newListeners.size()];
            newListeners.toArray(newCacheListeners);
            cacheListeners = newCacheListeners;
          }
          closeCacheCallback(aListener);
          if (newListeners.isEmpty()) {
            changed = true;
          }
        }
      }
    }
    if (changed) {
      cacheListenersChanged(false);
    }
  }

  @Override
  public CacheLoader setCacheLoader(CacheLoader cacheLoader) {
    readWriteLockForCacheLoader.writeLock().lock();
    try {
      checkReadiness();
      CacheLoader oldLoader = this.cacheLoader;
      this.cacheLoader = cacheLoader;
      cacheLoaderChanged(oldLoader);
      return oldLoader;
    } finally {
      readWriteLockForCacheLoader.writeLock().unlock();
    }
  }

  private void assignCacheLoader(CacheLoader cl) {
    readWriteLockForCacheLoader.writeLock().lock();
    try {
      cacheLoader = cl;
    } finally {
      readWriteLockForCacheLoader.writeLock().unlock();
    }
  }

  @Override
  public CacheWriter setCacheWriter(CacheWriter cacheWriter) {
    readWriteLockForCacheWriter.writeLock().lock();
    try {
      checkReadiness();
      CacheWriter oldWriter = this.cacheWriter;
      this.cacheWriter = cacheWriter;
      cacheWriterChanged(oldWriter);
      return oldWriter;
    } finally {
      readWriteLockForCacheWriter.writeLock().unlock();
    }
  }

  private void assignCacheWriter(CacheWriter cacheWriter) {
    readWriteLockForCacheWriter.writeLock().lock();
    try {
      this.cacheWriter = cacheWriter;
    } finally {
      readWriteLockForCacheWriter.writeLock().unlock();
    }
  }

  void checkEntryTimeoutAction(String mode, ExpirationAction ea) {
    if ((getDataPolicy().withReplication() || getDataPolicy().withPartitioning())
        && (ea == ExpirationAction.LOCAL_DESTROY || ea == ExpirationAction.LOCAL_INVALIDATE)) {
      throw new IllegalArgumentException(
          String.format("%s action is incompatible with this region's data policy.",
              mode));
    }
  }

  @Override
  public ExpirationAttributes setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    checkReadiness();
    if (idleTimeout == null) {
      throw new IllegalArgumentException(
          "idleTimeout must not be null");
    }
    checkEntryTimeoutAction("idleTimeout", idleTimeout.getAction());
    if (!statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }

    ExpirationAttributes oldAttrs = getEntryIdleTimeout();
    entryIdleTimeout = idleTimeout.getTimeout();
    entryIdleTimeoutExpirationAction = idleTimeout.getAction();
    setEntryIdleTimeoutAttributes();
    updateEntryExpiryPossible();
    idleTimeoutChanged(oldAttrs);
    return oldAttrs;
  }

  @Override
  public CustomExpiry setCustomEntryIdleTimeout(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }

    CustomExpiry old = getCustomEntryIdleTimeout();
    customEntryIdleTimeout = custom;
    updateEntryExpiryPossible();
    idleTimeoutChanged(getEntryIdleTimeout());
    return old;
  }

  @Override
  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive) {
    checkReadiness();
    if (timeToLive == null) {
      throw new IllegalArgumentException(
          "timeToLive must not be null");
    }
    checkEntryTimeoutAction("timeToLive", timeToLive.getAction());
    if (!statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set time to live when statistics are disabled");
    }
    ExpirationAttributes oldAttrs = getEntryTimeToLive();
    entryTimeToLive = timeToLive.getTimeout();
    entryTimeToLiveExpirationAction = timeToLive.getAction();
    setEntryTimeToLiveAttributes();
    updateEntryExpiryPossible();
    timeToLiveChanged(oldAttrs);
    return oldAttrs;
  }

  @Override
  public CustomExpiry setCustomEntryTimeToLive(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set custom time to live when statistics are disabled");
    }
    CustomExpiry old = getCustomEntryTimeToLive();
    customEntryTimeToLive = custom;
    updateEntryExpiryPossible();
    timeToLiveChanged(getEntryTimeToLive());
    return old;
  }

  public static void validatePRRegionExpirationAttributes(ExpirationAttributes expAtts) {
    if (expAtts.getTimeout() > 0) {
      ExpirationAction expAction = expAtts.getAction();
      if (expAction.isInvalidate() || expAction.isLocalInvalidate()) {
        throw new IllegalStateException(
            "ExpirationAction INVALIDATE or LOCAL_INVALIDATE for region is not supported for Partitioned Region.");
      } else if (expAction.isDestroy() || expAction.isLocalDestroy()) {
        throw new IllegalStateException(
            "ExpirationAction DESTROY or LOCAL_DESTROY for region is not supported for Partitioned Region.");
      }
    }
  }

  @Override
  public ExpirationAttributes setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    checkReadiness();
    if (idleTimeout == null) {
      throw new IllegalArgumentException(
          "idleTimeout must not be null");
    }
    if (getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(idleTimeout);
    }
    if (idleTimeout.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && getDataPolicy().withReplication()) {
      throw new IllegalArgumentException(
          String.format("%s action is incompatible with this region's data policy.",
              "idleTimeout"));
    }
    if (!statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }
    ExpirationAttributes oldAttrs = getRegionIdleTimeout();
    regionIdleTimeout = idleTimeout.getTimeout();
    regionIdleTimeoutExpirationAction = idleTimeout.getAction();
    setRegionIdleTimeoutAttributes();
    regionIdleTimeoutChanged(oldAttrs);
    return oldAttrs;
  }

  @Override
  public ExpirationAttributes setRegionTimeToLive(ExpirationAttributes timeToLive) {
    checkReadiness();
    if (timeToLive == null) {
      throw new IllegalArgumentException(
          "timeToLive must not be null");
    }
    if (getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(timeToLive);
    }
    if (timeToLive.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && getDataPolicy().withReplication()) {
      throw new IllegalArgumentException(
          String.format("%s action is incompatible with this region's data policy.",
              "timeToLive"));
    }
    if (!statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set time to live when statistics are disabled");
    }

    ExpirationAttributes oldAttrs = getRegionTimeToLive();
    regionTimeToLive = timeToLive.getTimeout();
    regionTimeToLiveExpirationAction = timeToLive.getAction();
    setRegionTimeToLiveAtts();
    regionTimeToLiveChanged(timeToLive);
    return oldAttrs;
  }

  @Override
  public void becomeLockGrantor() {
    checkReadiness();
    checkForLimitedOrNoAccess();
    if (scope != Scope.GLOBAL) {
      throw new IllegalStateException(
          "Cannot set lock grantor when scope is not global");
    }
    if (isCurrentlyLockGrantor())
      return; // nothing to do... already lock grantor
    isLockGrantor = true;
  }

  @Override
  public CacheStatistics getStatistics() {
    // prefer region destroyed exception over statistics disabled exception
    checkReadiness();
    if (!statisticsEnabled) {
      throw new StatisticsDisabledException(
          String.format("Statistics disabled for region ' %s '",
              getFullPath()));
    }
    return this;
  }

  /**
   * The logical lastModifiedTime of a region is the most recent lastModifiedTime of the region and
   * all its subregions. This implementation trades performance of stat retrieval for performance of
   * get/put, which is more critical.
   */
  @Override
  public synchronized long getLastModifiedTime() {
    checkReadiness();
    long mostRecent = basicGetLastModifiedTime();

    // don't need to wait on getInitialImage for this operation in subregions
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
    try {
      for (Object region : subregions(false)) {
        try {
          LocalRegion localRegion = (LocalRegion) region;
          if (localRegion.isInitialized()) {
            mostRecent = Math.max(mostRecent, localRegion.getLastModifiedTime());
          }
        } catch (RegionDestroyedException ignore) {
          // pass over destroyed region
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return mostRecent;
  }

  private long basicGetLastModifiedTime() {
    return lastModifiedTime.get();
  }

  private long basicGetLastAccessedTime() {
    return lastAccessedTime.get();
  }

  private void basicSetLastModifiedTime(long t) {
    lastModifiedTime.set(t);
  }

  private void basicSetLastAccessedTime(long t) {
    lastAccessedTime.set(t);
  }

  /**
   * The logical lastAccessedTime of a region is the most recent lastAccessedTime of the region and
   * all its subregions. This implementation trades performance of stat retrieval for performance of
   * get/put, which is more critical.
   */
  @Override
  public synchronized long getLastAccessedTime() {
    checkReadiness();
    long mostRecent = basicGetLastAccessedTime();
    // don't need to wait on getInitialImage for this operation in subregions
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
    try {
      for (Object region : subregions(false)) {
        try {
          LocalRegion localRegion = (LocalRegion) region;
          if (localRegion.isInitialized()) {
            mostRecent = Math.max(mostRecent, localRegion.getLastAccessedTime());
          }
        } catch (RegionDestroyedException ignore) {
          // pass over destroyed region
        }
      }
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return mostRecent;
  }

  /**
   * Update the lastAccessedTime and lastModifiedTimes to reflects those in the subregions
   */
  protected synchronized void updateStats() {
    long mostRecentAccessed = basicGetLastAccessedTime();
    long mostRecentModified = basicGetLastModifiedTime();

    // don't need to wait on getInitialImage for this operation in subregions
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
    try {
      for (Object region : subregions(false)) {
        try {
          LocalRegion localRegion = (LocalRegion) region;
          if (localRegion.isInitialized()) {
            mostRecentAccessed = Math.max(mostRecentAccessed, localRegion.getLastAccessedTime());
            mostRecentModified = Math.max(mostRecentModified, localRegion.getLastModifiedTime());
          }
        } catch (RegionDestroyedException ignore) {
          // pass over destroyed region
        }
      }
      basicSetLastAccessedTime(Math.max(mostRecentAccessed, mostRecentModified));
      basicSetLastModifiedTime(mostRecentModified);
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  protected void setLastModifiedTime(long time) {
    if (time > lastModifiedTime.get()) {
      lastModifiedTime.set(time);
    }
    if (time > lastAccessedTime.get()) {
      lastAccessedTime.set(time);
    }
  }

  void setLastAccessedTime(long time, boolean hit) {
    lastAccessedTime.set(time);
    if (hit) {
      if (trackHits) {
        hitCount.getAndIncrement();
      }
    } else {
      if (trackMisses) {
        missCount.getAndIncrement();
      }
    }
  }

  @Override
  public float getHitRatio() {
    long hits = getHitCount();
    long total = hits + getMissCount();
    return total == 0L ? 0.0f : (float) hits / total;
  }

  @Override
  public long getHitCount() {
    return hitCount.get();
  }

  @Override
  public long getMissCount() {
    return missCount.get();
  }

  @Override
  public void resetCounts() {
    if (trackMisses) {
      missCount.set(0);
    }
    if (trackHits) {
      hitCount.set(0);
    }
  }

  void closeCacheCallback(CacheCallback cb) {
    if (cb != null) {
      try {
        cb.close();
      } catch (RuntimeException ex) {
        logger.warn("CacheCallback close exception",
            ex);
      }
    }
  }

  protected void cacheLoaderChanged(CacheLoader oldLoader) {
    readWriteLockForCacheLoader.readLock().lock();
    try {
      if (cacheLoader != oldLoader) {
        closeCacheCallback(oldLoader);
      }
    } finally {
      readWriteLockForCacheLoader.readLock().unlock();
    }

  }

  /**
   * Called if when we go from no listeners to at least one or from at least one to no listeners
   *
   * @param nowHasListener true if we now have at least one listener; false if we now have no
   *        listeners
   */
  protected void cacheListenersChanged(boolean nowHasListener) {
    // nothing needed by default
  }

  protected void cacheWriterChanged(CacheWriter oldWriter) {
    readWriteLockForCacheWriter.readLock().lock();
    try {
      if (cacheWriter != oldWriter) {
        closeCacheCallback(oldWriter);
      }
    } finally {
      readWriteLockForCacheWriter.readLock().unlock();
    }
  }

  void timeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    // nothing
  }

  void idleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
    // nothing
  }

  void regionTimeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    // nothing
  }

  void regionIdleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
    // nothing
  }

  /**
   * Returns true if this region has no storage
   *
   * @since GemFire 5.0
   */
  @Override
  public boolean isProxy() {
    return getDataPolicy().isEmpty();
  }

  /**
   * Returns true if this region has no storage and is only interested in what it contains (which is
   * nothing)
   *
   * @since GemFire 5.0
   */
  boolean isCacheContentProxy() {
    // method added to fix bug 35195
    return isProxy() && getSubscriptionAttributes().getInterestPolicy().isCacheContent();
  }

  /**
   * Returns true if region subscribes to all events or is a replicate.
   *
   * @since GemFire 5.0
   */
  @Override
  public boolean isAllEvents() {
    return getDataPolicy().withReplication()
        || getSubscriptionAttributes().getInterestPolicy().isAll();
  }

  private boolean entryExpiryPossible = false;

  protected void updateEntryExpiryPossible() {
    entryExpiryPossible = !isProxy() && (hasTimeToLive() || hasIdleTimeout());
  }

  private boolean hasTimeToLive() {
    return entryTimeToLive > 0 || customEntryTimeToLive != null;
  }

  private boolean hasIdleTimeout() {
    return entryIdleTimeout > 0 || customEntryIdleTimeout != null;
  }

  /**
   * Returns true if this region could expire an entry
   */
  @Override
  public boolean isEntryExpiryPossible() {
    return entryExpiryPossible;
  }

  ExpirationAction getEntryExpirationAction() {
    if (entryIdleTimeoutExpirationAction != null) {
      return entryIdleTimeoutExpirationAction;
    }
    if (entryTimeToLiveExpirationAction != null) {
      return entryTimeToLiveExpirationAction;
    }
    return null;
  }

  /**
   * Returns true if this region can evict entries.
   */
  @Override
  public boolean isEntryEvictionPossible() {
    return evictionAttributes != null && !evictionAttributes.getAlgorithm().isNone();
  }

  /** is this a region that supports versioning? */
  protected abstract boolean supportsConcurrencyChecks();

  /**
   * Returns the pool this region is using or null if it does not have one or the pool does not
   * exist.
   *
   * @since GemFire 5.7
   */
  private PoolImpl getPool() {
    PoolImpl result = null;
    if (getPoolName() != null) {
      result = poolFinder.find(getPoolName());
    }
    return result;
  }

  @Override
  public boolean existsValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return !query(queryPredicate).isEmpty();
  }

  @Override
  public Object selectValue(String queryPredicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    SelectResults result = query(queryPredicate);
    if (result.isEmpty()) {
      return null;
    }
    if (result.size() > 1)
      throw new FunctionDomainException(
          String.format("selectValue expects results of size 1, but found results of size %s",
              result.size()));
    return result.iterator().next();
  }

  @Override
  public EvictionAttributes getEvictionAttributes() {
    return evictionAttributes;
  }

  @Override
  public EvictionAttributesMutator getEvictionAttributesMutator() {
    return new EvictionAttributesMutatorImpl(this, evictionAttributes);
  }

  /**
   * Throws RegionAccessException if required roles are missing and the LossAction is NO_ACCESS
   *
   * @throws RegionAccessException if required roles are missing and the LossAction is NO_ACCESS
   */
  protected void checkForNoAccess() {
    // nothing
  }

  /**
   * Throws RegionAccessException is required roles are missing and the LossAction is either
   * NO_ACCESS or LIMITED_ACCESS.
   *
   * @throws RegionAccessException if required roles are missing and the LossAction is either
   *         NO_ACCESS or LIMITED_ACCESS
   */
  public void checkForLimitedOrNoAccess() {
    // nothing
  }

  /**
   * Makes sure that the data was distributed to every required role. If it was not it either queues
   * the data for later delivery or it throws an exception.
   *
   * @param successfulRecipients the successful recipients
   * @throws RoleException if a required role was not sent the message and the LossAction is either
   *         NO_ACCESS or LIMITED_ACCESS.
   * @since GemFire 5.0
   *
   */
  @Override
  public void handleReliableDistribution(Set successfulRecipients) {
    // do nothing by default
  }

  /** Returns true if region requires a reliability check. */
  @Override
  public boolean requiresReliabilityCheck() {
    return false;
  }

  /**
   * Returns the serial number which identifies the static order in which this region was created in
   * relation to other regions or other instances of this region during the life of this JVM.
   */
  public int getSerialNumber() {
    return serialNumber;
  }

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public long cacheTimeMillis() {
    return cache.getInternalDistributedSystem().getClock().cacheTimeMillis();
  }

  @Override
  public RegionService getRegionService() {
    return cache;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return getCache().getInternalDistributedSystem();
  }

  @Override
  public StatisticsFactory getStatisticsFactory() {
    return getSystem();
  }

  @Override
  public int getDSFID() {
    return REGION;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeRegion(this, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    // should never be called since the special DataSerializer.readRegion is used.
    throw new UnsupportedOperationException("fromData is not implemented");
  }

  boolean forceCompaction() {
    throw new UnsupportedOperationException("forceCompaction is not implemented");
  }

  @Override
  public boolean getCloningEnabled() {
    return cloningEnable;
  }

  @Override
  public void setCloningEnabled(boolean cloningEnable) {
    this.cloningEnable = cloningEnable;
  }

  public static Object handleNotAvailable(Object object) {
    if (object == Token.NOT_AVAILABLE) {
      object = null;
    }
    return object;
  }

  public InternalCache getGemFireCache() {
    return cache;
  }

  @Override
  public InternalCache getInternalCache() {
    return cache;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RegionSnapshotService getSnapshotService() {
    return new RegionSnapshotServiceImpl(this);
  }

  @Override
  public Compressor getCompressor() {
    return compressor;
  }

  /**
   * @since GemFire 8.1
   */
  @Override
  public ExtensionPoint<Region<?, ?>> getExtensionPoint() {
    return extensionPoint;
  }

  @Override
  public boolean getOffHeap() {
    return offHeap;
  }

  @Override
  public void incRecentlyUsed() {
    // nothing
  }

  /**
   * Only subclasses of {@code AbstractRegion} should use this supplier to acquire the
   * {@code StatisticsClock}.
   *
   * <p>
   * Please do not use this accessor from any class other than a Region.
   */
  protected StatisticsClock getStatisticsClock() {
    return statisticsClock;
  }

  protected interface PoolFinder {
    PoolImpl find(String poolName);
  }
}
