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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.TestingOnly;
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
import org.apache.geode.cache.client.PoolManager;
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
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.snapshot.RegionSnapshotServiceImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.pdx.internal.PeerTypeRegistration;

/**
 * Takes care of RegionAttributes, AttributesMutator, and some no-brainer method implementations.
 */
@SuppressWarnings("deprecation")
public abstract class AbstractRegion implements InternalRegion, AttributesMutator, CacheStatistics,
    DataSerializableFixedID, Extensible<Region<?, ?>>, EvictableRegion {

  private static final Logger logger = LogService.getLogger();

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

  private volatile CacheLoader cacheLoader;

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

  private boolean isGatewaySenderEnabled = false;

  protected Set<String> asyncEventQueueIds;

  private Set<String> visibleAsyncEventQueueIds;

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
      !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ignoreHits");

  private static final boolean trackMisses =
      !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ignoreMisses");

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

  /** Creates a new instance of AbstractRegion */
  protected AbstractRegion(InternalCache cache, RegionAttributes attrs, String regionName,
      InternalRegionArguments internalRegionArgs) {
    this.cache = cache;
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.isPdxTypesRegion = PeerTypeRegistration.REGION_NAME.equals(regionName);
    this.lastAccessedTime = new AtomicLong(cacheTimeMillis());
    this.lastModifiedTime = new AtomicLong(this.lastAccessedTime.get());
    this.dataPolicy = attrs.getDataPolicy(); // do this one first
    this.scope = attrs.getScope();

    this.offHeap = attrs.getOffHeap();

    // fix bug #52033 by invoking setOffHeap now (localMaxMemory may now be the temporary
    // placeholder for off-heap until DistributedSystem is created
    // found non-null PartitionAttributes and offHeap is true so let's setOffHeap on PA now
    PartitionAttributes<?, ?> partitionAttributes1 = attrs.getPartitionAttributes();
    if (this.offHeap && partitionAttributes1 != null) {
      PartitionAttributesImpl impl = (PartitionAttributesImpl) partitionAttributes1;
      impl.setOffHeap(true);
    }

    evictionAttributes = new EvictionAttributesImpl(attrs.getEvictionAttributes());
    if (attrs.getPartitionAttributes() != null && this.evictionAttributes != null
        && this.evictionAttributes.getAlgorithm().isLRUMemory()
        && attrs.getPartitionAttributes().getLocalMaxMemory() != 0 && this.evictionAttributes
            .getMaximum() != attrs.getPartitionAttributes().getLocalMaxMemory()) {
      logger.warn(
          "For region {} with data policy PARTITION, memory LRU eviction attribute maximum has been reset from {}MB to local-max-memory {}MB",
          new Object[] {regionName, this.evictionAttributes.getMaximum(),
              attrs.getPartitionAttributes().getLocalMaxMemory()});
      this.evictionAttributes.setMaximum(attrs.getPartitionAttributes().getLocalMaxMemory());
    }

    storeCacheListenersField(attrs.getCacheListeners());
    assignCacheLoader(attrs.getCacheLoader());
    assignCacheWriter(attrs.getCacheWriter());
    this.regionTimeToLive = attrs.getRegionTimeToLive().getTimeout();
    this.regionTimeToLiveExpirationAction = attrs.getRegionTimeToLive().getAction();
    setRegionTimeToLiveAtts();
    this.regionIdleTimeout = attrs.getRegionIdleTimeout().getTimeout();
    this.regionIdleTimeoutExpirationAction = attrs.getRegionIdleTimeout().getAction();
    setRegionIdleTimeoutAttributes();
    this.entryTimeToLive = attrs.getEntryTimeToLive().getTimeout();
    this.entryTimeToLiveExpirationAction = attrs.getEntryTimeToLive().getAction();
    setEntryTimeToLiveAttributes();
    this.customEntryTimeToLive = attrs.getCustomEntryTimeToLive();
    this.entryIdleTimeout = attrs.getEntryIdleTimeout().getTimeout();
    this.entryIdleTimeoutExpirationAction = attrs.getEntryIdleTimeout().getAction();
    setEntryIdleTimeoutAttributes();
    this.customEntryIdleTimeout = attrs.getCustomEntryIdleTimeout();
    updateEntryExpiryPossible();
    this.statisticsEnabled = attrs.getStatisticsEnabled();
    this.ignoreJTA = attrs.getIgnoreJTA();
    this.isLockGrantor = attrs.isLockGrantor();
    this.keyConstraint = attrs.getKeyConstraint();
    this.valueConstraint = attrs.getValueConstraint();
    this.initialCapacity = attrs.getInitialCapacity();
    this.loadFactor = attrs.getLoadFactor();
    this.concurrencyLevel = attrs.getConcurrencyLevel();
    this.setConcurrencyChecksEnabled(
        attrs.getConcurrencyChecksEnabled() && supportsConcurrencyChecks());
    this.earlyAck = attrs.getEarlyAck();
    this.gatewaySenderIds = attrs.getGatewaySenderIds();
    this.asyncEventQueueIds = attrs.getAsyncEventQueueIds();
    initializeVisibleAsyncEventQueueIds(internalRegionArgs);
    setAllGatewaySenderIds();
    this.enableSubscriptionConflation = attrs.getEnableSubscriptionConflation();
    this.publisher = attrs.getPublisher();
    this.enableAsyncConflation = attrs.getEnableAsyncConflation();
    this.indexMaintenanceSynchronous = attrs.getIndexMaintenanceSynchronous();
    this.mcastEnabled = attrs.getMulticastEnabled();
    this.partitionAttributes = attrs.getPartitionAttributes();
    this.membershipAttributes = attrs.getMembershipAttributes();
    this.subscriptionAttributes = attrs.getSubscriptionAttributes();
    this.cloningEnable = attrs.getCloningEnabled();
    this.poolName = attrs.getPoolName();
    if (this.poolName != null) {
      PoolImpl cp = getPool();
      if (cp == null) {
        throw new IllegalStateException(
            String.format("The connection pool %s has not been created",
                this.poolName));
      }
      cp.attach();
      if (cp.getMultiuserAuthentication() && !this.getDataPolicy().isEmpty()) {
        throw new IllegalStateException(
            "Region must have empty data-policy " + "when multiuser-authentication is true.");
      }
    }

    this.diskStoreName = attrs.getDiskStoreName();
    this.isDiskSynchronous = attrs.isDiskSynchronous();
    if (this.diskStoreName == null) {
      this.diskWriteAttributes = attrs.getDiskWriteAttributes();
      this.isDiskSynchronous = this.diskWriteAttributes.isSynchronous(); // fixes bug 41313
      this.diskDirs = attrs.getDiskDirs();
      this.diskSizes = attrs.getDiskDirSizes();
    }

    this.compressor = attrs.getCompressor();
    // enable concurrency checks for persistent regions
    if (!attrs.getConcurrencyChecksEnabled() && attrs.getDataPolicy().withPersistence()
        && supportsConcurrencyChecks()) {
      throw new IllegalStateException(
          "Concurrency checks cannot be disabled for regions that use persistence");
    }
  }

  @TestingOnly
  AbstractRegion() {
    this.cache = null;
    this.serialNumber = 0;
    this.isPdxTypesRegion = false;
    this.lastAccessedTime = new AtomicLong(0);
    this.lastModifiedTime = new AtomicLong(0);
    evictionAttributes = new EvictionAttributesImpl();
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
    this.ignoreJTA = ignore;
  }

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
        .append("';dataPolicy=").append(this.getDataPolicy());
    if (this.getConcurrencyChecksEnabled()) {
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
    return this.cacheLoader;
  }

  @Override
  public CacheWriter getCacheWriter() {
    return this.cacheWriter;
  }

  /**
   * Return a cache loader if this region has one. Note if region's loader is used to implement
   * bridge then null is returned.
   *
   * @since GemFire 5.7
   */
  CacheLoader basicGetLoader() {
    return this.cacheLoader;
  }

  /**
   * Return a cache writer if this region has one. Note if region's writer is used to implement
   * bridge then null is returned.
   *
   * @since GemFire 5.7
   */
  @Override
  public CacheWriter basicGetWriter() {
    return this.cacheWriter;
  }

  @Override
  public Class getKeyConstraint() {
    return this.keyConstraint;
  }

  @Override
  public Class getValueConstraint() {
    return this.valueConstraint;
  }

  private volatile ExpirationAttributes regionTimeToLiveAtts;

  private void setRegionTimeToLiveAtts() {
    this.regionTimeToLiveAtts =
        new ExpirationAttributes(this.regionTimeToLive, this.regionTimeToLiveExpirationAction);
  }

  @Override
  public ExpirationAttributes getRegionTimeToLive() {
    return this.regionTimeToLiveAtts;
  }

  private volatile ExpirationAttributes regionIdleTimeoutAttributes;

  private void setRegionIdleTimeoutAttributes() {
    this.regionIdleTimeoutAttributes =
        new ExpirationAttributes(this.regionIdleTimeout, this.regionIdleTimeoutExpirationAction);
  }

  @Override
  public ExpirationAttributes getRegionIdleTimeout() {
    return this.regionIdleTimeoutAttributes;
  }

  private volatile ExpirationAttributes entryTimeToLiveAtts;

  void setEntryTimeToLiveAttributes() {
    this.entryTimeToLiveAtts =
        new ExpirationAttributes(this.entryTimeToLive, this.entryTimeToLiveExpirationAction);
  }

  @Override
  public ExpirationAttributes getEntryTimeToLive() {
    return this.entryTimeToLiveAtts;
  }

  @Override
  public CustomExpiry getCustomEntryTimeToLive() {
    return this.customEntryTimeToLive;
  }

  private volatile ExpirationAttributes entryIdleTimeoutAttributes;

  private void setEntryIdleTimeoutAttributes() {
    this.entryIdleTimeoutAttributes =
        new ExpirationAttributes(this.entryIdleTimeout, this.entryIdleTimeoutExpirationAction);
  }

  @Override
  public ExpirationAttributes getEntryIdleTimeout() {
    return this.entryIdleTimeoutAttributes;
  }

  @Override
  public CustomExpiry getCustomEntryIdleTimeout() {
    return this.customEntryIdleTimeout;
  }

  @Override
  public MirrorType getMirrorType() {
    if (this.getDataPolicy().isNormal() || this.getDataPolicy().isPreloaded()
        || this.getDataPolicy().isEmpty() || this.getDataPolicy().withPartitioning()) {
      return MirrorType.NONE;
    } else if (this.getDataPolicy().withReplication()) {
      return MirrorType.KEYS_VALUES;
    } else {
      throw new IllegalStateException(
          String.format("No mirror type corresponds to data policy %s",
              this.getDataPolicy()));
    }
  }

  @Override
  public String getPoolName() {
    return this.poolName;
  }

  @Override
  public DataPolicy getDataPolicy() {
    return this.dataPolicy;
  }

  @Override
  public Scope getScope() {
    return this.scope;
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
    return this.isPdxTypesRegion;
  }

  @Override
  public Set<String> getGatewaySenderIds() {
    return this.gatewaySenderIds;
  }

  @Override
  public Set<String> getAsyncEventQueueIds() {
    return this.asyncEventQueueIds;
  }

  Set<String> getVisibleAsyncEventQueueIds() {
    return this.visibleAsyncEventQueueIds;
  }

  @Override
  public Set<String> getAllGatewaySenderIds() {
    return Collections.unmodifiableSet(this.allGatewaySenderIds);
  }

  /**
   * Return the remote DS IDs that need to receive events for this region.
   *
   * @param allGatewaySenderIds the set of gateway sender IDs to consider
   */
  List<Integer> getRemoteDsIds(Set<String> allGatewaySenderIds) throws IllegalStateException {
    int sz = allGatewaySenderIds.size();
    Set<GatewaySender> allGatewaySenders = this.cache.getAllGatewaySenders();
    if ((sz > 0 || this.isPdxTypesRegion) && !allGatewaySenders.isEmpty()) {
      List<Integer> allRemoteDSIds = new ArrayList<>(sz);
      for (GatewaySender sender : allGatewaySenders) {
        // This is for all regions except pdx Region
        if (!this.isPdxTypesRegion) {
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

  boolean isGatewaySenderEnabled() {
    return this.isGatewaySenderEnabled;
  }

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
    synchronized (this.clSync) {
      if (value != null && value.length != 0) {
        CacheListener[] cacheListeners = new CacheListener[value.length];
        System.arraycopy(value, 0, cacheListeners, 0, cacheListeners.length);
        value = cacheListeners;
      } else {
        value = EMPTY_LISTENERS;
      }
      this.cacheListeners = value;
    }
  }

  /**
   * Fetches the value in the cacheListeners field. NOTE: callers should not modify the contents of
   * the returned array.
   */
  CacheListener[] fetchCacheListenersField() {
    return this.cacheListeners;
  }

  @Override
  public int getInitialCapacity() {
    return this.initialCapacity;
  }

  @Override
  public float getLoadFactor() {
    return this.loadFactor;
  }

  protected abstract boolean isCurrentlyLockGrantor();

  @Override
  public boolean isLockGrantor() {
    return this.isLockGrantor;
  }

  /**
   * RegionAttributes implementation. Returns true if multicast can be used by the cache for this
   * region
   */
  @Override
  public boolean getMulticastEnabled() {
    return this.mcastEnabled;
  }

  @Override
  public boolean getStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  @Override
  public boolean getIgnoreJTA() {
    return this.ignoreJTA;
  }

  @Override
  public int getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  @Override
  public boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
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
    return this.earlyAck;
  }

  /*
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  @Override
  public boolean getPublisher() {
    return this.publisher;
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
    return this.enableSubscriptionConflation;
  }

  @Override
  public boolean getEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  /*
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  @Override
  public DiskWriteAttributes getDiskWriteAttributes() {
    return this.diskWriteAttributes;
  }

  @Override
  public abstract File[] getDiskDirs();

  @Override
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  @Override
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  @Override
  public boolean getIndexMaintenanceSynchronous() {
    return this.indexMaintenanceSynchronous;
  }

  @Override
  public PartitionAttributes getPartitionAttributes() {
    return this.partitionAttributes;
  }

  @Override
  public MembershipAttributes getMembershipAttributes() {
    return this.membershipAttributes;
  }

  @Override
  public SubscriptionAttributes getSubscriptionAttributes() {
    return this.subscriptionAttributes;
  }

  /**
   * Get IndexManger for region
   */
  @Override
  public IndexManager getIndexManager() {
    return this.indexManager;
  }

  /**
   * This method call is guarded by imSync lock created for each region. Set IndexManger for region.
   */
  @Override
  public IndexManager setIndexManager(IndexManager indexManager) {
    checkReadiness();
    IndexManager oldIdxManager = this.indexManager;
    this.indexManager = indexManager;
    return oldIdxManager;
  }

  /**
   * Use ONLY imSync for IndexManager get and set.
   *
   * @return {@link IndexManager} lock.
   */
  @Override
  public Object getIMSync() {
    return this.imSync;
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
      this.allGatewaySenderIds = Collections.emptySet(); // fix for bug 45774
    }
    Set<String> tmp = new CopyOnWriteArraySet<String>();
    tmp.addAll(this.getGatewaySenderIds());
    for (String asyncQueueId : this.getAsyncEventQueueIds()) {
      tmp.add(AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncQueueId));
    }
    this.allGatewaySenderIds = tmp;
  }

  private void initializeVisibleAsyncEventQueueIds(InternalRegionArguments internalRegionArgs) {
    Set<String> visibleAsyncEventQueueIds = new CopyOnWriteArraySet<>();
    // Add all configured aeqIds
    visibleAsyncEventQueueIds.addAll(getAsyncEventQueueIds());
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
    synchronized (this.clSync) {
      CacheListener[] oldListeners = this.cacheListeners;
      if (oldListeners == null || oldListeners.length == 0) {
        this.cacheListeners = new CacheListener[] {wcl};
        changed = true;
      } else {
        List<CacheListener> listeners = Arrays.asList(oldListeners);
        if (!listeners.contains(aListener)) {
          this.cacheListeners =
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
    synchronized (this.clSync) {
      DistributedMember[] members = null;
      CacheListener[] newListeners = null;
      for (int i = 0; i < this.cacheListeners.length; i++) {
        CacheListener cl = this.cacheListeners[i];
        if (cl instanceof WrappedRegionMembershipListener) {
          WrappedRegionMembershipListener wrml = (WrappedRegionMembershipListener) cl;
          if (!wrml.isInitialized()) {
            if (members == null) {
              members = (DistributedMember[]) initialMembers
                  .toArray(new DistributedMember[initialMembers.size()]);
            }
            wrml.initialMembers(this, members);
            if (newListeners == null) {
              newListeners = new CacheListener[this.cacheListeners.length];
              System.arraycopy(this.cacheListeners, 0, newListeners, 0, newListeners.length);
            }
            newListeners[i] = wrml.getWrappedListener();
          }
        }
      }
      if (newListeners != null) {
        this.cacheListeners = newListeners;
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
    synchronized (this.clSync) {
      oldListeners = this.cacheListeners;
      if (listenersToAdd == null || listenersToAdd.length == 0) {
        this.cacheListeners = EMPTY_LISTENERS;
      } else { // we have some listeners to add
        if (Arrays.asList(listenersToAdd).contains(null)) {
          throw new IllegalArgumentException(
              "initCacheListeners parameter had a null element");
        }
        CacheListener[] newCacheListeners = new CacheListener[listenersToAdd.length];
        System.arraycopy(listenersToAdd, 0, newCacheListeners, 0, newCacheListeners.length);
        this.cacheListeners = newCacheListeners;
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
    synchronized (this.clSync) {
      CacheListener[] oldListeners = this.cacheListeners;
      if (oldListeners != null && oldListeners.length > 0) {
        List newListeners = new ArrayList(Arrays.asList(oldListeners));
        if (newListeners.remove(aListener)) {
          if (newListeners.isEmpty()) {
            this.cacheListeners = EMPTY_LISTENERS;
          } else {
            CacheListener[] newCacheListeners = new CacheListener[newListeners.size()];
            newListeners.toArray(newCacheListeners);
            this.cacheListeners = newCacheListeners;
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
  public synchronized CacheLoader setCacheLoader(CacheLoader cacheLoader) {
    checkReadiness();
    CacheLoader oldLoader = this.cacheLoader;
    assignCacheLoader(cacheLoader);
    cacheLoaderChanged(oldLoader);
    return oldLoader;
  }

  private synchronized void assignCacheLoader(CacheLoader cl) {
    this.cacheLoader = cl;
  }

  @Override
  public synchronized CacheWriter setCacheWriter(CacheWriter cacheWriter) {
    checkReadiness();
    CacheWriter oldWriter = this.cacheWriter;
    assignCacheWriter(cacheWriter);
    cacheWriterChanged(oldWriter);
    return oldWriter;
  }

  private synchronized void assignCacheWriter(CacheWriter cacheWriter) {
    this.cacheWriter = cacheWriter;
  }

  void checkEntryTimeoutAction(String mode, ExpirationAction ea) {
    if ((this.getDataPolicy().withReplication() || this.getDataPolicy().withPartitioning())
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
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }

    ExpirationAttributes oldAttrs = getEntryIdleTimeout();
    this.entryIdleTimeout = idleTimeout.getTimeout();
    this.entryIdleTimeoutExpirationAction = idleTimeout.getAction();
    setEntryIdleTimeoutAttributes();
    updateEntryExpiryPossible();
    idleTimeoutChanged(oldAttrs);
    return oldAttrs;
  }

  @Override
  public CustomExpiry setCustomEntryIdleTimeout(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }

    CustomExpiry old = getCustomEntryIdleTimeout();
    this.customEntryIdleTimeout = custom;
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
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set time to live when statistics are disabled");
    }
    ExpirationAttributes oldAttrs = getEntryTimeToLive();
    this.entryTimeToLive = timeToLive.getTimeout();
    this.entryTimeToLiveExpirationAction = timeToLive.getAction();
    setEntryTimeToLiveAttributes();
    updateEntryExpiryPossible();
    timeToLiveChanged(oldAttrs);
    return oldAttrs;
  }

  @Override
  public CustomExpiry setCustomEntryTimeToLive(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set custom time to live when statistics are disabled");
    }
    CustomExpiry old = getCustomEntryTimeToLive();
    this.customEntryTimeToLive = custom;
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
    if (this.getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(idleTimeout);
    }
    if (idleTimeout.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && this.getDataPolicy().withReplication()) {
      throw new IllegalArgumentException(
          String.format("%s action is incompatible with this region's data policy.",
              "idleTimeout"));
    }
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set idle timeout when statistics are disabled.");
    }
    ExpirationAttributes oldAttrs = getRegionIdleTimeout();
    this.regionIdleTimeout = idleTimeout.getTimeout();
    this.regionIdleTimeoutExpirationAction = idleTimeout.getAction();
    this.setRegionIdleTimeoutAttributes();
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
    if (this.getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(timeToLive);
    }
    if (timeToLive.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && this.getDataPolicy().withReplication()) {
      throw new IllegalArgumentException(
          String.format("%s action is incompatible with this region's data policy.",
              "timeToLive"));
    }
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(
          "Cannot set time to live when statistics are disabled");
    }

    ExpirationAttributes oldAttrs = getRegionTimeToLive();
    this.regionTimeToLive = timeToLive.getTimeout();
    this.regionTimeToLiveExpirationAction = timeToLive.getAction();
    this.setRegionTimeToLiveAtts();
    regionTimeToLiveChanged(timeToLive);
    return oldAttrs;
  }

  @Override
  public void becomeLockGrantor() {
    checkReadiness();
    checkForLimitedOrNoAccess();
    if (this.scope != Scope.GLOBAL) {
      throw new IllegalStateException(
          "Cannot set lock grantor when scope is not global");
    }
    if (isCurrentlyLockGrantor())
      return; // nothing to do... already lock grantor
    this.isLockGrantor = true;
  }

  @Override
  public CacheStatistics getStatistics() {
    // prefer region destroyed exception over statistics disabled exception
    checkReadiness();
    if (!this.statisticsEnabled) {
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
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
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
    return this.lastModifiedTime.get();
  }

  private long basicGetLastAccessedTime() {
    return this.lastAccessedTime.get();
  }

  private void basicSetLastModifiedTime(long t) {
    this.lastModifiedTime.set(t);
  }

  private void basicSetLastAccessedTime(long t) {
    this.lastAccessedTime.set(t);
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
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
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
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
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
    if (time > this.lastModifiedTime.get()) {
      this.lastModifiedTime.set(time);
    }
    if (time > this.lastAccessedTime.get()) {
      this.lastAccessedTime.set(time);
    }
  }

  void setLastAccessedTime(long time, boolean hit) {
    this.lastAccessedTime.set(time);
    if (hit) {
      if (trackHits) {
        this.hitCount.getAndIncrement();
      }
    } else {
      if (trackMisses) {
        this.missCount.getAndIncrement();
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
    return this.hitCount.get();
  }

  @Override
  public long getMissCount() {
    return this.missCount.get();
  }

  @Override
  public void resetCounts() {
    if (trackMisses) {
      this.missCount.set(0);
    }
    if (trackHits) {
      this.hitCount.set(0);
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
    if (this.cacheLoader != oldLoader) {
      closeCacheCallback(oldLoader);
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
    if (this.cacheWriter != oldWriter) {
      closeCacheCallback(oldWriter);
    }
  }

  protected void timeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    // nothing
  }

  protected void idleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
    // nothing
  }

  protected void regionTimeToLiveChanged(ExpirationAttributes oldTimeToLive) {
    // nothing
  }

  protected void regionIdleTimeoutChanged(ExpirationAttributes oldIdleTimeout) {
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
    this.entryExpiryPossible = !isProxy() && (hasTimeToLive() || hasIdleTimeout());
  }

  private boolean hasTimeToLive() {
    return this.entryTimeToLive > 0 || this.customEntryTimeToLive != null;
  }

  private boolean hasIdleTimeout() {
    return this.entryIdleTimeout > 0 || this.customEntryIdleTimeout != null;
  }

  /**
   * Returns true if this region could expire an entry
   */
  @Override
  public boolean isEntryExpiryPossible() {
    return this.entryExpiryPossible;
  }

  ExpirationAction getEntryExpirationAction() {
    if (this.entryIdleTimeoutExpirationAction != null) {
      return this.entryIdleTimeoutExpirationAction;
    }
    if (this.entryTimeToLiveExpirationAction != null) {
      return this.entryTimeToLiveExpirationAction;
    }
    return null;
  }

  /**
   * Returns true if this region can evict entries.
   */
  @Override
  public boolean isEntryEvictionPossible() {
    return this.evictionAttributes != null && !this.evictionAttributes.getAlgorithm().isNone();
  }

  /** is this a region that supports versioning? */
  public abstract boolean supportsConcurrencyChecks();

  /**
   * Returns the pool this region is using or null if it does not have one or the pool does not
   * exist.
   *
   * @since GemFire 5.7
   */
  private PoolImpl getPool() {
    PoolImpl result = null;
    if (getPoolName() != null) {
      result = (PoolImpl) PoolManager.find(getPoolName());
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
    return this.evictionAttributes;
  }

  @Override
  public EvictionAttributesMutator getEvictionAttributesMutator() {
    return new EvictionAttributesMutatorImpl(this, this.evictionAttributes);
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
    return this.serialNumber;
  }

  @Override
  public InternalCache getCache() {
    return this.cache;
  }

  @Override
  public long cacheTimeMillis() {
    return this.cache.getInternalDistributedSystem().getClock().cacheTimeMillis();
  }

  @Override
  public RegionService getRegionService() {
    return this.cache;
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
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeRegion(this, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // should never be called since the special DataSerializer.readRegion is used.
    throw new UnsupportedOperationException("fromData is not implemented");
  }

  public boolean forceCompaction() {
    throw new UnsupportedOperationException("forceCompaction is not implemented");
  }

  @Override
  public boolean getCloningEnabled() {
    return this.cloningEnable;
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
    return this.cache;
  }

  @Override
  public InternalCache getInternalCache() {
    return cache;
  }

  @Override
  public RegionSnapshotService getSnapshotService() {
    return new RegionSnapshotServiceImpl(this);
  }

  @Override
  public Compressor getCompressor() {
    return this.compressor;
  }

  /**
   * @since GemFire 8.1
   */
  @Override
  public ExtensionPoint<Region<?, ?>> getExtensionPoint() {
    return this.extensionPoint;
  }

  @Override
  public boolean getOffHeap() {
    return this.offHeap;
  }

  @Override
  public void incRecentlyUsed() {
    // nothing
  }

}
