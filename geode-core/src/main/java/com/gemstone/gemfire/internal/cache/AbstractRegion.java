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

package com.gemstone.gemfire.internal.cache;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskWriteAttributes;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.EvictionAttributesMutator;
import com.gemstone.gemfire.cache.EvictionCriteria;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAccessException;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionMembershipListener;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.RoleException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.StatisticsDisabledException;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.ExtensionPoint;
import com.gemstone.gemfire.internal.cache.extension.SimpleExtensionPoint;
import com.gemstone.gemfire.internal.cache.lru.LRUAlgorithm;
import com.gemstone.gemfire.internal.cache.snapshot.RegionSnapshotServiceImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;
import com.google.common.util.concurrent.Service.State;

/**
 * Takes care of RegionAttributes, AttributesMutator, and some no-brainer method
 * implementations.
 *
 * @author Eric Zoerner
 */
@SuppressWarnings("deprecation")
public abstract class AbstractRegion implements Region, RegionAttributes,
                                                AttributesMutator, CacheStatistics,
                                                DataSerializableFixedID, RegionEntryContext,
                                                Extensible<Region<?,?>>
{
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Identifies the static order in which this region was created in relation
   * to other regions or other instances of this region during the life of
   * this JVM.
   */
  private final int serialNumber;

  // RegionAttributes //
  /**
   * Used to synchronize WRITES to cacheListeners.
   * Always do copy on write.
   */
  private final Object clSync = new Object();

  // Used to synchronize creation of IndexManager
  private final Object imSync = new Object();

  /**
   * NOTE: care must be taken to never modify the array of cacheListeners in
   * place. Instead allocate a new array and modify it.
   * This field is volatile so that it can be read w/o getting clSync.
   */
  private volatile CacheListener[] cacheListeners;

  private volatile CacheLoader cacheLoader;

  private volatile CacheWriter cacheWriter;

  private LRUAlgorithm evictionController;

  protected int entryIdleTimeout;
  protected ExpirationAction entryIdleTimeoutExpirationAction;
  protected CustomExpiry customEntryIdleTimeout;

  protected int entryTimeToLive;
  protected ExpirationAction entryTimeToLiveExpirationAction;
  protected CustomExpiry customEntryTimeToLive;

  protected int initialCapacity;

  protected Class keyConstraint;

  protected Class valueConstraint;

  protected float loadFactor;

  protected DataPolicy dataPolicy;

  protected int regionIdleTimeout;

  protected ExpirationAction regionIdleTimeoutExpirationAction;

  protected int regionTimeToLive;

  protected ExpirationAction regionTimeToLiveExpirationAction;

  public static final Scope DEFAULT_SCOPE = Scope.DISTRIBUTED_NO_ACK;
  protected Scope scope = DEFAULT_SCOPE;

  protected boolean statisticsEnabled;

  protected boolean isLockGrantor;

  protected boolean mcastEnabled;

  protected int concurrencyLevel;
  
  protected volatile boolean concurrencyChecksEnabled;

  protected boolean earlyAck;

  //merge42004: revision 42004 has not defined isPdxTypesRegion. It has come to cheetah branch from merge revision 39860. This is getting used in method getRemoteDsIds.
  
  protected final boolean isPdxTypesRegion;
  
  protected Set<String> gatewaySenderIds;
  
  protected boolean isGatewaySenderEnabled = false;
  
  protected Set<String> asyncEventQueueIds;

  protected Set<String> allGatewaySenderIds;
  
  protected boolean enableSubscriptionConflation;

  protected boolean publisher;

  protected boolean enableAsyncConflation;

  /**
   * True if this region uses off-heap memory; otherwise false (default)
   * @since 9.0
   */
  protected boolean offHeap;

  protected boolean cloningEnable = false;

  protected DiskWriteAttributes diskWriteAttributes;

  protected File[] diskDirs;
  protected int[] diskSizes;
  protected String diskStoreName;
  protected boolean isDiskSynchronous;
  protected boolean indexMaintenanceSynchronous = false;

  protected volatile IndexManager indexManager = null;

  //Asif : The ThreadLocal is used to identify if the thread is an
  //index creation thread. This identification helps skip the synchronization
  // block
  //if the value is "REMOVED" token. This prevents the dead lock , in case the
  // lock
  // over the entry is held by any Index Update Thread.
  // This is used to fix Bug # 33336.
  private final ThreadLocal isIndexCreator = new ThreadLocal();

  /** Attributes that define this Region as a PartitionedRegion */
  protected PartitionAttributes partitionAttributes;

  protected EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();

  protected CustomEvictionAttributes customEvictionAttributes;

  /** The membership attributes defining required roles functionality */
  protected MembershipAttributes membershipAttributes;

  /** The subscription attributes defining required roles functionality */
  protected SubscriptionAttributes subscriptionAttributes;

  /** should this region ignore in-progress JTA transactions? */
  protected boolean ignoreJTA;

  private final AtomicLong lastAccessedTime;

  private final AtomicLong lastModifiedTime;

  private static final boolean trackHits = !Boolean.getBoolean("gemfire.ignoreHits");
  private static final boolean trackMisses = !Boolean.getBoolean("gemfire.ignoreMisses");

  private final AtomicLong hitCount = new AtomicLong();

  private final AtomicLong missCount = new AtomicLong();
  
  protected String poolName;
  
  protected String hdfsStoreName;
  
  protected boolean hdfsWriteOnly;
  
  protected Compressor compressor;
  
  /**
   * @see #getExtensionPoint()
   * @since 8.1
   */
  protected ExtensionPoint<Region<?,?>> extensionPoint = new SimpleExtensionPoint<Region<?,?>>(this, this);
  
  protected final GemFireCacheImpl cache;

  /** Creates a new instance of AbstractRegion */
  protected AbstractRegion(GemFireCacheImpl cache, RegionAttributes attrs,
      String regionName, InternalRegionArguments internalRegionArgs) {
    this.cache = cache;
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.isPdxTypesRegion = PeerTypeRegistration.REGION_NAME.equals(regionName);
    this.lastAccessedTime = new AtomicLong(cacheTimeMillis());
    this.lastModifiedTime = new AtomicLong(lastAccessedTime.get());
    setAttributes(attrs, regionName, internalRegionArgs);
  }

  /**
   * Unit test constructor. DO NOT USE!
   * 
   * @since 8.1
   * @deprecated For unit testing only. Use
   *             {@link #AbstractRegion(GemFireCacheImpl, RegionAttributes, String, InternalRegionArguments)}
   *             .
   */
  @Deprecated
  AbstractRegion(GemFireCacheImpl cache, int serialNumber, boolean isPdxTypeRegion, long lastAccessedTime, long lastModifiedTime ) {
    this.cache = cache;
    this.serialNumber = serialNumber;
    this.isPdxTypesRegion = isPdxTypeRegion;
    this.lastAccessedTime = new AtomicLong(lastAccessedTime);
    this.lastModifiedTime = new AtomicLong(lastModifiedTime);
  }
  
  /** ******************** No-Brainer methods ******************************** */

  /**
   * configure this region to ignore or not ignore in-progress JTA transactions.
   * Setting this to true will cause cache operations to no longer notice JTA
   * transactions. The default setting is false
   *
   * @deprecated in 5.0 and later releases, use the region attribute ignoreJTA
   *             to configure this
   */
  @Deprecated
  public void setIgnoreJTA(boolean ignore)
  {
    ignoreJTA = ignore;
  }

  public final void create(Object key, Object value) throws TimeoutException,
      EntryExistsException, CacheWriterException
  {
    create(key, value, null);
  }

  public final Object destroy(Object key) throws TimeoutException,
      EntryNotFoundException, CacheWriterException
  {
    return destroy(key, null);
  }

  public final Object get(Object name) throws CacheLoaderException,
      TimeoutException
  {
    return get(name, null, true, null);
  }

  public final Object put(Object name, Object value) throws TimeoutException,
      CacheWriterException
  {
    return put(name, value, null);
  }

  public Object get(Object name, Object aCallbackArgument)
      throws CacheLoaderException, TimeoutException
  {
    return get(name, aCallbackArgument, true, null);
  }

  public final void localDestroyRegion()
  {
    localDestroyRegion(null);
  }

  /**
   * @param key  the key to find
   * @param aCallbackArgument argument for callbacks
   * @param generateCallbacks whether callbacks should be invoked
   * @param clientEvent client-side event, if any (used to pass back version information)
   * @return the value associated with the key
   * @throws TimeoutException
   * @throws CacheLoaderException
   */
  abstract Object get(Object key, Object aCallbackArgument,
      boolean generateCallbacks, EntryEventImpl clientEvent) throws TimeoutException, CacheLoaderException;

  public final void localDestroy(Object key) throws EntryNotFoundException {
    localDestroy(key, null);
  }

  public final void destroyRegion() throws CacheWriterException,
      TimeoutException
  {
    destroyRegion(null);
  }

  public final void invalidate(Object key) throws TimeoutException,
      EntryNotFoundException
  {
    invalidate(key, null);
  }

  public final void localInvalidate(Object key) throws EntryNotFoundException
  {
    localInvalidate(key, null);
  }

  public final void localInvalidateRegion()
  {
    localInvalidateRegion(null);
  }

  public final void invalidateRegion() throws TimeoutException
  {
    invalidateRegion(null);
  }

  abstract void basicClear(RegionEventImpl regionEvent);

  /**
   * The default Region implementation will generate EvenTID in the EntryEvent
   * object. This method is overridden in special Region objects like HARegion
   * or SingleWriteSingleReadRegionQueue.SingleReadWriteMetaRegion to return
   * false as the event propagation from those regions do not need EventID
   * objects. This method is made abstract to directly use it in clear operations.
   *(clear and localclear)
   * @return boolean indicating whether to generate eventID or not
   */
  abstract boolean generateEventID();

  protected abstract DistributedMember getMyId();

  public void clear()
  {
    checkReadiness();
    checkForLimitedOrNoAccess();
    RegionEventImpl regionEvent = new RegionEventImpl(this,
        Operation.REGION_CLEAR, null, false, getMyId(),generateEventID());
    basicClear(regionEvent);
  }

  abstract void basicLocalClear(RegionEventImpl rEvent);

  public void localClear()
  {
    checkReadiness();
    checkForNoAccess();
    RegionEventImpl event = new RegionEventImpl(this,
        Operation.REGION_LOCAL_CLEAR, null, false, getMyId(),generateEventID()/* generate EventID */);
    basicLocalClear(event);
  }

  @Override
  public Map getAll(Collection keys) {
    return getAll(keys, null);
  }

  @Override
  public Map getAll(Collection keys, Object callback) {
    if (keys == null) {
      throw new NullPointerException("The collection of keys for getAll cannot be null");
    }
    checkReadiness();
    checkForLimitedOrNoAccess();
    return keys.isEmpty()? new HashMap(): basicGetAll(keys, callback);
  }

  abstract Map basicGetAll(Collection keys, Object callback);

  public abstract RegionEntry basicGetEntry(Object key);

  protected StringBuilder getStringBuilder() {
    StringBuilder buf = new StringBuilder();
    buf.append(getClass().getName());
    buf.append("[path='")
       .append(getFullPath())
       .append("';scope=")
       .append(getScope())
       .append("';dataPolicy=")
       .append(this.dataPolicy);
    if (this.concurrencyChecksEnabled) {
      buf.append("; concurrencyChecksEnabled");
    }
    return buf;
  }

  @Override
  public String toString() {
    return getStringBuilder().append(']').toString();
  }

  /** ********************* RegionAttributes ********************************* */

  public CacheLoader getCacheLoader()
  {
    //checkReadiness();
    return this.cacheLoader;
  }

  public CacheWriter getCacheWriter()
  {
    //checkReadiness();
    return this.cacheWriter;
  }

  /**
   * Return a cache loader if this region has one.
   * Note if region's loader is used to implement bridge then null is returned.
   * @since 5.7
   */
  public CacheLoader basicGetLoader() {
    CacheLoader result = this.cacheLoader;
    return result;
  }
  /**
   * Return a cache writer if this region has one.
   * Note if region's writer is used to implement bridge then null is returned.
   * @since 5.7
   */
  public CacheWriter basicGetWriter() {
    CacheWriter result = this.cacheWriter;
    return result;
  }
  
  public Class getKeyConstraint()
  {
    //checkReadiness();
    return this.keyConstraint;
  }

  public Class getValueConstraint()
  {
    return this.valueConstraint;
  }

  private volatile ExpirationAttributes regionTimeToLiveAtts;
  
  private void setRegionTimeToLiveAtts() {
    this.regionTimeToLiveAtts = new ExpirationAttributes(this.regionTimeToLive, this.regionTimeToLiveExpirationAction);
  }

  public ExpirationAttributes getRegionTimeToLive()
  {
    return this.regionTimeToLiveAtts;
  }

  private volatile ExpirationAttributes regionIdleTimeoutAtts;
  
  private void setRegionIdleTimeoutAtts() {
    this.regionIdleTimeoutAtts = new ExpirationAttributes(this.regionIdleTimeout, this.regionIdleTimeoutExpirationAction);
  }

  public ExpirationAttributes getRegionIdleTimeout()
  {
    return this.regionIdleTimeoutAtts;
  }
  
  private volatile ExpirationAttributes entryTimeToLiveAtts;
  
  protected void setEntryTimeToLiveAtts() {
    this.entryTimeToLiveAtts = new ExpirationAttributes(this.entryTimeToLive, this.entryTimeToLiveExpirationAction);
  }

  public ExpirationAttributes getEntryTimeToLive()
  {
    return this.entryTimeToLiveAtts;
  }
  
  public CustomExpiry getCustomEntryTimeToLive() {
    return this.customEntryTimeToLive;
  }

  private volatile ExpirationAttributes entryIdleTimeoutAtts;
  
  protected void setEntryIdleTimeoutAtts() {
    this.entryIdleTimeoutAtts = new ExpirationAttributes(this.entryIdleTimeout, this.entryIdleTimeoutExpirationAction);
  }

  public ExpirationAttributes getEntryIdleTimeout()
  {
    return this.entryIdleTimeoutAtts;
  }
  
  public CustomExpiry getCustomEntryIdleTimeout() {
    return this.customEntryIdleTimeout;
  }

  public MirrorType getMirrorType() {
    if (this.dataPolicy.isNormal() || this.dataPolicy.isPreloaded()
        || this.dataPolicy.isEmpty() || this.dataPolicy.withPartitioning()) {
      return MirrorType.NONE;
    }
    else if (this.dataPolicy.withReplication()) {
      return MirrorType.KEYS_VALUES;
    }
    else {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0.toLocalizedString(this.dataPolicy));
    }
  }

  
  public String getPoolName()
  {
    //checkReadiness();
    return this.poolName;
  }
  
  public DataPolicy getDataPolicy()
  {
    //checkReadiness();
    return this.dataPolicy;
  }

  public Scope getScope()
  {
    //checkReadiness();
    return this.scope;
  }

  public CacheListener getCacheListener()
  {
    //checkReadiness();
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners == null || listeners.length == 0) {
      return null;
    }
    if (listeners.length == 1) {
      return listeners[0];
    }
    else {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_MORE_THAN_ONE_CACHE_LISTENER_EXISTS.toLocalizedString());
    }
  }

  public final boolean isPdxTypesRegion() {
    return this.isPdxTypesRegion;
  }
  
  public Set<String> getGatewaySenderIds() {
    return this.gatewaySenderIds;
  }

  public Set<String> getAsyncEventQueueIds() {
    return this.asyncEventQueueIds;
  }

  public final Set<String> getAllGatewaySenderIds() {
    return Collections.unmodifiableSet(this.allGatewaySenderIds);
  }

  public final boolean checkNotifyGatewaySender() {
    return (this.cache.getAllGatewaySenders().size() > 0
        && this.allGatewaySenderIds.size() > 0);
  }

  public final Set<String> getActiveGatewaySenderIds() {
    final Set<GatewaySender> allGatewaySenders;
    HashSet<String> activeGatewaySenderIds = null;
    final int sz = this.gatewaySenderIds.size();
    if (sz > 0
        && (allGatewaySenders = this.cache.getGatewaySenders()).size() > 0) {
      for (GatewaySender sender : allGatewaySenders) {
        if (sender.isRunning()
            && this.gatewaySenderIds.contains(sender.getId())) {
          if (activeGatewaySenderIds == null) {
            activeGatewaySenderIds = new HashSet<String>();
          }
          activeGatewaySenderIds.add(sender.getId());
        }
      }
    }
    return activeGatewaySenderIds;
  }

  public final Set<String> getActiveAsyncQueueIds() {
    final Set<AsyncEventQueue> allAsyncQueues;
    HashSet<String> activeAsyncQueueIds = null;
    final int sz = this.asyncEventQueueIds.size();
    if (sz > 0
        && (allAsyncQueues = this.cache.getAsyncEventQueues()).size() > 0) {
      for (AsyncEventQueue asyncQueue : allAsyncQueues) {
        //merge42004:In cheetah asyncEventQueue has isRunning Method. It has come from merging branches. A mail regarding the asyncEventQueue is sent to Barry to get more clarification. We need to sort this out.
        if (/*asyncQueue.isRunning()
            &&*/ this.asyncEventQueueIds.contains(asyncQueue.getId())) {
          if (activeAsyncQueueIds == null) {
            activeAsyncQueueIds = new HashSet<String>();
          }
          activeAsyncQueueIds.add(asyncQueue.getId());
        }
      }
    }
    return activeAsyncQueueIds;
  }

  /**
   * Return the remote DS IDs that need to receive events for this region.
   *
   * @param allGatewaySenderIds the set of gateway sender IDs to consider
   */
  public final List<Integer> getRemoteDsIds(Set<String> allGatewaySenderIds)
      throws IllegalStateException {
    final Set<GatewaySender> allGatewaySenders;
    final int sz = allGatewaySenderIds.size();
    if ((sz > 0 || isPdxTypesRegion)
        && (allGatewaySenders = this.cache.getAllGatewaySenders()).size() > 0) {
      List<Integer> allRemoteDSIds = new ArrayList<Integer>(sz);
      for (GatewaySender sender : allGatewaySenders) {
        // This is for all regions except pdx Region
        if (!isPdxTypesRegion) {
          // Make sure we are distributing to only those senders whose id
          // is avaialble on this region
          if (allGatewaySenderIds.contains(sender.getId())) {
            /*// ParalleGatewaySender with DR is not allowed
            if (this.partitionAttributes == null && sender.isParallel()) {
              throw new IllegalStateException(LocalizedStrings
                  .AttributesFactory_PARALLELGATEWAYSENDER_0_IS_INCOMPATIBLE_WITH_DISTRIBUTED_REPLICATION
                      .toLocalizedString(sender.getId()));
            }*/
            allRemoteDSIds.add(sender.getRemoteDSId());
          }
        }
        else { // this else is for PDX region
          allRemoteDSIds.add(sender.getRemoteDSId());
        }
      }
      return allRemoteDSIds;
    }
    return null;
  }

//  protected final void initAllGatewaySenderIds() {
//    HashSet<String> senderIds = new HashSet<String>();
//    this.allGatewaySenderIds = senderIds;
//    if (getGatewaySenderIds().isEmpty() && getAsyncEventQueueIds().isEmpty()) {
//      return Collections.emptySet(); // fix for bug 45774
//    }
//    Set<String> tmp = new CopyOnWriteArraySet<String>();
//    tmp.addAll(this.getGatewaySenderIds());
//    for(String asyncQueueId : this.getAsyncEventQueueIds()){
//      tmp.add(AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncQueueId));
//    }
//    return tmp;
//  }
  
  public boolean isGatewaySenderEnabled() {
    return this.isGatewaySenderEnabled;
  }
  
  private static final CacheListener[] EMPTY_LISTENERS = new CacheListener[0];

  public CacheListener[] getCacheListeners()
  {
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners == null || listeners.length == 0) {
      return EMPTY_LISTENERS;
    }
    else {
      CacheListener[] result = new CacheListener[listeners.length];
      System.arraycopy(listeners, 0, result, 0, listeners.length);
      return result;
    }
  }

  /**
   * Sets the cacheListeners field.
   */
  private final void storeCacheListenersField(CacheListener[] value)
  {
    synchronized (this.clSync) {
      if (value != null && value.length != 0) {
        CacheListener[] nv = new CacheListener[value.length];
        System.arraycopy(value, 0, nv, 0, nv.length);
        value = nv;
      }
      else {
        value = EMPTY_LISTENERS;
      }
      this.cacheListeners = value;
    }
  }

  /**
   * Fetches the value in the cacheListeners field. NOTE: callers should not
   * modify the contents of the returned array.
   */
  protected final CacheListener[] fetchCacheListenersField()
  {
    return this.cacheListeners;
  }

  public int getInitialCapacity()
  {
    //checkReadiness();
    return this.initialCapacity;
  }

  public float getLoadFactor()
  {
    //checkReadiness();
    return this.loadFactor;
  }

  protected abstract boolean isCurrentlyLockGrantor();

  public boolean isLockGrantor()
  {
    //checkReadiness();
    return this.isLockGrantor;
  }

  /**
   * RegionAttributes implementation. Returns true if multicast can be used by
   * the cache for this region
   */
  public boolean getMulticastEnabled()
  {
    //checkReadiness();
    return this.mcastEnabled;
  }

  public boolean getStatisticsEnabled()
  {
    //checkReadiness();
    return this.statisticsEnabled;
  }

  public boolean getIgnoreJTA()
  {
    //checkRediness();
    return this.ignoreJTA;
  }

  public int getConcurrencyLevel()
  {
    //checkReadiness();
    return this.concurrencyLevel;
  }
  
  public boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  public boolean getPersistBackup()
  {
    //checkReadiness();
    return getDataPolicy().withPersistence();
  }

  public boolean getEarlyAck()
  {
    //checkReadiness();
    return this.earlyAck;
  }

  /*
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  public boolean getPublisher()
  {
    return this.publisher;
  }

  public boolean getEnableConflation() { // deprecated in 5.0
    return getEnableSubscriptionConflation();
  }

  public boolean getEnableBridgeConflation() {// deprecated in 5.7
    return getEnableSubscriptionConflation();
  }
  public boolean getEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  public boolean getEnableAsyncConflation()
  {
    return this.enableAsyncConflation;
  }

  /*
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public DiskWriteAttributes getDiskWriteAttributes()
  {
    //checkReadiness();
    return this.diskWriteAttributes;
  }

  public abstract File[] getDiskDirs();

  public final String getDiskStoreName() {
    return this.diskStoreName;
  }
  
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  public boolean getIndexMaintenanceSynchronous()
  {
    return this.indexMaintenanceSynchronous;
  }

  public PartitionAttributes getPartitionAttributes()
  {
    return this.partitionAttributes;
  }

  public MembershipAttributes getMembershipAttributes()
  {
    return this.membershipAttributes;
  }

  public SubscriptionAttributes getSubscriptionAttributes()
  {
    return this.subscriptionAttributes;
  }
  
  @Override
  public final String getHDFSStoreName() {
    return this.hdfsStoreName;
  }
  
  @Override
  public final boolean getHDFSWriteOnly() {
    return this.hdfsWriteOnly;
  }
  
  /**
   * Get IndexManger for region
   */
  public IndexManager getIndexManager()
  {
    return this.indexManager;
  }

  /**
   * This method call is guarded by imSync lock created for each region.
   * Set IndexManger for region.
   */
  public IndexManager setIndexManager(IndexManager indexManager)
  {
    checkReadiness();
    IndexManager oldIdxManager = this.indexManager;
    this.indexManager = indexManager;
    return oldIdxManager;
  }

  /**
   * Use ONLY imSync for IndexManager get and set.
   * @return {@link IndexManager} lock.
   */
  public Object getIMSync() {
    return imSync;
  }

  //Asif : The ThreadLocal is used to identify if the thread is an
  //index creation thread. This is used to fix Bug # 33336. The value
  // is set from IndexManager ,if the thread happens to be an IndexCreation
  // Thread.
  // Once the thread has created the Index , it will unset the value in the
  // ThreadLocal Object
  public void setFlagForIndexCreationThread(boolean bool)
  {
    this.isIndexCreator.set(bool ? Boolean.TRUE : null);
  }

  //Asif : The boolean is used in AbstractRegionEntry to skip the synchronized
  // block
  // in case the value of the entry is "REMOVED" token. This prevents dead lock
  // caused by the Bug # 33336
  boolean isIndexCreationThread()
  {
    Boolean bool = (Boolean)this.isIndexCreator.get();
    return (bool != null) ? bool.booleanValue() : false;
  }

  /** ********************* AttributesMutator ******************************** */

  public Region getRegion()
  {
    return this;
  }

  //   /**
  //    * A CacheListener implementation that delegates to an array of listeners.
  //    */
  //   public static class ArrayCacheListener implements CacheListener {
  //     private final CacheListener [] listeners;
  //     /**
  //      * Creates a cache listener given the list of listeners it will delegate to.
  //      */
  //     public ArrayCacheListener(CacheListener[] listeners) {
  //       this.listeners = listeners;
  //     }
  //   }
  public CacheListener setCacheListener(CacheListener aListener)
  {
    checkReadiness();
    CacheListener result = null;
    CacheListener[] oldListeners = null;
    synchronized (this.clSync) {
      oldListeners = this.cacheListeners;
      if (oldListeners != null && oldListeners.length > 1) {
        throw new IllegalStateException(LocalizedStrings.AbstractRegion_MORE_THAN_ONE_CACHE_LISTENER_EXISTS.toLocalizedString());
      }
      this.cacheListeners = new CacheListener[] { aListener };
    }
    // moved the following out of the sync for bug 34512
    if (oldListeners != null && oldListeners.length > 0) {
      if (oldListeners.length == 1) {
        result = oldListeners[0];
      }
      for (int i = 0; i < oldListeners.length; i++) {
        if (aListener != oldListeners[i]) {
          closeCacheCallback(oldListeners[i]);
        }
      }
      if (aListener == null) {
        cacheListenersChanged(false);
      }
    }
    else { // we have no old listeners
      if (aListener != null) {
        // we have added a new listener
        cacheListenersChanged(true);
      }
    }
    return result;
  }

  public void addGatewaySenderId(String gatewaySenderId) {
    getGatewaySenderIds().add(gatewaySenderId);
    setAllGatewaySenderIds();
  }
  
  public void removeGatewaySenderId(String gatewaySenderId){
    getGatewaySenderIds().remove(gatewaySenderId);
    setAllGatewaySenderIds();
  }
  
  public void addAsyncEventQueueId(String asyncEventQueueId) {
    getAsyncEventQueueIds().add(asyncEventQueueId);
    setAllGatewaySenderIds();
  }
  
  public void removeAsyncEventQueueId(String asyncEventQueueId) {
    getAsyncEventQueueIds().remove(asyncEventQueueId);
    setAllGatewaySenderIds();
  }
  
  private void setAllGatewaySenderIds() {
    if (getGatewaySenderIds().isEmpty() && getAsyncEventQueueIds().isEmpty()) {
      allGatewaySenderIds = Collections.emptySet(); // fix for bug 45774
    }
    Set<String> tmp = new CopyOnWriteArraySet<String>();
    tmp.addAll(this.getGatewaySenderIds());
    for (String asyncQueueId : this.getAsyncEventQueueIds()) {
      tmp.add(AsyncEventQueueImpl
          .getSenderIdFromAsyncEventQueueId(asyncQueueId));
    }
    allGatewaySenderIds = tmp;
  }

  public void addCacheListener(CacheListener cl)
  {
    checkReadiness();
    if (cl == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_ADDCACHELISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    CacheListener wcl = wrapRegionMembershipListener(cl);
    boolean changed = false;
    synchronized (this.clSync) {
      CacheListener[] oldListeners = this.cacheListeners;
      if (oldListeners == null || oldListeners.length == 0) {
        this.cacheListeners = new CacheListener[] { wcl };
        changed = true;
      }
      else {
        List l = Arrays.asList(oldListeners);
        if (!l.contains(cl)) {
          this.cacheListeners = (CacheListener[])
              ArrayUtils.insert(oldListeners, oldListeners.length, wcl);
        }
      }
    }
    if (changed) {
      // moved the following out of the sync for bug 34512
      cacheListenersChanged(true);
    }
  }
  
  /**
   * We wrap RegionMembershipListeners in a container when adding them at
   * runtime, so that we can properly initialize their membership set prior
   * to delivering events to them.
   * @param cl a cache listener to be added to the region
   */
  private CacheListener wrapRegionMembershipListener(CacheListener cl) {
    if (cl instanceof RegionMembershipListener) {
      return new WrappedRegionMembershipListener((RegionMembershipListener)cl);
    }
    return cl;
  }
  
  /**
   * Initialize any wrapped RegionMembershipListeners in the cache listener list
   */
  void initPostCreateRegionMembershipListeners(Set initialMembers) {
    DistributedMember[] initMbrs = null;
    CacheListener[] newcl = null;
    synchronized(clSync) { 
      for (int i = 0; i < cacheListeners.length; i++) {
        CacheListener cl = cacheListeners[i];
        if (cl instanceof WrappedRegionMembershipListener) {
          WrappedRegionMembershipListener wrml = (WrappedRegionMembershipListener)cl;
          if (!wrml.isInitialized()) {
            if (initMbrs == null) {
              initMbrs = (DistributedMember[])initialMembers
                          .toArray(new DistributedMember[initialMembers.size()]);
            }
            wrml.initialMembers(this, initMbrs);
            if (newcl == null) {
              newcl = new CacheListener[cacheListeners.length];
              System.arraycopy(cacheListeners, 0, newcl, 0, newcl.length);
            }
            newcl[i] = wrml.getWrappedListener();
          }
        }
      }
      if (newcl != null) {
        cacheListeners = newcl;
      }
    }
  }
  

  public void initCacheListeners(CacheListener[] addedListeners)
  {
    checkReadiness();
    CacheListener[] oldListeners = null;
    CacheListener[] listenersToAdd = null;
    if (addedListeners != null) {
      listenersToAdd = new CacheListener[addedListeners.length];
      for (int i=0; i<addedListeners.length; i++) {
        listenersToAdd[i] = wrapRegionMembershipListener(addedListeners[i]);
      }
    }
    synchronized (this.clSync) {
      oldListeners = this.cacheListeners;
      if (listenersToAdd == null || listenersToAdd.length == 0) {
        this.cacheListeners = EMPTY_LISTENERS;
      }
      else { // we have some listeners to add
        if (Arrays.asList(listenersToAdd).contains(null)) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_INITCACHELISTENERS_PARAMETER_HAD_A_NULL_ELEMENT.toLocalizedString());
        }
        CacheListener[] newListeners = new CacheListener[listenersToAdd.length];
        System.arraycopy(listenersToAdd, 0, newListeners, 0,
            newListeners.length);
        this.cacheListeners = newListeners;
      }
    }
    // moved the following out of the sync for bug 34512
    if (listenersToAdd == null || listenersToAdd.length == 0) {
      if (oldListeners != null && oldListeners.length > 0) {
        for (int i = 0; i < oldListeners.length; i++) {
          closeCacheCallback(oldListeners[i]);
        }
        cacheListenersChanged(false);
      }
    }
    else { // we had some listeners to add
      if (oldListeners != null && oldListeners.length > 0) {
        for (int i = 0; i < oldListeners.length; i++) {
          closeCacheCallback(oldListeners[i]);
        }
      }
      else {
        cacheListenersChanged(true);
      }
    }
  }

  public void removeCacheListener(CacheListener cl)
  {
    checkReadiness();
    if (cl == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_REMOVECACHELISTENER_PARAMETER_WAS_NULL.toLocalizedString());
    }
    boolean changed = false;
    synchronized (this.clSync) {
      CacheListener[] oldListeners = this.cacheListeners;
      if (oldListeners != null && oldListeners.length > 0) {
        List l = new ArrayList(Arrays.asList(oldListeners));
        if (l.remove(cl)) {
          if (l.isEmpty()) {
            this.cacheListeners = EMPTY_LISTENERS;
          }
          else {
            CacheListener[] newListeners = new CacheListener[l.size()];
            l.toArray(newListeners);
            this.cacheListeners = newListeners;
          }
          closeCacheCallback(cl);
          if (l.isEmpty()) {
            changed = true;
          }
        }
      }
    }
    if (changed) {
      cacheListenersChanged(false);
    }
  }

  // synchronized so not reentrant
  public synchronized CacheLoader setCacheLoader(CacheLoader cl) {
    checkReadiness();
    CacheLoader oldLoader = this.cacheLoader;
    assignCacheLoader(cl);
    cacheLoaderChanged(oldLoader);
    return oldLoader;
  }

  private synchronized void assignCacheLoader(CacheLoader cl) {
    this.cacheLoader = cl;
  }

  // synchronized so not reentrant
  public synchronized CacheWriter setCacheWriter(CacheWriter cacheWriter)
  {
    checkReadiness();
    CacheWriter oldWriter = this.cacheWriter;
    assignCacheWriter(cacheWriter);
    cacheWriterChanged(oldWriter);
    return oldWriter;
  }

  private synchronized void assignCacheWriter(CacheWriter cacheWriter)
  {
    this.cacheWriter = cacheWriter;
  }

  void checkEntryTimeoutAction(String mode, ExpirationAction ea) {
    if ((this.dataPolicy.withReplication()
         || this.dataPolicy.withPartitioning())
        && (ea == ExpirationAction.LOCAL_DESTROY
            || ea == ExpirationAction.LOCAL_INVALIDATE)) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_0_ACTION_IS_INCOMPATIBLE_WITH_THIS_REGIONS_DATA_POLICY.toLocalizedString(mode));
    }
  }
  
  public ExpirationAttributes setEntryIdleTimeout(
      ExpirationAttributes idleTimeout) {
    checkReadiness();
    if (idleTimeout == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_IDLETIMEOUT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    checkEntryTimeoutAction("idleTimeout", idleTimeout.getAction());
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_CANNOT_SET_IDLE_TIMEOUT_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
    }

    ExpirationAttributes oldAttrs = getEntryIdleTimeout();
    this.entryIdleTimeout = idleTimeout.getTimeout();
    this.entryIdleTimeoutExpirationAction = idleTimeout.getAction();
    setEntryIdleTimeoutAtts();
    updateEntryExpiryPossible();
    idleTimeoutChanged(oldAttrs);
    return oldAttrs;
  }
  
  public CustomExpiry setCustomEntryIdleTimeout(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !this.statisticsEnabled) {
      throw new IllegalStateException(
        LocalizedStrings.AbstractRegion_CANNOT_SET_IDLE_TIMEOUT_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
    }

    CustomExpiry old = getCustomEntryIdleTimeout();
    this.customEntryIdleTimeout = custom;
    updateEntryExpiryPossible();
    idleTimeoutChanged(getEntryIdleTimeout());
    return old;
  }

  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive)
  {
    checkReadiness();
    if (timeToLive == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_TIMETOLIVE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    checkEntryTimeoutAction("timeToLive", timeToLive.getAction());
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_CANNOT_SET_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
    }
    ExpirationAttributes oldAttrs = getEntryTimeToLive();
    this.entryTimeToLive = timeToLive.getTimeout();
    this.entryTimeToLiveExpirationAction = timeToLive.getAction();
    setEntryTimeToLiveAtts();
    updateEntryExpiryPossible();
    timeToLiveChanged(oldAttrs);
    return oldAttrs;
  }
  
  public CustomExpiry setCustomEntryTimeToLive(CustomExpiry custom) {
    checkReadiness();
    if (custom != null && !this.statisticsEnabled) {
      throw new IllegalStateException(
        LocalizedStrings.AbstractRegion_CANNOT_SET_CUSTOM_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
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
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_INVALIDATE_REGION_NOT_SUPPORTED_FOR_PR.toLocalizedString());
      } else if (expAction.isDestroy() || expAction.isLocalDestroy()) {
        throw new IllegalStateException(LocalizedStrings.AttributesFactory_DESTROY_REGION_NOT_SUPPORTED_FOR_PR.toLocalizedString());
      }
    }
  }

  public ExpirationAttributes setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    checkReadiness();
    if (idleTimeout == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_IDLETIMEOUT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    if (this.getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(idleTimeout);
    }
    if (idleTimeout.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && this.dataPolicy.withReplication()) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_0_ACTION_IS_INCOMPATIBLE_WITH_THIS_REGIONS_DATA_POLICY.toLocalizedString("idleTimeout"));
    }
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_CANNOT_SET_IDLE_TIMEOUT_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
    }
    ExpirationAttributes oldAttrs = getRegionIdleTimeout();
    this.regionIdleTimeout = idleTimeout.getTimeout();
    this.regionIdleTimeoutExpirationAction = idleTimeout.getAction();
    this.setRegionIdleTimeoutAtts();
    regionIdleTimeoutChanged(oldAttrs);
    return oldAttrs;
  }

  public ExpirationAttributes setRegionTimeToLive(ExpirationAttributes timeToLive) {
    checkReadiness();
    if (timeToLive == null) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_TIMETOLIVE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    if (this.getAttributes().getDataPolicy().withPartitioning()) {
      validatePRRegionExpirationAttributes(timeToLive);
    }
    if (timeToLive.getAction() == ExpirationAction.LOCAL_INVALIDATE
        && this.dataPolicy.withReplication()) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractRegion_0_ACTION_IS_INCOMPATIBLE_WITH_THIS_REGIONS_DATA_POLICY.toLocalizedString("timeToLive"));
    }
    if (!this.statisticsEnabled) {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_CANNOT_SET_TIME_TO_LIVE_WHEN_STATISTICS_ARE_DISABLED.toLocalizedString());
    }

    ExpirationAttributes oldAttrs = getRegionTimeToLive();
    this.regionTimeToLive = timeToLive.getTimeout();
    this.regionTimeToLiveExpirationAction = timeToLive.getAction();
    this.setRegionTimeToLiveAtts();
    regionTimeToLiveChanged(timeToLive);
    return oldAttrs;
  }

  public void becomeLockGrantor()
  {
    checkReadiness();
    checkForLimitedOrNoAccess();
    if (this.scope != Scope.GLOBAL) {
      throw new IllegalStateException(LocalizedStrings.AbstractRegion_CANNOT_SET_LOCK_GRANTOR_WHEN_SCOPE_IS_NOT_GLOBAL.toLocalizedString());
    }
    if (isCurrentlyLockGrantor())
      return; // nothing to do... already lock grantor
    this.isLockGrantor = true;
  }

  /** ********************* CacheStatistics ******************************** */

  public CacheStatistics getStatistics()
  {
    // prefer region destroyed exception over statistics disabled exception
    checkReadiness();
    if (!this.statisticsEnabled) {
      throw new StatisticsDisabledException(LocalizedStrings.AbstractRegion_STATISTICS_DISABLED_FOR_REGION_0.toLocalizedString(getFullPath()));
    }
    return this;
  }

  /**
   * The logical lastModifiedTime of a region is the most recent
   * lastModifiedTime of the region and all its subregions. This implementation
   * trades performance of stat retrieval for performance of get/put, which is
   * more critical.
   */
  public synchronized long getLastModifiedTime()
  {
    checkReadiness();
    long mostRecent = basicGetLastModifiedTime();
    // don't need to wait on getInitialImage for this operation in subregions
    int oldLevel = LocalRegion
        .setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
      Iterator subIt = subregions(false).iterator();
      while (subIt.hasNext()) {
        try {
          LocalRegion r = (LocalRegion)subIt.next();
          if (r.isInitialized()) {
            mostRecent = Math.max(mostRecent, r.getLastModifiedTime());
          }
        }
        catch (RegionDestroyedException e) {
          // pass over destroyed region
        }
      }
    }
    finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return mostRecent;
  }

  protected long basicGetLastModifiedTime()
  {
    return this.lastModifiedTime.get();
  }

  protected long basicGetLastAccessedTime()
  {
    return this.lastAccessedTime.get();
  }

  protected void basicSetLastModifiedTime(long t)
  {
    this.lastModifiedTime.set(t);
  }

  protected void basicSetLastAccessedTime(long t)
  {
    this.lastAccessedTime.set(t);
  }

  /**
   * The logical lastAccessedTime of a region is the most recent
   * lastAccessedTime of the region and all its subregions. This implementation
   * trades performance of stat retrieval for performance of get/put, which is
   * more critical.
   */
  public synchronized long getLastAccessedTime()
  {
    checkReadiness();
    long mostRecent = basicGetLastAccessedTime();
    // don't need to wait on getInitialImage for this operation in subregions
    int oldLevel = LocalRegion
        .setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
      Iterator subIt = subregions(false).iterator();
      while (subIt.hasNext()) {
        try {
          LocalRegion r = (LocalRegion)subIt.next();
          if (r.isInitialized()) {
            mostRecent = Math.max(mostRecent, r.getLastAccessedTime());
          }
        }
        catch (RegionDestroyedException e) {
          // pass over destroyed region
        }
      }
    }
    finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
    return mostRecent;
  }

  /**
   * Update the lastAccessedTime and lastModifiedTimes to reflects those in the
   * subregions
   */
  protected synchronized void updateStats()
  {
    long mostRecentAccessed = basicGetLastAccessedTime();
    long mostRecentModified = basicGetLastModifiedTime();
    // don't need to wait on getInitialImage for this operation in subregions
    int oldLevel = LocalRegion
        .setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
    try {
      Iterator subIt = subregions(false).iterator();
      while (subIt.hasNext()) {
        try {
          LocalRegion r = (LocalRegion)subIt.next();
          if (r.isInitialized()) {
            mostRecentAccessed = Math.max(mostRecentAccessed, r
                .getLastAccessedTime());
            mostRecentModified = Math.max(mostRecentModified, r
                .getLastModifiedTime());
          }
        }
        catch (RegionDestroyedException e) {
          // pass over destroyed region
        }
      }
      basicSetLastAccessedTime(Math.max(mostRecentAccessed, mostRecentModified));
      basicSetLastModifiedTime(mostRecentModified);
    }
    finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
    }
  }

  protected void setLastModifiedTime(long time)
  {
    //checkReadiness();
    if (time > this.lastModifiedTime.get()) {
      this.lastModifiedTime.set(time);
    }
    if (time > this.lastAccessedTime.get()) {
      this.lastAccessedTime.set(time);
    }
  }

  protected void setLastAccessedTime(long time, boolean hit)
  {
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

  public final float getHitRatio()
  {
    //checkReadiness();
    long hits = getHitCount();
    long total = hits + getMissCount();
    return total == 0L ? 0.0f : ((float)hits / total);
  }

  public long getHitCount()
  {
    //checkReadiness();
    return this.hitCount.get();
  }

  public long getMissCount()
  {
    //checkReadiness();
    return this.missCount.get();
  }

  public void resetCounts()
  {
    //checkReadiness();
    if (trackMisses) {
      this.missCount.set(0);
    }
    if (trackHits) {
      this.hitCount.set(0);
    }
  }

  /** ****************** Protected Methods *********************************** */

  protected void closeCacheCallback(CacheCallback cb)
  {
    if (cb != null) {
      try {
        cb.close();
      }
      catch (RuntimeException ex) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.AbstractRegion_CACHECALLBACK_CLOSE_EXCEPTION), ex);
      }
    }
  }

  protected void cacheLoaderChanged(CacheLoader oldLoader)
  {
    if (this.cacheLoader != oldLoader) {
      closeCacheCallback(oldLoader);
    }
  }

  /**
   * Called if when we go from no listeners to at least one or from at least one
   * to no listeners
   *
   * @param nowHasListener
   *          true if we now have at least one listener; false if we now have no
   *          listeners
   */
  protected void cacheListenersChanged(boolean nowHasListener)
  {
    // nothing needed by default
  }

  protected void cacheWriterChanged(CacheWriter oldWriter)
  {
    if (this.cacheWriter != oldWriter) {
      closeCacheCallback(oldWriter);
    }
  }

  protected void timeToLiveChanged(ExpirationAttributes oldTimeToLive)
  {
  }

  protected void idleTimeoutChanged(ExpirationAttributes oldIdleTimeout)
  {
  }

  protected void regionTimeToLiveChanged(ExpirationAttributes oldTimeToLive)
  {
  }

  protected void regionIdleTimeoutChanged(ExpirationAttributes oldIdleTimeout)
  {
  };

  /** Throws CacheClosedException or RegionDestroyedException */
  abstract void checkReadiness();

  /**
   * Returns true if this region has no storage
   *
   * @since 5.0
   */
  protected final boolean isProxy()
  {
    return getDataPolicy().isEmpty();
  }

  /**
   * Returns true if this region has no storage and is only interested in what
   * it contains (which is nothing)
   *
   * @since 5.0
   */
  protected final boolean isCacheContentProxy()
  {
    // method added to fix bug 35195
    return isProxy()
        && getSubscriptionAttributes().getInterestPolicy().isCacheContent();
  }

  /**
   * Returns true if region subscribes to all events or is a replicate.
   *
   * @since 5.0
   */
  final boolean isAllEvents()
  {
    return getDataPolicy().withReplication()
        || getSubscriptionAttributes().getInterestPolicy().isAll();
  }

  private boolean entryExpiryPossible = false;

  protected void updateEntryExpiryPossible()
  {
    this.entryExpiryPossible = !isProxy()
        && (this.entryTimeToLive > 0 
            || this.entryIdleTimeout > 0 
            || this.customEntryIdleTimeout != null
            || this.customEntryTimeToLive != null
            );
  }

  /**
   * Returns true if this region could expire an entry
   */
  protected boolean isEntryExpiryPossible()
  {
    return this.entryExpiryPossible;
  }
  
  public ExpirationAction getEntryExpirationAction() {
    if(this.entryIdleTimeoutExpirationAction != null) {
      return entryIdleTimeoutExpirationAction;
    }
    if(this.entryTimeToLiveExpirationAction != null) {
      return entryTimeToLiveExpirationAction;
    }
    return null;
  }
  
  /**
   * Returns true if this region can evict entries.
   */
  public boolean isEntryEvictionPossible() {
    return this.evictionController != null;
  }

  /** ****************** Private Methods ************************************* */
  private void setAttributes(RegionAttributes attrs,String regionName, InternalRegionArguments internalRegionArgs)
  {
    this.dataPolicy = attrs.getDataPolicy(); // do this one first
    this.scope = attrs.getScope();
    
    this.offHeap = attrs.getOffHeap();

    // fix bug #52033 by invoking setOffHeap now (localMaxMemory may now be the temporary placeholder for off-heap until DistributedSystem is created
    // found non-null PartitionAttributes and offHeap is true so let's setOffHeap on PA now
    PartitionAttributes<?, ?> pa = attrs.getPartitionAttributes();
    if (this.offHeap && pa != null) {
      PartitionAttributesImpl impl = (PartitionAttributesImpl)pa;
      impl.setOffHeap(this.offHeap);
    }

    this.evictionAttributes = new EvictionAttributesImpl((EvictionAttributesImpl)attrs
        .getEvictionAttributes());
    if (attrs.getPartitionAttributes() != null
        && this.evictionAttributes != null
        && this.evictionAttributes.getAlgorithm().isLRUMemory()
        && attrs.getPartitionAttributes().getLocalMaxMemory() != 0
        && this.evictionAttributes.getMaximum() != attrs
            .getPartitionAttributes().getLocalMaxMemory()) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.Mem_LRU_Eviction_Attribute_Reset,
          new Object[] { regionName,this.evictionAttributes.getMaximum(),
              attrs.getPartitionAttributes().getLocalMaxMemory() }));
      this.evictionAttributes.setMaximum(attrs.getPartitionAttributes()
          .getLocalMaxMemory());
    }
    //final boolean isNotPartitionedRegion = !(attrs.getPartitionAttributes() != null || attrs
    //            .getDataPolicy().withPartitioning());
    
    //if (isNotPartitionedRegion && this.evictionAttributes != null
    if (this.evictionAttributes != null
        && !this.evictionAttributes.getAlgorithm().isNone()) {
      this.setEvictionController(this.evictionAttributes
          .createEvictionController(this, attrs.getOffHeap()));
    }
    this.customEvictionAttributes = attrs.getCustomEvictionAttributes();
    storeCacheListenersField(attrs.getCacheListeners());
    assignCacheLoader(attrs.getCacheLoader());
    assignCacheWriter(attrs.getCacheWriter());
    this.regionTimeToLive = attrs.getRegionTimeToLive().getTimeout();
    this.regionTimeToLiveExpirationAction = attrs.getRegionTimeToLive()
        .getAction();
    setRegionTimeToLiveAtts();
    this.regionIdleTimeout = attrs.getRegionIdleTimeout().getTimeout();
    this.regionIdleTimeoutExpirationAction = attrs.getRegionIdleTimeout()
        .getAction();
    setRegionIdleTimeoutAtts();
    this.entryTimeToLive = attrs.getEntryTimeToLive().getTimeout();
    this.entryTimeToLiveExpirationAction = attrs.getEntryTimeToLive()
        .getAction();
    setEntryTimeToLiveAtts();
    this.customEntryTimeToLive = attrs.getCustomEntryTimeToLive();
    this.entryIdleTimeout = attrs.getEntryIdleTimeout().getTimeout();
    this.entryIdleTimeoutExpirationAction = attrs.getEntryIdleTimeout()
        .getAction();
    setEntryIdleTimeoutAtts();
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
    this.concurrencyChecksEnabled =  attrs.getConcurrencyChecksEnabled() && supportsConcurrencyChecks();
    this.earlyAck = attrs.getEarlyAck();
    this.gatewaySenderIds = attrs.getGatewaySenderIds();
    this.asyncEventQueueIds = attrs.getAsyncEventQueueIds();
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
        throw new IllegalStateException(LocalizedStrings.
            AbstractRegion_THE_CONNECTION_POOL_0_HAS_NOT_BEEN_CREATED.toLocalizedString(this.poolName));
      }
      cp.attach();
      if (cp.getMultiuserAuthentication() && !this.dataPolicy.isEmpty()) {
        throw new IllegalStateException("Region must have empty data-policy "
            + "when multiuser-authentication is true.");
      }
    }
    this.hdfsStoreName = attrs.getHDFSStoreName();
    this.hdfsWriteOnly = attrs.getHDFSWriteOnly();

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
    if(!attrs.getConcurrencyChecksEnabled() 
        && attrs.getDataPolicy().withPersistence()
        && supportsConcurrencyChecks()) {
      throw new IllegalStateException(LocalizedStrings.AttributesFactory_CONCURRENCY_CHECKS_MUST_BE_ENABLED.toLocalizedString());
    }
  }
  
  /** is this a region that supports versioning? */
  public abstract boolean supportsConcurrencyChecks();

  /**
   * Returns the pool this region is using or null if it does not have one
   * or the pool does not exist.
   * @since 5.7
   */
  private PoolImpl getPool() {
    PoolImpl result = null;
    if (getPoolName() != null) {
      result = (PoolImpl)PoolManager.find(getPoolName());
    }
    return result;
  }

  public boolean existsValue(String predicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException
  {
    return !query(predicate).isEmpty();
  }

  public Object selectValue(String predicate) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException
  {
    SelectResults result = query(predicate);
    if (result.isEmpty()) {
      return null;
    }
    if (result.size() > 1)
      throw new FunctionDomainException(LocalizedStrings.AbstractRegion_SELECTVALUE_EXPECTS_RESULTS_OF_SIZE_1_BUT_FOUND_RESULTS_OF_SIZE_0.toLocalizedString(Integer.valueOf(result.size())));
    return result.iterator().next();
  }

  public Set entrySet(boolean recursive)
  {
    return entries(recursive);
  }

  public EvictionAttributes getEvictionAttributes()
  {
    return this.evictionAttributes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CustomEvictionAttributes getCustomEvictionAttributes() {
    return this.customEvictionAttributes;
  }

  public EvictionAttributesMutator getEvictionAttributesMutator()
  {
    return this.evictionAttributes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CustomEvictionAttributes setCustomEvictionAttributes(long newStart,
      long newInterval) {
    checkReadiness();

    if (this.customEvictionAttributes == null) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractRegion_NO_CUSTOM_EVICTION_SET
              .toLocalizedString(getFullPath()));
    }

    if (newStart == 0) {
      newStart = this.customEvictionAttributes.getEvictorStartTime();
    }
    this.customEvictionAttributes = new CustomEvictionAttributesImpl(
        this.customEvictionAttributes.getCriteria(), newStart, newInterval,
        newStart == 0 && newInterval == 0);

//    if (this.evService == null) {
//      initilializeCustomEvictor();
//    } else {// we are changing the earlier one which is already started.
//      EvictorService service = getEvictorTask();
//      service.changeEvictionInterval(newInterval);
//      if (newStart != 0)
//        service.changeStartTime(newStart);
//    }

    return this.customEvictionAttributes;
  }
  
  public void setEvictionController(LRUAlgorithm evictionController)
  {
    this.evictionController = evictionController;
  }

  public LRUAlgorithm getEvictionController()
  {
    return evictionController;
  }

  /**
   * Throws RegionAccessException if required roles are missing and the
   * LossAction is NO_ACCESS
   *
   * @throws RegionAccessException
   *           if required roles are missing and the LossAction is NO_ACCESS
   */
  protected void checkForNoAccess()
  {
  }

  /**
   * Throws RegionAccessException is required roles are missing and the
   * LossAction is either NO_ACCESS or LIMITED_ACCESS.
   *
   * @throws RegionAccessException
   *           if required roles are missing and the LossAction is either
   *           NO_ACCESS or LIMITED_ACCESS
   */
  protected void checkForLimitedOrNoAccess()
  {
  }

  /**
   * Makes sure that the data was distributed to every required role. If it was
   * not it either queues the data for later delivery or it throws an exception.
   *
   * @param data
   *          the data that needs to be reliably distributed
   * @param successfulRecipients
   *          the successful recipients
   * @throws RoleException
   *           if a required role was not sent the message and the LossAction is
   *           either NO_ACCESS or LIMITED_ACCESS.
   * @since 5.0
   *
   */
  protected void handleReliableDistribution(ReliableDistributionData data,
      Set successfulRecipients)
  {
    // do nothing by default
  }

  /** Returns true if region requires a reliability check. */
  public boolean requiresReliabilityCheck()
  {
    return false;
  }


  /**
   * Returns the serial number which identifies the static order in which this
   * region was created in relation to other regions or other instances of
   * this region during the life of this JVM.
   */
  public int getSerialNumber() {
    return this.serialNumber;
  }

  public final GemFireCacheImpl getCache() {
    return this.cache;
  }

  public final long cacheTimeMillis() {
    return this.cache.getDistributedSystem().getClock().cacheTimeMillis();
  }

  public final RegionService getRegionService() {
    return this.cache;
  }
  
  public final DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  public final InternalDistributedSystem getSystem() {
    return getCache().getDistributedSystem();
  }
  
  // DataSerializableFixedID support
  public final int getDSFID() {
    return REGION;
  }

  // DataSerializableFixedID support
  public final void toData(DataOutput out) throws IOException {
    DataSerializer.writeRegion(this, out); 
  }

  // DataSerializableFixedID support
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // should never be called since the special DataSerializer.readRegion is used.
    throw new UnsupportedOperationException();
  }

  public boolean forceCompaction() {
    throw new UnsupportedOperationException();
  }

  public boolean getCloningEnabled() {
    return this.cloningEnable;
  }
  
  public void setCloningEnabled(boolean cloningEnable){
    this.cloningEnable = cloningEnable;
  }

  protected static Object handleNotAvailable(Object v) {
    if (v == Token.NOT_AVAILABLE) {
      v = null;
    }
    return v;
  }
  
  public GemFireCacheImpl getGemFireCache() {
    return this.cache;
  }
  
  public RegionSnapshotService<?, ?> getSnapshotService() {
    return new RegionSnapshotServiceImpl(this);
  }
  
  public Compressor getCompressor() {
    return this.compressor;
  }
  
  /**
  * @since 8.1
  * property used to find region operations that reach out to HDFS multiple times
  */
  @Override
  public ExtensionPoint<Region<?, ?>> getExtensionPoint() {
    return extensionPoint;
  }

  public boolean getOffHeap() {
    return this.offHeap;
  }
  /**
   * property used to find region operations that reach out to HDFS multiple times
   */
  private static final boolean DEBUG_HDFS_CALLS = Boolean.getBoolean("DebugHDFSCalls");

  /**
   * throws exception if region operation goes out to HDFS multiple times
   */
  private static final boolean THROW_ON_MULTIPLE_HDFS_CALLS = Boolean.getBoolean("throwOnMultipleHDFSCalls");

  private ThreadLocal<CallLog> logHDFSCalls = DEBUG_HDFS_CALLS ? new ThreadLocal<CallLog>() : null;

  public void hdfsCalled(Object key) {
    if (!DEBUG_HDFS_CALLS) {
      return;
    }
    logHDFSCalls.get().addStack(new Throwable());
    logHDFSCalls.get().setKey(key);
  }
  public final void operationStart() {
    if (!DEBUG_HDFS_CALLS) {
      return;
    }
    if (logHDFSCalls.get() == null) {
      logHDFSCalls.set(new CallLog());
      //InternalDistributedSystem.getLoggerI18n().warning(LocalizedStrings.DEBUG, "SWAP:operationStart", new Throwable());
    } else {
      logHDFSCalls.get().incNestedCall();
      //InternalDistributedSystem.getLoggerI18n().warning(LocalizedStrings.DEBUG, "SWAP:incNestedCall:", new Throwable());
    }
  }
  public final void operationCompleted() {
    if (!DEBUG_HDFS_CALLS) {
      return;
    }
    //InternalDistributedSystem.getLoggerI18n().warning(LocalizedStrings.DEBUG, "SWAP:operationCompleted", new Throwable());
    if (logHDFSCalls.get() != null && logHDFSCalls.get().decNestedCall() < 0) {
      logHDFSCalls.get().assertCalls();
      logHDFSCalls.set(null);
    }
  }

  public static class CallLog {
    private List<Throwable> stackTraces = new ArrayList<Throwable>();
    private Object key;
    private int nestedCall = 0;
    public void incNestedCall() {
      nestedCall++;
    }
    public int decNestedCall() {
      return --nestedCall;
    }
    public void addStack(Throwable stack) {
      this.stackTraces.add(stack);
    }
    public void setKey(Object key) {
      this.key = key;
    }
    public void assertCalls() {
      if (stackTraces.size() > 1) {
        Throwable firstTrace = new Throwable();
        Throwable lastTrace = firstTrace;
        for (Throwable t : this.stackTraces) {
          lastTrace.initCause(t);
          lastTrace = t;
        }
        if (THROW_ON_MULTIPLE_HDFS_CALLS) {
          throw new RuntimeException("SWAP:For key:"+key+" HDFS get called more than once: ", firstTrace);
        } else {
          InternalDistributedSystem.getLoggerI18n().warning(LocalizedStrings.DEBUG, "SWAP:For key:"+key+" HDFS get called more than once: ", firstTrace);
        }
      }
    }
  }

  public EvictionCriteria getEvictionCriteria() {
    EvictionCriteria criteria = null;
    if (this.customEvictionAttributes != null
        && !this.customEvictionAttributes.isEvictIncoming()) {
      criteria = this.customEvictionAttributes.getCriteria();
    }
    return criteria;
  }
}
