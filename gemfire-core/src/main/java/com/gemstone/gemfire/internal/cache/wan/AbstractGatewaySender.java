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

package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayEventSubstitutionFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.GatewayCancelledException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HasCachePerfStats;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.offheap.Releasable;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * Abstract implementation of both Serial and Parallel GatewaySener. It handles
 * common functionality like initializing proxy.
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * 
 * @since 7.0
 */

public abstract class AbstractGatewaySender implements GatewaySender,
    DistributionAdvisee {

  private static final Logger logger = LogService.getLogger();
  
  protected Cache cache;

  protected String id;
  
  protected long startTime;

  protected PoolImpl proxy;

  protected int remoteDSId;

  protected String locName;

  protected int socketBufferSize;

  protected int socketReadTimeout;

  protected int queueMemory;
  
  protected int maxMemoryPerDispatcherQueue;

  protected int batchSize;

  protected int batchTimeInterval;

  protected boolean isConflation;

  protected boolean isPersistence;

  protected int alertThreshold;

  protected boolean manualStart;
  
  protected boolean isParallel;
  
  protected boolean isForInternalUse;

  protected boolean isDiskSynchronous;
  
  protected String diskStoreName;

  protected List<GatewayEventFilter> eventFilters;

  protected List<GatewayTransportFilter> transFilters;

  protected List<AsyncEventListener> listeners;
  
  protected GatewayEventSubstitutionFilter substitutionFilter;
  
  protected LocatorDiscoveryCallback locatorDiscoveryCallback;
  
  public final ReentrantReadWriteLock lifeCycleLock = new ReentrantReadWriteLock();
  
  protected GatewaySenderAdvisor senderAdvisor;
  
  private int serialNumber;
  
  protected GatewaySenderStats statistics;
  
  private Stopper stopper;
  
  private OrderPolicy policy;
  
  private int dispatcherThreads;
  
  protected boolean isBucketSorted;
  
  protected boolean isHDFSQueue;
  
  protected boolean isMetaQueue;
  
  private int parallelismForReplicatedRegion;
  
  protected AbstractGatewaySenderEventProcessor eventProcessor;
  
  private com.gemstone.gemfire.internal.cache.GatewayEventFilter filter = DefaultGatewayEventFilter.getInstance();
  
  private ServerLocation serverLocation;
  
  protected Object queuedEventsSync = new Object();
  
  protected volatile boolean enqueuedAllTempQueueEvents = false; 
  
  protected volatile ConcurrentLinkedQueue<TmpQueueEvent> tmpQueuedEvents = new ConcurrentLinkedQueue<>();
  /**
   * The number of seconds to wait before stopping the GatewaySender.
   * Default is 0 seconds.
   */
  public static int MAXIMUM_SHUTDOWN_WAIT_TIME = Integer.getInteger(
		  "GatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME", 0).intValue();

  /**
   * The number of times to peek on shutdown before giving up and shutting down.
   */
  protected static final int MAXIMUM_SHUTDOWN_PEEKS = Integer.getInteger(
      "GatewaySender.MAXIMUM_SHUTDOWN_PEEKS", 20).intValue();
  
  public static final int QUEUE_SIZE_THRESHOLD = Integer.getInteger(
      "GatewaySender.QUEUE_SIZE_THRESHOLD", 5000).intValue();

  public static int TOKEN_TIMEOUT = Integer.getInteger(
      "GatewaySender.TOKEN_TIMEOUT", 15000).intValue();

  /**
   * The name of the DistributedLockService used when accessing the GatewaySender's
   * meta data region.
   */
  public static final String LOCK_SERVICE_NAME = "gatewayEventIdIndexMetaData_lockService";
  
  /**
   * The name of the GatewaySender's meta data region.
   */
  protected static final String META_DATA_REGION_NAME = "gatewayEventIdIndexMetaData";

  protected int myDSId = DEFAULT_DISTRIBUTED_SYSTEM_ID;
  
  protected int connectionIdleTimeOut = GATEWAY_CONNECTION_IDLE_TIMEOUT;
  
  private boolean removeFromQueueOnException = GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION;
  
  /**
   * A unique (per <code>GatewaySender</code> id) index used when modifying
   * <code>EventIDs</code>. Unlike the serialNumber, the eventIdIndex matches
   * for the same <code>GatewaySender</code> across all members of the
   * <code>DistributedSystem</code>.
   */
  private int eventIdIndex;
   
  /** 
   * A <code>Region</code> used for storing <code>GatewaySender</code> event id
   * indexes. This <code>Region</code> along with a <code>DistributedLock</code>
   * facilitates creation of unique indexes across members.
   */
  private Region<String,Integer> eventIdIndexMetaDataRegion;
  
  final Object lockForConcurrentDispatcher = new Object();

  protected AbstractGatewaySender() {
  }

  public AbstractGatewaySender(Cache cache, GatewaySenderAttributes attrs){
    this.cache = cache;
    this.id = attrs.getId();
    this.socketBufferSize = attrs.getSocketBufferSize();
    this.socketReadTimeout = attrs.getSocketReadTimeout();
    this.queueMemory = attrs.getMaximumQueueMemory();
    this.batchSize = attrs.getBatchSize();
    this.batchTimeInterval = attrs.getBatchTimeInterval();
    this.isConflation = attrs.isBatchConflationEnabled();
    this.isPersistence = attrs.isPersistenceEnabled();
    this.alertThreshold = attrs.getAlertThreshold();
    this.manualStart = attrs.isManualStart();
    this.isParallel = attrs.isParallel();
    this.isForInternalUse = attrs.isForInternalUse();
    this.diskStoreName = attrs.getDiskStoreName();
    this.remoteDSId = attrs.getRemoteDSId();
    this.eventFilters = attrs.getGatewayEventFilters();
    this.transFilters = attrs.getGatewayTransportFilters();
    this.listeners = attrs.getAsyncEventListeners();
    this.substitutionFilter = attrs.getGatewayEventSubstitutionFilter();
    this.locatorDiscoveryCallback = attrs.getGatewayLocatoDiscoveryCallback();
    this.isDiskSynchronous = attrs.isDiskSynchronous();
    this.policy = attrs.getOrderPolicy();
    this.dispatcherThreads = attrs.getDispatcherThreads();
    this.parallelismForReplicatedRegion = attrs.getParallelismForReplicatedRegion();
    //divide the maximumQueueMemory of sender equally using number of dispatcher threads.
    //if dispatcherThreads is 1 then maxMemoryPerDispatcherQueue will be same as maximumQueueMemory of sender
    this.maxMemoryPerDispatcherQueue = this.queueMemory / this.dispatcherThreads;
    this.myDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager().getDistributedSystemId();
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.isHDFSQueue = attrs.isHDFSQueue();
    this.isMetaQueue = attrs.isMetaQueue();
    if (!(this.cache instanceof CacheCreation)) {
      this.stopper = new Stopper(cache.getCancelCriterion());
      this.senderAdvisor = GatewaySenderAdvisor.createGatewaySenderAdvisor(this);
      if (!this.isForInternalUse()) {
        this.statistics = new GatewaySenderStats(cache.getDistributedSystem(),
            id);
      }
      if (!attrs.isHDFSQueue())
        initializeEventIdIndex();
    }
    this.isBucketSorted = attrs.isBucketSorted();
  }
  
  public void createSender(Cache cache, GatewaySenderAttributes attrs){
    this.cache = cache;
    this.id = attrs.getId();
    this.socketBufferSize = attrs.getSocketBufferSize();
    this.socketReadTimeout = attrs.getSocketReadTimeout();
    this.queueMemory = attrs.getMaximumQueueMemory();
    this.batchSize = attrs.getBatchSize();
    this.batchTimeInterval = attrs.getBatchTimeInterval();
    this.isConflation = attrs.isBatchConflationEnabled();
    this.isPersistence = attrs.isPersistenceEnabled();
    this.alertThreshold = attrs.getAlertThreshold();
    this.manualStart = attrs.isManualStart();
    this.isParallel = attrs.isParallel();
    this.isForInternalUse = attrs.isForInternalUse();
    this.diskStoreName = attrs.getDiskStoreName();
    this.remoteDSId = attrs.getRemoteDSId();
    this.eventFilters = attrs.getGatewayEventFilters();
    this.transFilters = attrs.getGatewayTransportFilters();
    this.listeners = attrs.getAsyncEventListeners();
    this.substitutionFilter = attrs.getGatewayEventSubstitutionFilter();
    this.locatorDiscoveryCallback = attrs.getGatewayLocatoDiscoveryCallback();
    this.isDiskSynchronous = attrs.isDiskSynchronous();
    this.policy = attrs.getOrderPolicy();
    this.dispatcherThreads = attrs.getDispatcherThreads();
    this.parallelismForReplicatedRegion = attrs.getParallelismForReplicatedRegion();
    //divide the maximumQueueMemory of sender equally using number of dispatcher threads.
    //if dispatcherThreads is 1 then maxMemoryPerDispatcherQueue will be same as maximumQueueMemory of sender
    this.maxMemoryPerDispatcherQueue = this.queueMemory / this.dispatcherThreads;
    this.myDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager().getDistributedSystemId();
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    if (!(this.cache instanceof CacheCreation)) {
      this.stopper = new Stopper(cache.getCancelCriterion());
      this.senderAdvisor = GatewaySenderAdvisor.createGatewaySenderAdvisor(this);
      if (!this.isForInternalUse()) {
        this.statistics = new AsyncEventQueueStats(cache.getDistributedSystem(),
            id);
      }
      else {// this sender lies underneath the AsyncEventQueue. Need to have
            // AsyncEventQueueStats
        this.statistics = new AsyncEventQueueStats(
            cache.getDistributedSystem(), AsyncEventQueueImpl
                .getAsyncEventQueueIdFromSenderId(id));
      }
      if (!attrs.isHDFSQueue())
        initializeEventIdIndex();
    }
    this.isBucketSorted = attrs.isBucketSorted();
    this.isHDFSQueue = attrs.isHDFSQueue();
   
  }
  
  public GatewaySenderAdvisor getSenderAdvisor() {
    return senderAdvisor;
  }

  public GatewaySenderStats getStatistics() {
    return statistics;
  }
  
  public void initProxy() {
    //no op
  }

  public boolean isPrimary() {
    return this.getSenderAdvisor().isPrimary();
  }
  
  public void setIsPrimary(boolean isPrimary){
    this.getSenderAdvisor().setIsPrimary(isPrimary);
  }
  
  public Cache getCache() {
    return this.cache;
  }

  public int getAlertThreshold() {
    return this.alertThreshold;
  }
  
  public int getBatchSize() {
    return this.batchSize;
  }
  
  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public List<GatewayEventFilter> getGatewayEventFilters() {
    return this.eventFilters;
  }
  
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return this.substitutionFilter;
  }

  public String getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public int getRemoteDSId() {
    return this.remoteDSId;
  }

  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilters;
  }

  public List<AsyncEventListener> getAsyncEventListeners() {
    return this.listeners;
  }

  public boolean hasListeners() {
    return !this.listeners.isEmpty();
  }
  
  public boolean isManualStart() {
    return this.manualStart;
  }

  public int getMaximumQueueMemory() {
    return this.queueMemory;
  }
  
  public int getMaximumMemeoryPerDispatcherQueue() {
    return this.maxMemoryPerDispatcherQueue;
  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public int getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  public boolean isBatchConflationEnabled() {
    return this.isConflation;
  }
  
  public void test_setBatchConflationEnabled(boolean enableConflation) {
    this.isConflation = enableConflation;
  }
  
  public boolean isPersistenceEnabled() {
    return this.isPersistence;
  }

  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  public int getMaxParallelismForReplicatedRegion() {
    return this.parallelismForReplicatedRegion;
  }  
    
  public LocatorDiscoveryCallback getLocatorDiscoveryCallback() {
    return this.locatorDiscoveryCallback;
  }

  public DistributionAdvisor getDistributionAdvisor() {
    return this.senderAdvisor;
  }

  public DM getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  public String getFullPath() {
    return getId();
  }

  public String getName() {
    return getId();
  }

  public DistributionAdvisee getParentAdvisee() {
    return null;
  }
  
  public int getDispatcherThreads() {
    return this.dispatcherThreads;
  }
  
  public OrderPolicy getOrderPolicy() {
    return this.policy;
  }

  public Profile getProfile() {
    return this.senderAdvisor.createProfile();
  }

  public int getSerialNumber() {
    return this.serialNumber;
  }
  
  public boolean getBucketSorted() {
    return this.isBucketSorted;
  }

  public boolean getIsHDFSQueue() {
    return this.isHDFSQueue;
  }
  
  public boolean getIsMetaQueue() {
    return this.isMetaQueue;
  }
  
  public InternalDistributedSystem getSystem() {
    return (InternalDistributedSystem)this.cache.getDistributedSystem();
  }
  
  public int getEventIdIndex() {
    return this.eventIdIndex;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GatewaySender)) {
      return false;
    }
    AbstractGatewaySender sender = (AbstractGatewaySender)obj;
    if (sender.getId().equals(this.getId())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.getId().hashCode();
  }

  public PoolImpl getProxy() {
    return proxy;
  }

  public void removeGatewayEventFilter(GatewayEventFilter filter) {
    this.eventFilters.remove(filter);
  }

  public void addGatewayEventFilter(GatewayEventFilter filter) {
    if (this.eventFilters.isEmpty()) {
      this.eventFilters = new ArrayList<GatewayEventFilter>();
    }
    if (filter == null) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderImpl_NULL_CANNNOT_BE_ADDED_TO_GATEWAY_EVENT_FILTER_LIST
              .toLocalizedString());
    }
    this.eventFilters.add(filter);
  }

  public boolean isParallel() {
    return this.isParallel;
  }
  
  public boolean isForInternalUse() {
    return this.isForInternalUse;
  }
  
  abstract public void start();
  abstract public void stop();
  
  /**
   * Destroys the GatewaySender. Before destroying the sender, caller needs to to ensure 
   * that the sender is stopped so that all the resources (threads, connection pool etc.) 
   * will be released properly. Stopping the sender is not handled in the destroy.
   * Destroy is carried out in following steps:
   * 1. Take the lifeCycleLock. 
   * 2. If the sender is attached to any application region, throw an exception.
   * 3. Close the GatewaySenderAdvisor.
   * 4. Remove the sender from the cache.
   * 5. Destroy the region underlying the GatewaySender.
   * 
   * In case of ParallelGatewaySender, the destroy operation does distributed destroy of the 
   * QPR. In case of SerialGatewaySender, the queue region is destroyed locally.
   */
  public void destroy() {
    try {
      this.lifeCycleLock.writeLock().lock();
      // first, check if this sender is attached to any region. If so, throw
      // GatewaySenderException
      Set<LocalRegion> regions = ((GemFireCacheImpl)this.cache)
          .getApplicationRegions();
      Iterator regionItr = regions.iterator();
      while (regionItr.hasNext()) {
        LocalRegion region = (LocalRegion)regionItr.next();

        if (region.getAttributes().getGatewaySenderIds().contains(this.id)) {
          throw new GatewaySenderException(
              LocalizedStrings.GatewaySender_COULD_NOT_DESTROY_SENDER_AS_IT_IS_STILL_IN_USE
                  .toLocalizedString(this));
        }
      }
      // close the GatewaySenderAdvisor
      GatewaySenderAdvisor advisor = this.getSenderAdvisor();
      if (advisor != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Stopping the GatewaySender advisor");
        }
        advisor.close();
      }

      // remove the sender from the cache
      ((GemFireCacheImpl)this.cache).removeGatewaySender(this);

      // destroy the region underneath the sender's queue
      Set<RegionQueue> regionQueues = getQueues();
      if (regionQueues != null) {
        for (RegionQueue regionQueue : regionQueues) {
          try {
            if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
              Set<PartitionedRegion> queueRegions = ((ConcurrentParallelGatewaySenderQueue)regionQueue)
                  .getRegions();
              for (PartitionedRegion queueRegion : queueRegions) {
                queueRegion.destroyRegion();
              }
            }
            else {// For SerialGatewaySenderQueue, do local destroy
              regionQueue.getRegion().localDestroyRegion();
            }
          }
          // Can occur in case of ParallelGatewaySenderQueue, when the region is
          // being destroyed
          // by several nodes simultaneously
          catch (RegionDestroyedException e) {
            // the region might have already been destroyed by other node. Just
            // log
            // the exception.
            this
                .logger.info(LocalizedMessage.create(LocalizedStrings.AbstractGatewaySender_REGION_0_UNDERLYING_GATEWAYSENDER_1_IS_ALREADY_DESTROYED,
                    new Object[] { e.getRegionFullPath(), this }));
          }
        }
      }//END if (regionQueues != null)
    }
    finally {
      this.lifeCycleLock.writeLock().unlock();
    }
  }
  
  public void rebalance() {
    try {
      // Pause the sender
      pause();

      // Rebalance the event processor if necessary
      if (this.eventProcessor != null) {
        this.eventProcessor.rebalance();
      }
    } finally {
      // Resume the sender
      resume();
    }
    logger.info(LocalizedMessage.create(LocalizedStrings.GatewayImpl_GATEWAY_0_HAS_BEEN_REBALANCED, this));
  }

  public boolean beforeEnque(GatewayQueueEvent gatewayEvent) {
    boolean enque = true;
    for (GatewayEventFilter filter : getGatewayEventFilters()) {
      enque = filter.beforeEnqueue(gatewayEvent);
      if (!enque) {
        return enque;
      }
    }
    return enque;
  }
  
  protected void stompProxyDead() {
    Runnable stomper = new Runnable() {
      public void run() {
        PoolImpl bpi = proxy;
        if (bpi != null) {
          try {
            bpi.destroy();
          } catch (Exception e) {/* ignore */
          }
        }
      }
    };
    ThreadGroup tg = LoggingThreadGroup.createThreadGroup("Proxy Stomper Group", logger);
    Thread t = new Thread(tg, stomper, "GatewaySender Proxy Stomper");
    t.setDaemon(true);
    t.start();
    try {
      t.join(GATEWAY_SENDER_TIMEOUT * 1000);
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_GATEWAY_0_IS_NOT_CLOSING_CLEANLY_FORCING_CANCELLATION, this));
    // OK, either we've timed out or been interrupted. Time for
    // violence.
    t.interrupt(); // give up
    proxy.emergencyClose(); // VIOLENCE!
    this.proxy = null;
  }
  
  public int getMyDSId() {
    return this.myDSId;
  }
  
  /**
   * @param removeFromQueueOnException the removeFromQueueOnException to set
   */
  public void setRemoveFromQueueOnException(boolean removeFromQueueOnException) {
    this.removeFromQueueOnException = removeFromQueueOnException;
  }

  /**
   * @return the removeFromQueueOnException
   */
  public boolean isRemoveFromQueueOnException() {
    return removeFromQueueOnException;
  }

  public CancelCriterion getStopper() {
    return this.stopper;
  }
  
  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }
  
  public synchronized ServerLocation getServerLocation() {
    return serverLocation;
  }

  public synchronized boolean setServerLocation(ServerLocation location) {
    this.serverLocation = location;
    return true;
  }
  
  private class Stopper extends CancelCriterion {
    final CancelCriterion stper;

    Stopper(CancelCriterion stopper) {
      this.stper = stopper;
    }

    @Override
    public String cancelInProgress() {
      // checkFailure(); // done by stopper
      return stper.cancelInProgress();
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      RuntimeException result = stper.generateCancelledException(e);
      return result;
    }
  }

  final public RegionQueue getQueue() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        return this.eventProcessor.getQueue();
      }
      else {
        throw new IllegalArgumentException(
            "getQueue() for concurrent serial gateway sender");
      }
    }
    return null;
  }

  final public Set<RegionQueue> getQueues() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        Set<RegionQueue> queues = new HashSet<RegionQueue>();
        queues.add(this.eventProcessor.getQueue());
        return queues;
      }
      return ((ConcurrentSerialGatewaySenderEventProcessor)this.eventProcessor)
          .getQueues();
    }
    return null;
  }
  
  final public Set<RegionQueue> getQueuesForConcurrentSerialGatewaySender() {
    if (this.eventProcessor != null
        && (this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
      return ((ConcurrentSerialGatewaySenderEventProcessor)this.eventProcessor)
          .getQueues();
    }
    return null;
  }
  
  final protected void waitForRunningStatus() {
    synchronized (this.eventProcessor.runningStateLock) {
      while (this.eventProcessor.getException() == null
          && this.eventProcessor.isStopped()) {
        try {
          this.eventProcessor.runningStateLock.wait();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      Exception ex = this.eventProcessor.getException();
      if (ex != null) {
        throw new GatewaySenderException(
            LocalizedStrings.Sender_COULD_NOT_START_GATEWAYSENDER_0_BECAUSE_OF_EXCEPTION_1
                .toLocalizedString(new Object[] { this.getId(), ex.getMessage() }),
            ex.getCause());
      }
    }
  }
  
  final public void pause() {
    if (this.eventProcessor != null) {
      this.lifeCycleLock.writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.pauseDispatching();

        InternalDistributedSystem system = (InternalDistributedSystem) this.cache
            .getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);
        
        logger.info(LocalizedMessage.create(LocalizedStrings.GatewaySender_PAUSED__0, this));

        enqueTempEvents();
      } finally {
        this.lifeCycleLock.writeLock().unlock();
      }
    }
  }

  final public void resume() {
    if (this.eventProcessor != null) {
      this.lifeCycleLock.writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.resumeDispatching();

        
        InternalDistributedSystem system = (InternalDistributedSystem) this.cache
            .getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_RESUME, this);
        
        logger.info(LocalizedMessage.create(LocalizedStrings.GatewaySender_RESUMED__0, this));
        
        enqueTempEvents();
      } finally {
        this.lifeCycleLock.writeLock().unlock();
      }
    }
  }
  
  final public boolean isPaused() {
    if (this.eventProcessor != null) {
      return this.eventProcessor.isPaused();
    }
    return false;
  }

  final public boolean isRunning() {
    if (this.eventProcessor != null) {
      return !this.eventProcessor.isStopped();
    }
    return false;
  }

  final public AbstractGatewaySenderEventProcessor getEventProcessor() {
    return this.eventProcessor;
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> allRemoteDSIds) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    final GatewaySenderStats stats = getStatistics();
    stats.incEventsReceived();
    // If the event is local (see bug 35831) or an expiration ignore it.
    //removed the check of isLocal as in notifyGAtewayHub this has been taken care
    if (/*event.getOperation().isLocal() || */event.getOperation().isExpiration()
        || event.getRegion().getDataPolicy().equals(DataPolicy.NORMAL)) {
      getStatistics().incEventsNotQueued();
      return;
    }
    
    if (getIsHDFSQueue() && event.getOperation().isEviction()) {
      if (logger.isDebugEnabled())
        logger.debug("Eviction event not queued: " + event);
      stats.incEventsNotQueued();
      return;
    }
    // this filter is defined by Asif which exist in old wan too. new wan has
    // other GatewaEventFilter. Do we need to get rid of this filter. Cheetah is
    // not cinsidering this filter
    if (!this.filter.enqueueEvent(event)) {
      getStatistics().incEventsFiltered();
      return;
    }
    
    EntryEventImpl clonedEvent = new EntryEventImpl(event, false);
    boolean freeClonedEvent = true;
    try {

    Region region = event.getRegion();

    setModifiedEventId(clonedEvent);
    Object callbackArg = clonedEvent.getRawCallbackArgument();
    
    if (isDebugEnabled) {
      // We can't deserialize here for logging purposes so don't
      // call getNewValue.
      // event.getNewValue(); // to deserialize the value if necessary
      logger.debug("{} : About to notify {} to perform operation {} for {} callback arg {}",
          this.isPrimary(), getId(), operation, clonedEvent, callbackArg);
    }

    if (callbackArg instanceof GatewaySenderEventCallbackArgument) {
      GatewaySenderEventCallbackArgument seca = (GatewaySenderEventCallbackArgument)callbackArg;
      if (isDebugEnabled) {
        logger.debug("{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
            this, seca.getOriginatingDSId(), this.getMyDSId(), this.getRemoteDSId(), seca.getRecipientDSIds());
      }
      if (seca.getOriginatingDSId() == DEFAULT_DISTRIBUTED_SYSTEM_ID) {
        if (isDebugEnabled) {
          logger.debug("{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
              this, seca.getOriginatingDSId(), this.getMyDSId(), this.getRemoteDSId(), seca.getRecipientDSIds());
        }

        seca.setOriginatingDSId(this.getMyDSId());
        seca.initializeReceipientDSIds(allRemoteDSIds);

        } else {
          //if the dispatcher is GatewaySenderEventCallbackDispatcher (which is the case of WBCL), skip the below check of remoteDSId.
          //Fix for #46517
          AbstractGatewaySenderEventProcessor ep = getEventProcessor();
        if (ep != null && !(ep.getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
          if (seca.getOriginatingDSId() == this.getRemoteDSId()) {
            if (isDebugEnabled) {
              logger.debug("{}: Event originated in {}. My DS id is {}. It is being dropped as remote is originator.",
                  this, seca.getOriginatingDSId(), getMyDSId());
            }
            return;
          } else if (seca.getRecipientDSIds().contains(this.getRemoteDSId())) {
            if (isDebugEnabled) {
              logger.debug("{}: Event originated in {}. My DS id is {}. The remote DS id is {}.. It is being dropped as remote ds is already a recipient. Recipients are: {}",
                  this, seca.getOriginatingDSId(), getMyDSId(), this.getRemoteDSId(), seca.getRecipientDSIds());
            }
            return;
          }
        }
        seca.getRecipientDSIds().addAll(allRemoteDSIds);
      }
    } else {
      GatewaySenderEventCallbackArgument geCallbackArg = new GatewaySenderEventCallbackArgument(
          callbackArg, this.getMyDSId(), allRemoteDSIds, true);
      clonedEvent.setCallbackArgument(geCallbackArg);
    }

    if (!this.lifeCycleLock.readLock().tryLock()) {
      synchronized (this.queuedEventsSync) {
        if (!this.enqueuedAllTempQueueEvents) {
          if (!this.lifeCycleLock.readLock().tryLock()) {
            Object substituteValue = getSubstituteValue(clonedEvent, operation);
            this.tmpQueuedEvents.add(new TmpQueueEvent(operation, clonedEvent, substituteValue));
            freeClonedEvent = false;
            stats.incTempQueueSize();
            if (isDebugEnabled) {
              logger.debug("Event : {} is added to TempQueue", clonedEvent);
            }
            return;
          }
        }
      }
      if(this.enqueuedAllTempQueueEvents) {
        this.lifeCycleLock.readLock().lock();
      }
    }
    try {
      // If this gateway is not running, return
      if (!isRunning()) {
        if (isDebugEnabled) {
          logger.debug("Returning back without putting into the gateway sender queue");
        }
        return;
      }

      try {
        AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
        if (ev == null) {
          getStopper().checkCancelInProgress(null);
          this.getCache().getDistributedSystem().getCancelCriterion()
              .checkCancelInProgress(null);
          // event processor will be null if there was an authorization
          // problem
          // connecting to the other site (bug #40681)
          if (ev == null) {
            throw new GatewayCancelledException(
                "Event processor thread is gone");
          }
        }
        
        // Get substitution value to enqueue if necessary
        Object substituteValue = getSubstituteValue(clonedEvent, operation);
        
        ev.enqueueEvent(operation, clonedEvent, substituteValue);
      } catch (CancelException e) {
        logger.debug("caught cancel exception", e);
      } catch (RegionDestroyedException e) {
        logger.warn(LocalizedMessage.create(
        LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
        new Object[] { this, getId(), operation, clonedEvent }), e);
      } catch (Exception e) {
        logger.fatal(LocalizedMessage.create(
                LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
                new Object[] { this, getId(), operation, clonedEvent }), e);
      }
    } finally {
      this.lifeCycleLock.readLock().unlock();
    }
    } finally {
      if (freeClonedEvent) {
        clonedEvent.release(); // fix for bug 48035
      }
    }
  }
  
  /**
   * During sender is getting started, if there are any cache operation on queue then that event will be stored in temp queue. 
   * Once sender is started, these event from tmp queue will be added to sender queue.
   * 
   * Apart from sender's start() method, this method also gets called from ParallelGatewaySenderQueue.addPartitionedRegionForRegion(). 
   * This is done to support the postCreateRegion scenario i.e. the sender is already running and region is created later.
   * The eventProcessor can be null when the method gets invoked through this flow: 
   * ParallelGatewaySenderImpl.start() -> ParallelGatewaySenderQueue.<init> -> ParallelGatewaySenderQueue.addPartitionedRegionForRegion 
   */
  public void enqueTempEvents() {
    if (this.eventProcessor != null) {//Fix for defect #47308
      TmpQueueEvent nextEvent = null;
      final GatewaySenderStats stats = getStatistics();
      try {
        // Now finish emptying the queue with synchronization to make
        // sure we don't miss any events.
        synchronized (this.queuedEventsSync) {
          while ((nextEvent = tmpQueuedEvents.poll()) != null) {
          try {
            if (logger.isDebugEnabled()) {
              logger.debug("Event :{} is enqueued to GatewaySenderQueue from TempQueue", nextEvent);
            }
            stats.decTempQueueSize();
            this.eventProcessor.enqueueEvent(nextEvent.getOperation(),
                nextEvent.getEvent(), nextEvent.getSubstituteValue());
          } finally {
            nextEvent.release();
          }
          }
          this.enqueuedAllTempQueueEvents = true;
        }
      }
      catch (CacheException e) {
        logger.debug("caught cancel exception", e);
      }
      catch (IOException e) {
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
                new Object[] { this, getId(), nextEvent.getOperation(), nextEvent }), e);
      }
    }
  }
  
  /**
   * Removes the EntryEventImpl, whose tailKey matches with the provided tailKey, 
   * from tmpQueueEvents. 
   * @param tailKey
   */
  public boolean removeFromTempQueueEvents(Object tailKey) {
    synchronized (this.queuedEventsSync) {
      Iterator<TmpQueueEvent> itr = this.tmpQueuedEvents.iterator();
      while (itr.hasNext()) {
        TmpQueueEvent event = itr.next();
        if (tailKey.equals(event.getEvent().getTailKey())) {
          if (logger.isDebugEnabled()) {
            logger.debug("shadowKey {} is found in tmpQueueEvents at AbstractGatewaySender level. Removing from there..", tailKey);
          }
          event.release();
          itr.remove();
          return true;
        }
      }
      return false;
    }
  }
  
  /**
   * During sender is getting stopped, if there are any cache operation on queue then that event will be stored in temp queue. 
   * Once sender is started, these event from tmp queue will be cleared.
   */
  public void clearTempEventsAfterSenderStopped() {
    TmpQueueEvent nextEvent = null;
    while ((nextEvent = tmpQueuedEvents.poll()) != null) {
      nextEvent.release();
    }
    synchronized (this.queuedEventsSync) {
      while ((nextEvent = tmpQueuedEvents.poll()) != null) {
        nextEvent.release();
      }
      this.enqueuedAllTempQueueEvents = false;
    }
    
    statistics.setQueueSize(0);
    statistics.setTempQueueSize(0);
  }
  
  public Object getSubstituteValue(EntryEventImpl clonedEvent, EnumListenerEvent operation) {
    // Get substitution value to enqueue if necessary
    Object substituteValue = null;
    if (this.substitutionFilter != null) {
      try {
        substituteValue = this.substitutionFilter.getSubstituteValue(clonedEvent);
        // If null is returned from the filter, null is set in the value
        if (substituteValue == null) {
          substituteValue = GatewaySenderEventImpl.TOKEN_NULL;
        }
      } catch (Exception e) {
        // Log any exceptions that occur in the filter and use the original value.
        logger.warn(LocalizedMessage.create(LocalizedStrings.GatewayImpl_0_AN_EXCEPTION_OCCURRED_WHILE_QUEUEING_1_TO_PERFORM_OPERATION_2_FOR_3,
                new Object[] { this, getId(), operation, clonedEvent }), e);
      }
    }
    return substituteValue;
  }
  
  private void initializeEventIdIndex() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    boolean gotLock = false;
    try {
      // Obtain the distributed lock
      gotLock = ((GemFireCacheImpl) getCache()).getGatewaySenderLockService().lock(META_DATA_REGION_NAME, -1, -1);
      if (!gotLock) {
        throw new IllegalStateException(
            LocalizedStrings.AbstractGatewaySender_FAILED_TO_LOCK_META_REGION_0
                .toLocalizedString(this));
      } else { 
        if (isDebugEnabled) {
          logger.debug("{}: Locked the metadata region", this);
        }
        // Get metadata region
        Region<String,Integer> region = getEventIdIndexMetaDataRegion();
        
        // Get or create the index
        int index = 0;
        String messagePrefix = null;
        if (region.containsKey(getId())) {
          index = region.get(getId());
          if (isDebugEnabled) {
            messagePrefix = "Using existing";
          }
        } else {
          index = region.size();
          region.put(getId(), index);
          if (isDebugEnabled) {
            messagePrefix = "Created new";
          }
        }
        
        // Store the index locally
        this.eventIdIndex = index;
        if (logger.isDebugEnabled()) {
          logger.debug("{}: {} event id index: {}", this, messagePrefix, this.eventIdIndex);
        }
      }
    } finally {
      // Unlock the lock if necessary
      if (gotLock) {
        ((GemFireCacheImpl) getCache()).getGatewaySenderLockService().unlock(META_DATA_REGION_NAME);
        if (isDebugEnabled) {
          logger.debug("{}: Unlocked the metadata region", this);
        }
      }
    }
  }

  private Region<String,Integer> getEventIdIndexMetaDataRegion() {
    if (this.eventIdIndexMetaDataRegion == null) {
      this.eventIdIndexMetaDataRegion = initializeEventIdIndexMetaDataRegion(this);
    }
    return this.eventIdIndexMetaDataRegion;
  }

  @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
  private static synchronized Region<String, Integer> initializeEventIdIndexMetaDataRegion(
      AbstractGatewaySender sender) {
    final Cache cache = sender.getCache();
    Region<String,Integer> region = cache.getRegion(META_DATA_REGION_NAME);
    if (region == null) {
      // Create region attributes (must be done this way to use InternalRegionArguments)
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      RegionAttributes ra = factory.create();

      // Create a stats holder for the meta data stats
      final HasCachePerfStats statsHolder = new HasCachePerfStats() {
        public CachePerfStats getCachePerfStats() {
          return new CachePerfStats(cache.getDistributedSystem(),
              META_DATA_REGION_NAME);
        }
      };
      
      // Create internal region arguments
      InternalRegionArguments ira = new InternalRegionArguments()
          .setIsUsedForMetaRegion(true).setCachePerfStatsHolder(statsHolder);

      // Create the region
      try {
        region = ((GemFireCacheImpl) cache).createVMRegion(META_DATA_REGION_NAME, ra, ira);
      } catch (RegionExistsException e) {
        region = cache.getRegion(META_DATA_REGION_NAME);
      } catch (Exception e) {
        throw new IllegalStateException(
            LocalizedStrings.AbstractGatewaySender_META_REGION_CREATION_EXCEPTION_0
                .toLocalizedString(sender),
            e);
      }
    }
    return region;
  }

  /**
   * @param clonedEvent
   */
  abstract protected void setModifiedEventId(EntryEventImpl clonedEvent);
  
  public static class DefaultGatewayEventFilter implements com.gemstone.gemfire.internal.cache.GatewayEventFilter {
    private static final DefaultGatewayEventFilter singleton = new DefaultGatewayEventFilter();

    private DefaultGatewayEventFilter() {
    }

    public static com.gemstone.gemfire.internal.cache.GatewayEventFilter getInstance() {
      return singleton;
    }

    public boolean enqueueEvent(EntryEventImpl event) {
      return true;
    }
  }

  
  public int getTmpQueuedEventSize() {
    if (tmpQueuedEvents != null) {
      return tmpQueuedEvents.size();
    }
    return 0;
  }
  
  public void setEnqueuedAllTempQueueEvents(boolean enqueuedAllTempQueueEvents) {
    this.enqueuedAllTempQueueEvents = enqueuedAllTempQueueEvents;
  }
  
  protected boolean isAsyncEventQueue() {
    return this.getAsyncEventListeners() != null && !this.getAsyncEventListeners().isEmpty();
  }
  
  public Object getLockForConcurrentDispatcher() {
    return this.lockForConcurrentDispatcher;
  }
  /**
   * Has a reference to a GatewayEventImpl and has a timeout value.
   */
  public static class EventWrapper  {
    /**
     * Timeout events received from secondary after 5 minutes
     */
    static private final int EVENT_TIMEOUT
      = Integer.getInteger("Gateway.EVENT_TIMEOUT", 5 * 60 * 1000).intValue();
    public final long timeout;
    public final GatewaySenderEventImpl event;
    public EventWrapper(GatewaySenderEventImpl e) {
      this.event = e;
      this.timeout = System.currentTimeMillis() + EVENT_TIMEOUT;
    }
  }
  
  /**
   * Instances of this class allow us to delay queuing an incoming event.
   * What used to happen was that the tmpQ would have a GatewaySenderEventImpl
   * added to it. But then when we took it out we had to ask it for its EntryEventImpl.
   * Then we created another GatewaySenderEventImpl.
   * As part of the off-heap work, the GatewaySenderEventImpl no longer has a EntryEventImpl.
   * So this class allows us to defer creation of the GatewaySenderEventImpl until we
   * are ready to actually enqueue it.
   * The caller is responsible for giving us an EntryEventImpl that we own and that
   * we will release. This is done by making a copy/clone of the original event.
   * This fixes bug 52029.
   * 
   * @author dschneider
   *
   */
  public static class TmpQueueEvent implements Releasable {
    private final EnumListenerEvent operation;
    private final @Retained EntryEventImpl event;
    private final Object substituteValue;
    public TmpQueueEvent(EnumListenerEvent op, @Retained EntryEventImpl e, Object subValue) {
      this.operation = op;
      this.event = e;
      this.substituteValue = subValue;
    }
    
    public EnumListenerEvent getOperation() {
      return this.operation;
    }
    
    public @Unretained EntryEventImpl getEvent() {
      return this.event;
    }
    
    public Object getSubstituteValue() {
      return this.substituteValue;
    }

    @Override
    public void release() {
      this.event.release();
    }
  }
}
