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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.GatewayCancelledException;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.HasCachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.WaitUntilParallelGatewaySenderFlushedCoordinator;
import org.apache.geode.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Abstract implementation of both Serial and Parallel GatewaySender. It handles common
 * functionality like initializing proxy.
 *
 * @since GemFire 7.0
 */
public abstract class AbstractGatewaySender implements InternalGatewaySender, DistributionAdvisee {

  private static final Logger logger = LogService.getLogger();

  protected InternalCache cache;

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

  protected boolean forwardExpirationDestroy;

  protected GatewayEventSubstitutionFilter substitutionFilter;

  protected LocatorDiscoveryCallback locatorDiscoveryCallback;

  private final ReentrantReadWriteLock lifeCycleLock = new ReentrantReadWriteLock();

  protected GatewaySenderAdvisor senderAdvisor;

  private int serialNumber;

  protected GatewaySenderStats statistics;

  private Stopper stopper;

  private OrderPolicy policy;

  private int dispatcherThreads;

  protected boolean isBucketSorted;

  protected boolean isMetaQueue;

  private int parallelismForReplicatedRegion;

  protected AbstractGatewaySenderEventProcessor eventProcessor;

  private org.apache.geode.internal.cache.GatewayEventFilter filter =
      DefaultGatewayEventFilter.getInstance();

  private ServerLocation serverLocation;

  protected Object queuedEventsSync = new Object();

  protected volatile boolean enqueuedAllTempQueueEvents = false;

  protected volatile ConcurrentLinkedQueue<TmpQueueEvent> tmpQueuedEvents =
      new ConcurrentLinkedQueue<>();

  protected volatile ConcurrentLinkedQueue<EntryEventImpl> tmpDroppedEvents =
      new ConcurrentLinkedQueue<>();
  /**
   * The number of seconds to wait before stopping the GatewaySender. Default is 0 seconds.
   */
  @MutableForTesting
  public static int MAXIMUM_SHUTDOWN_WAIT_TIME =
      Integer.getInteger("GatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME", 0).intValue();

  /**
   * The number of times to peek on shutdown before giving up and shutting down.
   */
  protected static final int MAXIMUM_SHUTDOWN_PEEKS =
      Integer.getInteger("GatewaySender.MAXIMUM_SHUTDOWN_PEEKS", 20).intValue();

  public static final int QUEUE_SIZE_THRESHOLD =
      Integer.getInteger("GatewaySender.QUEUE_SIZE_THRESHOLD", 5000).intValue();

  @MutableForTesting
  public static int TOKEN_TIMEOUT =
      Integer.getInteger("GatewaySender.TOKEN_TIMEOUT", 120000).intValue();

  /**
   * The name of the DistributedLockService used when accessing the GatewaySender's meta data
   * region.
   */
  public static final String LOCK_SERVICE_NAME = "gatewayEventIdIndexMetaData_lockService";

  /**
   * The name of the GatewaySender's meta data region.
   */
  protected static final String META_DATA_REGION_NAME = "gatewayEventIdIndexMetaData";

  protected boolean startEventProcessorInPausedState = false;

  protected int myDSId = DEFAULT_DISTRIBUTED_SYSTEM_ID;

  protected int connectionIdleTimeOut = GATEWAY_CONNECTION_IDLE_TIMEOUT;

  private boolean removeFromQueueOnException = GatewaySender.REMOVE_FROM_QUEUE_ON_EXCEPTION;

  /**
   * A unique (per <code>GatewaySender</code> id) index used when modifying <code>EventIDs</code>.
   * Unlike the serialNumber, the eventIdIndex matches for the same <code>GatewaySender</code>
   * across all members of the <code>DistributedSystem</code>.
   */
  private int eventIdIndex;

  /**
   * A <code>Region</code> used for storing <code>GatewaySender</code> event id indexes. This
   * <code>Region</code> along with a <code>DistributedLock</code> facilitates creation of unique
   * indexes across members.
   */
  private Region<String, Integer> eventIdIndexMetaDataRegion;

  final Object lockForConcurrentDispatcher = new Object();

  private final StatisticsClock statisticsClock;

  protected AbstractGatewaySender() {
    statisticsClock = disabledClock();
  }

  public AbstractGatewaySender(InternalCache cache, StatisticsClock statisticsClock,
      GatewaySenderAttributes attrs) {
    this.cache = cache;
    this.statisticsClock = statisticsClock;
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
    // divide the maximumQueueMemory of sender equally using number of dispatcher threads.
    // if dispatcherThreads is 1 then maxMemoryPerDispatcherQueue will be same as maximumQueueMemory
    // of sender
    this.maxMemoryPerDispatcherQueue = this.queueMemory / this.dispatcherThreads;
    this.serialNumber = DistributionAdvisor.createSerialNumber();
    this.isMetaQueue = attrs.isMetaQueue();
    if (!(this.cache instanceof CacheCreation)) {
      this.myDSId = this.cache.getInternalDistributedSystem().getDistributionManager()
          .getDistributedSystemId();
      this.stopper = new Stopper(cache.getCancelCriterion());
      this.senderAdvisor = GatewaySenderAdvisor.createGatewaySenderAdvisor(this);
      if (!this.isForInternalUse()) {
        this.statistics = new GatewaySenderStats(cache.getDistributedSystem(),
            "gatewaySenderStats-", id, statisticsClock);
      }
      initializeEventIdIndex();
    }
    this.isBucketSorted = attrs.isBucketSorted();
    this.forwardExpirationDestroy = attrs.isForwardExpirationDestroy();
  }

  public GatewaySenderAdvisor getSenderAdvisor() {
    return senderAdvisor;
  }

  @Override
  public GatewaySenderStats getStatistics() {
    return statistics;
  }

  @Override
  public StatisticsClock getStatisticsClock() {
    return statisticsClock;
  }

  public void initProxy() {
    // no op
  }

  @Override
  public boolean isPrimary() {
    return this.getSenderAdvisor().isPrimary();
  }

  public void setIsPrimary(boolean isPrimary) {
    this.getSenderAdvisor().setIsPrimary(isPrimary);
  }

  @Override
  public InternalCache getCache() {
    return this.cache;
  }

  @Override
  public int getAlertThreshold() {
    return this.alertThreshold;
  }

  @Override
  public int getBatchSize() {
    return this.batchSize;
  }

  @Override
  public int getBatchTimeInterval() {
    return this.batchTimeInterval;
  }

  @Override
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  @Override
  public List<GatewayEventFilter> getGatewayEventFilters() {
    return this.eventFilters;
  }

  @Override
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return this.substitutionFilter;
  }

  @Override
  public String getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  @Override
  public int getRemoteDSId() {
    return this.remoteDSId;
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return this.transFilters;
  }

  public List<AsyncEventListener> getAsyncEventListeners() {
    return this.listeners;
  }

  public boolean hasListeners() {
    return !this.listeners.isEmpty();
  }

  @Override
  public boolean isForwardExpirationDestroy() {
    return this.forwardExpirationDestroy;
  }

  @Override
  public boolean isManualStart() {
    return this.manualStart;
  }

  @Override
  public int getMaximumQueueMemory() {
    return this.queueMemory;
  }

  public int getMaximumMemeoryPerDispatcherQueue() {
    return this.maxMemoryPerDispatcherQueue;
  }

  @Override
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  @Override
  public int getSocketReadTimeout() {
    return this.socketReadTimeout;
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return this.isConflation;
  }

  public void test_setBatchConflationEnabled(boolean enableConflation) {
    this.isConflation = enableConflation;
  }

  @Override
  public boolean isPersistenceEnabled() {
    return this.isPersistence;
  }

  @Override
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }

  @Override
  public int getMaxParallelismForReplicatedRegion() {
    return this.parallelismForReplicatedRegion;
  }

  public LocatorDiscoveryCallback getLocatorDiscoveryCallback() {
    return this.locatorDiscoveryCallback;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return this.senderAdvisor;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return getSystem().getDistributionManager();
  }

  @Override
  public String getFullPath() {
    return getId();
  }

  @Override
  public String getName() {
    return getId();
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public int getDispatcherThreads() {
    return this.dispatcherThreads;
  }

  @Override
  public OrderPolicy getOrderPolicy() {
    return this.policy;
  }

  @Override
  public Profile getProfile() {
    return this.senderAdvisor.createProfile();
  }

  @Override
  public int getSerialNumber() {
    return this.serialNumber;
  }

  public boolean getBucketSorted() {
    return this.isBucketSorted;
  }

  @Override
  public boolean getIsMetaQueue() {
    return this.isMetaQueue;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return this.cache.getInternalDistributedSystem();
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
    AbstractGatewaySender sender = (AbstractGatewaySender) obj;
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

  @Override
  public void removeGatewayEventFilter(GatewayEventFilter filter) {
    this.eventFilters.remove(filter);
  }

  @Override
  public void addGatewayEventFilter(GatewayEventFilter filter) {
    if (this.eventFilters.isEmpty()) {
      this.eventFilters = new ArrayList<GatewayEventFilter>();
    }
    if (filter == null) {
      throw new IllegalStateException(
          "null value can not be added to gateway-event-filters list");
    }
    this.eventFilters.add(filter);
  }

  @Override
  public boolean isParallel() {
    return this.isParallel;
  }

  public boolean isForInternalUse() {
    return this.isForInternalUse;
  }

  @Override
  public abstract void start();

  @Override
  public abstract void stop();

  /**
   * Destroys the GatewaySender. Before destroying the sender, caller needs to to ensure that the
   * sender is stopped so that all the resources (threads, connection pool etc.) will be released
   * properly. Stopping the sender is not handled in the destroy. Destroy is carried out in
   * following steps: 1. Take the lifeCycleLock. 2. If the sender is attached to any application
   * region, throw an exception. 3. Close the GatewaySenderAdvisor. 4. Remove the sender from the
   * cache. 5. Destroy the region underlying the GatewaySender.
   *
   * In case of ParallelGatewaySender, the destroy operation does distributed destroy of the QPR. In
   * case of SerialGatewaySender, the queue region is destroyed locally.
   */
  @Override
  public void destroy() {
    destroy(true);
  }

  @Override
  public void destroy(boolean initiator) {
    try {
      this.getLifeCycleLock().writeLock().lock();
      // first, check if this sender is attached to any region. If so, throw
      // GatewaySenderException
      Set<InternalRegion> regions = this.cache.getApplicationRegions();
      Iterator regionItr = regions.iterator();
      while (regionItr.hasNext()) {
        LocalRegion region = (LocalRegion) regionItr.next();

        if (region.getAttributes().getGatewaySenderIds().contains(this.id)) {
          throw new GatewaySenderException(
              String.format(
                  "The GatewaySender %s could not be destroyed as it is still used by region(s).",
                  this));
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
      this.cache.removeGatewaySender(this);

      // destroy the region underneath the sender's queue
      if (initiator) {
        Set<RegionQueue> regionQueues = getQueues();
        if (regionQueues != null) {
          for (RegionQueue regionQueue : regionQueues) {
            try {
              if (regionQueue instanceof ConcurrentParallelGatewaySenderQueue) {
                Set<PartitionedRegion> queueRegions =
                    ((ConcurrentParallelGatewaySenderQueue) regionQueue).getRegions();
                for (PartitionedRegion queueRegion : queueRegions) {
                  queueRegion.destroyRegion();
                }
              } else {// For SerialGatewaySenderQueue, do local destroy
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
              this.logger.info(
                  "Region {} that underlies the GatewaySender {} is already destroyed.",
                  e.getRegionFullPath(), this);
            }
          }
        } // END if (regionQueues != null)
      }
    } finally {
      this.getLifeCycleLock().writeLock().unlock();
    }
  }

  @Override
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
    logger.info(
        "GatewaySender {} has been rebalanced", this);
  }

  public boolean beforeEnqueue(GatewayQueueEvent gatewayEvent) {
    boolean enqueue = true;
    for (GatewayEventFilter filter : getGatewayEventFilters()) {
      enqueue = filter.beforeEnqueue(gatewayEvent);
      if (!enqueue) {
        return enqueue;
      }
    }
    return enqueue;
  }

  protected void stopProcessing() {
    // Stop the dispatcher
    AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
    if (ev != null && !ev.isStopped()) {
      ev.stopProcessing();
    }

    if (ev != null && ev.getDispatcher() != null) {
      ev.getDispatcher().shutDownAckReaderConnection();
    }
  }

  protected void stompProxyDead() {
    Runnable stomper = new Runnable() {
      @Override
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
    Thread t = new LoggingThread("GatewaySender Proxy Stomper", stomper);
    t.start();
    try {
      t.join(GATEWAY_SENDER_TIMEOUT * 1000);
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    logger.warn("Gateway <{}> is not closing cleanly; forcing cancellation.", this);
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

  public RegionQueue getQueue() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        return this.eventProcessor.getQueue();
      } else {
        throw new IllegalArgumentException("getQueue() for concurrent serial gateway sender");
      }
    }
    return null;
  }

  @Override
  public Set<RegionQueue> getQueues() {
    if (this.eventProcessor != null) {
      if (!(this.eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        Set<RegionQueue> queues = new HashSet<RegionQueue>();
        queues.add(this.eventProcessor.getQueue());
        return queues;
      }
      return ((ConcurrentSerialGatewaySenderEventProcessor) this.eventProcessor).getQueues();
    }
    return null;
  }

  protected void waitForRunningStatus() {
    synchronized (this.eventProcessor.getRunningStateLock()) {
      while (this.eventProcessor.getException() == null && this.eventProcessor.isStopped()) {
        try {
          this.eventProcessor.getRunningStateLock().wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      Exception ex = this.eventProcessor.getException();
      if (ex != null) {
        throw new GatewaySenderException(
            String.format("Could not start a gateway sender %s because of exception %s",
                new Object[] {this.getId(), ex.getMessage()}),
            ex.getCause());
      }
    }
  }

  public boolean isStartEventProcessorInPausedState() {
    return startEventProcessorInPausedState;
  }

  @Override
  public void setStartEventProcessorInPausedState() {
    startEventProcessorInPausedState = true;
  }

  /**
   * This pause will set the pause flag even if the
   * processor has not yet started.
   */
  public void pauseEvenIfProcessorStopped() {
    if (this.eventProcessor != null) {
      this.getLifeCycleLock().writeLock().lock();
      try {
        this.eventProcessor.pauseDispatching();
        InternalDistributedSystem system =
            (InternalDistributedSystem) this.cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);
        logger.info("Paused {}", this);

        enqueueTempEvents();
      } finally {
        this.getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public void pause() {
    if (this.eventProcessor != null) {
      this.getLifeCycleLock().writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.pauseDispatching();

        InternalDistributedSystem system =
            (InternalDistributedSystem) this.cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);

        logger.info("Paused {}", this);

        enqueueTempEvents();
      } finally {
        this.getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public void resume() {
    if (this.eventProcessor != null) {
      this.getLifeCycleLock().writeLock().lock();
      try {
        if (this.eventProcessor.isStopped()) {
          return;
        }
        this.eventProcessor.resumeDispatching();


        InternalDistributedSystem system =
            (InternalDistributedSystem) this.cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_RESUME, this);

        logger.info("Resumed {}", this);

        enqueueTempEvents();
      } finally {
        this.getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public boolean isPaused() {
    if (this.eventProcessor != null) {
      return this.eventProcessor.isPaused();
    }
    return false;
  }

  @Override
  public boolean isRunning() {
    if (this.eventProcessor != null) {
      return !this.eventProcessor.isStopped();
    }
    return false;
  }

  @Override
  public AbstractGatewaySenderEventProcessor getEventProcessor() {
    return this.eventProcessor;
  }

  /**
   * Check if this event can be distributed by senders.
   *
   * @return boolean True if the event is allowed.
   */
  private boolean checkForDistribution(EntryEventImpl event, GatewaySenderStats stats) {
    if (event.getRegion().getDataPolicy().equals(DataPolicy.NORMAL)) {
      return false;
    }
    // Check for eviction and expiration events.
    if (event.getOperation().isLocal() || event.getOperation().isExpiration()) {
      // Check if its AEQ and is configured to forward expiration destroy events.
      if (event.getOperation().isExpiration() && this.isAsyncEventQueue()
          && this.isForwardExpirationDestroy()) {
        return true;
      }
      return false;
    }
    return true;
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> allRemoteDSIds) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    // released by this method or transfers ownership to TmpQueueEvent
    @Released
    EntryEventImpl clonedEvent = new EntryEventImpl(event, false);
    boolean freeClonedEvent = true;
    try {

      final GatewaySenderStats stats = getStatistics();
      stats.incEventsReceived();

      if (!checkForDistribution(event, stats)) {
        stats.incEventsNotQueued();
        return;
      }

      // this filter is defined by Asif which exist in old wan too. new wan has
      // other GatewaEventFilter. Do we need to get rid of this filter. Cheetah is
      // not considering this filter
      if (!this.filter.enqueueEvent(event)) {
        stats.incEventsFiltered();
        return;
      }

      // start to distribute
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
        GatewaySenderEventCallbackArgument seca = (GatewaySenderEventCallbackArgument) callbackArg;
        if (isDebugEnabled) {
          logger.debug(
              "{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
              this, seca.getOriginatingDSId(), this.getMyDSId(), this.getRemoteDSId(),
              seca.getRecipientDSIds());
        }
        if (seca.getOriginatingDSId() == DEFAULT_DISTRIBUTED_SYSTEM_ID) {
          if (isDebugEnabled) {
            logger.debug(
                "{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
                this, seca.getOriginatingDSId(), this.getMyDSId(), this.getRemoteDSId(),
                seca.getRecipientDSIds());
          }

          seca.setOriginatingDSId(this.getMyDSId());
          seca.initializeReceipientDSIds(allRemoteDSIds);

        } else {
          // if the dispatcher is GatewaySenderEventCallbackDispatcher (which is the case of WBCL),
          // skip the below check of remoteDSId.
          // Fix for #46517
          AbstractGatewaySenderEventProcessor ep = getEventProcessor();
          // if manual-start is true, ep is null
          if (ep == null || !(ep.getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
            if (seca.getOriginatingDSId() == this.getRemoteDSId()) {
              if (isDebugEnabled) {
                logger.debug(
                    "{}: Event originated in {}. My DS id is {}. It is being dropped as remote is originator.",
                    this, seca.getOriginatingDSId(), getMyDSId());
              }
              return;
            } else if (seca.getRecipientDSIds().contains(this.getRemoteDSId())) {
              if (isDebugEnabled) {
                logger.debug(
                    "{}: Event originated in {}. My DS id is {}. The remote DS id is {}.. It is being dropped as remote ds is already a recipient. Recipients are: {}",
                    this, seca.getOriginatingDSId(), getMyDSId(), this.getRemoteDSId(),
                    seca.getRecipientDSIds());
              }
              return;
            }
          }
          seca.getRecipientDSIds().addAll(allRemoteDSIds);
        }
      } else {
        GatewaySenderEventCallbackArgument geCallbackArg =
            new GatewaySenderEventCallbackArgument(callbackArg, this.getMyDSId(), allRemoteDSIds);
        clonedEvent.setCallbackArgument(geCallbackArg);
      }

      // If this gateway is not running, return
      if (!isRunning()) {
        if (this.isPrimary()) {
          tmpDroppedEvents.add(clonedEvent);
          if (isDebugEnabled) {
            logger.debug("add to tmpDroppedEvents for evnet {}", clonedEvent);
          }
        }
        if (isDebugEnabled) {
          logger.debug("Returning back without putting into the gateway sender queue:" + event);
        }
        return;
      }

      if (!this.getLifeCycleLock().readLock().tryLock()) {
        synchronized (this.queuedEventsSync) {
          if (!this.enqueuedAllTempQueueEvents) {
            if (!this.getLifeCycleLock().readLock().tryLock()) {
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
        if (this.enqueuedAllTempQueueEvents) {
          this.getLifeCycleLock().readLock().lock();
        }
      }
      try {
        // If this gateway is not running, return
        // The sender may have stopped, after we have checked the status in the beginning.
        if (!isRunning()) {
          if (isDebugEnabled) {
            logger.debug("Returning back without putting into the gateway sender queue:" + event);
          }
          if (this.eventProcessor != null) {
            this.eventProcessor.registerEventDroppedInPrimaryQueue(event);
          }
          return;
        }

        try {
          AbstractGatewaySenderEventProcessor ev = this.eventProcessor;
          if (ev == null) {
            getStopper().checkCancelInProgress(null);
            this.getCache().getDistributedSystem().getCancelCriterion().checkCancelInProgress(null);
            // event processor will be null if there was an authorization
            // problem
            // connecting to the other site (bug #40681)
            if (ev == null) {
              throw new GatewayCancelledException("Event processor thread is gone");
            }
          }

          // Get substitution value to enqueue if necessary
          Object substituteValue = getSubstituteValue(clonedEvent, operation);

          ev.enqueueEvent(operation, clonedEvent, substituteValue);
        } catch (CancelException e) {
          logger.debug("caught cancel exception", e);
          throw e;
        } catch (RegionDestroyedException e) {
          logger.warn(String.format(
              "%s: An Exception occurred while queueing %s to perform operation %s for %s",
              new Object[] {this, getId(), operation, clonedEvent}),
              e);
        } catch (Exception e) {
          logger.fatal(String.format(
              "%s: An Exception occurred while queueing %s to perform operation %s for %s",
              new Object[] {this, getId(), operation, clonedEvent}),
              e);
        }
      } finally {
        this.getLifeCycleLock().readLock().unlock();
      }
    } finally {
      if (freeClonedEvent) {
        clonedEvent.release(); // fix for bug 48035
      }
    }
  }

  @VisibleForTesting
  int getTmpDroppedEventSize() {
    return tmpDroppedEvents.size();
  }

  /**
   * During sender is getting started, if there are any cache operation on queue then that event
   * will be stored in temp queue. Once sender is started, these event from tmp queue will be added
   * to sender queue.
   *
   * Apart from sender's start() method, this method also gets called from
   * ParallelGatewaySenderQueue.addPartitionedRegionForRegion(). This is done to support the
   * postCreateRegion scenario i.e. the sender is already running and region is created later. The
   * eventProcessor can be null when the method gets invoked through this flow:
   * ParallelGatewaySenderImpl.start() -> ParallelGatewaySenderQueue.<init> ->
   * ParallelGatewaySenderQueue.addPartitionedRegionForRegion
   */
  public void enqueueTempEvents() {
    if (this.eventProcessor != null) {// Fix for defect #47308
      // process tmpDroppedEvents
      EntryEventImpl droppedEvent = null;
      while ((droppedEvent = tmpDroppedEvents.poll()) != null) {
        this.eventProcessor.registerEventDroppedInPrimaryQueue(droppedEvent);
      }

      TmpQueueEvent nextEvent = null;
      final GatewaySenderStats stats = getStatistics();
      try {
        // Now finish emptying the queue with synchronization to make
        // sure we don't miss any events.
        synchronized (this.queuedEventsSync) {
          while ((nextEvent = tmpQueuedEvents.poll()) != null) {
            try {
              if (logger.isDebugEnabled()) {
                logger.debug("Event :{} is enqueued to GatewaySenderQueue from TempQueue",
                    nextEvent);
              }
              stats.decTempQueueSize();
              this.eventProcessor.enqueueEvent(nextEvent.getOperation(), nextEvent.getEvent(),
                  nextEvent.getSubstituteValue());
            } finally {
              nextEvent.release();
            }
          }
          this.enqueuedAllTempQueueEvents = true;
        }
      } catch (CacheException e) {
        logger.debug("caught cancel exception", e);
      } catch (IOException e) {
        logger.fatal(String.format(
            "%s: An Exception occurred while queueing %s to perform operation %s for %s",
            new Object[] {this, getId(), nextEvent.getOperation(), nextEvent}),
            e);
      }
    }
  }

  /**
   * Removes the EntryEventImpl, whose tailKey matches with the provided tailKey, from
   * tmpQueueEvents.
   *
   */
  public boolean removeFromTempQueueEvents(Object tailKey) {
    synchronized (this.queuedEventsSync) {
      Iterator<TmpQueueEvent> itr = this.tmpQueuedEvents.iterator();
      while (itr.hasNext()) {
        TmpQueueEvent event = itr.next();
        if (tailKey.equals(event.getEvent().getTailKey())) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "shadowKey {} is found in tmpQueueEvents at AbstractGatewaySender level. Removing from there..",
                tailKey);
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
   * During sender is getting stopped, if there are any cache operation on queue then that event
   * will be stored in temp queue. Once sender is started, these event from tmp queue will be
   * cleared.
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
    statistics.setSecondaryQueueSize(0);
    statistics.setEventsProcessedByPQRM(0);
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
        logger.warn(String.format(
            "%s: An Exception occurred while queueing %s to perform operation %s for %s",
            new Object[] {this, getId(), operation, clonedEvent}),
            e);
      }
    }
    return substituteValue;
  }

  protected void initializeEventIdIndex() {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    boolean gotLock = false;
    try {
      // Obtain the distributed lock
      gotLock = getCache().getGatewaySenderLockService().lock(META_DATA_REGION_NAME, -1, -1);
      if (!gotLock) {
        throw new IllegalStateException(
            String.format("%s: Failed to lock gateway event id index metadata region",
                this));
      } else {
        if (isDebugEnabled) {
          logger.debug("{}: Locked the metadata region", this);
        }
        // Get metadata region
        Region<String, Integer> region = getEventIdIndexMetaDataRegion();

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
          if (index > ThreadIdentifier.Bits.GATEWAY_ID.mask()) {
            throw new IllegalStateException(
                String.format(
                    "Cannot create GatewaySender %s because the maximum (%s) has been reached",
                    getId(), ThreadIdentifier.Bits.GATEWAY_ID.mask() + 1));
          }
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
        getCache().getGatewaySenderLockService().unlock(META_DATA_REGION_NAME);
        if (isDebugEnabled) {
          logger.debug("{}: Unlocked the metadata region", this);
        }
      }
    }
  }

  private Region<String, Integer> getEventIdIndexMetaDataRegion() {
    if (this.eventIdIndexMetaDataRegion == null) {
      this.eventIdIndexMetaDataRegion = initializeEventIdIndexMetaDataRegion(this);
    }
    return this.eventIdIndexMetaDataRegion;
  }

  private static synchronized Region<String, Integer> initializeEventIdIndexMetaDataRegion(
      AbstractGatewaySender sender) {
    final InternalCache cache = sender.getCache();
    Region<String, Integer> region = cache.getRegion(META_DATA_REGION_NAME);
    if (region == null) {
      InternalRegionFactory<String, Integer> factory =
          cache.createInternalRegionFactory(RegionShortcut.REPLICATE);

      // Create a stats holder for the meta data stats
      final HasCachePerfStats statsHolder = new HasCachePerfStats() {
        @Override
        public CachePerfStats getCachePerfStats() {
          return new CachePerfStats(cache.getDistributedSystem(),
              "RegionStats-" + META_DATA_REGION_NAME, sender.statisticsClock);
        }
      };
      factory.setIsUsedForMetaRegion(true);
      factory.setCachePerfStatsHolder(statsHolder);
      try {
        region = factory.create(META_DATA_REGION_NAME);
      } catch (RegionExistsException e) {
        region = cache.getRegion(META_DATA_REGION_NAME);
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format(
                "%s: Caught the following exception attempting to create gateway event id index metadata region:",
                sender),
            e);
      }
    }
    return region;
  }

  public abstract void setModifiedEventId(EntryEventImpl clonedEvent);

  public static class DefaultGatewayEventFilter
      implements org.apache.geode.internal.cache.GatewayEventFilter {
    @Immutable
    private static final DefaultGatewayEventFilter singleton = new DefaultGatewayEventFilter();

    private DefaultGatewayEventFilter() {}

    public static org.apache.geode.internal.cache.GatewayEventFilter getInstance() {
      return singleton;
    }

    @Override
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

  @Override
  public int getEventQueueSize() {
    AbstractGatewaySenderEventProcessor localProcessor = this.eventProcessor;
    return localProcessor == null ? 0 : localProcessor.eventQueueSize();
  }

  public int getSecondaryEventQueueSize() {
    AbstractGatewaySenderEventProcessor localProcessor = this.eventProcessor;
    return localProcessor == null ? 0 : localProcessor.secondaryEventQueueSize();
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

  public ReentrantReadWriteLock getLifeCycleLock() {
    return lifeCycleLock;
  }

  @Override
  public boolean waitUntilFlushed(long timeout, TimeUnit unit) throws InterruptedException {
    boolean result = false;
    if (isParallel()) {
      try {
        WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
            new WaitUntilParallelGatewaySenderFlushedCoordinator(this, timeout, unit, true);
        result = coordinator.waitUntilFlushed();
      } catch (BucketMovedException | CancelException | RegionDestroyedException e) {
        logger.warn(
            "Caught the following exception attempting waitUntilFlushed and will retry:",
            e);
        throw e;
      } catch (Throwable t) {
        logger.warn(
            "Caught the following exception attempting waitUntilFlushed and will return:",
            t);
        throw new InternalGemFireError(t);
      }
      return result;
    } else {
      // Serial senders are currently not supported
      throw new UnsupportedOperationException(
          "waitUntilFlushed is not currently supported for serial gateway senders");
    }
  }

  /**
   * Has a reference to a GatewayEventImpl and has a timeout value.
   */
  public static class EventWrapper {
    /**
     * Timeout events received from secondary after 5 minutes
     */
    private static final int EVENT_TIMEOUT =
        Integer.getInteger("Gateway.EVENT_TIMEOUT", 5 * 60 * 1000).intValue();
    public final long timeout;
    public final GatewaySenderEventImpl event;

    public EventWrapper(GatewaySenderEventImpl e) {
      this.event = e;
      this.timeout = System.currentTimeMillis() + EVENT_TIMEOUT;
    }
  }

  /**
   * Instances of this class allow us to delay queuing an incoming event. What used to happen was
   * that the tmpQ would have a GatewaySenderEventImpl added to it. But then when we took it out we
   * had to ask it for its EntryEventImpl. Then we created another GatewaySenderEventImpl. As part
   * of the off-heap work, the GatewaySenderEventImpl no longer has a EntryEventImpl. So this class
   * allows us to defer creation of the GatewaySenderEventImpl until we are ready to actually
   * enqueue it. The caller is responsible for giving us an EntryEventImpl that we own and that we
   * will release. This is done by making a copy/clone of the original event. This fixes bug 52029.
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

  protected GatewayQueueEvent getSynchronizationEvent(Object key, long timestamp) {
    GatewayQueueEvent event = null;
    for (RegionQueue queue : getQueues()) {
      Region region = queue.getRegion();
      if (region == null) {
        continue;
      }
      for (Iterator i = region.values().iterator(); i.hasNext();) {
        GatewaySenderEventImpl gsei = (GatewaySenderEventImpl) i.next();
        if (gsei.getKey().equals(key) && gsei.getVersionTimeStamp() == timestamp) {
          event = gsei;
          logger.info("{}: Providing synchronization event for key={}; timestamp={}: {}",
              this, key, timestamp, event);
          getStatistics().incSynchronizationEventsProvided();
          break;
        }
      }
    }
    return event;
  }

  protected void putSynchronizationEvent(GatewayQueueEvent event) {
    if (this.eventProcessor != null) {
      this.lifeCycleLock.readLock().lock();
      try {
        logger.info("{}: Enqueueing synchronization event: {}",
            this, event);
        this.eventProcessor.enqueueEvent(event);
        this.statistics.incSynchronizationEventsEnqueued();
      } catch (Throwable t) {
        logger.warn(String.format(
            "%s: Caught the following exception attempting to enqueue synchronization event=%s:",
            new Object[] {this, event}),
            t);
      } finally {
        this.lifeCycleLock.readLock().unlock();
      }
    }
  }
}
