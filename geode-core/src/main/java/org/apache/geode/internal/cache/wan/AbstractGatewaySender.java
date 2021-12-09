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
import java.util.Collections;
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

  protected volatile int alertThreshold;

  protected boolean manualStart;

  protected boolean isParallel;

  protected boolean groupTransactionEvents;

  protected int retriesToGetTransactionEventsFromQueue;

  protected boolean isForInternalUse;

  protected boolean isDiskSynchronous;

  protected String diskStoreName;

  protected volatile List<GatewayEventFilter> eventFilters;

  protected volatile List<GatewayTransportFilter> transFilters;

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

  private String expectedReceiverUniqueId = "";

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

  protected boolean enforceThreadsConnectSameReceiver;

  protected AbstractGatewaySender() {
    statisticsClock = disabledClock();
  }

  public AbstractGatewaySender(InternalCache cache, StatisticsClock statisticsClock,
      GatewaySenderAttributes attrs) {
    this.cache = cache;
    this.statisticsClock = statisticsClock;
    id = attrs.getId();
    socketBufferSize = attrs.getSocketBufferSize();
    socketReadTimeout = attrs.getSocketReadTimeout();
    queueMemory = attrs.getMaximumQueueMemory();
    batchSize = attrs.getBatchSize();
    batchTimeInterval = attrs.getBatchTimeInterval();
    isConflation = attrs.isBatchConflationEnabled();
    isPersistence = attrs.isPersistenceEnabled();
    alertThreshold = attrs.getAlertThreshold();
    manualStart = attrs.isManualStart();
    isParallel = attrs.isParallel();
    groupTransactionEvents = attrs.mustGroupTransactionEvents();
    retriesToGetTransactionEventsFromQueue = attrs.getRetriesToGetTransactionEventsFromQueue();
    isForInternalUse = attrs.isForInternalUse();
    diskStoreName = attrs.getDiskStoreName();
    remoteDSId = attrs.getRemoteDSId();
    eventFilters = Collections.unmodifiableList(attrs.getGatewayEventFilters());
    transFilters = Collections.unmodifiableList(attrs.getGatewayTransportFilters());
    listeners = attrs.getAsyncEventListeners();
    substitutionFilter = attrs.getGatewayEventSubstitutionFilter();
    locatorDiscoveryCallback = attrs.getGatewayLocatoDiscoveryCallback();
    isDiskSynchronous = attrs.isDiskSynchronous();
    policy = attrs.getOrderPolicy();
    dispatcherThreads = attrs.getDispatcherThreads();
    parallelismForReplicatedRegion = attrs.getParallelismForReplicatedRegion();
    // divide the maximumQueueMemory of sender equally using number of dispatcher threads.
    // if dispatcherThreads is 1 then maxMemoryPerDispatcherQueue will be same as maximumQueueMemory
    // of sender
    maxMemoryPerDispatcherQueue = queueMemory / dispatcherThreads;
    serialNumber = DistributionAdvisor.createSerialNumber();
    isMetaQueue = attrs.isMetaQueue();
    enforceThreadsConnectSameReceiver = attrs.getEnforceThreadsConnectSameReceiver();
    if (!(cache instanceof CacheCreation)) {
      myDSId = cache.getInternalDistributedSystem().getDistributionManager()
          .getDistributedSystemId();
      stopper = new Stopper(cache.getCancelCriterion());
      senderAdvisor = GatewaySenderAdvisor.createGatewaySenderAdvisor(this);
      if (!isForInternalUse()) {
        statistics = new GatewaySenderStats(cache.getDistributedSystem(),
            "gatewaySenderStats-", id, statisticsClock);
      }
      initializeEventIdIndex();
    }
    isBucketSorted = attrs.isBucketSorted();
    forwardExpirationDestroy = attrs.isForwardExpirationDestroy();
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
    return getSenderAdvisor().isPrimary();
  }

  public void setIsPrimary(boolean isPrimary) {
    getSenderAdvisor().setIsPrimary(isPrimary);
  }

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public int getAlertThreshold() {
    return alertThreshold;
  }

  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public int getBatchTimeInterval() {
    return batchTimeInterval;
  }

  @Override
  public String getDiskStoreName() {
    return diskStoreName;
  }

  @Override
  public List<GatewayEventFilter> getGatewayEventFilters() {
    return eventFilters;
  }

  @Override
  public GatewayEventSubstitutionFilter getGatewayEventSubstitutionFilter() {
    return substitutionFilter;
  }

  @Override
  public String getId() {
    return id;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public int getRemoteDSId() {
    return remoteDSId;
  }

  @Override
  public List<GatewayTransportFilter> getGatewayTransportFilters() {
    return transFilters;
  }

  public List<AsyncEventListener> getAsyncEventListeners() {
    return listeners;
  }

  public boolean hasListeners() {
    return !listeners.isEmpty();
  }

  @Override
  public boolean isForwardExpirationDestroy() {
    return forwardExpirationDestroy;
  }

  @Override
  public boolean isManualStart() {
    return manualStart;
  }

  @Override
  public int getMaximumQueueMemory() {
    return queueMemory;
  }

  public int getMaximumMemeoryPerDispatcherQueue() {
    return maxMemoryPerDispatcherQueue;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public int getSocketReadTimeout() {
    return socketReadTimeout;
  }

  @Override
  public boolean isBatchConflationEnabled() {
    return isConflation;
  }

  public void test_setBatchConflationEnabled(boolean enableConflation) {
    isConflation = enableConflation;
  }

  @Override
  public boolean isPersistenceEnabled() {
    return isPersistence;
  }

  @Override
  public boolean isDiskSynchronous() {
    return isDiskSynchronous;
  }

  @Override
  public int getMaxParallelismForReplicatedRegion() {
    return parallelismForReplicatedRegion;
  }

  public LocatorDiscoveryCallback getLocatorDiscoveryCallback() {
    return locatorDiscoveryCallback;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return senderAdvisor;
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
    return dispatcherThreads;
  }

  @Override
  public OrderPolicy getOrderPolicy() {
    return policy;
  }

  @Override
  public Profile getProfile() {
    return senderAdvisor.createProfile();
  }

  @Override
  public int getSerialNumber() {
    return serialNumber;
  }

  public boolean getBucketSorted() {
    return isBucketSorted;
  }

  @Override
  public boolean getIsMetaQueue() {
    return isMetaQueue;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  public int getEventIdIndex() {
    return eventIdIndex;
  }

  @Override
  public boolean getEnforceThreadsConnectSameReceiver() {
    return enforceThreadsConnectSameReceiver;
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
    if (sender.getId().equals(getId())) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  public PoolImpl getProxy() {
    return proxy;
  }

  @Override
  public void removeGatewayEventFilter(GatewayEventFilter filter) {
    if (filter == null) {
      return;
    }
    if (eventFilters.isEmpty()) {
      return;
    }
    List<GatewayEventFilter> templist = new ArrayList<>(eventFilters);
    templist.remove(filter);
    eventFilters = Collections.unmodifiableList(templist);
  }

  @Override
  public void addGatewayEventFilter(GatewayEventFilter filter) {
    if (filter == null) {
      throw new IllegalStateException(
          "null value can not be added to gateway-event-filters list");
    }
    List<GatewayEventFilter> templist = new ArrayList<>(eventFilters);
    templist.add(filter);
    eventFilters = Collections.unmodifiableList(templist);
  }

  @Override
  public boolean isParallel() {
    return isParallel;
  }

  @Override
  public boolean mustGroupTransactionEvents() {
    return groupTransactionEvents;
  }

  /**
   * Returns retriesToGetTransactionEventsFromQueue int property for this GatewaySender.
   *
   * @return retriesToGetTransactionEventsFromQueue int property for this GatewaySender
   *
   */
  public int getRetriesToGetTransactionEventsFromQueue() {
    return retriesToGetTransactionEventsFromQueue;
  }

  public boolean isForInternalUse() {
    return isForInternalUse;
  }

  @Override
  public abstract void start();

  @Override
  public abstract void startWithCleanQueue();

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
      getLifeCycleLock().writeLock().lock();
      // first, check if this sender is attached to any region. If so, throw
      // GatewaySenderException
      Set<InternalRegion> regions = cache.getApplicationRegions();
      Iterator regionItr = regions.iterator();
      while (regionItr.hasNext()) {
        LocalRegion region = (LocalRegion) regionItr.next();

        if (region.getAttributes().getGatewaySenderIds().contains(id)) {
          throw new GatewaySenderException(
              String.format(
                  "The GatewaySender %s could not be destroyed as it is still used by region(s).",
                  this));
        }
      }
      // close the GatewaySenderAdvisor
      GatewaySenderAdvisor advisor = getSenderAdvisor();
      if (advisor != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Stopping the GatewaySender advisor");
        }
        advisor.close();
      }

      // remove the sender from the cache
      cache.removeGatewaySender(this);

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
              logger.info(
                  "Region {} that underlies the GatewaySender {} is already destroyed.",
                  e.getRegionFullPath(), this);
            }
          }
        } // END if (regionQueues != null)
      }
    } finally {
      getLifeCycleLock().writeLock().unlock();
    }
  }

  @Override
  public void rebalance() {
    try {
      // Pause the sender
      pause();

      // Rebalance the event processor if necessary
      if (eventProcessor != null) {
        eventProcessor.rebalance();
      }
    } finally {
      // Resume the sender
      resume();
    }
    logger.info(
        "GatewaySender {} has been rebalanced", this);
  }

  /**
   * Set AlertThreshold for this GatewaySender.
   *
   * Care must be taken to set this consistently across all gateway senders in the cluster and only
   * when safe to do so.
   *
   * @since Geode 1.15
   *
   */
  public void setAlertThreshold(int alertThreshold) {
    this.alertThreshold = alertThreshold;
  };

  /**
   * Set BatchSize for this GatewaySender.
   *
   * Care must be taken to set this consistently across all gateway senders in the cluster and only
   * when safe to do so.
   *
   * @since Geode 1.15
   *
   */
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    if (eventProcessor != null) {
      eventProcessor.setBatchSize(batchSize);
    }
  }

  /**
   * Set BatchTimeInterval for this GatewaySender.
   *
   * Care must be taken to set this consistently across all gateway senders in the cluster and only
   * when safe to do so.
   *
   * @since Geode 1.15
   *
   */
  public void setBatchTimeInterval(int batchTimeInterval) {
    this.batchTimeInterval = batchTimeInterval;
    if (eventProcessor != null) {
      eventProcessor.setBatchTimeInterval(batchTimeInterval);
    }
  };

  /**
   * Set GroupTransactionEvents for this GatewaySender.
   *
   * Care must be taken to set this consistently across all gateway senders in the cluster and only
   * when safe to do so.
   *
   * @since Geode 1.15
   *
   */
  public void setGroupTransactionEvents(boolean groupTransactionEvents) {
    this.groupTransactionEvents = groupTransactionEvents;
  };

  /**
   * Set GatewayEventFilters for this GatewaySender.
   *
   * Care must be taken to set this consistently across all gateway senders in the cluster and only
   * when safe to do so.
   *
   * @since Geode 1.15
   *
   */
  public void setGatewayEventFilters(List<GatewayEventFilter> filters) {
    if (filters.isEmpty()) {
      eventFilters = Collections.emptyList();
    } else {
      eventFilters = Collections.unmodifiableList(filters);
    }
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
    AbstractGatewaySenderEventProcessor ev = eventProcessor;
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
    proxy = null;
  }

  public int getMyDSId() {
    return myDSId;
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
    return stopper;
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  public synchronized ServerLocation getServerLocation() {
    return serverLocation;
  }

  public synchronized boolean setServerLocation(ServerLocation location) {
    serverLocation = location;
    return true;
  }

  private class Stopper extends CancelCriterion {
    final CancelCriterion stper;

    Stopper(CancelCriterion stopper) {
      stper = stopper;
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
    if (eventProcessor != null) {
      if (!(eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        return eventProcessor.getQueue();
      } else {
        throw new IllegalArgumentException("getQueue() for concurrent serial gateway sender");
      }
    }
    return null;
  }

  @Override
  public Set<RegionQueue> getQueues() {
    if (eventProcessor != null) {
      if (!(eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor)) {
        Set<RegionQueue> queues = new HashSet<RegionQueue>();
        queues.add(eventProcessor.getQueue());
        return queues;
      }
      return ((ConcurrentSerialGatewaySenderEventProcessor) eventProcessor).getQueues();
    }
    return null;
  }

  protected void waitForRunningStatus() {
    synchronized (eventProcessor.getRunningStateLock()) {
      while (eventProcessor.getException() == null && eventProcessor.isStopped()) {
        try {
          eventProcessor.getRunningStateLock().wait(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      Exception ex = eventProcessor.getException();
      if (ex != null) {
        throw new GatewaySenderException(
            String.format("Could not start a gateway sender %s because of exception %s",
                new Object[] {getId(), ex.getMessage()}),
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
    if (eventProcessor != null) {
      getLifeCycleLock().writeLock().lock();
      try {
        eventProcessor.pauseDispatching();
        InternalDistributedSystem system =
            (InternalDistributedSystem) cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);
        logger.info("Paused {}", this);

        enqueueTempEvents();
      } finally {
        getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public void pause() {
    if (eventProcessor != null) {
      getLifeCycleLock().writeLock().lock();
      try {
        if (eventProcessor.isStopped()) {
          return;
        }
        eventProcessor.pauseDispatching();

        InternalDistributedSystem system =
            (InternalDistributedSystem) cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_PAUSE, this);

        logger.info("Paused {}", this);

        enqueueTempEvents();
      } finally {
        getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public void resume() {
    if (eventProcessor != null) {
      getLifeCycleLock().writeLock().lock();
      try {
        if (eventProcessor.isStopped()) {
          return;
        }
        eventProcessor.resumeDispatching();


        InternalDistributedSystem system =
            (InternalDistributedSystem) cache.getDistributedSystem();
        system.handleResourceEvent(ResourceEvent.GATEWAYSENDER_RESUME, this);

        logger.info("Resumed {}", this);

        enqueueTempEvents();
      } finally {
        getLifeCycleLock().writeLock().unlock();
      }
    }
  }

  @Override
  public boolean isPaused() {
    if (eventProcessor != null) {
      return eventProcessor.isPaused();
    }
    return false;
  }

  @Override
  public boolean isRunning() {
    if (eventProcessor != null) {
      return !eventProcessor.isStopped();
    }
    return false;
  }

  @Override
  public AbstractGatewaySenderEventProcessor getEventProcessor() {
    return eventProcessor;
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
      if (event.getOperation().isExpiration() && isAsyncEventQueue()
          && isForwardExpirationDestroy()) {
        return true;
      }
      return false;
    }
    return true;
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> allRemoteDSIds) {
    distribute(operation, event, allRemoteDSIds, false);
  }

  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> allRemoteDSIds, boolean isLastEventInTransaction) {

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
      if (!filter.enqueueEvent(event)) {
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
            isPrimary(), getId(), operation, clonedEvent, callbackArg);
      }

      if (callbackArg instanceof GatewaySenderEventCallbackArgument) {
        GatewaySenderEventCallbackArgument seca = (GatewaySenderEventCallbackArgument) callbackArg;
        if (isDebugEnabled) {
          logger.debug(
              "{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
              this, seca.getOriginatingDSId(), getMyDSId(), getRemoteDSId(),
              seca.getRecipientDSIds());
        }
        if (seca.getOriginatingDSId() == DEFAULT_DISTRIBUTED_SYSTEM_ID) {
          if (isDebugEnabled) {
            logger.debug(
                "{}: Event originated in {}. My DS id is {}. The remote DS id is {}. The recipients are: {}",
                this, seca.getOriginatingDSId(), getMyDSId(), getRemoteDSId(),
                seca.getRecipientDSIds());
          }

          seca.setOriginatingDSId(getMyDSId());
          seca.initializeReceipientDSIds(allRemoteDSIds);

        } else {
          // if the dispatcher is GatewaySenderEventCallbackDispatcher (which is the case of WBCL),
          // skip the below check of remoteDSId.
          // Fix for #46517
          AbstractGatewaySenderEventProcessor ep = getEventProcessor();
          // if manual-start is true, ep is null
          if (ep == null || !(ep.getDispatcher() instanceof GatewaySenderEventCallbackDispatcher)) {
            if (seca.getOriginatingDSId() == getRemoteDSId()) {
              if (isDebugEnabled) {
                logger.debug(
                    "{}: Event originated in {}. My DS id is {}. It is being dropped as remote is originator.",
                    this, seca.getOriginatingDSId(), getMyDSId());
              }
              return;
            } else if (seca.getRecipientDSIds().contains(getRemoteDSId())) {
              if (isDebugEnabled) {
                logger.debug(
                    "{}: Event originated in {}. My DS id is {}. The remote DS id is {}.. It is being dropped as remote ds is already a recipient. Recipients are: {}",
                    this, seca.getOriginatingDSId(), getMyDSId(), getRemoteDSId(),
                    seca.getRecipientDSIds());
              }
              return;
            }
          }
          seca.getRecipientDSIds().addAll(allRemoteDSIds);
        }
      } else {
        GatewaySenderEventCallbackArgument geCallbackArg =
            new GatewaySenderEventCallbackArgument(callbackArg, getMyDSId(), allRemoteDSIds);
        clonedEvent.setCallbackArgument(geCallbackArg);
      }

      // If this gateway is not running, return
      if (!getIsRunningAndDropEventIfNotRunning(event, isDebugEnabled, clonedEvent)) {
        return;
      }

      if (!getLifeCycleLock().readLock().tryLock()) {
        synchronized (queuedEventsSync) {
          if (!enqueuedAllTempQueueEvents) {
            if (!getLifeCycleLock().readLock().tryLock()) {
              Object substituteValue = getSubstituteValue(clonedEvent, operation);
              tmpQueuedEvents.add(new TmpQueueEvent(operation, clonedEvent, substituteValue));
              freeClonedEvent = false;
              stats.incTempQueueSize();
              if (isDebugEnabled) {
                logger.debug("Event : {} is added to TempQueue", clonedEvent);
              }
              return;
            }
          }
        }
        if (enqueuedAllTempQueueEvents) {
          try {
            while (!getLifeCycleLock().readLock().tryLock(10, TimeUnit.MILLISECONDS)) {
              if (!getIsRunningAndDropEventIfNotRunning(event, isDebugEnabled, clonedEvent)) {
                return;
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      try {
        // If this gateway is not running, return
        // The sender may have stopped, after we have checked the status in the beginning.
        if (!getIsRunningAndDropEventIfNotRunning(event, isDebugEnabled, clonedEvent)) {
          return;
        }

        try {
          AbstractGatewaySenderEventProcessor ev = eventProcessor;
          if (ev == null) {
            getStopper().checkCancelInProgress(null);
            getCache().getDistributedSystem().getCancelCriterion().checkCancelInProgress(null);
            // event processor will be null if there was an authorization
            // problem
            // connecting to the other site (bug #40681)
            if (ev == null) {
              throw new GatewayCancelledException("Event processor thread is gone");
            }
          }

          // Get substitution value to enqueue if necessary
          Object substituteValue = getSubstituteValue(clonedEvent, operation);

          ev.enqueueEvent(operation, clonedEvent, substituteValue, isLastEventInTransaction);
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
        getLifeCycleLock().readLock().unlock();
      }
    } finally {
      if (freeClonedEvent) {
        clonedEvent.release(); // fix for bug 48035
      }
    }
  }

  private boolean getIsRunningAndDropEventIfNotRunning(EntryEventImpl event, boolean isDebugEnabled,
      EntryEventImpl clonedEvent) {
    if (isRunning()) {
      return true;
    }
    if (isPrimary()) {
      recordDroppedEvent(clonedEvent);
    }
    if (isDebugEnabled) {
      logger.debug("Returning back without putting into the gateway sender queue:" + event);
    }
    return false;
  }

  private void recordDroppedEvent(EntryEventImpl event) {
    if (eventProcessor != null) {
      eventProcessor.registerEventDroppedInPrimaryQueue(event);
    } else {
      tmpDroppedEvents.add(event);
      if (logger.isDebugEnabled()) {
        logger.debug("added to tmpDroppedEvents event: {}", event);
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
    if (eventProcessor != null) {// Fix for defect #47308
      // process tmpDroppedEvents
      EntryEventImpl droppedEvent;
      while ((droppedEvent = tmpDroppedEvents.poll()) != null) {
        eventProcessor.registerEventDroppedInPrimaryQueue(droppedEvent);
      }

      TmpQueueEvent nextEvent = null;
      final GatewaySenderStats stats = getStatistics();
      try {
        // Now finish emptying the queue with synchronization to make
        // sure we don't miss any events.
        synchronized (queuedEventsSync) {
          while ((nextEvent = tmpQueuedEvents.poll()) != null) {
            try {
              if (logger.isDebugEnabled()) {
                logger.debug("Event :{} is enqueued to GatewaySenderQueue from TempQueue",
                    nextEvent);
              }
              stats.decTempQueueSize();
              eventProcessor.enqueueEvent(nextEvent.getOperation(), nextEvent.getEvent(),
                  nextEvent.getSubstituteValue());
            } finally {
              nextEvent.release();
            }
          }
          enqueuedAllTempQueueEvents = true;
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
    synchronized (queuedEventsSync) {
      Iterator<TmpQueueEvent> itr = tmpQueuedEvents.iterator();
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
    synchronized (queuedEventsSync) {
      while ((nextEvent = tmpQueuedEvents.poll()) != null) {
        nextEvent.release();
      }
      enqueuedAllTempQueueEvents = false;
    }

    statistics.setQueueSize(0);
    statistics.setSecondaryQueueSize(0);
    statistics.setEventsProcessedByPQRM(0);
    statistics.setTempQueueSize(0);
  }

  public Object getSubstituteValue(EntryEventImpl clonedEvent, EnumListenerEvent operation) {
    // Get substitution value to enqueue if necessary
    Object substituteValue = null;
    if (substitutionFilter != null) {
      try {
        substituteValue = substitutionFilter.getSubstituteValue(clonedEvent);
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
        eventIdIndex = index;
        if (logger.isDebugEnabled()) {
          logger.debug("{}: {} event id index: {}", this, messagePrefix, eventIdIndex);
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
    if (eventIdIndexMetaDataRegion == null) {
      eventIdIndexMetaDataRegion = initializeEventIdIndexMetaDataRegion(this);
    }
    return eventIdIndexMetaDataRegion;
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
    AbstractGatewaySenderEventProcessor localProcessor = eventProcessor;
    return localProcessor == null ? 0 : localProcessor.eventQueueSize();
  }

  public int getSecondaryEventQueueSize() {
    AbstractGatewaySenderEventProcessor localProcessor = eventProcessor;
    return localProcessor == null ? 0 : localProcessor.secondaryEventQueueSize();
  }

  public void setEnqueuedAllTempQueueEvents(boolean enqueuedAllTempQueueEvents) {
    this.enqueuedAllTempQueueEvents = enqueuedAllTempQueueEvents;
  }

  protected boolean isAsyncEventQueue() {
    return getAsyncEventListeners() != null && !getAsyncEventListeners().isEmpty();
  }

  public Object getLockForConcurrentDispatcher() {
    return lockForConcurrentDispatcher;
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

  public void setExpectedReceiverUniqueId(String expectedReceiverUniqueId) {
    this.expectedReceiverUniqueId = expectedReceiverUniqueId;
  }

  public String getExpectedReceiverUniqueId() {
    return expectedReceiverUniqueId;
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
      event = e;
      timeout = System.currentTimeMillis() + EVENT_TIMEOUT;
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
      operation = op;
      event = e;
      substituteValue = subValue;
    }

    public EnumListenerEvent getOperation() {
      return operation;
    }

    public @Unretained EntryEventImpl getEvent() {
      return event;
    }

    public Object getSubstituteValue() {
      return substituteValue;
    }

    @Override
    public void release() {
      event.release();
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
    if (eventProcessor != null) {
      lifeCycleLock.readLock().lock();
      try {
        logger.info("{}: Enqueueing synchronization event: {}",
            this, event);
        eventProcessor.enqueueEvent(event);
        statistics.incSynchronizationEventsEnqueued();
      } catch (Throwable t) {
        logger.warn(String.format(
            "%s: Caught the following exception attempting to enqueue synchronization event=%s:",
            new Object[] {this, event}),
            t);
      } finally {
        lifeCycleLock.readLock().unlock();
      }
    }
  }
}
