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
package org.apache.geode.internal.cache.control;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.cache.partitioned.SizedBasedLoadProbe;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.runtime.RestoreRedundancyResults;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Implementation of ResourceManager with additional internal-only methods.
 */
public class InternalResourceManager implements ResourceManager {
  private static final Logger logger = LogService.getLogger();

  final int MAX_RESOURCE_MANAGER_EXE_THREADS =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "resource.manager.threads", 1);

  private final Collection<CompletableFuture<Void>> startupTasks = new ArrayList<>();

  public enum ResourceType {
    HEAP_MEMORY(0x1), OFFHEAP_MEMORY(0x2), MEMORY(0x3), ALL(0xFFFFFFFF);

    final int id;

    ResourceType(final int id) {
      this.id = id;
    }
  }

  private final Map<ResourceType, Set<ResourceListener<?>>> listeners = new HashMap<>();

  private final ScheduledExecutorService scheduledExecutor;
  private final ExecutorService notifyExecutor;

  // A map of in progress rebalance operations. The value is Boolean because ConcurrentHashMap does
  // not support null values.
  private final Map<RebalanceOperation, Boolean> inProgressRebalanceOperations =
      new ConcurrentHashMap<>();

  // A map of in progress restore redundancy completable futures. The value is Boolean because
  // ConcurrentHashMap does not support null values.
  private final Map<CompletableFuture<RestoreRedundancyResults>, Boolean> inProgressRedundancyOperations =
      new ConcurrentHashMap<>();

  final InternalCache cache;

  private LoadProbe loadProbe;

  private final ResourceManagerStats stats;
  private final ResourceAdvisor resourceAdvisor;
  private boolean closed;

  private final Map<ResourceType, ResourceMonitor> resourceMonitors;

  @MutableForTesting
  private static ResourceObserver observer = new ResourceObserverAdapter();

  private static final String PR_LOAD_PROBE_CLASS =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "ResourceManager.PR_LOAD_PROBE_CLASS",
          SizedBasedLoadProbe.class.getName());

  public static InternalResourceManager getInternalResourceManager(Cache cache) {
    return (InternalResourceManager) cache.getResourceManager();
  }

  public static InternalResourceManager createResourceManager(final InternalCache cache) {
    return new InternalResourceManager(cache);
  }

  private InternalResourceManager(InternalCache cache) {
    this.cache = cache;
    resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    stats = new ResourceManagerStats(cache.getDistributedSystem());

    // Create a new executor that other classes may use for handling resource
    // related tasks
    scheduledExecutor = LoggingExecutors
        .newScheduledThreadPool(MAX_RESOURCE_MANAGER_EXE_THREADS, "ResourceManagerRecoveryThread ");

    // Initialize the load probe
    try {
      Class<?> loadProbeClass = ClassPathLoader.getLatest().forName(PR_LOAD_PROBE_CLASS);
      loadProbe = (LoadProbe) loadProbeClass.newInstance();
    } catch (Exception e) {
      throw new InternalGemFireError("Unable to instantiate " + PR_LOAD_PROBE_CLASS, e);
    }

    // Create a new executor the resource manager and the monitors it creates
    // can use to handle dispatching of notifications.
    notifyExecutor =
        CoreLoggingExecutors.newSerialThreadPoolWithFeedStatistics(0,
            stats.getResourceEventQueueStatHelper(), "Notification Handler",
            thread -> thread.setPriority(Thread.MAX_PRIORITY), null,
            stats.getResourceEventPoolStatHelper(), getThreadMonitorObj());

    // Create the monitors
    Map<ResourceType, ResourceMonitor> tempMonitors = new HashMap<>();
    tempMonitors.put(ResourceType.HEAP_MEMORY, new HeapMemoryMonitor(this, cache, stats));
    tempMonitors.put(ResourceType.OFFHEAP_MEMORY,
        new OffHeapMemoryMonitor(this, cache, cache.getOffHeapStore(), stats));
    resourceMonitors = Collections.unmodifiableMap(tempMonitors);

    // Initialize the listener sets so that it only needs to be done once
    for (ResourceType resourceType : new ResourceType[] {ResourceType.HEAP_MEMORY,
        ResourceType.OFFHEAP_MEMORY}) {
      Set<ResourceListener<?>> emptySet = new CopyOnWriteArraySet<>();
      listeners.put(resourceType, emptySet);
    }
  }

  public void close() {
    for (ResourceMonitor monitor : resourceMonitors.values()) {
      monitor.stopMonitoring();
    }

    stopExecutor(scheduledExecutor);
    stopExecutor(notifyExecutor);

    stats.close();
    closed = true;
  }

  boolean isClosed() {
    return closed;
  }

  public void fillInProfile(final Profile profile) {
    assert profile instanceof ResourceManagerProfile;

    for (ResourceMonitor monitor : resourceMonitors.values()) {
      monitor.fillInProfile((ResourceManagerProfile) profile);
    }
  }

  public void addResourceListener(final ResourceListener<?> listener) {
    addResourceListener(ResourceType.ALL, listener);
  }

  public void addResourceListener(final ResourceType resourceType,
      final ResourceListener<?> listener) {
    for (Map.Entry<ResourceType, Set<ResourceListener<?>>> mapEntry : listeners.entrySet()) {
      if ((mapEntry.getKey().id & resourceType.id) != 0) {
        mapEntry.getValue().add(listener);
      }
    }
  }

  public void removeResourceListener(final ResourceListener<?> listener) {
    removeResourceListener(ResourceType.ALL, listener);
  }

  public void removeResourceListener(final ResourceType resourceType,
      final ResourceListener<?> listener) {
    for (Map.Entry<ResourceType, Set<ResourceListener<?>>> mapEntry : listeners.entrySet()) {
      if ((mapEntry.getKey().id & resourceType.id) != 0) {
        mapEntry.getValue().remove(listener);
      }
    }
  }

  public Set<ResourceListener<?>> getResourceListeners(final ResourceType resourceType) {
    return listeners.get(resourceType);
  }

  /**
   * Deliver an event received from remote resource managers to the local listeners.
   *
   * @param event Event to deliver.
   */
  public void deliverEventFromRemote(final ResourceEvent event) {
    assert !event.isLocal();

    if (logger.isTraceEnabled()) {
      logger.trace(
          "New remote event to deliver for member " + event.getMember() + ": event=" + event);
    }

    runWithNotifyExecutor(() -> deliverLocalEvent(event));
  }

  void deliverLocalEvent(ResourceEvent event) {
    // Wait for an event to be handled by all listeners before starting to send another event
    synchronized (listeners) {
      resourceMonitors.get(event.getType()).notifyListeners(listeners.get(event.getType()), event);
    }

    stats.incResourceEventsDelivered();
  }

  public HeapMemoryMonitor getHeapMonitor() {
    return (HeapMemoryMonitor) resourceMonitors.get(ResourceType.HEAP_MEMORY);
  }

  public OffHeapMemoryMonitor getOffHeapMonitor() {
    return (OffHeapMemoryMonitor) resourceMonitors.get(ResourceType.OFFHEAP_MEMORY);
  }

  public MemoryMonitor getMemoryMonitor(boolean offheap) {
    if (offheap) {
      return getOffHeapMonitor();
    } else {
      return getHeapMonitor();
    }
  }

  /**
   * Use threshold event processor to execute the event embedded in the runnable.
   *
   * @param runnable Runnable to execute.
   */
  void runWithNotifyExecutor(Runnable runnable) {
    try {
      notifyExecutor.execute(runnable);
    } catch (RejectedExecutionException ignore) {
      if (!closed) {
        logger.warn("No memory events will be delivered because of RejectedExecutionException");
      }
    }
  }

  @Override
  public RebalanceFactory createRebalanceFactory() {
    return new RebalanceFactoryImpl();
  }

  @Override
  public Set<RebalanceOperation> getRebalanceOperations() {
    return Collections.unmodifiableSet(inProgressRebalanceOperations.keySet());
  }

  void addInProgressRebalance(RebalanceOperation op) {
    inProgressRebalanceOperations.put(op, Boolean.TRUE);
  }

  void removeInProgressRebalance(RebalanceOperation op) {
    inProgressRebalanceOperations.remove(op);
  }

  class RebalanceFactoryImpl implements RebalanceFactory {

    private Set<String> includedRegions;
    private Set<String> excludedRegions;

    @Override
    public RebalanceOperation simulate() {
      RegionFilter filter = new FilterByPath(includedRegions, excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(cache, true, filter);
      op.start();
      return op;
    }

    @Override
    public RebalanceOperation start() {
      RegionFilter filter = new FilterByPath(includedRegions, excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(cache, false, filter);
      op.start();
      return op;
    }

    @Override
    public RebalanceFactory excludeRegions(Set<String> regions) {
      excludedRegions = regions;
      return this;
    }

    @Override
    public RebalanceFactory includeRegions(Set<String> regions) {
      includedRegions = regions;
      return this;
    }
  }

  @Override
  public RestoreRedundancyOperation createRestoreRedundancyOperation() {
    return new RestoreRedundancyOperationImpl(cache);
  }

  @Override
  public Set<CompletableFuture<RestoreRedundancyResults>> getRestoreRedundancyFutures() {
    return Collections.unmodifiableSet(inProgressRedundancyOperations.keySet());
  }

  void addInProgressRestoreRedundancy(
      CompletableFuture<RestoreRedundancyResults> completableFuture) {
    inProgressRedundancyOperations.put(completableFuture, Boolean.TRUE);
  }

  void removeInProgressRestoreRedundancy(
      CompletableFuture<RestoreRedundancyResults> completableFuture) {
    inProgressRedundancyOperations.remove(completableFuture);
  }

  void stopExecutor(ExecutorService executor) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    final int secToWait =
        Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "prrecovery-close-timeout", 5);
    try {
      executor.awaitTermination(secToWait, TimeUnit.SECONDS);
    } catch (InterruptedException x) {
      Thread.currentThread().interrupt();
      logger.debug("Failed in interrupting the Resource Manager Thread due to interrupt");
    }
    if (!executor.isTerminated()) {
      logger.warn("Failed to stop resource manager threads in {} seconds, forcing shutdown",
          secToWait);
      List<Runnable> remainingTasks = executor.shutdownNow();
      remainingTasks.forEach(runnable -> {
        if (runnable instanceof Future) {
          ((Future<?>) runnable).cancel(true);
        }
      });
    }
  }

  public ScheduledExecutorService getExecutor() {
    return scheduledExecutor;
  }

  public ResourceManagerStats getStats() {
    return stats;
  }

  /**
   * For testing only, an observer which is called when rebalancing is started and finished for a
   * particular region. This observer is called even the "rebalancing" is actually redundancy
   * recovery for a particular region.
   */
  public static void setResourceObserver(ResourceObserver observer) {
    if (observer == null) {
      observer = new ResourceObserverAdapter();
    }
    InternalResourceManager.observer = observer;
  }


  /**
   * Get the resource observer used for testing. Never returns null.
   */
  public static ResourceObserver getResourceObserver() {
    return observer;
  }

  /**
   * For testing only. Receives callbacks for resource related events.
   */
  public interface ResourceObserver {
    /**
     * Indicates that rebalancing has started on a given region.
     */
    void rebalancingStarted(Region<?, ?> region);

    /**
     * Indicates that rebalancing has finished on a given region.
     */
    void rebalancingFinished(Region<?, ?> region);

    /**
     * Indicates that recovery has started on a given region.
     */
    void recoveryStarted(Region<?, ?> region);

    /**
     * Indicates that recovery has finished on a given region.
     */
    void recoveryFinished(Region<?, ?> region);

    /**
     * Indicated that a membership event triggered a recovery operation, but the recovery operation
     * will not be executed because there is already an existing recovery operation waiting to
     * happen on this region.
     */
    void recoveryConflated(PartitionedRegion region);

    /**
     * Indicates that a bucket is being moved from the source member to the target member.
     *
     * @param region the region
     * @param bucketId the bucket being moved
     * @param source the member the bucket is moving from
     * @param target the member the bucket is moving to
     */
    void movingBucket(Region<?, ?> region, int bucketId, DistributedMember source,
        DistributedMember target);

    /**
     * Indicates that a bucket primary is being moved from the source member to the target member.
     *
     * @param region the region
     * @param bucketId the bucket primary being moved
     * @param source the member the bucket primary is moving from
     * @param target the member the bucket primary is moving to
     */
    void movingPrimary(Region<?, ?> region, int bucketId, DistributedMember source,
        DistributedMember target);
  }

  public static class ResourceObserverAdapter implements ResourceObserver {

    @Override
    public void rebalancingFinished(Region<?, ?> region) {
      rebalancingOrRecoveryFinished(region);
    }

    @Override
    public void rebalancingStarted(Region<?, ?> region) {
      rebalancingOrRecoveryStarted(region);
    }

    @Override
    public void recoveryFinished(Region<?, ?> region) {
      rebalancingOrRecoveryFinished(region);
    }

    @Override
    public void recoveryStarted(Region<?, ?> region) {
      rebalancingOrRecoveryStarted(region);
    }

    /**
     * Indicated the a rebalance or a recovery has started.
     */
    public void rebalancingOrRecoveryStarted(Region<?, ?> region) {
      // do nothing
    }

    /**
     * Indicated the a rebalance or a recovery has finished.
     */
    public void rebalancingOrRecoveryFinished(Region<?, ?> region) {
      // do nothing
    }

    @Override
    public void recoveryConflated(PartitionedRegion region) {
      // do nothing
    }

    @Override
    public void movingBucket(Region<?, ?> region, int bucketId, DistributedMember source,
        DistributedMember target) {
      // do nothing
    }

    @Override
    public void movingPrimary(Region<?, ?> region, int bucketId, DistributedMember source,
        DistributedMember target) {
      // do nothing
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DistributionAdvisee#getCancelCriterion()
   */
  public CancelCriterion getCancelCriterion() {
    return cache.getCancelCriterion();
  }

  public ResourceAdvisor getResourceAdvisor() {
    return resourceAdvisor;
  }

  public LoadProbe getLoadProbe() {
    return loadProbe;
  }

  /**
   * This method is test purposes only.
   */
  public LoadProbe setLoadProbe(LoadProbe probe) {
    LoadProbe old = loadProbe;
    loadProbe = probe;
    return old;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.cache.control.ResourceManager#setEvictionHeapPercentage(int)
   */
  @Override
  public void setCriticalOffHeapPercentage(float offHeapPercentage) {
    getOffHeapMonitor().setCriticalThreshold(offHeapPercentage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getCriticalOffHeapPercentage() {
    return getOffHeapMonitor().getCriticalThreshold();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEvictionOffHeapPercentage(float offHeapPercentage) {
    getOffHeapMonitor().setEvictionThreshold(offHeapPercentage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getEvictionOffHeapPercentage() {
    return getOffHeapMonitor().getEvictionThreshold();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCriticalHeapPercentage(float heapPercentage) {
    getHeapMonitor().setCriticalThreshold(heapPercentage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getCriticalHeapPercentage() {
    return getHeapMonitor().getCriticalThreshold();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setEvictionHeapPercentage(float heapPercentage) {
    getHeapMonitor().setEvictionThreshold(heapPercentage);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getEvictionHeapPercentage() {
    return getHeapMonitor().getEvictionThreshold();
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = cache.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    } else {
      return null;
    }
  }

  /**
   * Adds a task that represents an asynchronous action during startup
   *
   * @param startupTask the CompletableFuture startup task
   */
  public void addStartupTask(CompletableFuture<Void> startupTask) {
    Objects.requireNonNull(startupTask);
    synchronized (startupTasks) {
      startupTasks.add(startupTask);
    }
  }

  /**
   * Clears the startup tasks and returns a CompletableFuture that completes when all of the startup
   * tasks complete.
   *
   * @return a CompletableFuture that completes when all of the startup tasks complete
   */
  public CompletableFuture<Void> allOfStartupTasks() {
    synchronized (startupTasks) {
      CompletableFuture<?>[] completableFutures = startupTasks.toArray(new CompletableFuture[0]);
      startupTasks.clear();
      return CompletableFuture.allOf(completableFutures);
    }
  }

  @VisibleForTesting
  Collection<CompletableFuture<Void>> getStartupTasks() {
    return startupTasks;
  }
}
