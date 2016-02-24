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
package com.gemstone.gemfire.internal.cache.control;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats;
import com.gemstone.gemfire.distributed.internal.SerialQueuedExecutorWithDMStats;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import com.gemstone.gemfire.internal.cache.partitioned.LoadProbe;
import com.gemstone.gemfire.internal.cache.partitioned.SizedBasedLoadProbe;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Implementation of ResourceManager with additional internal-only methods.
 * TODO: cleanup raw and typed collections
 * 
 */
public class InternalResourceManager implements ResourceManager {
  private static final Logger logger = LogService.getLogger();

  final int MAX_RESOURCE_MANAGER_EXE_THREADS = Integer.getInteger("gemfire.resource.manager.threads", 1);
  
  public enum ResourceType {
    HEAP_MEMORY(0x1), OFFHEAP_MEMORY(0x2), MEMORY(0x3), ALL(0xFFFFFFFF);
    
    int id;
    private ResourceType(final int id) {
      this.id = id;
    }
  };
  
  private Map<ResourceType, Set<ResourceListener>> listeners = new HashMap<ResourceType, Set<ResourceListener>>();
  
  private final ScheduledExecutorService scheduledExecutor;
  private final ExecutorService notifyExecutor;
  
  //The set of in progress rebalance operations.
  private final Set<RebalanceOperation> inProgressOperations = new HashSet<RebalanceOperation>(); 
  private final Object inProgressOperationsLock = new Object();

  final GemFireCacheImpl cache;
  
  private LoadProbe loadProbe;
  
  private final ResourceManagerStats stats;
  private final ResourceAdvisor resourceAdvisor;
  private boolean closed = true;
  
  private final Map<ResourceType, ResourceMonitor> resourceMonitors;
  
  private static ResourceObserver observer = new ResourceObserverAdapter();
  
  private static String PR_LOAD_PROBE_CLASS = System.getProperty(
      "gemfire.ResourceManager.PR_LOAD_PROBE_CLASS", SizedBasedLoadProbe.class
          .getName());
  
  public static InternalResourceManager getInternalResourceManager(Cache cache) {
    return (InternalResourceManager) cache.getResourceManager();
  }
  
  public static InternalResourceManager createResourceManager(final GemFireCacheImpl cache) {
    return new InternalResourceManager(cache);
  }

  private InternalResourceManager(GemFireCacheImpl cache) {
    this.cache = cache;
    this.resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    this.stats = new ResourceManagerStats(cache.getDistributedSystem());
    
    // Create a new executor that other classes may use for handling resource
    // related tasks
    final ThreadGroup threadGroup = LoggingThreadGroup.createThreadGroup(
        "ResourceManagerThreadGroup", logger);

    ThreadFactory tf = new ThreadFactory() {
      AtomicInteger ai = new AtomicInteger();
      @Override
      public Thread newThread(Runnable r) {
        int tId = ai.getAndIncrement();
        Thread thread = new Thread(threadGroup, r,
        "ResourceManagerRecoveryThread " + tId);
        thread.setDaemon(true);
        return thread;
      }
    };
    int nThreads = MAX_RESOURCE_MANAGER_EXE_THREADS;

    this.scheduledExecutor = new ScheduledThreadPoolExecutor(nThreads, tf);

    // Initialize the load probe
    try {
      Class loadProbeClass = ClassPathLoader.getLatest().forName(PR_LOAD_PROBE_CLASS);
      this.loadProbe = (LoadProbe) loadProbeClass.newInstance();
    } catch (Exception e) {
      throw new InternalGemFireError("Unable to instantiate " + PR_LOAD_PROBE_CLASS, e);
    }

    // Create a new executor the resource manager and the monitors it creates
    // can use to handle dispatching of notifications.
    final ThreadGroup listenerInvokerthrdGrp = LoggingThreadGroup.createThreadGroup(
        "ResourceListenerInvokerThreadGroup", logger);

    ThreadFactory eventProcessorFactory = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(listenerInvokerthrdGrp, r, "Notification Handler");
        thread.setDaemon(true);
        thread.setPriority(Thread.MAX_PRIORITY);
        return thread;
      }
    };
    BlockingQueue<Runnable> threadQ = new OverflowQueueWithDMStats(this.stats.getResourceEventQueueStatHelper());
    this.notifyExecutor = new SerialQueuedExecutorWithDMStats(threadQ, this.stats.getResourceEventPoolStatHelper(),
        eventProcessorFactory);
    
    // Create the monitors
    Map<ResourceType, ResourceMonitor> tempMonitors = new HashMap<ResourceType, ResourceMonitor>();
    tempMonitors.put(ResourceType.HEAP_MEMORY, new HeapMemoryMonitor(this, cache, this.stats));
    tempMonitors.put(ResourceType.OFFHEAP_MEMORY, new OffHeapMemoryMonitor(this, cache, cache.getOffHeapStore(), this.stats));
    this.resourceMonitors = Collections.unmodifiableMap(tempMonitors);
    
    // Initialize the listener sets so that it only needs to be done once
    for (ResourceType resourceType : new ResourceType[] { ResourceType.HEAP_MEMORY, ResourceType.OFFHEAP_MEMORY }) {
      Set<ResourceListener> emptySet = new CopyOnWriteArraySet<ResourceListener>();
      this.listeners.put(resourceType, emptySet);
    }

    this.closed = false;
  }

  public void close() {
    for (ResourceMonitor monitor : this.resourceMonitors.values()) {
      monitor.stopMonitoring();
    }
    
    stopExecutor(this.scheduledExecutor);
    stopExecutor(this.notifyExecutor);
    
    this.stats.close();
    this.closed = true;
  }
  
  boolean isClosed() {
    return this.closed;
  }

  public void fillInProfile(final Profile profile) {
    assert profile instanceof ResourceManagerProfile;
    
    for (ResourceMonitor monitor : this.resourceMonitors.values()) {
      monitor.fillInProfile((ResourceManagerProfile) profile);
    }
  }
  
  public void addResourceListener(final ResourceListener listener) {
    addResourceListener(ResourceType.ALL, listener);
  }
  
  public void addResourceListener(final ResourceType resourceType, final ResourceListener listener) {
    for (Map.Entry<ResourceType, Set<ResourceListener>> mapEntry : this.listeners.entrySet()) {
      if ((mapEntry.getKey().id & resourceType.id) != 0) {
        mapEntry.getValue().add(listener);
      }
    }
  }
  
  public void removeResourceListener(final ResourceListener listener) {
    removeResourceListener(ResourceType.ALL, listener);
  }
  
  public void removeResourceListener(final ResourceType resourceType, final ResourceListener listener) {
    for (Map.Entry<ResourceType, Set<ResourceListener>> mapEntry : this.listeners.entrySet()) {
      if ((mapEntry.getKey().id & resourceType.id) != 0) {
        mapEntry.getValue().remove(listener);
      }
    }
  }
  
  public Set<ResourceListener> getResourceListeners(final ResourceType resourceType) {
    return this.listeners.get(resourceType);
  }
  
  /**
   * Deliver an event received from remote resource managers to the local
   * listeners.
   * 
   * @param event
   *          Event to deliver.
   */
  public void deliverEventFromRemote(final ResourceEvent event) {
    assert !event.isLocal();

    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n().fine("New remote event to deliver for member " + event.getMember() + ": event=" + event);
    }

    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n().fine("Remote event to deliver for member " + event.getMember() + ":" + event);
    }

    runWithNotifyExecutor(new Runnable() {
      @Override
      public void run() {
        deliverLocalEvent(event);
      }
    });
    return;
  }
  
  void deliverLocalEvent(ResourceEvent event) {
    // TODO OFFHEAP MERGE: the fix for 49555 did the following:
//       if (listener instanceof LocalRegion) {
//         LocalRegion lr = (LocalRegion)listener;
//         if (lr.isDestroyed()) {
//           // do not added destroyed region to fix bug 49555
//           return;
//         }
//       }
// need to figure out where this code should now be    

    // Wait for an event to be handled by all listeners before starting to send another event
    synchronized (this.listeners) {
      this.resourceMonitors.get(event.getType()).notifyListeners(this.listeners.get(event.getType()), event);
    }
    
    this.stats.incResourceEventsDelivered();
  }
 
  public HeapMemoryMonitor getHeapMonitor() {
    return (HeapMemoryMonitor) this.resourceMonitors.get(ResourceType.HEAP_MEMORY);
  }
  
  public OffHeapMemoryMonitor getOffHeapMonitor() {
    return (OffHeapMemoryMonitor) this.resourceMonitors.get(ResourceType.OFFHEAP_MEMORY);
  }
  
  /**
   * Use threshold event processor to execute the event embedded in the
   * runnable.
   * 
   * @param runnable
   *          Runnable to execute.
   */
  void runWithNotifyExecutor(Runnable runnable) {
    try {
      this.notifyExecutor.execute(runnable);
    } catch (RejectedExecutionException e) {
      if (!isClosed()) {
        this.cache.getLoggerI18n().warning(LocalizedStrings.ResourceManager_REJECTED_EXECUTION_CAUSE_NOHEAP_EVENTS);
      }
    }
  }
  
  @Override
  public RebalanceFactory createRebalanceFactory() {
    return new RebalanceFactoryImpl();
  }
    
  @Override
  public Set<RebalanceOperation> getRebalanceOperations() {
    synchronized(this.inProgressOperationsLock) {
      return new HashSet<RebalanceOperation>(this.inProgressOperations);
    }
  }
  
  void addInProgressRebalance(RebalanceOperation op) {
    synchronized(this.inProgressOperationsLock) {
      this.inProgressOperations.add(op);
    }
  }
  
  void removeInProgressRebalance(RebalanceOperation op) {
    synchronized(this.inProgressOperationsLock) {
      this.inProgressOperations.remove(op);
    }
  }
  
  class RebalanceFactoryImpl implements RebalanceFactory {

    private Set<String> includedRegions; 
    private Set<String> excludedRegions;
    
    @Override
    public RebalanceOperation simulate() {
      RegionFilter filter = new FilterByPath(this.includedRegions, this.excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(InternalResourceManager.this.cache, true, filter);
      op.start();
      return op;
    }

    @Override
    public RebalanceOperation start() {
      RegionFilter filter = new FilterByPath(this.includedRegions, this.excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(InternalResourceManager.this.cache, false,filter);
      op.start();
      return op;
    }

    @Override
    public RebalanceFactory excludeRegions(Set<String> regions) {
      this.excludedRegions = regions;
      return this;
    }

    @Override
    public RebalanceFactory includeRegions(Set<String> regions) {
      this.includedRegions = regions;
      return this;
    }

  }
  
  void stopExecutor(ExecutorService executor) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    final int secToWait = Integer.getInteger("gemfire.prrecovery-close-timeout", 120).intValue();
    try {
      executor.awaitTermination(secToWait, TimeUnit.SECONDS);
    }
    catch (InterruptedException x) {
      Thread.currentThread().interrupt();
      logger.debug("Failed in interrupting the Resource Manager Thread due to interrupt");
    }
    if (! executor.isTerminated()) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.ResourceManager_FAILED_TO_STOP_RESOURCE_MANAGER_THREADS,
            new Object[]{secToWait}));
    }
  }

  public ScheduledExecutorService getExecutor() {
    return this.scheduledExecutor;
  }
  
  public ResourceManagerStats getStats() {
    return this.stats;
  }
  
  /**
   * For testing only, an observer which is called when
   * rebalancing is started and finished for a particular region.
   * This observer is called even the "rebalancing" is actually
   * redundancy recovery for a particular region.
   * @param observer
   */
  public static void setResourceObserver(ResourceObserver observer) {
    if(observer == null) {
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
  public static interface ResourceObserver {
    /**
     * Indicates that rebalancing has started on a given region.
     * @param region
     */
    public void rebalancingStarted(Region region);
    
    /**
     * Indicates that rebalancing has finished on a given region.
     * @param region
     */
    public void rebalancingFinished(Region region);
    
    /**
     * Indicates that recovery has started on a given region.
     * @param region
     */
    public void recoveryStarted(Region region);
    
    /**
     * Indicates that recovery has finished on a given region.
     * @param region
     */
    public void recoveryFinished(Region region);

    /**
     * Indicated that a membership event triggered a recovery
     * operation, but the recovery operation will not be executed
     * because there is already an existing recovery operation
     * waiting to happen on this region.
     * @param region
     */
    public void recoveryConflated(PartitionedRegion region);
   
    /**
     * Indicates that a bucket is being moved from the source member to the
     * target member.
     * @param region the region
     * @param bucketId the bucket being moved
     * @param source the member the bucket is moving from
     * @param target the member the bucket is moving to
     */
    public void movingBucket(Region region,
                             int bucketId, 
                             DistributedMember source, 
                             DistributedMember target);
    
    /**
     * Indicates that a bucket primary is being moved from the source member 
     * to the target member.
     * @param region the region
     * @param bucketId the bucket primary being moved
     * @param source the member the bucket primary is moving from
     * @param target the member the bucket primary is moving to
     */
    public void movingPrimary(Region region,
                              int bucketId, 
                              DistributedMember source, 
                              DistributedMember target);
  }
  
  public static class ResourceObserverAdapter implements ResourceObserver {
    
    
    @Override
    public void rebalancingFinished(Region region) {
      rebalancingOrRecoveryFinished(region);
      
    }
    @Override
    public void rebalancingStarted(Region region) {
      rebalancingOrRecoveryStarted(region);
      
    }
    @Override
    public void recoveryFinished(Region region) {
      rebalancingOrRecoveryFinished(region);
      
    }
    @Override
    public void recoveryStarted(Region region) {
      rebalancingOrRecoveryStarted(region);
    }
    /**
     * Indicated the a rebalance or a recovery has started.
     */
    @SuppressWarnings("unused")
    public void rebalancingOrRecoveryStarted(Region region) {
      //do nothing
    }
    /**
     * Indicated the a rebalance or a recovery has finished.
     */
    @SuppressWarnings("unused")
    public void rebalancingOrRecoveryFinished(Region region) {
      //do nothing
    }
    @Override
    public void recoveryConflated(PartitionedRegion region) {
      //do nothing
    }
    @Override
    public void movingBucket(Region region,
                             int bucketId, 
                             DistributedMember source, 
                             DistributedMember target) {
      //do nothing
    }
    @Override
    public void movingPrimary(Region region,
                              int bucketId, 
                              DistributedMember source, 
                              DistributedMember target) {
      //do nothing
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DistributionAdvisee#getCancelCriterion()
   */
  public CancelCriterion getCancelCriterion() {
    return this.cache.getCancelCriterion();
  }

  public ResourceAdvisor getResourceAdvisor() {
    return this.resourceAdvisor;
  }

  public LoadProbe getLoadProbe() {
    return this.loadProbe;
  }

  /**
   * This method is test purposes only.
   */
  public LoadProbe setLoadProbe(LoadProbe probe) {
    LoadProbe old = this.loadProbe;
    this.loadProbe = probe;
    return old;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#setEvictionHeapPercentage(int)
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
}
