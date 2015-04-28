/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ListenerNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.OverflowQueueWithDMStats;
import com.gemstone.gemfire.distributed.internal.SerialQueuedExecutorWithDMStats;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.GemFireStatSampler;
import com.gemstone.gemfire.internal.LocalStatListener;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.StatisticsImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEvictorTask;
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
 * @author Kirk Lund
 * @author Mitch Thomas
 * @author Swapnil Bawaskar
 */
@SuppressWarnings("synthetic-access")
public class InternalResourceManager implements ResourceManager, NotificationListener {
  private static final Logger logger = LogService.getLogger();

  private final ScheduledExecutorService executor;
  private final ExecutorService thresholdEventProcessor;
  //The set of in progress rebalance operations.
  private final Set<RebalanceOperation> inProgressOperations = new HashSet<RebalanceOperation>(); 
  private final Object inProgressOperationsLock = new Object();
  private ScheduledExecutorService pollerExecutor;

  final GemFireCacheImpl cache;
  
  
  private Map<ResourceListener<? extends ResourceEvent>,Boolean> resourceListeners = 
    new ConcurrentHashMap<ResourceListener<? extends ResourceEvent>, Boolean>();
  
  private Map<ResourceListener<MemoryEvent>,Boolean> memoryEventListeners = 
    new ConcurrentHashMap<ResourceListener<MemoryEvent>,Boolean>();
  
  private Map<ResourceListener<? extends ResourceEvent>,Boolean> rebalanceEventListeners = 
    new ConcurrentHashMap<ResourceListener<? extends ResourceEvent>, Boolean>();
  
  private LocalStatListener statListener = new LocalHeapStatListener();
  
  private final Object resourceListenersLock = new Object();
  
  private final LoadProbe loadProbe;
  
  private final ResourceManagerStats stats;
  private volatile boolean isClosed = false;
  private final ResourceAdvisor resourceAdvisor;
  
  private static ResourceObserver observer = new ResourceObserverAdapter();
  
  private static String PR_LOAD_PROBE_CLASS = System.getProperty(
      "gemfire.ResourceManager.PR_LOAD_PROBE_CLASS", SizedBasedLoadProbe.class
          .getName());
  
  // Allow for an unknown heap pool for VMs we may support in the future.
  private static final String HEAP_POOL = 
    System.getProperty("gemfire.ResourceManager.HEAP_POOL");
  
  private boolean backgroundThreadsDisabledForTest;

  public static InternalResourceManager getInternalResourceManager(Cache cache) {
    return (InternalResourceManager) cache.getResourceManager();
  }
  
  public static InternalResourceManager createResourceManager(final GemFireCacheImpl cache) {
    return new InternalResourceManager(cache).init();
  }

  private InternalResourceManager(GemFireCacheImpl cache) {
    this.cache = cache;
    this.resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    final ThreadGroup thrdGrp = LoggingThreadGroup.createThreadGroup(
        "ResourceManagerThreadGroup", logger);

    ThreadFactory tf = new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(thrdGrp, r,
        "ResourceManagerRecoveryThread");
        thread.setDaemon(true);
        return thread;
      }
    };
    
    this.executor = new ScheduledThreadPoolExecutor(1, tf);
    final ThreadGroup listenerInvokerthrdGrp = LoggingThreadGroup.createThreadGroup(
        "ResourceListenerInvokerThreadGroup", logger);

    this.stats = new ResourceManagerStats(cache.getDistributedSystem());

    ThreadFactory eventProcessorFactory = new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(listenerInvokerthrdGrp, r,
            "ThresholdEventProcessor");
        thread.setDaemon(true);
        thread.setPriority(Thread.MAX_PRIORITY);
        return thread;
      }
    };
    BlockingQueue<Runnable> threadQ = new OverflowQueueWithDMStats(
        this.stats.getResourceEventQueueStatHelper());

    this.thresholdEventProcessor = new SerialQueuedExecutorWithDMStats(
        threadQ, this.stats.getResourceEventPoolStatHelper(), eventProcessorFactory);

    try {
      Class loadProbeClass = ClassPathLoader.getLatest().forName(PR_LOAD_PROBE_CLASS);
      this.loadProbe = (LoadProbe) loadProbeClass.newInstance();
    } catch (Exception e) {
      throw new InternalGemFireError("Unable to instantiate " + PR_LOAD_PROBE_CLASS, e);
    }
  }

  // This method is mainly intended  to prevent early escaped references, please 
  // use this method (and not the constructor) whenever publishing a reference to this instance (particularly
  // if that reference will be available from other threads).
  private InternalResourceManager init() {
    //if stat sampling is enabled, use the HostStatSampler thread for polling
    final GemFireStatSampler sampler = getSystem().getStatSampler();
    boolean startPoller = true;
    if (sampler != null) {
      try {
        sampler.waitForInitialization();        //Fix for #40424
        String tenuredPoolName = getTenuredMemoryPoolMXBean().getName();
        List list = getSystem().getStatsList();
        synchronized (list) {
          for (Object o : list) {
            if (o instanceof StatisticsImpl) {
              StatisticsImpl si = (StatisticsImpl)o;
              if (si.getTextId().contains(tenuredPoolName) && si.getType().getName().contains("PoolStats")) {
                sampler.addLocalStatListener(statListener, si, "currentUsedMemory");
                startPoller = false;
                if (logger.isDebugEnabled()) {
                  logger.debug("Registered stat listener for {}", si.getTextId());
                }
              }
            }
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        cache.getCancelCriterion().checkCancelInProgress(e);
        //start the poller thread
      }
    }
    if (startPoller && getTenuredMemoryPoolMXBean() != DUMMYPOOLBEAN) {
      final ThreadGroup grp = LoggingThreadGroup.createThreadGroup("HeapPoller", logger);
      ThreadFactory tf = new ThreadFactory(){
        public Thread newThread(Runnable r) {
          Thread t = new Thread(grp, r, "GemfireHeapPoller");
          t.setDaemon(true);
          return t;
        }
      };
      this.pollerExecutor = Executors.newScheduledThreadPool(1, tf);
      this.pollerExecutor.scheduleAtFixedRate(new HeapPoller(), 
          POLLER_INTERVAL, POLLER_INTERVAL, TimeUnit.MILLISECONDS);
      if (logger.isDebugEnabled()) {
        logger.debug("Started GemfireHeapPoller to poll the heap every {} milliseconds", POLLER_INTERVAL);
      }
    }
    registerLocalVMThresholdListener(true, this.thresholds.get());
    return this;
  }
  
  public void close() {
    isClosed = true;
    unregisterLocalVMThresholdListener(true);
    closeHeapMonitoring();
    stopExecutor(this.executor);
    this.stats.close();
  }

  public RebalanceFactory createRebalanceFactory() {
    return new RebalanceFactoryImpl();
  }
    
  public Set<RebalanceOperation> getRebalanceOperations() {
    synchronized(inProgressOperationsLock) {
      return new HashSet<RebalanceOperation>(this.inProgressOperations);
    }
  }
  
  void addInProgressRebalance(RebalanceOperation op) {
    synchronized(inProgressOperationsLock) {
      inProgressOperations.add(op);
    }
  }
  
  void removeInProgressRebalance(RebalanceOperation op) {
    synchronized(inProgressOperationsLock) {
      inProgressOperations.remove(op);
    }
  }

  public void addResourceListener(ResourceListener listener) {
    addResourceListener(listener, listener.getClass());
  }
  
  private void addResourceListener(ResourceListener listener, Class listenerClass) {
    Type[] types = listenerClass.getGenericInterfaces();    
    if (listenerClass.getCanonicalName() != null &&     //not an anonymous class
        listenerClass.getCanonicalName().equals(Object.class.getCanonicalName())) {
      return;
    }
    assert types != null;

    synchronized (this.resourceListenersLock) {
      if (listener instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion)listener;
        if (lr.isDestroyed()) {
          // do not added destroyed region to fix bug 49555
          return;
        }
      }
      for (int i=0; i < types.length; i++) {
        String stringType = types[i].toString();
        if (stringType.contains(MemoryEvent.class.getName())) {
          this.memoryEventListeners.put(listener, Boolean.TRUE);
          if (listener instanceof LocalRegion) {
            LocalRegion lr = (LocalRegion)listener;
            lr.initialCriticalMembers(isHeapCritical(), getResourceAdvisor().adviseCritialMembers());
          }
        } else if (stringType.contains(PartitionRebalanceEvent.class.getName())) {
          this.rebalanceEventListeners.put(listener, Boolean.TRUE);
        } else if (stringType.contains(ResourceListener.class.getName())) {
          this.resourceListeners.put(listener, Boolean.TRUE);
        }
      }
    }
    Class superClass = listenerClass.getSuperclass();
    if (listenerClass.getCanonicalName() != null) {     //not an anonymous class
      addResourceListener(listener, superClass);
    }
  }

  public void removeResourceListener(ResourceListener<? extends ResourceEvent> listener) {
    synchronized (this.resourceListenersLock) {
      this.memoryEventListeners.remove(listener);
      this.rebalanceEventListeners.remove(listener);
      this.resourceListeners.remove(listener);
    }
  }
  
  public Set<ResourceListener<? extends ResourceEvent>> getResourceListeners() {
    Set<ResourceListener<? extends ResourceEvent>>listenerSet = 
      new HashSet<ResourceListener<? extends ResourceEvent>>();
    synchronized (this.resourceListenersLock) {
      listenerSet.addAll(this.memoryEventListeners.keySet());
      listenerSet.addAll(this.rebalanceEventListeners.keySet());
      listenerSet.addAll(this.resourceListeners.keySet());
    }
    return listenerSet;
  }
  
  public Set<ResourceListener<MemoryEvent>> getMemoryEventListeners() {
    return this.memoryEventListeners.keySet();
  }
  
  public Set<ResourceListener<? extends ResourceEvent>> getRebalanceListeners() {
    return this.rebalanceEventListeners.keySet();
  }
  
  /////////////////////////////// Heap Thresholds /////////////////////////////
  private volatile long originalByteThreshold;
  private final AtomicBoolean localHeapCritical = new AtomicBoolean(false);
  private final AtomicReference<Thresholds> thresholds = new AtomicReference<Thresholds>(new Thresholds());
  /**
   * When this property is set to true, a {@link LowMemoryException} is not thrown, even when heap usage
   * crosses critical threshold
   */
  private static final boolean DISABLE_LOW_MEM_EXCEPTION = Boolean.getBoolean("gemfire.disableLowMemoryException");
  /**
   * Heap usage must fall below THRESHOLD-THRESHOLD_THICKNESS before we deliver a down event
   */
  private static final double THRESHOLD_THICKNESS = Double.parseDouble(System.getProperty("gemfire.thresholdThickness","2.00"));
  /**
   * Time interval after which the genfireHeapPoller thread should poll the heap for usage information
   */
  private static final int POLLER_INTERVAL = Integer.getInteger("gemfire.heapPollerInterval",500).intValue();
  /**
   * Number of UP events that should be received before we deliver a UP events. This was introduced because
   * we saw sudden memory usage spikes in jrockit VM. 
   */
  private static final int MEMORY_EVENT_TOLERANCE;
  static {
    String vendor = System.getProperty("java.vendor");
    if (vendor.contains("Sun") || vendor.contains("Oracle")) {
      MEMORY_EVENT_TOLERANCE = Integer.getInteger("gemfire.memoryEventTolerance",1);
    } else {
      MEMORY_EVENT_TOLERANCE = Integer.getInteger("gemfire.memoryEventTolerance",5);
    }
  }
  private static long testTenuredGenUsedBytes = 0;

  /**
   * Keeps track of the current state of heap listener invocations
   */
  private final AtomicReference<MemoryEventImpl>
    heapListenerInvocationState = new AtomicReference<MemoryEventImpl>(MemoryEventImpl.UNKOWN);
  /**
   * keeps track of the number of critical up events received, for tolerance purpose.
   * We only deliver UP events when this counter exceeds {@link InternalResourceManager#MEMORY_EVENT_TOLERANCE}
   */
  private int criticalToleranceCounter;
  private int evictionToleranceCounter;
  /**
   * Registers this InternalResourceManager with the VM to get heap events.
   * package level access for tests
   * @param saveOriginalThreshold
   */
  void registerLocalVMThresholdListener(final boolean saveOriginalThreshold,
      final Thresholds currMem) {
    if (currMem.getCriticalThreshold() > 0) {
      //calculate the threshold
      final MemoryPoolMXBean p = getTenuredMemoryPoolMXBean();
      if (p!=null) {
        final long usageThreshold = p.getUsageThreshold();
        if (saveOriginalThreshold) {
          this.originalByteThreshold = usageThreshold;
        }
        logger.info(LocalizedMessage.create(
            LocalizedStrings.ResourceManager_OVERRIDDING_MEMORYPOOLMXBEAN_HEAP_0_NAME_1_WITH_2,
              new Object[] {Long.valueOf(usageThreshold),
              p.getName(),
              Long.valueOf(currMem.getCriticalThresholdBytes())}));
        setUsageThreshold(p, p.getUsage().getUsed(), currMem);
        //Set collection threshold to a low value, so that we can get notifications 
        //after every GC run. After each such collection threshold notification        
        //we set the usage thresholds to an appropriate value
        if (!inTestMode()) {
          p.setCollectionUsageThreshold(1);
        }
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        //account for young generation before setting the threshold
        NotificationEmitter emitter = (NotificationEmitter)mbean;
        emitter.addNotificationListener(this, null, null);
      } else {
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.ResourceManager_NO_MEMORY_POOL_FOUND_TO_ADD_NOTIFICATION_LISTENER));
      }
    }
  }

  /**
   * Unregister the MXMemory Bean Listener
   * @param restoreOriginalThreshold
   */
  private void unregisterLocalVMThresholdListener(boolean restoreOriginalThreshold) {
    NotificationEmitter emitter = 
      (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    try {
      emitter.removeNotificationListener(this, null, null);
      if (logger.isDebugEnabled()) {
        logger.debug("Removed Memory MXBean notification listener {}", this);
      }
    }
    catch (ListenerNotFoundException e) {
      logger.debug("This instance '{}' was not registered as a Memory MXBean listener", toString());
    }

    // Restore the original if told to do so, otherwise restore when the threshold was set to zero 
    if (restoreOriginalThreshold) {
      MemoryPoolMXBean p = getTenuredMemoryPoolMXBean();
      if (p!=null) {
        logger.info(LocalizedMessage.create(
            LocalizedStrings.ResourceManager_RESETTING_ORIGINAL_MEMORYPOOLMXBEAN_HEAP_THRESHOLD_BYTES,
            new Object[]{this.originalByteThreshold, p.getName()}));        
        p.setUsageThreshold(this.originalByteThreshold);
      }
    }
  }
  
  /**
   * Sets the usage threshold on the tenured pool to either the eviction threshold or
   * the critical threshold depending on the current number of bytes used
   * @param pool the tenured pool
   * @param currentUsage currently used bytes in tenured pool
   */
  private void setUsageThreshold(final MemoryPoolMXBean pool, final long currentUsage,
      final Thresholds t) {
    // this method has been made a no-op to fix bug 49064
  }
  
  /**
   * Sets the usage threshold on the tenured pool to either the eviction threshold or
   * the critical threshold depending on the current number of bytes used
   * @return current number of bytes used in the tenured pool
   */
  private long setUsageThresholdAndGetCurrentUsage(Thresholds t) {
    final MemoryPoolMXBean p = getTenuredMemoryPoolMXBean();
    final long usage = p.getUsage().getUsed();
    setUsageThreshold(p, usage, t);
    return usage;
  }

  public static final DummyMemoryPoolMXBean DUMMYPOOLBEAN = new DummyMemoryPoolMXBean();
  private static class DummyMemoryPoolMXBean implements MemoryPoolMXBean {
    private static MemoryUsage dummyUsage = new MemoryUsage(0, 0, 0, 0);
    private static final String name = "DummyMemoryPoolMXBean";
    private static final ObjectName objectName;
    static {
      try {
        objectName = new ObjectName(ManagementFactory.MEMORY_POOL_MXBEAN_DOMAIN_TYPE + ",name=" + name);
      } catch (MalformedObjectNameException e) {
        throw new IllegalStateException();
      }
    }
    private DummyMemoryPoolMXBean() {}
    public MemoryUsage getCollectionUsage() {
      return dummyUsage;
    }
    public long getCollectionUsageThreshold() {
      return 0;
    }
    public long getCollectionUsageThresholdCount() {
      return 0;
    }
    public String[] getMemoryManagerNames() {
      return new String[0];
    }
    public String getName() {
      return name;
    }
    public MemoryUsage getPeakUsage() {
      return dummyUsage;
    }
    public MemoryType getType() {
      return MemoryType.HEAP;
    }
    public MemoryUsage getUsage() {
      return dummyUsage;
    }
    public long getUsageThreshold() {
      return 0;
    }
    public long getUsageThresholdCount() {
      return 0;
    }
    public boolean isCollectionUsageThresholdExceeded() {
      return false;
    }
    public boolean isCollectionUsageThresholdSupported() {
      return false;
    }
    public boolean isUsageThresholdExceeded() {
      return false;
    }
    public boolean isUsageThresholdSupported() {
      return false;
    }
    public boolean isValid() {
      return false;
    }
    public void resetPeakUsage() {
    }
    public void setCollectionUsageThreshold(long threhsold) {
    }
    public void setUsageThreshold(long threshold) {
    }
    @Override
    public ObjectName getObjectName() {
      return objectName;
    }
  }

  /**
   * @return the tenured memory pool bean or null if none is found
   */
  public static MemoryPoolMXBean getTenuredMemoryPoolMXBean() {
    MemoryPoolMXBean ret = null;
    List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    for (MemoryPoolMXBean p : pools) {
      if (p.isUsageThresholdSupported() && isTenured(p)) {
        ret = p;
        // found it
        break;
      }
    }
    if (ret == null) {
      logger.error(LocalizedMessage.create(LocalizedStrings.ResourceManager_NO_POOL_FOUND_POOLS_0, getAllMemoryPoolNames()));
      ret = DUMMYPOOLBEAN;
    }
    return ret;
  }

  /**
   * Calculates the max memory for tenured pool. Works around JDK bug:
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7078465
   * by getting max memory from runtime and subtracting all other
   * heap pools from it.
   */
  public static long getTenuredPoolMaxMemory() {
    long max = getTenuredMemoryPoolMXBean().getUsage().getMax();
    if (max == -1) {
      max = Runtime.getRuntime().maxMemory();
      List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean p : pools) {
        if (p.getType() == MemoryType.HEAP && p.getUsage().getMax() != -1) {
          max -= p.getUsage().getMax();
        }
      }
    }
    return max;
  }

  private static String getAllMemoryPoolNames() {
    StringBuilder sb;
    Iterator<MemoryPoolMXBean> i = ManagementFactory.getMemoryPoolMXBeans().iterator();
    if (i.hasNext()) {
      sb = new StringBuilder("[");
    } else {
      return "";
    }
    while (i.hasNext()) {
      MemoryPoolMXBean p = i.next();
      sb.append("(Name=")
      .append(p.getName())
      .append(";Type=")
      .append(p.getType())
      .append(";UsageThresholdSupported=")
      .append(p.isUsageThresholdSupported())
      .append(")");
      if (i.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Select what kind of pools we register the listener on.
   * Package level visibility for testing purposes 
   * @param p
   * @return true if is a pool we need to register on.
   */
   static boolean isTenured(MemoryPoolMXBean p) {
    if (p.getType() == MemoryType.HEAP) {
      String name = p.getName();
      return name.equals("CMS Old Gen") // Sun Concurrent Mark Sweep GC
      || name.equals("PS Old Gen") // Sun Parallel GC
      || name.equals("G1 Old Gen") // Sun G1 GC
      || name.equals("Old Space") // BEA JRockit 1.5, 1.6 GC
      || name.equals("Tenured Gen") // Hitachi 1.5 GC 
      || name.equals("Java heap")   // IBM 1.5, 1.6 GC
      // Allow an unknown pool name to monitor
      || (HEAP_POOL != null && name.equals(HEAP_POOL));
    } else {
      return false;
    }
  }

  /* (non-Javadoc)
   * @see javax.management.NotificationListener#handleNotification
   * (javax.management.Notification, java.lang.Object)
   */
  public void handleNotification(Notification notification, Object callback) {
    executeInThresholdProcessor(new Runnable() {
      public void run() {
        //Not using the information given by the notification in favor
        //of constructing fresh information ourselves.
        if (!isBackgroundThreadsDisabledForTest()) {
          triggerMemoryEvent();
        }
      }
    });
  }

  /**
   * Triggers a memory event based on the current usage and threshold settings.
   */
  public void triggerMemoryEvent() {
    Thresholds t = thresholds.get();
    final long currentUsed = setUsageThresholdAndGetCurrentUsage(t);
    assert currentUsed >= 0;
    invokeMemoryEventListeners(currentUsed, t);  
  }
  
  /**
   * Use threshold event processor to execute the event embedded in the runnable
   * @param runnable
   */
  private void executeInThresholdProcessor(Runnable runnable) {
    try {
      this.thresholdEventProcessor.execute(runnable);
    } catch (RejectedExecutionException e) {
      if (!isClosed) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.ResourceManager_REJECTED_EXECUTION_CAUSE_NOHEAP_EVENTS));
      }
    }
  }

  protected void invokeMemoryEventListeners(final long currentOldGenUsage, Thresholds t) {
    assert currentOldGenUsage >= 0;
    stats.changeTenuredHeapUsed(currentOldGenUsage);
    if (currentOldGenUsage == 0) {
      return;
    }
    MemoryEventImpl event = new MemoryEventImpl(MemoryEventType.UNKNOWN,
                          cache.getMyId(),
                          0,
                          testTenuredGenUsedBytes==0 ? currentOldGenUsage:testTenuredGenUsedBytes,
                          0,
                          true, t);
    informListenersOfLocalEvent(event);
  }  

  /**
   * Invoke listeners after receiving and event from the local heap observer(s)
   * public for tests
   * @param newEvent
   */
  public void informListenersOfLocalEvent(MemoryEventImpl newEvent) {
    assert newEvent.isLocal();

    if (logger.isDebugEnabled()) {
      logger.debug("Handling new local event {}", newEvent);
    }
    synchronized (this.heapListenerInvocationState) {
      MemoryEventImpl oldState = this.heapListenerInvocationState.get();
      MemoryEventImpl[] eventsToDeliver = getValidatedEvents(newEvent, oldState);
      if (eventsToDeliver == null) {
        eventsToDeliver = getEvictMore(oldState, newEvent);
        if (eventsToDeliver == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("No events to deliver");
          }
          return;
        }
      }
      MemoryEventImpl newState = eventsToDeliver[eventsToDeliver.length-1];

      if (!newState.getType().isDisabledType() && !newState.skipValidation()
          && !newState.getType().isEvictMore()) {
        this.heapListenerInvocationState.set(newState);
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Local events to deliver: {}", getEventArrayAsString(eventsToDeliver));
      }

      logCriticalEventsAndSetQueryMonitor(eventsToDeliver);

      if (!newState.getType().isEvictMore()) {
        informRemoteResourceManagers(eventsToDeliver);
      }

      invokeLocalListeners(eventsToDeliver);
    }
  }

  /**
   * A profile has arrived with information about a remote state change.
   * Invoke local listeners of the change
   * @param oldEvent
   * @param newEvent
   * @return true if the listeners were invoked, otherwise false
   */
  boolean informListenersOfRemoteEvent(final MemoryEventImpl newEvent,
      final MemoryEventImpl oldEvent) {
    assert ! (newEvent.isLocal() && oldEvent.isLocal());
    assert Assert.assertHoldsLock(this,false);

    // egrep -i "remote events* to deliver for member"
    if (logger.isDebugEnabled()) {
      logger.debug("New remote event to deliver for member {}: new={} old={}",
          newEvent.getMember(), newEvent, oldEvent);
    }
    final MemoryEventImpl[] eventsToDeliver = getValidatedEvents(newEvent, oldEvent);
    if (eventsToDeliver == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("No remote events to deliver for member {}", newEvent.getMember());
      }
      return false;
    } else if (eventsToDeliver[0].equals(MemoryEventImpl.NO_DELIVERY)) {
      if (logger.isDebugEnabled()) {
        logger.debug("Suppressed remote events to deliver for member {}", newEvent.getMember());
      }
      return true;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Remote events to deliver for member {}:{}",
          newEvent.getMember(), getEventArrayAsString(eventsToDeliver));
    }
    // Sync added to co-ordinate remove VM state (via the ResourceAdvisor)
    // so that listeners get a stable snapshot of remote critical VMs
    synchronized(this.resourceListenersLock) {
      executeInThresholdProcessor(new Runnable(){
        public void run() {
          invokeLocalListeners(eventsToDeliver);
        }
      });
    }
    return true;
  }

  private void logCriticalEventsAndSetQueryMonitor(MemoryEventImpl[] eventsToDeliver) {
    for (int i=0; i<eventsToDeliver.length; i++) {
      final MemoryEventImpl event = eventsToDeliver[i];
      if (event.isLocal()) {
        if (event.getType().isCriticalUp()) {
          logger.error(LocalizedMessage.create(LocalizedStrings.ResourceManager_MEMBER_ABOVE_CRITICAL_THRESHOLD,
              new Object[] {event.getMember()}));
          if (!cache.isQueryMonitorDisabledForLowMemory()) {
            QueryMonitor.setLowMemory(true, event.getCurrentHeapBytesUsed());
            cache.getQueryMonitor().cancelAllQueriesDueToMemory();
          }
        } else if (event.getType().isCriticalDown()) {
          logger.error(LocalizedMessage.create(LocalizedStrings.ResourceManager_MEMBER_BELOW_CRITICAL_THRESHOLD,
                new Object[] {event.getMember()}));
          if (!cache.isQueryMonitorDisabledForLowMemory()) {
            QueryMonitor.setLowMemory(false, event.getBytesFromThreshold());
          }
        } else if (event.getType().isEvictionUp()) {
          logger.info(LocalizedMessage.create(LocalizedStrings.ResourceManager_MEMBER_ABOVE_HIGH_THRESHOLD,
                new Object[] {event.getMember()}));
        } else if (event.getType().isEvictionDown()) {
          logger.info(LocalizedMessage.create(LocalizedStrings.ResourceManager_MEMBER_BELOW_HIGH_THRESHOLD,
                new Object[] {event.getMember()}));
        }
      }
      setLocalHeapStatus(event);
    }
  }

  private void invokeLocalListeners(MemoryEventImpl[] eventsToDeliver) {
    for (int i=0; i<eventsToDeliver.length; i++) {
      final MemoryEventImpl event = eventsToDeliver[i];
      Set<ResourceListener<MemoryEvent>> memoryListeners = getMemoryEventListeners();
      for (ResourceListener<MemoryEvent> listener : memoryListeners) {
        try {
          listener.onEvent(event);
        } catch (CancelException ignore) {
          // ignore
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.error(LocalizedMessage.create(
              LocalizedStrings.ResourceManager_EXCEPTION_OCCURED_IN_RESOURCELISTENER, t));
        }
      }
      stats.incResourceEventsDelivered();
    }
  }
  /**
   * @param eventsToDeliver
   */
  private void informRemoteResourceManagers(MemoryEventImpl[] eventsToDeliver) {
    assert Assert.assertHoldsLock(this.heapListenerInvocationState,true);
    if (logger.isDebugEnabled()) {
      logger.debug("Informing remote members of events {}", getEventArrayAsString(eventsToDeliver));
    }

    // Send message to remote VMs with event details
    this.resourceAdvisor.informRemoteManagers(eventsToDeliver);
  }

  /**
   * @param event
   */
  private void setLocalHeapStatus(MemoryEventImpl event) {
    if (event.isLocal()) {
       if (event.getType().isCriticalUp()) {
         this.localHeapCritical.set(true);
         stats.incHeapCriticalEvents();
       } else if (event.getType().isCriticalDown()) {
         this.localHeapCritical.set(false);
         stats.incHeapSafeEvents();
       } else if (event.getType().isEvictionUp()) {
         stats.incEvictionStartEvents();
       } else if (event.getType().isEvictionDown()) {
         stats.incEvictionStopEvents();
       } else if (event.getType().isEvictMore()) {
         stats.incEvictMoreEvents();
       }
    }
  }

  /**
   * Ensures that all the events are delivered to the listeners. For eg. it is possible that
   * we get a notification from the JVM only when it exceeded the critical threshold. This 
   * method will detect that the eviction event was skipped, and therefore will return
   * an array of two events, eviction event and critical event.
   * This method returns null if the event should be ignored
   * @param nev the new event to be evaluated
   * @param oev the old event previously delivered
   * @return array of events to be delivered (including missing events); null if the event should
   * be ignored
   */
  private MemoryEventImpl[] getValidatedEvents(final MemoryEventImpl nev, final MemoryEventImpl oev) {
    assert nev != null;
    final MemoryEventImpl event;
    MemoryEventImpl[] retVal = null;
    final MemoryEventType oldState = oev.getType();

    if (nev.getType().isUnknown()) {
      event = buildEvent(nev, oldState);
    } else {
      event = nev;
    }
    assert event != null;
    //check for forced event
    if (event.skipValidation()) {
      return new MemoryEventImpl[] {event};
    }
    //check if event is in THRESHOLD_THICKNESS
    if (isEventInThresholdThickness(event)) {
      return null;
    }

    final boolean isCriticalDisabled = nev.getThresholds().isCriticalThresholdDisabled();
    final boolean isEvictionDisabled = nev.getThresholds().isEvictionThresholdDisabled();
    //don't deliver events if threshold is disabled
    if ((isCriticalDisabled && event.getType().isCriticalType()) ||
        (isEvictionDisabled && event.getType().isEvictionType())) {
      if (logger.isDebugEnabled()) {
        logger.debug("Threshold disabled: {} not delivered", event);
      }
      return null;
    }
    if ( !oldState.isEvictionUp() && oldState.equals(event.getType())) {
      //ignore the duplicate event
      resetThresholdCounters();
      if (logger.isDebugEnabled()) {
        logger.debug("Ignoring duplicate event {}", event);
      }
      return null;
    }
    //Just return the disable events, duplicate disable events will not reach here
    if (event.isDisableEvent()) {
      retVal = new MemoryEventImpl[1];
      retVal[0] = event;
      return retVal;
    }
    MemoryEventImpl[] events = oev.getType().getMissingEvents(event);
    return applyEventTolerance(events);
  }

  /**
   * To avoid memory spikes in jrockit, we only deliver up events if we receive
   * more than {@link InternalResourceManager#MEMORY_EVENT_TOLERANCE} events
   * @param events events on which tolerance is to be applied
   * @return events that should be delivered if the tolerance criterion is met,
   *  null otherwise
   */
  private MemoryEventImpl[] applyEventTolerance(MemoryEventImpl[] events) {
    if (events == null) {
      resetThresholdCounters();
      return null;
    }
    MemoryEventImpl newState = events[events.length-1];
    // don't apply tolerance to remote events
    if (!newState.isLocal()) {
      return events;
    }
    if (newState.getType().isEvictionUp()) {
      evictionToleranceCounter++;
      criticalToleranceCounter = 0;
      if (evictionToleranceCounter <= getMemoryEventTolerance()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Event {} ignored. toleranceCounter:{} MEMORY_EVENT_TOLERANCE:{}",
              newState, evictionToleranceCounter, getMemoryEventTolerance());
        }
        return null;
      }
    } else if (newState.getType().isCriticalUp()) {
      criticalToleranceCounter++;
      evictionToleranceCounter = 0;
      if (criticalToleranceCounter <= getMemoryEventTolerance()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Event {} ignored. toleranceCounter:{} MEMORY_EVENT_TOLERANCE:{}",
              newState, evictionToleranceCounter, getMemoryEventTolerance());
        }
        return null;
      }
    } else {
      resetThresholdCounters();
    }
    return events;
  }

  private void resetThresholdCounters() {
    criticalToleranceCounter = 0;
    evictionToleranceCounter = 0;
    if (logger.isDebugEnabled()) {
      logger.debug("TOLERANCE counters reset");
    }
  }

  /**
   * 
   * @param event
   * @param prevType
   * @return the event to deliver, null if the event is between THRESHOLD_THICKNESS
   */
  private MemoryEventImpl buildEvent(MemoryEventImpl event, MemoryEventType prevType) {
    MemoryEventImpl retVal = null;
    Thresholds currMem = event.getThresholds();
    if (currMem.isCriticalThresholdEnabled() && 
	event.getCurrentHeapBytesUsed() >= currMem.getCriticalThresholdBytes()) {      
      retVal = new MemoryEventImpl(MemoryEventType.CRITICAL_UP,
              event.getMember(),
              convertToIntPercent(event.getCurrentHeapBytesUsed()/currMem.getTenuredGenMaxBytes()),
              event.getCurrentHeapBytesUsed(),
              event.getCurrentHeapBytesUsed() - currMem.getCriticalThresholdBytes(),
              event.isLocal(), event.getThresholds());
    } else if (event.getCurrentHeapBytesUsed() < currMem.getEvictionThresholdSafeBytes()) {
      retVal = new MemoryEventImpl(MemoryEventType.EVICTION_DOWN,
              event.getMember(),
              convertToIntPercent(event.getCurrentHeapBytesUsed()/currMem.getTenuredGenMaxBytes()),
              event.getCurrentHeapBytesUsed(),
              currMem.getEvictionThresholdBytes() - event.getCurrentHeapBytesUsed(),
              event.isLocal(), event.getThresholds());
    } else {
      //in between critical and eviction threshold, so use prevType to decide
      //we will loose events here if the sampling rate is too slow
      if (prevType.isUnknown() || prevType.isEvictionType()) {
        //we generate a event even if the previous event was a EVICTION_UP
        retVal = new MemoryEventImpl(MemoryEventType.EVICTION_UP,
              event.getMember(),
              convertToIntPercent(event.getCurrentHeapBytesUsed()/currMem.getTenuredGenMaxBytes()),
              event.getCurrentHeapBytesUsed(),
              event.getCurrentHeapBytesUsed() - currMem.getEvictionThresholdBytes(),
              event.isLocal(), event.getThresholds());
      } else {
        //in between thresholds and prevType was critical
        retVal = new MemoryEventImpl(MemoryEventType.CRITICAL_DOWN,
              event.getMember(),
              convertToIntPercent(event.getCurrentHeapBytesUsed()/currMem.getTenuredGenMaxBytes()),
              event.getCurrentHeapBytesUsed(),
              currMem.getCriticalThresholdBytes() - event.getCurrentHeapBytesUsed(),
              event.isLocal(), event.getThresholds());
      }
    }
    return retVal;
  }
  
  private boolean isEventInThresholdThickness(final MemoryEventImpl event) {
    Thresholds currMem = event.getThresholds();
    if (event.getType().isCriticalDown() &&
        event.getCurrentHeapBytesUsed() > currMem.getCriticalThresholdSafeBytes()) {
      return true;      
    } else if (event.getType().isEvictionDown() &&
        event.getCurrentHeapBytesUsed() > currMem.getEvictionThresholdSafeBytes()) {
      return true;
    }
    return false;
  }

  private MemoryEventImpl[] getEvictMore(MemoryEventImpl oldState, MemoryEventImpl newEvent) {
    MemoryEventImpl[] retVal = null;
    if (!oldState.getType().isEvictionUp()
        && !oldState.getType().isCriticalType()) {
      return retVal;
    }
    if (cache != null &&            //fix for #40553
        cache.getHeapEvictor().getRunningAndScheduledTasks() == 0) {
      // EvictionBurstPauseTimeMillis and LastTaskCompletionTime are zero for tests
      long lastTaskCompletion = RegionEvictorTask.getLastTaskCompletionTime();
      if (lastTaskCompletion == 0 // for bug 41938
          || (System.currentTimeMillis() - lastTaskCompletion) >= RegionEvictorTask.getEvictionBurstPauseTimeMillis()) {
        retVal = new MemoryEventImpl[] {new MemoryEventImpl(newEvent, MemoryEventType.EVICT_MORE)};
      }
    }
    return retVal;
  }

  class LocalHeapStatListener implements LocalStatListener {
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.LocalStatListener#statValueChanged(double)
     */
    public void statValueChanged(double value) {
      final long usedBytes = (long)value;
      try {
        getThresholdEventProcessor().execute(new Runnable(){
          public void run() {
            if (!isBackgroundThreadsDisabledForTest()) {
              invokeMemoryEventListeners(usedBytes, thresholds.get());
            }
          }
        });
        if (logger.isDebugEnabled()) {
          logger.debug("StatSampler scheduled a handleNotification call with {} bytes", usedBytes);
        }
      } catch (RejectedExecutionException e) {
        if (!isClosed) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ResourceManager_REJECTED_EXECUTION_CAUSE_NOHEAP_EVENTS));
        }
      } catch (CacheClosedException e) {
        // nothing to do
      }
    }
  }
  
  private void closeHeapMonitoring() {
    stopExecutor(this.thresholdEventProcessor);
    stopExecutor(this.pollerExecutor);    
    final GemFireStatSampler sampler = getSystem().getStatSampler();
    if (sampler != null) {
      sampler.removeLocalStatListener(statListener);
    }
  }
  
  public ExecutorService getThresholdEventProcessor() {
    return thresholdEventProcessor;
  }
  ////////////////////////////////End Heap Threshold///////////////////////////
  
  class RebalanceFactoryImpl implements RebalanceFactory {

    private Set<String> includedRegions; 
    private Set<String> excludedRegions;
    
    public RebalanceOperation simulate() {
      RegionFilter filter = new FilterByPath(includedRegions, excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(cache, true, filter);
      op.start();
      return op;
    }

    public RebalanceOperation start() {
      RegionFilter filter = new FilterByPath(includedRegions, excludedRegions);
      RebalanceOperationImpl op = new RebalanceOperationImpl(cache, false,filter);
      op.start();
      return op;
    }

    public RebalanceFactory excludeRegions(Set<String> regions) {
      this.excludedRegions = regions;
      return this;
    }

    public RebalanceFactory includeRegions(Set<String> regions) {
      this.includedRegions = regions;
      return this;
    }

  }
  
  private void stopExecutor(@SuppressWarnings("hiding")
  ExecutorService executor) {
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
    return executor;
  }
  
  public ResourceManagerStats getStats() {
    return stats;
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
   * @author dsmith
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
    
    
    public void rebalancingFinished(Region region) {
      rebalancingOrRecoveryFinished(region);
      
    }
    public void rebalancingStarted(Region region) {
      rebalancingOrRecoveryStarted(region);
      
    }
    public void recoveryFinished(Region region) {
      rebalancingOrRecoveryFinished(region);
      
    }
    public void recoveryStarted(Region region) {
      rebalancingOrRecoveryStarted(region);
    }
    /**
     * Indicated the a rebalance or a recovery has started.
     */
    public void rebalancingOrRecoveryStarted(Region region) {
      //do nothing
    }
    /**
     * Indicated the a rebalance or a recovery has finished.
     */
    public void rebalancingOrRecoveryFinished(Region region) {
      //do nothing
    }
    public void recoveryConflated(PartitionedRegion region) {
      //do nothing
    }
    public void movingBucket(Region region,
                             int bucketId, 
                             DistributedMember source, 
                             DistributedMember target) {
      //do nothing
    }
    public void movingPrimary(Region region,
                              int bucketId, 
                              DistributedMember source, 
                              DistributedMember target) {
      //do nothing
    }
  }

  /**
   * Convenience method which delegates to the {@link ResourceAdvisor}
   * @see ResourceAdvisor#adviseCritialMembers()
   */
  public Set<InternalDistributedMember> getHeapCriticalMembers() {
    return this.resourceAdvisor.adviseCritialMembers();
  }

  /**
   * Get local critical heap state 
   * @return true if exceeded otherwise false
   */
  public boolean isHeapCritical() {    
    return this.localHeapCritical.get();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#getCriticalHeapPercentage()
   */
  public float getCriticalHeapPercentage() {
    return thresholds.get().getCriticalThreshold();
  }
  public boolean hasCriticalThreshold() {
    return this.thresholds.get().hasCriticalThreshold();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#setCriticalHeapPercentage(int)
   */
  public void setCriticalHeapPercentage(final float heapPercentage) {
    if (heapPercentage > 100.0f || heapPercentage < 0.0f) {
      throw new IllegalArgumentException(
          LocalizedStrings.ResourceManager_CRITICAL_PERCENTAGE_GT_ZERO_AND_LTE_100
          .toLocalizedString());
    }
    if (getTenuredMemoryPoolMXBean() == DUMMYPOOLBEAN) {
      throw new IllegalStateException(LocalizedStrings.ResourceManager_NO_POOL_FOUND_POOLS_0.toLocalizedString(getAllMemoryPoolNames()));
    }
    // Use sync to order configuration of the thresholds
    final boolean disabled = heapPercentage == 0.0f;
    synchronized (this.heapListenerInvocationState) {
      Thresholds currMem = this.thresholds.get();
      if (!disabled &&
          currMem.isEvictionThresholdEnabled() &&
          heapPercentage < currMem.getEvictionThreshold()) {
        throw new IllegalArgumentException(
            LocalizedStrings.ResourceManager_CRITICAL_PERCENTAGE_GTE_EVICTION_PERCENTAGE
            .toLocalizedString());
      }
      boolean wasEqual = heapPercentage == currMem.getCriticalThreshold();
      final Thresholds newMem = new Thresholds(currMem.getTenuredGenMaxBytes(), false,
                                               heapPercentage, true,
                                               currMem.getEvictionThreshold(), currMem.hasEvictionThreshold());
      if (wasEqual) {
        // need to do this so that the has flag will be set correctly
        this.thresholds.set(newMem);
        return;
      }
      unregisterLocalVMThresholdListener(disabled);
      cache.setQueryMonitorRequiredForResourceManager(!disabled);
      if (! disabled) {
        // Deliver critical event if in "the red zone"
        MemoryEventImpl eventAfterEnable = getEventAfterEnablingThreshold(
            MemoryEventType.CRITICAL_UP, newMem);
        if (eventAfterEnable != null) {
          informListenersOfLocalEvent(eventAfterEnable);
        }
      } else {
        long tenuresUsage = getTenuredHeapUsage();
        informListenersOfLocalEvent(new MemoryEventImpl(MemoryEventType.CRITICAL_DISABLED,
            this.cache.getMyId(), 
            convertToIntPercent(tenuresUsage/newMem.getTenuredGenMaxBytes()), 
            tenuresUsage, 0, true, newMem));
        resetOldState(newMem);
      }
      // Set this *after* invoking listeners so no other threads can deliver events
      // for the new threshold before this setter thread has delivered its events
      this.thresholds.set(newMem);
      registerLocalVMThresholdListener(false, newMem);
//      heapListenerInvocationState.set(MemoryEventType.UNKNOWN);
      stats.changeCriticalThreshold(newMem.getCriticalThresholdBytes());
    }
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("ResourceManager@").append(System.identityHashCode(this))
      .append("[")
      .append("criticalHeapPercentage=").append(getCriticalHeapPercentage())
      .append(";evictionHeapPercentage=").append(getEvictionHeapPercentage())
      .append("]")
      .toString();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#getEvictionHeapPercentage()
   */
  public float getEvictionHeapPercentage() {
    return this.thresholds.get().getEvictionThreshold();
  }
  public boolean hasEvictionThreshold() {
    return this.thresholds.get().hasEvictionThreshold();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.control.ResourceManager#setEvictionHeapPercentage(int)
   */
  public void setEvictionHeapPercentage(final float heapPercentage) {
    if (heapPercentage > 100.0f || heapPercentage < 0.0f) {
      throw new IllegalArgumentException(
          LocalizedStrings.ResourceManager_EVICTION_PERCENTAGE_GT_ZERO_AND_LTE_100
          .toLocalizedString());
    }
    if (getTenuredMemoryPoolMXBean() == DUMMYPOOLBEAN) {
      throw new IllegalStateException(LocalizedStrings.ResourceManager_NO_POOL_FOUND_POOLS_0.toLocalizedString(getAllMemoryPoolNames()));
    }
    // Use sync to order configuration of the thresholds
    final boolean disabled = heapPercentage == 0.0f;
    synchronized (this.heapListenerInvocationState) {
      Thresholds currentMemory = this.thresholds.get();
      if (! disabled &&
          currentMemory.isCriticalThresholdEnabled() &&
          heapPercentage > currentMemory.getCriticalThreshold()) {
        throw new IllegalArgumentException(
            LocalizedStrings.ResourceManager_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE
            .toLocalizedString());
      }
      boolean wasEqual = currentMemory.getEvictionThreshold() == heapPercentage;
      Thresholds newMem = new Thresholds(currentMemory.getTenuredGenMaxBytes(), false,
                                         currentMemory.getCriticalThreshold(), currentMemory.hasCriticalThreshold(),
                                         heapPercentage, true);
      if (wasEqual) {
        // need to do this so that the has flag will be set correctly
        this.thresholds.set(newMem);
        return;
      }
      unregisterLocalVMThresholdListener(disabled);
      if (! disabled) {
        // Deliver eviction event if tenured heap equal to or exceeds threshold
        MemoryEventImpl eventAfterEnable = getEventAfterEnablingThreshold(
            MemoryEventType.EVICTION_UP, newMem);
        if (eventAfterEnable != null) {
          informListenersOfLocalEvent(eventAfterEnable);
        }
      } else {
        long tenuredUsage = getTenuredHeapUsage();
        informListenersOfLocalEvent(new MemoryEventImpl(MemoryEventType.EVICTION_DISABLED,
            this.cache.getMyId(),
            convertToIntPercent(tenuredUsage/newMem.getTenuredGenMaxBytes()), 
            tenuredUsage, 0, true, newMem));
        resetOldState(newMem);
        // TODO prevent new regions from being added that call for heap eviction
      }
      // Set this *after* invoking listeners so no other threads can deliver events
      // for the new threshold before this setter thread has delivered its events
      this.thresholds.set(newMem);
      registerLocalVMThresholdListener(false, newMem);
      stats.changeEvictionThreshold(newMem.getEvictionThresholdBytes());
    }
  }

  /**
   * When both the thresholds become zero, makes previous state UNKNOWN
   * @param t the new Thresholds object
   */
  private void resetOldState(Thresholds t) {
    assert Assert.assertHoldsLock(this.heapListenerInvocationState,true);
    if (t.getCriticalThreshold() == 0f && t.getEvictionThreshold() == 0f) {
      heapListenerInvocationState.set(MemoryEventImpl.UNKOWN);
    }
  }

  /**
   * Generates a special event which will be delivered irrespective of the 
   * current state machine. This method is meant to be used from the threshold
   * setters. 
   * @param type threshold being set (only accepts {@link MemoryEventType#CRITICAL_UP}
   * and {@link MemoryEventType#EVICTION_UP}
   * @param newThreshold the newly computed threshold
   * @return the event to be delivered if the current heap usage is above the 
   * enabled threshold, null otherwise
   */
  private MemoryEventImpl getEventAfterEnablingThreshold(
      MemoryEventType type, Thresholds newThreshold) {
    assert Assert.assertHoldsLock(this.heapListenerInvocationState,true);
    MemoryEventImpl retVal = null;
    MemoryEventImpl oldState = heapListenerInvocationState.get();
    boolean skipValidation = true;
    final long currentHeapUsage = getTenuredHeapUsage();
    long thresholdBytes = MemoryEventType.getThresholdBytesForForcedEvents(type, newThreshold);
    if (currentHeapUsage > thresholdBytes) {
      if (type.isEvictionUp() && (oldState.getType().isEvictionDown()
                                  || oldState.getType().isUnknown())) {
        skipValidation = false;
      } else if (type.isCriticalUp() && !oldState.getType().isCriticalUp()) {
        skipValidation = false;
      }
      retVal = new MemoryEventImpl(type, currentHeapUsage, cache.getMyId(), 
          true/*isLocal*/, newThreshold, skipValidation);
    }
    return retVal;
  }

  /**
   * Convert a percentage as a double to an integer e.g. 0.09 => 9
   * also legal is 0.095 => 9
   * @param percentHeap a percentage value expressed as a double e.g. 9.5% => 0.095
   * @return the calculated percent as an integer >= 0 and <= 100
   */
  public static int convertToIntPercent(final double percentHeap) {
    assert percentHeap >= 0.0 && percentHeap <= 1.0;
    int ret = (int) Math.ceil(percentHeap * 100.0);
    assert ret >= 0 && ret <= 100;
    return ret;
  }

  /**
   * Convert a percentage as an integer to a double e.g. 9 => 0.09
   * @param percentHeap a percentage value expressed as an integer
   * @return the calculated percent as a double >= 0.0 and <= 1.0
   */
  public static double convertToDoublePercent(final int percentHeap) {
    assert percentHeap >= 0 && percentHeap <= 100;
    double ret = percentHeap * 0.01;
    assert ret >= 0.0 && ret <= 1.0;
    return ret;
  }

  /**
   * Fetch the number of bytes used in tenured heap
   * @return the number of bytes used
   */
  public static long getTenuredHeapUsage() {
    if (inTestMode()) {
      return testTenuredGenUsedBytes;
    }
    MemoryPoolMXBean p = getTenuredMemoryPoolMXBean();
    assert p!=null;
    MemoryUsage usage = p.getUsage();
    assert usage!=null;
    final long used = usage.getUsed();
    return used;
  }  

  private int getMemoryEventTolerance() {
    if (inTestMode()) {
      return 0;
    }
    return MEMORY_EVENT_TOLERANCE;
  }
  /**
   * For testing purpose only
   */
  private void setTenuredGenerationMaxBytesForTest(long tenuredGenerationBytes) {
    final Thresholds currMem = this.thresholds.get();
    Thresholds newMem;
    if (tenuredGenerationBytes == 0) {
      newMem = new Thresholds();
    } else {
      newMem = new Thresholds(tenuredGenerationBytes, true,
                              currMem.getCriticalThreshold(), false,
                              currMem.getEvictionThreshold(), false);
    }    
    this.thresholds.set(newMem);
    StringBuilder builder = new StringBuilder("In testing, the following values were set");
    builder.append(" tenuredGenerationMaxBytes:"+newMem.getTenuredGenMaxBytes());
    builder.append(" criticalThresholdBytes:"+newMem.getCriticalThresholdBytes());
    builder.append(" evictionThresholdBytes:"+newMem.getEvictionThresholdBytes());
    logger.debug(builder.toString());
  }
  
  /**
   * For testing purpose only
   */
  public static void setTenuredGenUsedBytesForTest(long usedBytes) {
    testTenuredGenUsedBytes = usedBytes;
  }
  
  /**
   * For testing purpose only.
   * @param usedBytes what we want the tenured pool to say when asked about used bytes
   * @param maxBytes what we want the tenured pool to say when asked about maxBytes
   */
  public void setTenuredGenBytesForTest(long usedBytes, long maxBytes) {
    if (usedBytes > maxBytes) {
      throw new IllegalArgumentException("Used bytes has to be less than maxBytes");
    }
    setTenuredGenUsedBytesForTest(usedBytes);
    setTenuredGenerationMaxBytesForTest(maxBytes);
  }
  
  protected static boolean inTestMode() {
    return testTenuredGenUsedBytes != 0;
  }

  private String getEventArrayAsString(MemoryEventImpl[] eventsToDeliver) {
    StringBuilder str = new StringBuilder("");
    for (int i=0; i<eventsToDeliver.length; i++) {
      str.append("["+eventsToDeliver[i]+"],");
    }
    return str.toString();
  }

  public static class Thresholds {
    private final double tenuredGenerationMaxBytes;
    private final boolean hasTenuredGenerationMaxBytes;
    private final float criticalThreshold;
    private final boolean hasCriticalThreshold;
    private final float evictionThreshold;
    private final boolean hasEvictionThreshold;

    Thresholds() {
      this(getTenuredPoolMaxMemory(), false,
           DEFAULT_CRITICAL_HEAP_PERCENTAGE, false,
           DEFAULT_EVICTION_HEAP_PERCENTAGE, false);
    }

    /**
     * public for tests
     * @param tenuredGenerationMaxBytes
     * @param criticalThreshold
     * @param evictionThreshold
     */
    public Thresholds(double tenuredGenerationMaxBytes,
                      boolean hasTenuredGenerationMaxBytes,
                      float criticalThreshold,
                      boolean hasCriticalThreshold,
                      float evictionThreshold,
                      boolean hasEvictionThreshold) {
      this.tenuredGenerationMaxBytes = tenuredGenerationMaxBytes;
      this.hasTenuredGenerationMaxBytes = hasTenuredGenerationMaxBytes;
      assert criticalThreshold >= 0.0f && criticalThreshold <= 100.0f;
      this.criticalThreshold = criticalThreshold;
      this.hasCriticalThreshold = hasCriticalThreshold;
      assert evictionThreshold >= 0.0f && evictionThreshold <= 100.0f;
      this.evictionThreshold = evictionThreshold;
      this.hasEvictionThreshold = hasEvictionThreshold;
    }
    
    @Override
    public String toString() {
      return new StringBuilder()
        .append("Thresholds@").append(System.identityHashCode(this))
        .append(" tenuredGenerationMaxBytes:"+tenuredGenerationMaxBytes)
        .append(" criticalThreshold:"+criticalThreshold)
        .append(" evictionThreshold:"+evictionThreshold).toString();
    }
    
    public double getTenuredGenMaxBytes() {
      return tenuredGenerationMaxBytes;
    }
    public boolean hasTenuredGenerationMaxBytes() {
      return this.hasTenuredGenerationMaxBytes;
    }
    public float getCriticalThreshold() {
      return criticalThreshold;
    }
    public boolean hasCriticalThreshold() {
      return this.hasCriticalThreshold;
    }
    public long getCriticalThresholdBytes() {
      return (long)(getCriticalThreshold() * 0.01 * getTenuredGenMaxBytes());
    }
    public long getCriticalThresholdSafeBytes() {
      return (long)(getCriticalThresholdBytes() -
          (0.01 * THRESHOLD_THICKNESS * tenuredGenerationMaxBytes));
    }
    public float getEvictionThreshold() {
      return evictionThreshold;
    }
    public boolean hasEvictionThreshold() {
      return this.hasEvictionThreshold;
    }
    public long getEvictionThresholdBytes() {
      return (long)(getEvictionThreshold() * 0.01 * getTenuredGenMaxBytes());
    }
    public long getEvictionThresholdSafeBytes() {
      return (long)(getEvictionThresholdBytes() -
          (0.01 * THRESHOLD_THICKNESS * this.tenuredGenerationMaxBytes));
    }
    public boolean isCriticalThresholdEnabled() {
      return criticalThreshold > 0.0f;
    }
    public boolean isEvictionThresholdEnabled() {
      return evictionThreshold > 0.0f;
    }
    public boolean isCriticalThresholdDisabled() {
      return !isCriticalThresholdEnabled();
    }
    public boolean isEvictionThresholdDisabled() {
      return !isEvictionThresholdEnabled();
    }

    /**
     * Generate a Thresholds object from data available from the DataInput
     * @param in DataInput from which to read the data
     * @return a new instance of Thresholds
     * @throws IOException
     */
    public static Thresholds fromData(DataInput in) throws IOException {
      double tenuredGenerationMaxBytes = in.readDouble();
      boolean hasTenuredGenerationMaxBytes = in.readBoolean();
      float criticalThreshold = in.readFloat();
      boolean hasCriticalThreshold = in.readBoolean();
      float evictionThreshold = in.readFloat();
      boolean hasEvictionThreshold = in.readBoolean();
      return new Thresholds(tenuredGenerationMaxBytes, hasTenuredGenerationMaxBytes,
                            criticalThreshold, hasCriticalThreshold,
                            evictionThreshold, hasEvictionThreshold);
    }

    /**
     * Write the state of this to the DataOutput
     * @param out DataOutput on which to write internal state
     * @throws IOException
     */
    public void toData(DataOutput out) throws IOException {
      out.writeDouble(this.tenuredGenerationMaxBytes);
      out.writeBoolean(this.hasTenuredGenerationMaxBytes);
      out.writeFloat(this.criticalThreshold);
      out.writeBoolean(this.hasCriticalThreshold);
      out.writeFloat(this.evictionThreshold);
      out.writeBoolean(this.hasEvictionThreshold);
    }
  }
  
  /**
   * Polls the heap if stat sampling is disabled
   * @author sbawaska
   */
  class HeapPoller implements Runnable {
    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    public void run() {
      if (isBackgroundThreadsDisabledForTest()) {
        return;
      }
      try {
        long usedTenuredBytes = getTenuredMemoryPoolMXBean().getUsage().getUsed();
        invokeMemoryEventListeners(usedTenuredBytes, thresholds.get());
      } catch (Exception e) {
        logger.debug("Poller Thread caught exception: {}", e.getMessage(), e);
      }
      //TODO: do we need to handle errors too?
    }
  }

  public void fillInProfile(Profile profile) {
    assert profile instanceof ResourceManagerProfile;
    ResourceManagerProfile rmp = (ResourceManagerProfile) profile;
    MemoryEventImpl lastEvent = this.heapListenerInvocationState.get();
    rmp.setEventState(lastEvent.getCurrentHeapUsagePercent(),
        lastEvent.getCurrentHeapBytesUsed(), lastEvent.getType(), lastEvent.getThresholds());
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

  private InternalDistributedSystem getSystem() {
    return (InternalDistributedSystem)this.cache.getDistributedSystem(); 
  }

  public Thresholds getThresholds() {
    return this.thresholds.get();
  }

  /**
   * Given a set of members, determine if any member in the set is above
   * critical threshold
   * @param members the set of members
   * @return true if the given set contains a member above critical threshold,
   * false otherwise
   * @see ResourceManager#setCriticalHeapPercentage(float)
   */
  public boolean containsHeapCriticalMembers(final Set<InternalDistributedMember> members) {
    if (members.contains(cache.getMyId()) && isHeapCritical()) {
      return true;
    }
    final Set<InternalDistributedMember> criticalMembers = getHeapCriticalMembers();    
    return SetUtils.intersectsWith(members, criticalMembers);
  }

  /**
   * @param member
   * @return true if the member's memory is above critical threshold
   */
  public boolean isMemberHeapCritical(final InternalDistributedMember member) {
    if (member.equals(cache.getMyId()) && isHeapCritical()) {
      return true;
    }
    return getHeapCriticalMembers().contains(member);
  }
  
  public LoadProbe getLoadProbe() {
    return this.loadProbe;
  }

  public final static boolean isLowMemoryExceptionDisabled() {
    return DISABLE_LOW_MEM_EXCEPTION;
  }

  public boolean isBackgroundThreadsDisabledForTest() {
    return backgroundThreadsDisabledForTest;
  }

  public void setBackgroundThreadsDisabledForTest(
      boolean disableBackgroundThreadsForTest) {
    this.backgroundThreadsDisabledForTest = disableBackgroundThreadsForTest;
  }
}
