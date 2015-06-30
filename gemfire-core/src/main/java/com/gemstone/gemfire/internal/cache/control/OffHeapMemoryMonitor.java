/*=========================================================================
 * Copyright (c) 2002-2013 VMware, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * more patents listed at http://www.vmware.com/go/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryThresholds.MemoryState;
import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.offheap.MemoryAllocator;
import com.gemstone.gemfire.internal.offheap.MemoryUsageListener;

/**
 * Allows for the setting of eviction and critical thresholds. These thresholds
 * are compared against current off-heap usage and, with the help of {#link
 * InternalResourceManager}, dispatches events when the thresholds are crossed.
 *
 * @author David Hoots
 * @since 9.0
 */
public class OffHeapMemoryMonitor implements ResourceMonitor, MemoryUsageListener {
  private static final Logger logger = LogService.getLogger();
  private volatile MemoryThresholds thresholds = new MemoryThresholds(0);
  private volatile MemoryEvent mostRecentEvent = new MemoryEvent(ResourceType.OFFHEAP_MEMORY, MemoryState.DISABLED,
      MemoryState.DISABLED, null, 0L, true, this.thresholds);
  private volatile MemoryState currentState = MemoryState.DISABLED;

  // This holds a new event as it transitions from updateStateAndSendEvent(...) to fillInProfile()
  private ThreadLocal<MemoryEvent> upcomingEvent = new ThreadLocal<MemoryEvent>();
  
  // Set when startMonitoring() and stopMonitoring() are called
  // Package private for testing
  Boolean started = false;
  
  // Set to true when setEvictionThreshold(...) is called.
  private boolean hasEvictionThreshold = false;

  private OffHeapMemoryUsageListener offHeapMemoryUsageListener;
  
  private final InternalResourceManager resourceManager;
  private final ResourceAdvisor resourceAdvisor;
  private final GemFireCacheImpl cache;
  private final ResourceManagerStats stats;
  private final MemoryAllocator memoryAllocator;
  private final LogWriterI18n log;

  OffHeapMemoryMonitor(final InternalResourceManager resourceManager, final GemFireCacheImpl cache, final ResourceManagerStats stats) {
    this.resourceManager = resourceManager;
    this.resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    this.cache = cache;
    this.stats = stats;
    
    this.memoryAllocator = cache.getOffHeapStore();
    if (this.memoryAllocator != null) {
      this.thresholds = new MemoryThresholds(this.memoryAllocator.getTotalMemory());
    }
    
    this.log = cache.getLoggerI18n();
  }

  /**
   * Start monitoring off-heap memory usage by adding this as a listener to the
   * off-heap memory allocator.
   */
  private void startMonitoring() {
    synchronized (this) {
      if (this.started) {
        return;
      }

      this.offHeapMemoryUsageListener = new OffHeapMemoryUsageListener(getBytesUsed());
      ThreadGroup group = LoggingThreadGroup.createThreadGroup("OffHeapMemoryMonitor Threads", logger);
      Thread memoryListenerThread = new Thread(group, this.offHeapMemoryUsageListener);
      memoryListenerThread.setName(memoryListenerThread.getName() + " OffHeapMemoryListener");
      memoryListenerThread.setPriority(Thread.MAX_PRIORITY);
      memoryListenerThread.setDaemon(true);
      memoryListenerThread.start();
      
      this.memoryAllocator.addMemoryUsageListener(this);
      
      this.started = true;
    }
  }

  /**
   * Stop monitoring off-heap usage.
   */
  @Override
  public void stopMonitoring() {
    synchronized (this) {
      if (!this.started) {
        return;
      }
      
      this.memoryAllocator.removeMemoryUsageListener(this);
      
      this.offHeapMemoryUsageListener.stopRequested = true;
      synchronized (this.offHeapMemoryUsageListener) {
        this.offHeapMemoryUsageListener.notifyAll();
      }

      this.started = false;
    }
  }

  @Override
  public void updateMemoryUsed(final long bytesUsed) {
    synchronized (this.offHeapMemoryUsageListener) {
      this.offHeapMemoryUsageListener.offHeapMemoryUsed = bytesUsed;
      this.offHeapMemoryUsageListener.notifyAll();
    }
  }
  
  void setCriticalThreshold(final float criticalThreshold) {
    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (criticalThreshold == this.thresholds.getCriticalThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (criticalThreshold > 100.0f || criticalThreshold < 0.0f) {
        throw new IllegalArgumentException(LocalizedStrings.MemoryThresholds_CRITICAL_PERCENTAGE_GT_ZERO_AND_LTE_100
            .toLocalizedString());
      }
      if (this.memoryAllocator == null) {
        throw new IllegalStateException(LocalizedStrings.OffHeapMemoryMonitor_NO_OFF_HEAP_MEMORY_HAS_BEEN_CONFIGURED
            .toLocalizedString());
      }
      if (criticalThreshold != 0 && this.thresholds.isEvictionThresholdEnabled()
          && criticalThreshold <= this.thresholds.getEvictionThreshold()) {
        throw new IllegalArgumentException(LocalizedStrings.MemoryThresholds_CRITICAL_PERCENTAGE_GTE_EVICTION_PERCENTAGE
            .toLocalizedString());
      }

      this.cache.setQueryMonitorRequiredForResourceManager(criticalThreshold != 0);

      this.thresholds = new MemoryThresholds(this.thresholds.getMaxMemoryBytes(), criticalThreshold, this.thresholds
          .getEvictionThreshold());

      updateStateAndSendEvent(getBytesUsed());

      // Start or stop monitoring based upon whether a threshold has been set
      if (this.thresholds.isEvictionThresholdEnabled() || this.thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!this.thresholds.isEvictionThresholdEnabled() && !this.thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      this.stats.changeOffHeapCriticalThreshold(this.thresholds.getCriticalThresholdBytes());
    }
  }

  float getCriticalThreshold() {
    return this.thresholds.getCriticalThreshold();
  }
  
  public boolean hasEvictionThreshold() {
    return this.hasEvictionThreshold;
  }
  
  void setEvictionThreshold(final float evictionThreshold) {
    this.hasEvictionThreshold = true;
    
    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (evictionThreshold == this.thresholds.getEvictionThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (evictionThreshold > 100.0f || evictionThreshold < 0.0f) {
        throw new IllegalArgumentException(LocalizedStrings.MemoryThresholds_EVICTION_PERCENTAGE_GT_ZERO_AND_LTE_100
            .toLocalizedString());
      }
      if (this.memoryAllocator == null) {
        throw new IllegalStateException(LocalizedStrings.OffHeapMemoryMonitor_NO_OFF_HEAP_MEMORY_HAS_BEEN_CONFIGURED
            .toLocalizedString());
      }
      if (evictionThreshold != 0 && this.thresholds.isCriticalThresholdEnabled()
          && evictionThreshold >= this.thresholds.getCriticalThreshold()) {
        throw new IllegalArgumentException(LocalizedStrings.MemoryMonitor_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE
            .toLocalizedString());
      }

      this.thresholds = new MemoryThresholds(this.thresholds.getMaxMemoryBytes(), this.thresholds.getCriticalThreshold(),
          evictionThreshold);

      updateStateAndSendEvent(getBytesUsed());

      // Start or stop monitoring based upon whether a threshold has been set
      if (this.thresholds.isEvictionThresholdEnabled() || this.thresholds.isCriticalThresholdEnabled()) {
        startMonitoring();
      } else if (!this.thresholds.isEvictionThresholdEnabled() && !this.thresholds.isCriticalThresholdEnabled()) {
        stopMonitoring();
      }

      this.stats.changeOffHeapEvictionThreshold(this.thresholds.getEvictionThresholdBytes());
    }
  }

  public float getEvictionThreshold() {
    return this.thresholds.getEvictionThreshold();
  }

  /**
   * Compare the number of bytes used (fetched from the JVM) to the thresholds.
   * If necessary, change the state and send an event for the state change.
   */
  public void updateStateAndSendEvent() {
    updateStateAndSendEvent(getBytesUsed());
  }

  /**
   * Compare the number of bytes used to the thresholds. If necessary, change
   * the state and send an event for the state change.
   * 
   * Public for testing.
   * 
   * @param bytesUsed
   *          Number of bytes of heap memory currently used.
   */
  public void updateStateAndSendEvent(long bytesUsed) {
    synchronized (this) {
      MemoryState oldState = this.mostRecentEvent.getState();
      MemoryState newState = this.thresholds.computeNextState(oldState, bytesUsed);
      if (oldState != newState) {
        this.currentState = newState;
        
        MemoryEvent event = new MemoryEvent(ResourceType.OFFHEAP_MEMORY, oldState, newState, this.cache.getMyId(), bytesUsed, true,
            this.thresholds);
        this.upcomingEvent.set(event);

        processLocalEvent(event);
        updateStatsFromEvent(event);
        
      // The state didn't change.  However, if the state isn't normal and we've
      // been in that state for a while, send another event with the updated
      // memory usage.
      } else if (!oldState.isNormal() && (System.currentTimeMillis() - this.mostRecentEvent.getEventTime()) > 1000) {
        MemoryEvent event = new MemoryEvent(ResourceType.OFFHEAP_MEMORY, oldState, newState, this.cache.getMyId(), bytesUsed, true,
            this.thresholds);
        this.upcomingEvent.set(event);
        processLocalEvent(event);
      }
    }
  }

  /**
   * Update resource manager stats based upon the given event.
   * 
   * @param event
   *          Event from which to derive data for updating stats.
   */
  private void updateStatsFromEvent(MemoryEvent event) {
    if (event.isLocal()) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
        this.stats.incOffHeapCriticalEvents();
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
        this.stats.incOffHeapSafeEvents();
      }

      if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
        this.stats.incOffHeapEvictionStartEvents();
      } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
        this.stats.incOffHeapEvictionStopEvents();
      }
    }
  }

  /**
   * Populate off-heap memory data in the given profile.
   * 
   * @param profile
   *          Profile to populate.
   */
  @Override
  public void fillInProfile(final ResourceManagerProfile profile) {
    final MemoryEvent tempEvent = this.upcomingEvent.get();
    if (tempEvent != null) {
      this.mostRecentEvent = tempEvent;
      this.upcomingEvent.set(null);
    }
    final MemoryEvent eventToPopulate = this.mostRecentEvent;
    profile.setOffHeapData(eventToPopulate.getBytesUsed(), eventToPopulate.getState(), eventToPopulate.getThresholds());
  }

  public MemoryState getState() {
    return this.currentState;
  }

  public MemoryThresholds getThresholds() {
    MemoryThresholds saveThresholds = this.thresholds;

    return new MemoryThresholds(saveThresholds.getMaxMemoryBytes(), saveThresholds.getCriticalThreshold(), saveThresholds
        .getEvictionThreshold());
  }

  /**
   * Returns the number of bytes of memory reported by the memory allocator as
   * currently in use.
   */
  public long getBytesUsed() {
    if (this.memoryAllocator == null) {
      return 0;
    }
    
    return this.memoryAllocator.getUsedMemory();
  }

  /**
   * Deliver a memory event from one of the monitors to both local listeners and
   * remote resource managers. Also, if a critical event is received and a query
   * monitor has been enabled, then the query monitor will be notified.
   * 
   * Package private for testing.
   * 
   * @param event
   *          Event to process.
   */
  void processLocalEvent(MemoryEvent event) {
    assert event.isLocal();

    if (this.log.fineEnabled()) {
      this.log.fine("Handling new local event " + event);
    }

    if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
      this.log.error(LocalizedStrings.MemoryMonitor_MEMBER_ABOVE_CRITICAL_THRESHOLD,
          new Object[] { event.getMember(), "off-heap" });
    } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
      this.log.error(LocalizedStrings.MemoryMonitor_MEMBER_BELOW_CRITICAL_THRESHOLD,
          new Object[] { event.getMember(), "off-heap" });
    }

    if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
      this.log.info(LocalizedStrings.MemoryMonitor_MEMBER_ABOVE_HIGH_THRESHOLD,
          new Object[] { event.getMember(), "off-heap" });
    } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
      this.log.info(LocalizedStrings.MemoryMonitor_MEMBER_BELOW_HIGH_THRESHOLD,
          new Object[] { event.getMember(),  "off-heap" });
    }

    if (this.log.fineEnabled()) {
      this.log.fine("Informing remote members of event " + event);
    }
    
    this.resourceAdvisor.updateRemoteProfile();
    this.resourceManager.deliverLocalEvent(event);
  }
  
  @Override
  public void notifyListeners(final Set<ResourceListener> listeners, final ResourceEvent event) {
    for (ResourceListener listener : listeners) {
      try {
        listener.onEvent(event);
      } catch (CancelException ignore) {
        // ignore
      } catch (Throwable t) {
        this.log.error(LocalizedStrings.MemoryMonitor_EXCEPTION_OCCURED_WHEN_NOTIFYING_LISTENERS, t);
      }
    }
  }
  
  LogWriter getLogWriter() {
    return this.log.convertToLogWriter();
  }
  
  @Override
  public String toString() {
    return "OffHeapMemoryMonitor [thresholds=" + this.thresholds + ", mostRecentEvent=" + this.mostRecentEvent + "]";
  }
  
  class OffHeapMemoryUsageListener implements Runnable {
    volatile boolean stopRequested = false;
    volatile long offHeapMemoryUsed; // In bytes
    
    OffHeapMemoryUsageListener(final long offHeapMemoryUsed) {
      this.offHeapMemoryUsed = offHeapMemoryUsed;
    }
    
    @Override
    public void run() {
      getLogWriter().fine("OffHeapMemoryUsageListener is starting " + this);

      while (!this.stopRequested) {
        final long saveOffHeapMemoryUsed = this.offHeapMemoryUsed;
        updateStateAndSendEvent(saveOffHeapMemoryUsed);

        synchronized (this) {
          if (saveOffHeapMemoryUsed == this.offHeapMemoryUsed && !this.stopRequested) {
            try {
              this.wait();
            } catch (InterruptedException iex) {
              getLogWriter().warning("OffHeapMemoryUsageListener was interrupted " + this);
              this.stopRequested = true;
            }
          }
        }
      }
        
      getLogWriter().fine("OffHeapMemoryUsageListener is stopping " + this);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append(" Thread").append(" #").append(System.identityHashCode(this));
      return sb.toString();
    }
  }
}
