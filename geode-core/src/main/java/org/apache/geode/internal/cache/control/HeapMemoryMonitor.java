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

import static org.apache.geode.cache.control.HeapUsageProvider.HEAP_USAGE_PROVIDER_CLASS_NAME;
import static org.apache.geode.internal.lang.SystemProperty.getProductStringProperty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import javax.management.NotificationListener;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.control.HeapUsageProvider;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;
import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;
import org.apache.geode.internal.cache.execute.AllowExecutionInLowMemory;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Allows for the setting of eviction and critical thresholds. These thresholds are compared against
 * current heap usage and, with the help of {#link InternalResourceManager}, dispatches events when
 * the thresholds are crossed. Gathering memory usage information from the JVM is done using a
 * listener on the MemoryMXBean, by polling the JVM and as a listener on GemFire Statistics output
 * in order to accommodate differences in the various JVMs.
 *
 * @since Geode 1.0
 */
public class HeapMemoryMonitor implements MemoryMonitor, LongConsumer {
  private static final Logger logger = LogService.getLogger();
  // Property for setting the JVM polling interval (below)
  public static final String POLLER_INTERVAL_PROP =
      GeodeGlossary.GEMFIRE_PREFIX + "heapPollerInterval";

  // Internal for polling the JVM for changes in heap memory usage.
  private static final int POLLER_INTERVAL = Integer.getInteger(POLLER_INTERVAL_PROP, 500);

  private final HeapUsageProvider heapUsageProvider;

  // This holds a new event as it transitions from updateStateAndSendEvent(...) to fillInProfile()
  private final ThreadLocal<MemoryEvent> upcomingEvent = new ThreadLocal<>();

  private volatile MemoryThresholds thresholds;
  private volatile MemoryEvent mostRecentEvent;
  private volatile MemoryState currentState = MemoryState.DISABLED;

  // Set when startMonitoring() and stopMonitoring() are called
  private boolean started = false;

  // Set to true when setEvictionThreshold(...) is called.
  private boolean hasEvictionThreshold = false;

  private final InternalResourceManager resourceManager;
  private final ResourceAdvisor resourceAdvisor;
  private final InternalCache cache;
  private final ResourceManagerStats stats;

  @MutableForTesting
  private static long testBytesUsedForThresholdSet = -1;

  HeapMemoryMonitor(final InternalResourceManager resourceManager, final InternalCache cache,
      final ResourceManagerStats stats) {
    this(resourceManager, cache, stats, createHeapUsageProvider());
  }

  @VisibleForTesting
  HeapMemoryMonitor(final InternalResourceManager resourceManager, final InternalCache cache,
      final ResourceManagerStats stats, final HeapUsageProvider heapUsageProvider) {
    this.resourceManager = resourceManager;
    resourceAdvisor = (ResourceAdvisor) cache.getDistributionAdvisor();
    this.cache = cache;
    this.stats = stats;
    this.heapUsageProvider = heapUsageProvider;
    thresholds = new MemoryThresholds(heapUsageProvider.getMaxMemory());
    mostRecentEvent = new MemoryEvent(ResourceType.HEAP_MEMORY,
        MemoryState.DISABLED, MemoryState.DISABLED, null, 0L, true, thresholds);
  }

  private static HeapUsageProvider createHeapUsageProvider() {
    Optional<String> propertyValue = getProductStringProperty(HEAP_USAGE_PROVIDER_CLASS_NAME);
    if (propertyValue.isPresent()) {
      String className = propertyValue.get();
      Class<?> clazz;
      try {
        clazz = ClassPathLoader.getLatest().forName(className);
      } catch (Exception ex) {
        throw new IllegalArgumentException("Could not load class \"" + className +
            "\" for system property \"geode." + HEAP_USAGE_PROVIDER_CLASS_NAME + "\"", ex);
      }
      Object instance;
      try {
        instance = clazz.newInstance();
      } catch (Exception ex) {
        throw new IllegalArgumentException("Could not create an instance of class \"" + className +
            "\" for system property \"geode." + HEAP_USAGE_PROVIDER_CLASS_NAME +
            "\". Make sure it has a public zero-arg constructor.", ex);
      }
      if (!(instance instanceof HeapUsageProvider)) {
        throw new IllegalArgumentException("The class \"" + className +
            "\" for system property \"geode." + HEAP_USAGE_PROVIDER_CLASS_NAME +
            "\" is not an instance of HeapUsageProvider.");
      }
      logger.info("Using custom HeapUsageProvider class: {}", className);
      return (HeapUsageProvider) instance;
    } else {
      return new MemoryPoolMXBeanHeapUsageProvider();
    }
  }

  public void setMemoryStateChangeTolerance(int memoryStateChangeTolerance) {
    thresholds.setMemoryStateChangeTolerance(memoryStateChangeTolerance);
  }

  public int getMemoryStateChangeTolerance() {
    return thresholds.getMemoryStateChangeTolerance();
  }

  private void startMonitoring() {
    synchronized (this) {
      if (started) {
        return;
      }
      if (!testDisableMemoryUpdates) {
        heapUsageProvider.startNotifications(this);
        startHeapUsagePoller();
      }
      started = true;
    }
  }

  @Override
  public void stopMonitoring() {
    synchronized (this) {
      if (!started) {
        return;
      }
      if (!testDisableMemoryUpdates) {
        heapUsageProvider.stopNotifications();
        stopHeapUsagePoller();
      }

      started = false;
    }
  }

  void setCriticalThreshold(final float criticalThreshold) {
    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (criticalThreshold == thresholds.getCriticalThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (criticalThreshold > 100.0f || criticalThreshold < 0.0f) {
        throw new IllegalArgumentException(
            "Critical percentage must be greater than 0.0 and less than or equal to 100.0.");
      }
      if (criticalThreshold != 0 && thresholds.isEvictionThresholdEnabled()
          && criticalThreshold <= thresholds.getEvictionThreshold()) {
        throw new IllegalArgumentException(
            "Critical percentage must be greater than the eviction percentage.");
      }

      cache.setQueryMonitorRequiredForResourceManager(criticalThreshold != 0);

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(), criticalThreshold,
          thresholds.getEvictionThreshold());

      handleThresholdChange();

      stats.changeCriticalThreshold(thresholds.getCriticalThresholdBytes());
    }
  }

  @Override
  public boolean hasEvictionThreshold() {
    return hasEvictionThreshold;
  }

  void setEvictionThreshold(final float evictionThreshold) {
    hasEvictionThreshold = true;

    synchronized (this) {
      // If the threshold isn't changing then don't do anything.
      if (evictionThreshold == thresholds.getEvictionThreshold()) {
        return;
      }

      // Do some basic sanity checking on the new threshold
      if (evictionThreshold > 100.0f || evictionThreshold < 0.0f) {
        throw new IllegalArgumentException(
            "Eviction percentage must be greater than 0.0 and less than or equal to 100.0.");
      }
      if (evictionThreshold != 0 && thresholds.isCriticalThresholdEnabled()
          && evictionThreshold >= thresholds.getCriticalThreshold()) {
        throw new IllegalArgumentException(
            "Eviction percentage must be less than the critical percentage.");
      }

      thresholds = new MemoryThresholds(thresholds.getMaxMemoryBytes(),
          thresholds.getCriticalThreshold(), evictionThreshold);

      handleThresholdChange();

      stats.changeEvictionThreshold(thresholds.getEvictionThresholdBytes());
    }
  }

  private void handleThresholdChange() {
    updateStateAndSendEvent();

    // Start or stop monitoring based upon whether a threshold has been set
    if (thresholds.isEvictionThresholdEnabled()
        || thresholds.isCriticalThresholdEnabled()) {
      startMonitoring();
    } else if (!thresholds.isEvictionThresholdEnabled()
        && !thresholds.isCriticalThresholdEnabled()) {
      stopMonitoring();
    }
  }

  /**
   * Compare the number of bytes used (fetched from the JVM) to the thresholds. If necessary, change
   * the state and send an event for the state change.
   */
  public void updateStateAndSendEvent() {
    updateStateAndSendEvent(
        testBytesUsedForThresholdSet != -1 ? testBytesUsedForThresholdSet : getBytesUsed(),
        "product-check");
  }

  /**
   * Compare the number of bytes used to the thresholds. If necessary, change the state and send an
   * event for the state change.
   *
   * @param bytesUsed Number of bytes of heap memory currently used.
   * @param eventOrigin Indicates where the event originated e.g. notification vs polling
   */
  @VisibleForTesting
  public void updateStateAndSendEvent(long bytesUsed, String eventOrigin) {
    stats.changeTenuredHeapUsed(bytesUsed);
    synchronized (this) {
      MemoryState oldState = mostRecentEvent.getState();
      MemoryState newState = thresholds.computeNextState(oldState, bytesUsed);
      if (oldState != newState) {
        currentState = newState;

        MemoryEvent event = new MemoryEvent(ResourceType.HEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);

        upcomingEvent.set(event);
        processLocalEvent(event, eventOrigin);
        updateStatsFromEvent(event);

        // The state didn't change. However, if the state isn't normal and the
        // number of bytes used changed, then go ahead and send the event
        // again with an updated number of bytes used.
      } else if (!oldState.isNormal() && bytesUsed != mostRecentEvent.getBytesUsed()) {
        MemoryEvent event = new MemoryEvent(ResourceType.HEAP_MEMORY, oldState, newState,
            cache.getMyId(), bytesUsed, true, thresholds);
        upcomingEvent.set(event);
        processLocalEvent(event, eventOrigin);
      }
    }
  }

  /**
   * Update resource manager stats based upon the given event.
   *
   * @param event Event from which to derive data for updating stats.
   */
  private void updateStatsFromEvent(MemoryEvent event) {
    if (event.isLocal()) {
      if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
        stats.incHeapCriticalEvents();
      } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
        stats.incHeapSafeEvents();
      }

      if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
        stats.incEvictionStartEvents();
      } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
        stats.incEvictionStopEvents();
      }
    }
  }

  /**
   * Populate heap memory data in the given profile.
   *
   * @param profile Profile to populate.
   */
  @Override
  public void fillInProfile(final ResourceManagerProfile profile) {
    final MemoryEvent tempEvent = upcomingEvent.get();
    if (tempEvent != null) {
      mostRecentEvent = tempEvent;
      upcomingEvent.set(null);
    }
    final MemoryEvent eventToPopulate = mostRecentEvent;
    profile.setHeapData(eventToPopulate.getBytesUsed(), eventToPopulate.getState(),
        eventToPopulate.getThresholds());
  }

  @Override
  public MemoryState getState() {
    return currentState;
  }

  @Override
  public MemoryThresholds getThresholds() {
    MemoryThresholds saveThresholds = thresholds;

    return new MemoryThresholds(saveThresholds.getMaxMemoryBytes(),
        saveThresholds.getCriticalThreshold(), saveThresholds.getEvictionThreshold());
  }

  /**
   * Returns the number of bytes of memory reported by the tenured pool as currently in use.
   */
  @Override
  public long getBytesUsed() {
    return heapUsageProvider.getBytesUsed();
  }

  /**
   * Deliver a memory event from one of the monitors to both local listeners and remote resource
   * managers. Also, if a critical event is received and a query monitor has been enabled, then the
   * query monitor will be notified.
   *
   * Package private for testing.
   *
   * @param event Event to process.
   * @param eventOrigin Indicates where the event originated e.g. notification vs polling
   */
  synchronized void processLocalEvent(MemoryEvent event, String eventOrigin) {
    assert event.isLocal();

    if (logger.isDebugEnabled()) {
      logger.debug("Handling new local event " + event);
    }

    if (event.getState().isCritical() && !event.getPreviousState().isCritical()) {
      logger.error(
          createCriticalThresholdLogMessage(event, eventOrigin, true));
      QueryMonitor queryMonitor = cache.getQueryMonitor();
      if (queryMonitor != null) {
        queryMonitor.setLowMemory(true, event.getBytesUsed());
      }

    } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()) {
      logger.error(
          createCriticalThresholdLogMessage(event, eventOrigin, false));
      QueryMonitor queryMonitor = cache.getQueryMonitor();
      if (queryMonitor != null) {
        queryMonitor.setLowMemory(false, event.getBytesUsed());
      }
    }

    if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
      logger.info(String.format("Member: %s above %s eviction threshold",
          event.getMember(), "heap"));
    } else if (!event.getState().isEviction() && event.getPreviousState().isEviction()) {
      logger.info(String.format("Member: %s below %s eviction threshold",
          event.getMember(), "heap"));
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Informing remote members of event " + event);
    }

    resourceAdvisor.updateRemoteProfile();
    resourceManager.deliverLocalEvent(event);
  }

  @Override
  public void notifyListeners(final Set<ResourceListener<?>> listeners,
      final ResourceEvent event) {
    for (ResourceListener listener : listeners) {
      try {
        listener.onEvent(event);
      } catch (CancelException ignore) {
        // ignore
      } catch (Throwable t) {
        Error err;
        if (t instanceof Error && SystemFailure.isJVMFailureError(err = (Error) t)) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned now, so
          // don't let this thread continue.
          throw err;
        }
        // Whenever you catch Error or Throwable, you must also
        // check for fatal JVM error (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error("Exception occurred when notifying listeners ", t);
      }
    }
  }

  public Set<DistributedMember> getHeapCriticalMembersFrom(
      Set<? extends DistributedMember> members) {
    Set<DistributedMember> criticalMembers = getCriticalMembers();
    criticalMembers.retainAll(members);
    return criticalMembers;
  }

  private Set<DistributedMember> getCriticalMembers() {
    Set<DistributedMember> criticalMembers = new HashSet<>(resourceAdvisor.adviseCriticalMembers());
    if (mostRecentEvent.getState().isCritical()) {
      criticalMembers.add(cache.getMyId());
    }
    return criticalMembers;
  }

  public void checkForLowMemory(Function<?> function, DistributedMember targetMember) {
    Set<DistributedMember> targetMembers = Collections.singleton(targetMember);
    checkForLowMemory(function, targetMembers);
  }

  public void checkForLowMemory(Function<?> function, Set<? extends DistributedMember> dest) {
    LowMemoryException exception = createLowMemoryIfNeeded(function, dest);
    if (exception != null) {
      throw exception;
    }
  }

  public LowMemoryException createLowMemoryIfNeeded(Function<?> function,
      DistributedMember targetMember) {
    Set<DistributedMember> targetMembers = Collections.singleton(targetMember);
    return createLowMemoryIfNeeded(function, targetMembers);
  }

  public LowMemoryException createLowMemoryIfNeeded(Function<?> function,
      Set<? extends DistributedMember> memberSet) {
    if (function.optimizeForWrite() && !(function instanceof AllowExecutionInLowMemory)
        && !MemoryThresholds.isLowMemoryExceptionDisabled()) {
      Set<DistributedMember> criticalMembersFrom = getHeapCriticalMembersFrom(memberSet);
      if (!criticalMembersFrom.isEmpty()) {
        return new LowMemoryException(
            String.format(
                "Function: %s cannot be executed because the members %s are running low on memory",
                function.getId(), criticalMembersFrom),
            criticalMembersFrom);
      }
    }
    return null;
  }


  /**
   * Determines if the given member is in a heap critical state.
   *
   * @param member Member to check.
   *
   * @return True if the member's heap memory is in a critical state, false otherwise.
   */
  public boolean isMemberHeapCritical(final InternalDistributedMember member) {
    if (member.equals(cache.getMyId())) {
      return mostRecentEvent.getState().isCritical();
    }
    return resourceAdvisor.isHeapCritical(member);
  }

  protected MemoryEvent getMostRecentEvent() {
    return mostRecentEvent;
  }

  @VisibleForTesting
  protected HeapMemoryMonitor setMostRecentEvent(
      MemoryEvent mostRecentEvent) {
    this.mostRecentEvent = mostRecentEvent;
    return this;
  }

  @Override
  public void accept(long usedMemory) {
    if (!testDisableMemoryUpdates) {
      resourceManager
          .runWithNotifyExecutor(() -> updateStateAndSendEvent(usedMemory, "notification"));
    }
  }

  @Override
  public String toString() {
    return "HeapMemoryMonitor [thresholds=" + thresholds + ", mostRecentEvent="
        + mostRecentEvent + "]";
  }

  /**
   * Overrides the value returned by the JVM as the number of bytes of available memory.
   *
   * @param testMaxMemoryBytes The value to use as the maximum number of bytes of memory available.
   */
  @TestOnly
  public void setTestMaxMemoryBytes(final long testMaxMemoryBytes) {
    synchronized (this) {
      MemoryThresholds newThresholds;

      if (testMaxMemoryBytes == 0) {
        newThresholds = new MemoryThresholds(heapUsageProvider.getMaxMemory());
      } else {
        newThresholds = new MemoryThresholds(testMaxMemoryBytes,
            thresholds.getCriticalThreshold(), thresholds.getEvictionThreshold());
      }

      thresholds = newThresholds;
      final String builder =
          "In testing, the following values were set" + " maxMemoryBytes:"
              + newThresholds.getMaxMemoryBytes()
              + " criticalThresholdBytes:" + newThresholds.getCriticalThresholdBytes()
              + " evictionThresholdBytes:" + newThresholds.getEvictionThresholdBytes();
      logger.debug(builder);
    }
  }

  /**
   * Since the setter methods for the eviction and critical thresholds immediately update state
   * based upon the new threshold value and the number of bytes currently used by the JVM, there
   * needs to be a way to override the number of bytes of memory reported as in use for testing.
   * That's what this method and the value it sets are for.
   *
   * @param newTestBytesUsedForThresholdSet Value to use as the amount of memory in use when calling
   *        the setEvictionThreshold or setCriticalThreshold methods are called.
   */
  @TestOnly
  public static void setTestBytesUsedForThresholdSet(final long newTestBytesUsedForThresholdSet) {
    testBytesUsedForThresholdSet = newTestBytesUsedForThresholdSet;
  }

  @MutableForTesting
  private static boolean testDisableMemoryUpdates = false;

  @TestOnly
  public static void setTestDisableMemoryUpdates(final boolean newTestDisableMemoryUpdates) {
    testDisableMemoryUpdates = newTestDisableMemoryUpdates;
  }

  private String createCriticalThresholdLogMessage(MemoryEvent event, String eventOrigin,
      boolean above) {
    return "Member: " + event.getMember() + " " + (above ? "above" : "below")
        + " heap critical threshold."
        + " Event generated via " + eventOrigin + "."
        + " Used bytes: " + event.getBytesUsed() + "."
        + " Memory thresholds: " + thresholds;
  }

  public long getMaxMemory() {
    return heapUsageProvider.getMaxMemory();
  }

  @TestOnly
  public NotificationListener getHeapUsageProvider() {
    return (MemoryPoolMXBeanHeapUsageProvider) heapUsageProvider;
  }

  private ScheduledFuture<?> heapUsagePollerFuture;

  /**
   * Start a separate thread for polling the JVM for heap memory usage.
   */
  private void startHeapUsagePoller() {
    heapUsagePollerFuture = resourceManager.getExecutor().scheduleAtFixedRate(new HeapUsagePoller(),
        POLLER_INTERVAL, POLLER_INTERVAL,
        TimeUnit.MILLISECONDS);

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Started GemfireHeapPoller to poll the heap every " + POLLER_INTERVAL + " milliseconds");
    }
  }

  private void stopHeapUsagePoller() {
    ScheduledFuture<?> future = heapUsagePollerFuture;
    if (future != null) {
      future.cancel(false);
      heapUsagePollerFuture = null;
    }
  }

  /**
   * Polls the heap usage
   */
  private class HeapUsagePoller implements Runnable {
    @Override
    public void run() {
      try {
        updateStateAndSendEvent(heapUsageProvider.getBytesUsed(), "polling");
      } catch (Exception e) {
        logger.debug("Poller Thread caught exception:", e);
      }
    }
  }

}
