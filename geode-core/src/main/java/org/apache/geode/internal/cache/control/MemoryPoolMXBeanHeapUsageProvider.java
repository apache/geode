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

import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.control.HeapUsageProvider;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This heap usage provider is used by default.
 * It determines heap usage by using a MemoryPoolMXBean that it gets from the JVM
 * using the ManagementFactory.
 */
public class MemoryPoolMXBeanHeapUsageProvider implements HeapUsageProvider, NotificationListener {
  private static final Logger logger = LogService.getLogger();
  // Allow for an unknown heap pool for VMs we may support in the future.
  private static final String HEAP_POOL =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "ResourceManager.HEAP_POOL");
  @Immutable
  private static final LongConsumer disabledHeapUsageListener = (usedMemory) -> {
  };

  private static MemoryPoolMXBean findTenuredMemoryPoolMXBean(
      Supplier<List<MemoryPoolMXBean>> memoryPoolSupplier) {
    if (HEAP_POOL != null && !HEAP_POOL.equals("")) {
      // give preference to using an existing pool that matches HEAP_POOL
      for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolSupplier.get()) {
        if (HEAP_POOL.equals(memoryPoolMXBean.getName())) {
          return memoryPoolMXBean;
        }
      }
      logger.warn("No memory pool was found with the name {}. Known pools are: {}",
          HEAP_POOL, getAllMemoryPoolNames(memoryPoolSupplier));

    }
    for (MemoryPoolMXBean memoryPoolMXBean : memoryPoolSupplier.get()) {
      if (isTenured(memoryPoolMXBean)) {
        if (HEAP_POOL != null && !HEAP_POOL.equals("")) {
          logger.warn(
              "Ignoring gemfire.ResourceManager.HEAP_POOL system property and using pool {}.",
              memoryPoolMXBean.getName());
        }
        return memoryPoolMXBean;
      }
    }
    logger.error("No tenured pools found.  Known pools are: {}",
        getAllMemoryPoolNames(memoryPoolSupplier));
    return null;
  }

  /*
   * Calculates the max memory for the tenured pool. Works around JDK bug:
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7078465 by getting max memory from runtime
   * and subtracting all other heap pools from it.
   */
  private static long calculateTenuredPoolMaxMemory(
      Supplier<List<MemoryPoolMXBean>> memoryPoolSupplier,
      LongSupplier maxJVMHeapSupplier,
      MemoryPoolMXBean poolBean) {
    if (poolBean != null && poolBean.getUsage().getMax() != -1) {
      return poolBean.getUsage().getMax();
    } else {
      long calculatedMaxMemory = maxJVMHeapSupplier.getAsLong();
      for (MemoryPoolMXBean p : memoryPoolSupplier.get()) {
        if (p.getType() == MemoryType.HEAP && p.getUsage().getMax() != -1) {
          calculatedMaxMemory -= p.getUsage().getMax();
        }
      }
      return calculatedMaxMemory;
    }
  }

  /**
   * Determines if the name of the memory pool MXBean provided matches a list of known tenured pool
   * names.
   *
   * @param memoryPoolMXBean The memory pool MXBean to check.
   * @return True if the pool name matches a known tenured pool name, false otherwise.
   */
  private static boolean isTenured(MemoryPoolMXBean memoryPoolMXBean) {
    if (memoryPoolMXBean.getType() != MemoryType.HEAP) {
      return false;
    }

    String name = memoryPoolMXBean.getName();

    return name.equals("CMS Old Gen") // Sun Concurrent Mark Sweep GC
        || name.equals("PS Old Gen") // Sun Parallel GC
        || name.equals("G1 Old Gen") // Sun G1 GC
        || name.equals("Old Space") // BEA JRockit 1.5, 1.6 GC
        || name.equals("Tenured Gen") // Hitachi 1.5 GC
        || name.equals("Java heap") // IBM 1.5, 1.6 GC
        || name.equals("GenPauseless Old Gen") // azul C4/GPGC collector
        || name.equals("ZHeap") // ZGC
    ;
  }

  /**
   * Returns the names of all available memory pools as a single string.
   */
  private static String getAllMemoryPoolNames(Supplier<List<MemoryPoolMXBean>> memoryPoolSupplier) {
    StringBuilder builder = new StringBuilder("[");

    for (MemoryPoolMXBean memoryPoolBean : memoryPoolSupplier.get()) {
      builder.append("(Name=").append(memoryPoolBean.getName()).append(";Type=")
          .append(memoryPoolBean.getType()).append(";collectionUsageThresholdSupported=")
          .append(memoryPoolBean.isCollectionUsageThresholdSupported()).append("), ");
    }

    if (builder.length() > 1) {
      builder.setLength(builder.length() - 2);
    }
    builder.append("]");

    return builder.toString();
  }

  private final Supplier<List<MemoryPoolMXBean>> memoryPoolSupplier;
  private final Supplier<NotificationEmitter> notificationEmitterSupplier;
  private final TenuredHeapConsumptionMonitor tenuredHeapConsumptionMonitor;
  // JVM MXBean used to report changes in heap memory usage
  private final MemoryPoolMXBean tenuredMemoryPoolMXBean;
  // Calculated value for the amount of JVM tenured heap memory available.
  private final long tenuredPoolMaxMemory;

  private volatile LongConsumer heapUsageListener = disabledHeapUsageListener;

  public MemoryPoolMXBeanHeapUsageProvider(Supplier<List<MemoryPoolMXBean>> memoryPoolSupplier,
      Supplier<NotificationEmitter> notificationEmitterSupplier,
      LongSupplier maxJVMHeapSupplier) {
    this.memoryPoolSupplier = memoryPoolSupplier;
    this.notificationEmitterSupplier = notificationEmitterSupplier;
    tenuredHeapConsumptionMonitor = new TenuredHeapConsumptionMonitor();
    tenuredMemoryPoolMXBean = findTenuredMemoryPoolMXBean(memoryPoolSupplier);
    tenuredPoolMaxMemory =
        calculateTenuredPoolMaxMemory(memoryPoolSupplier, maxJVMHeapSupplier,
            tenuredMemoryPoolMXBean);
  }

  @Override
  public void startNotifications(LongConsumer listener) {
    heapUsageListener = listener;
    startJVMThresholdListener();
  }

  @Override
  public void stopNotifications() {
    heapUsageListener = disabledHeapUsageListener;
    stopJVMThresholdListener();
  }

  @Override
  public long getMaxMemory() {
    return tenuredPoolMaxMemory;
  }

  @Override
  public long getBytesUsed() {
    return getTenuredMemoryPoolMXBean().getUsage().getUsed();
  }

  /**
   * Returns the tenured pool MXBean or throws an IllegalStateException if one couldn't be found.
   */
  private MemoryPoolMXBean getTenuredMemoryPoolMXBean() {
    if (tenuredMemoryPoolMXBean != null) {
      return tenuredMemoryPoolMXBean;
    }

    throw new IllegalStateException(String.format("No tenured pools found.  Known pools are: %s",
        getAllMemoryPoolNames(memoryPoolSupplier)));
  }

  /**
   * Register with the JVM to get threshold events.
   */
  private void startJVMThresholdListener() {
    final MemoryPoolMXBean memoryPoolMXBean = getTenuredMemoryPoolMXBean();

    if (memoryPoolMXBean.isCollectionUsageThresholdSupported()) {
      // Set collection threshold to a low value, so that we can get
      // notifications after every GC run.
      memoryPoolMXBean.setCollectionUsageThreshold(1);
      logger.info(
          String.format(
              "Setting MemoryPoolMXBean heap collection usage threshold to '1' on pool %s",
              memoryPoolMXBean.getName()));
    }

    getNotificationEmitter().addNotificationListener(this, null, null);
  }

  private NotificationEmitter getNotificationEmitter() {
    return notificationEmitterSupplier.get();
  }

  private void stopJVMThresholdListener() {
    try {
      getNotificationEmitter().removeNotificationListener(this, null, null);
      if (logger.isDebugEnabled()) {
        logger.debug("Removed Memory MXBean notification listener" + this);
      }
    } catch (ListenerNotFoundException ignore) {
      if (logger.isDebugEnabled()) {
        logger.debug("This instance '{}' was not registered as a Memory MXBean listener", this);
      }
    }
  }

  @Override
  public void handleNotification(final Notification notification, final Object callback) {
    tenuredHeapConsumptionMonitor.checkTenuredHeapConsumption(notification);
    // Not using the information given by the notification in favor
    // of constructing fresh information ourselves.
    heapUsageListener.accept(getBytesUsed());
  }
}
