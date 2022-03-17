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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.List;
import java.util.function.LongConsumer;

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

  private static MemoryPoolMXBean findTenuredMemoryPoolMXBean() {
    for (MemoryPoolMXBean memoryPoolMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
      if (memoryPoolMXBean.isUsageThresholdSupported() && isTenured(memoryPoolMXBean)) {
        return memoryPoolMXBean;
      }
    }
    logger.error("No tenured pools found.  Known pools are: {}",
        getAllMemoryPoolNames());
    return null;
  }

  /*
   * Calculates the max memory for the tenured pool. Works around JDK bug:
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7078465 by getting max memory from runtime
   * and subtracting all other heap pools from it.
   */
  private static long calculateTenuredPoolMaxMemory(MemoryPoolMXBean poolBean) {
    if (poolBean != null && poolBean.getUsage().getMax() != -1) {
      return poolBean.getUsage().getMax();
    } else {
      long calculatedMaxMemory = Runtime.getRuntime().maxMemory();
      List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean p : pools) {
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
   * checkTenuredHeapConsumption
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
        || name.equals("ZHeap") // Sun ZGC

        // Allow a user specified pool name to be monitored
        || (name.equals(HEAP_POOL));
  }

  /**
   * Returns the names of all available memory pools as a single string.
   */
  private static String getAllMemoryPoolNames() {
    StringBuilder builder = new StringBuilder("[");

    for (MemoryPoolMXBean memoryPoolBean : ManagementFactory.getMemoryPoolMXBeans()) {
      builder.append("(Name=").append(memoryPoolBean.getName()).append(";Type=")
          .append(memoryPoolBean.getType()).append(";UsageThresholdSupported=")
          .append(memoryPoolBean.isUsageThresholdSupported()).append("), ");
    }

    if (builder.length() > 1) {
      builder.setLength(builder.length() - 2);
    }
    builder.append("]");

    return builder.toString();
  }

  private final TenuredHeapConsumptionMonitor tenuredHeapConsumptionMonitor;
  // JVM MXBean used to report changes in heap memory usage
  private final MemoryPoolMXBean tenuredMemoryPoolMXBean;
  // Calculated value for the amount of JVM tenured heap memory available.
  private final long tenuredPoolMaxMemory;

  private volatile LongConsumer heapUsageListener = disabledHeapUsageListener;

  public MemoryPoolMXBeanHeapUsageProvider() {
    tenuredHeapConsumptionMonitor = new TenuredHeapConsumptionMonitor();
    tenuredMemoryPoolMXBean = findTenuredMemoryPoolMXBean();
    tenuredPoolMaxMemory = calculateTenuredPoolMaxMemory(tenuredMemoryPoolMXBean);
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
        getAllMemoryPoolNames()));
  }

  /**
   * Register with the JVM to get threshold events.
   */
  private void startJVMThresholdListener() {
    final MemoryPoolMXBean memoryPoolMXBean = getTenuredMemoryPoolMXBean();

    // Set collection threshold to a low value, so that we can get
    // notifications after every GC run. After each such collection
    // threshold notification we set the usage thresholds to an
    // appropriate value.
    memoryPoolMXBean.setCollectionUsageThreshold(1);

    final long usageThreshold = memoryPoolMXBean.getUsageThreshold();
    logger.info(
        String.format("Overriding MemoryPoolMXBean heap threshold bytes %s on pool %s",
            usageThreshold, memoryPoolMXBean.getName()));

    getNotificationEmitter().addNotificationListener(this, null, null);
  }

  private static NotificationEmitter getNotificationEmitter() {
    return (NotificationEmitter) ManagementFactory.getMemoryMXBean();
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
