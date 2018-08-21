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
package org.apache.geode.management.internal.beans.stats;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.management.internal.MBeanJMXAdapter;

/**
 * This class acts as a monitor and listen for VM stats update on behalf of MemberMBean.
 * <p>
 * There's only one dedicated sampler thread that mutates the fields and writes the statistics to a
 * file. The mutable fields are declared as {@code volatile} to make sure readers of the statistics
 * get the latest value recorded.
 * <p>
 * The class is not thread-safe. If multiple threads access an instance concurrently, it must be
 * synchronized externally.
 *
 * @see org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor
 */
public class VMStatsMonitor extends MBeanStatsMonitor {
  static final int VALUE_NOT_AVAILABLE = -1;
  private static final String PROCESS_CPU_TIME_ATTRIBUTE = "ProcessCpuTime";
  private long lastSystemTime = 0;
  private long lastProcessCpuTime = 0;
  private volatile float cpuUsage = 0;
  private final boolean processCPUTimeAvailable;

  public float getCpuUsage() {
    return cpuUsage;
  }

  public VMStatsMonitor(String name) {
    super(name);
    processCPUTimeAvailable = MBeanJMXAdapter.isAttributeAvailable(PROCESS_CPU_TIME_ATTRIBUTE,
        ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
    if (!processCPUTimeAvailable) {
      cpuUsage = VALUE_NOT_AVAILABLE;
    }
  }

  long currentTimeMillis() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
  }

  /**
   *
   * @param systemTime Current system time.
   * @param cpuTime Last gathered cpu time.
   * @return The time (as a percentage) that this member's process time with respect to Statistics
   *         sample time interval. If process time between two sample time t1 & t2 is p1 and p2
   *         cpuUsage = ((p2-p1) * 100) / ((t2-t1).
   */
  float calculateCpuUsage(long systemTime, long cpuTime) {
    // 10000 = (Nano conversion factor / 100 for percentage)
    long denom = (systemTime - lastSystemTime) * 10000;
    return (float) (cpuTime - lastProcessCpuTime) / denom;
  }

  /**
   * Right now it only refreshes CPU usage in terms of percentage. This method can be used for any
   * other computation based on Stats in future.
   */
  synchronized void refreshStats() {
    if (processCPUTimeAvailable) {
      Number processCpuTime = statsMap.get(StatsKey.VM_PROCESS_CPU_TIME);

      // Some JVM like IBM is not handled by Stats layer properly. Ignoring the attribute for such
      // cases
      if (processCpuTime == null) {
        cpuUsage = VALUE_NOT_AVAILABLE;
        return;
      }

      if (lastSystemTime == 0) {
        lastSystemTime = currentTimeMillis();
        return;
      }

      long cpuTime = processCpuTime.longValue();
      if (lastProcessCpuTime == 0) {
        lastProcessCpuTime = cpuTime;
        return;
      }

      long systemTime = currentTimeMillis();
      lastSystemTime = systemTime;
      lastProcessCpuTime = cpuTime;
      cpuUsage = calculateCpuUsage(systemTime, cpuTime);
    }
  }

  @Override
  public void handleNotification(StatisticsNotification notification) {
    for (StatisticId statId : notification) {
      StatisticDescriptor descriptor = statId.getStatisticDescriptor();
      String name = descriptor.getName();
      Number value;

      try {
        value = notification.getValue(statId);
      } catch (StatisticNotFoundException e) {
        value = 0;
      }

      log(name, value);
      statsMap.put(name, value);
    }

    refreshStats();
  }
}
