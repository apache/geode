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
package com.gemstone.gemfire.management.internal.beans.stats;

import java.lang.management.ManagementFactory;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.statistics.StatisticId;
import com.gemstone.gemfire.internal.statistics.StatisticNotFoundException;
import com.gemstone.gemfire.internal.statistics.StatisticsNotification;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;

/**
 * This class acts as a monitor and listen for VM stats update on behalf of
 * MemberMBean.
 * 
 * 
 */
public final class VMStatsMonitor extends MBeanStatsMonitor {

  private volatile float cpuUsage = 0;



  private static String processCPUTimeAttr = "ProcessCpuTime";

  private long lastSystemTime = 0;

  private long lastProcessCpuTime = 0;

  private boolean processCPUTimeAvailable;

  public VMStatsMonitor(String name) {
    super(name);
    processCPUTimeAvailable = MBeanJMXAdapter.isAttributeAvailable(
        processCPUTimeAttr, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
    if(!processCPUTimeAvailable){
      cpuUsage = MBeanJMXAdapter.VALUE_NOT_AVAILABLE;
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
      log(name,value);
      statsMap.put(name, value);
    }
    refreshStats();
  }


  /**
   * Right now it only refreshes CPU usage in terms of percentage. This method
   * can be used for any other computation based on Stats in future.
   * 
   * Returns the time (as a percentage) that this member's process time with
   * respect to Statistics sample time interval. If process time between two
   * sample time t1 & t2 is p1 and p2 cpuUsage = ((p2-p1) * 100) / ((t2-t1)
   *  
   */
  private void refreshStats() {

    if (processCPUTimeAvailable) {
      Number processCpuTime = statsMap.get(StatsKey.VM_PROCESS_CPU_TIME);
      
      //Some JVM like IBM is not handled by Stats layer properly. Ignoring the attribute for such cases
      if(processCpuTime == null){
        cpuUsage = MBeanJMXAdapter.VALUE_NOT_AVAILABLE;
        return;
      }


      if (lastSystemTime == 0) {
        lastSystemTime = System.currentTimeMillis();
        return;
      }

      long cpuTime = processCpuTime.longValue();
      if (lastProcessCpuTime == 0) {
        lastProcessCpuTime = cpuTime;
        return;
      }
      long systemTime = System.currentTimeMillis();

      long denom = (systemTime - lastSystemTime) * 10000; // 10000 = (Nano
      // conversion factor /
      // 100 for percentage)

      float processCpuUsage = (float) (cpuTime - lastProcessCpuTime) / denom;

      lastSystemTime = systemTime;
      lastProcessCpuTime = cpuTime;
      cpuUsage = processCpuUsage;
    }

  }

  public float getCpuUsage() {
    return cpuUsage;
  }

}
