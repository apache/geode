/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
 * @author rishim
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
