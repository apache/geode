/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.StatisticDescriptorImpl;

/**
 * Adds coarser-grained monitoring of entire Statistics instances.
 * <p/>
 * The following code is an example of how to find an existing Statistics 
 * instance and define a ValueMonitor listener for notifications of any stat 
 * sample containing updates to any stats in the instance being monitored:
 * <pre>
    StatisticsFactory factory = InternalDistributedSystem.getAnyInstance();
    Statistics[] statistics = f.findStatisticsByTextId("statSampler");
    if (statistics.length == 1) {
      ValueMonitor monitor = new ValueMonitor().addStatistics(statistics[0]);
        
      monitor.addListener(new StatisticsListener() {
        public void handleNotification(StatisticsNotification notify) {
          System.out.println("One or more statSampler stats changed at " 
              + notify.getTimeStamp());
          for (StatisticId statId : notify) {
            System.out.println("\t" 
                + statId.getStatisticDescriptor().getName() 
                + " = " + notify.getValue(statId));
          }
        }
      };
    }
 * 
 * @author Kirk Lund
 * @since 7.0
 * @see com.gemstone.gemfire.Statistics
 */
public final class ValueMonitor extends StatisticsMonitor {
  
  public enum Type {
    CHANGE, MATCH, DIFFER
  }
  
  private final CopyOnWriteHashSet<Statistics> statistics = new CopyOnWriteHashSet<Statistics>();
  
  public ValueMonitor() {
    super();
  }

  @Override
  public ValueMonitor addStatistic(StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }
  
  @Override
  public ValueMonitor removeStatistic(StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }
  
  public ValueMonitor addStatistics(Statistics statistics) {
    if (statistics == null) {
      throw new NullPointerException("Statistics is null");
    }
    this.statistics.add(statistics);
    return this;
  }
  
  public ValueMonitor removeStatistics(Statistics statistics) {
    if (statistics == null) {
      throw new NullPointerException("Statistics is null");
    }
    this.statistics.remove(statistics);
    return this;
  }

  protected void monitor(long millisTimeStamp, List<ResourceInstance> resourceInstances) {
    super.monitor(millisTimeStamp, resourceInstances);
    monitorStatistics(millisTimeStamp, resourceInstances);
  }
  
  protected void monitorStatistics(long millisTimeStamp, List<ResourceInstance> resourceInstances) {
    if (!this.statistics.isEmpty()) {
      Map<StatisticId, Number> stats = new HashMap<StatisticId, Number>();
      for (ResourceInstance resource : resourceInstances) {
        if (this.statistics.contains(resource.getStatistics())) {
          ResourceType resourceType = resource.getResourceType();
          StatisticDescriptor[] sds = resourceType.getStatisticDescriptors();
          int[] updatedStats = resource.getUpdatedStats();
          for (int i = 0; i < updatedStats.length; i++) {
            int idx = updatedStats[i];
            StatisticDescriptorImpl sdi = (StatisticDescriptorImpl)sds[idx];
            SimpleStatisticId statId = new SimpleStatisticId(sdi, resource.getStatistics());
            long rawbits = resource.getLatestStatValues()[idx];
            stats.put(statId, sdi.getNumberForRawBits(rawbits));
          }
        }
      }
      if (!stats.isEmpty()) {
        MapBasedStatisticsNotification notification = new MapBasedStatisticsNotification(
            millisTimeStamp, StatisticsNotification.Type.VALUE_CHANGED, stats);
        notifyListeners(notification);
      }
    }
  }

  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("statistics=").append(this.statistics);
    return sb;
  }
}
