/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.statistics.StatisticId;
import com.gemstone.gemfire.internal.statistics.StatisticNotFoundException;
import com.gemstone.gemfire.internal.statistics.StatisticsNotification;

/**
 * 
 * @author rishim
 *
 */
public class GCStatsMonitor extends MBeanStatsMonitor {

  private volatile long collections = 0;

  private volatile long collectionTime = 0;

  
  public GCStatsMonitor(String name) {
    super(name);
  }

  @Override
  public Number getStatistic(String statName) {
    if (statName.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
      return getCollections();
    }
    if (statName.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
      return getCollectionTime();
    }
    return 0;
  }

   
  @Override
  public void handleNotification(StatisticsNotification notification) {
    decreasePrevValues(statsMap);
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
      increaseStats(name, value);
      statsMap.put(name, value);
    }
  }

  private void decreasePrevValues(DefaultHashMap statsMap) {
    collections -= statsMap.get(StatsKey.VM_GC_STATS_COLLECTIONS).intValue();
    collectionTime -= statsMap.get(StatsKey.VM_GC_STATS_COLLECTION_TIME).intValue();
  }

  private void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
      collections += value.longValue();
      return;
    }
    if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
      collectionTime += value.longValue();
      return;
    }
  }

  public long getCollections() {
    return collections;
  }

  public long getCollectionTime() {
    return collectionTime;
  }
}
