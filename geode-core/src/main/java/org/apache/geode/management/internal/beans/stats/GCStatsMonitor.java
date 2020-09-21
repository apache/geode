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


import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsNotification;

/**
 * This class acts as a monitor and listen for GC statistics updates on behalf of MemberMBean.
 * <p>
 * There's only one dedicated thread that wakes up at the
 * {@link ConfigurationProperties#STATISTIC_SAMPLE_RATE} configured, samples all the statistics,
 * writes them to the {@link ConfigurationProperties#STATISTIC_ARCHIVE_FILE} configured (if any) and
 * notifies listeners of changes. The mutable fields are declared as {@code volatile} to make sure
 * readers of the statistics get the latest recorded value.
 * <p>
 * This class is conditionally thread-safe, there can be multiple concurrent readers accessing a
 * instance, but concurrent writers need to be synchronized externally.
 *
 * @see org.apache.geode.internal.statistics.HostStatSampler
 * @see org.apache.geode.distributed.ConfigurationProperties
 * @see org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor
 */
public class GCStatsMonitor extends MBeanStatsMonitor {
  // this class uses these volatile variables to make sure reads are reading the latest values
  // it is not using the parent's siteMap which is not volatile to keep the stats values.
  private volatile long collections = 0;
  private volatile long collectionTime = 0;

  public GCStatsMonitor(String name) {
    super(name);
  }

  @Override
  // this will be called multiple times with different collector's stats
  public void addStatisticsToMonitor(Statistics stats) {
    monitor.addListener(this);// if already listener is added this will be a no-op
    monitor.addStatistics(stats);

    // stats map should keep the sum of all the GC stats
    StatisticDescriptor[] descriptors = stats.getType().getStatistics();
    for (StatisticDescriptor d : descriptors) {
      String name = d.getName();
      Number value = stats.get(d);
      if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
        collections += value.longValue();
      } else if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
        collectionTime += value.longValue();
      }
    }
  }

  @Override
  public Number getStatistic(String statName) {
    if (statName.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
      return collections;
    }

    if (statName.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
      return collectionTime;
    }
    return 0;
  }

  @Override
  public void handleNotification(StatisticsNotification notification) {
    // sum up all the count and all the time in the stats included in this notification
    long totalCount = 0;
    long totalTime = 0;
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
      if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
        totalCount += value.longValue();
      }

      else if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
        totalTime += value.longValue();
      }
    }

    collections = totalCount;
    collectionTime = totalTime;
  }
}
