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

import java.util.Map;

import org.apache.geode.StatisticDescriptor;
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
  private volatile long collections = 0;
  private volatile long collectionTime = 0;

  long getCollections() {
    return collections;
  }

  long getCollectionTime() {
    return collectionTime;
  }

  public GCStatsMonitor(String name) {
    super(name);
  }

  void decreasePrevValues(Map<String, Number> statsMap) {
    collections -= statsMap.getOrDefault(StatsKey.VM_GC_STATS_COLLECTIONS, 0).longValue();
    collectionTime -= statsMap.getOrDefault(StatsKey.VM_GC_STATS_COLLECTION_TIME, 0).longValue();
  }

  void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
      collections += value.longValue();
      return;
    }

    if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
      collectionTime += value.longValue();
      return;
    }
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

      log(name, value);
      increaseStats(name, value);
      statsMap.put(name, value);
    }
  }
}
