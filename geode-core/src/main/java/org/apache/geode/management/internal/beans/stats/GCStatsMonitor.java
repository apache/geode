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


import java.util.HashMap;
import java.util.Map;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

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
  // it is not using the parent's siteMap
  // this stores each stat's uniqueId and its collection count
  private volatile Map<Long, Number> collections;

  // this stors eaech stat's uniqueId and its collectionTime
  private volatile Map<Long, Number> collectionTime;

  public GCStatsMonitor(String name) {
    this(name, new ValueMonitor());
  }

  @VisibleForTesting
  public GCStatsMonitor(String name, ValueMonitor valueMonitor) {
    super(name, valueMonitor);
    collections = new HashMap<>();
    collectionTime = new HashMap<>();
  }

  long getCollections() {
    return collections.values().stream().mapToLong(Number::longValue).sum();
  }

  long getCollectionTime() {
    return collectionTime.values().stream().mapToLong(Number::longValue).sum();
  }

  @Override
  // this will be called multiple times initially with different collector's stats
  public void addStatisticsToMonitor(Statistics stats) {
    monitor.addListener(this);// if already listener is added this will be a no-op
    monitor.addStatistics(stats);

    StatisticDescriptor[] descriptors = stats.getType().getStatistics();
    for (StatisticDescriptor d : descriptors) {
      String name = d.getName();
      Number value = stats.get(d);
      if (value == null) {
        continue;
      }
      if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
        collections.put(stats.getUniqueId(), value);
      } else if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
        collectionTime.put(stats.getUniqueId(), value);
      }
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
  // this will be called each time a collector's stats changed
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
      if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
        collections.put(statId.getStatistics().getUniqueId(), value);
      }

      else if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
        collectionTime.put(statId.getStatistics().getUniqueId(), value);
      }
    }
  }
}
