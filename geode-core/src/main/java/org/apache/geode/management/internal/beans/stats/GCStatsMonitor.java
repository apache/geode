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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsNotification;

public class GCStatsMonitor extends MBeanStatsMonitor {
  private AtomicLong collections = new AtomicLong(0);
  private AtomicLong collectionTime = new AtomicLong(0);

  long getCollections() {
    return collections.get();
  }

  long getCollectionTime() {
    return collectionTime.get();
  }

  public GCStatsMonitor(String name) {
    super(name);
  }

  void decreasePrevValues(DefaultHashMap statsMap) {
    collections.set(collections.get() - statsMap.get(StatsKey.VM_GC_STATS_COLLECTIONS).intValue());
    collectionTime
        .set(collectionTime.get() - statsMap.get(StatsKey.VM_GC_STATS_COLLECTION_TIME).intValue());
  }

  void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.VM_GC_STATS_COLLECTIONS)) {
      collections.set(collections.get() + value.longValue());
    }

    if (name.equals(StatsKey.VM_GC_STATS_COLLECTION_TIME)) {
      collectionTime.set(collectionTime.get() + value.longValue());
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
