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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsListener;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

public class AggregateRegionStatsMonitor extends MBeanStatsMonitor {
  private Map<Statistics, ValueMonitor> monitors;
  private Map<Statistics, MemberLevelRegionStatisticsListener> listeners;
  private AtomicLong diskSpace = new AtomicLong(0);
  private AtomicInteger bucketCount = new AtomicInteger(0);
  private AtomicLong lruDestroys = new AtomicLong(0);
  private AtomicLong lruEvictions = new AtomicLong(0);
  private AtomicInteger totalBucketSize = new AtomicInteger(0);
  private AtomicInteger primaryBucketCount = new AtomicInteger(0);

  public long getDiskSpace() {
    return diskSpace.get();
  }

  public int getTotalBucketCount() {
    return bucketCount.get();
  }

  public long getLruDestroys() {
    return lruDestroys.get();
  }

  public long getLruEvictions() {
    return lruEvictions.get();
  }

  public int getTotalBucketSize() {
    return totalBucketSize.get();
  }

  public int getTotalPrimaryBucketCount() {
    return primaryBucketCount.get();
  }

  Map<Statistics, ValueMonitor> getMonitors() {
    return monitors;
  }

  Map<Statistics, MemberLevelRegionStatisticsListener> getListeners() {
    return listeners;
  }

  public AggregateRegionStatsMonitor(String name) {
    super(name);
    monitors = new HashMap<>();
    listeners = new HashMap<>();
  }

  Number computeDelta(DefaultHashMap statsMap, String name, Number currentValue) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      Number prevValue = statsMap.get(StatsKey.PRIMARY_BUCKET_COUNT).intValue();
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      Number prevValue = statsMap.get(StatsKey.BUCKET_COUNT).intValue();
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      Number prevValue = statsMap.get(StatsKey.TOTAL_BUCKET_SIZE).intValue();
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      Number prevValue = statsMap.get(StatsKey.LRU_EVICTIONS).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      Number prevValue = statsMap.get(StatsKey.LRU_DESTROYS).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      Number prevValue = statsMap.get(StatsKey.DISK_SPACE).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    return 0;
  }

  void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      primaryBucketCount.set(primaryBucketCount.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      bucketCount.set(bucketCount.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      totalBucketSize.set(totalBucketSize.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      lruEvictions.set(lruEvictions.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      lruDestroys.set(lruDestroys.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      diskSpace.set(diskSpace.get() + value.intValue());
    }
  }

  private MemberLevelRegionStatisticsListener removeListener(Statistics stats) {
    ValueMonitor monitor = monitors.remove(stats);
    if (monitor != null) {
      monitor.removeStatistics(stats);
    }

    MemberLevelRegionStatisticsListener listener = listeners.remove(stats);
    if ((listener != null) && (monitor != null)) {
      monitor.removeListener(listener);
    }

    return listener;
  }

  public void removeLRUStatistics(Statistics stats) {
    removeListener(stats);
  }

  public void removeDirectoryStatistics(Statistics stats) {
    removeListener(stats);
  }

  public void removePartitionStatistics(Statistics stats) {
    MemberLevelRegionStatisticsListener listener = removeListener(stats);

    if (listener != null) {
      listener.decreaseParStats();
    }
  }

  @Override
  public Number getStatistic(String name) {
    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      return getLruEvictions();
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      return getLruDestroys();
    }

    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      return getTotalPrimaryBucketCount();
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      return getTotalBucketCount();
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      return getTotalBucketSize();
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      return getDiskSpace();
    }

    return 0;
  }

  @Override
  public void addStatisticsToMonitor(Statistics stats) {
    ValueMonitor regionMonitor = new ValueMonitor();
    MemberLevelRegionStatisticsListener listener = new MemberLevelRegionStatisticsListener();
    regionMonitor.addListener(listener);
    regionMonitor.addStatistics(stats);

    monitors.put(stats, regionMonitor);
    listeners.put(stats, listener);
  }

  @Override
  public void stopListener() {
    for (Statistics stat : listeners.keySet()) {
      ValueMonitor monitor = monitors.get(stat);
      monitor.removeListener(listeners.get(stat));
      monitor.removeStatistics(stat);
    }

    listeners.clear();
    monitors.clear();
  }

  @Override
  public void removeStatisticsFromMonitor(Statistics stats) {}

  class MemberLevelRegionStatisticsListener implements StatisticsListener {
    DefaultHashMap statsMap = new DefaultHashMap();
    private boolean removed = false;

    @Override
    public void handleNotification(StatisticsNotification notification) {
      synchronized (statsMap) {
        if (removed) {
          return;
        }

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
          Number deltaValue = computeDelta(statsMap, name, value);
          statsMap.put(name, value);
          increaseStats(name, deltaValue);
        }
      }
    }

    /**
     * Only decrease those values which can both increase and decrease and not values which can only
     * increase like read/writes
     *
     * Remove last sample value from the aggregate. Last Sampled value can be obtained from the
     * DefaultHashMap for the disk
     *
     */
    void decreaseParStats() {
      synchronized (statsMap) {
        primaryBucketCount
            .set(primaryBucketCount.get() - statsMap.get(StatsKey.PRIMARY_BUCKET_COUNT).intValue());
        bucketCount.set(bucketCount.get() - statsMap.get(StatsKey.BUCKET_COUNT).intValue());
        totalBucketSize
            .set(totalBucketSize.get() - statsMap.get(StatsKey.TOTAL_BUCKET_SIZE).intValue());
        removed = true;
      }
    }
  }
}
