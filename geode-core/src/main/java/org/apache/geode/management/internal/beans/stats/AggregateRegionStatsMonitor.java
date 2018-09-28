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
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.statistics.StatisticId;
import org.apache.geode.internal.statistics.StatisticNotFoundException;
import org.apache.geode.internal.statistics.StatisticsListener;
import org.apache.geode.internal.statistics.StatisticsNotification;
import org.apache.geode.internal.statistics.ValueMonitor;

/**
 * This class acts as a monitor and listen for Region statistics updates on behalf of MemberMBean.
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
public class AggregateRegionStatsMonitor extends MBeanStatsMonitor {
  private volatile long diskSpace = 0;
  private volatile int bucketCount = 0;
  private volatile long lruDestroys = 0;
  private volatile long lruEvictions = 0;
  private volatile int totalBucketSize = 0;
  private volatile int primaryBucketCount = 0;
  private final Map<Statistics, ValueMonitor> monitors;
  private final Map<Statistics, MemberLevelRegionStatisticsListener> listeners;

  public long getDiskSpace() {
    return diskSpace;
  }

  public int getTotalBucketCount() {
    return bucketCount;
  }

  long getLruDestroys() {
    return lruDestroys;
  }

  long getLruEvictions() {
    return lruEvictions;
  }

  public int getTotalBucketSize() {
    return totalBucketSize;
  }

  public int getTotalPrimaryBucketCount() {
    return primaryBucketCount;
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

  Number computeDelta(Map<String, Number> statsMap, String name, Number currentValue) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.PRIMARY_BUCKET_COUNT, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.BUCKET_COUNT, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.TOTAL_BUCKET_SIZE, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.LRU_EVICTIONS, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.LRU_DESTROYS, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.DISK_SPACE, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    return 0;
  }

  void increaseStats(String name, Number value) {
    if (name.equals(StatsKey.PRIMARY_BUCKET_COUNT)) {
      primaryBucketCount += value.intValue();
      return;
    }

    if (name.equals(StatsKey.BUCKET_COUNT)) {
      bucketCount += value.intValue();
      return;
    }

    if (name.equals(StatsKey.TOTAL_BUCKET_SIZE)) {
      totalBucketSize += value.intValue();
      return;
    }

    if (name.equals(StatsKey.LRU_EVICTIONS)) {
      lruEvictions += value.longValue();
      return;
    }

    if (name.equals(StatsKey.LRU_DESTROYS)) {
      lruDestroys += value.longValue();
      return;
    }

    if (name.equals(StatsKey.DISK_SPACE)) {
      diskSpace += value.longValue();
      return;
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
    final Map<String, Number> statsMap = new HashMap<>();
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
        bucketCount -= statsMap.getOrDefault(StatsKey.BUCKET_COUNT, 0).intValue();
        totalBucketSize -= statsMap.getOrDefault(StatsKey.TOTAL_BUCKET_SIZE, 0).intValue();
        primaryBucketCount -= statsMap.getOrDefault(StatsKey.PRIMARY_BUCKET_COUNT, 0).intValue();
        removed = true;
      }
    }
  }
}
