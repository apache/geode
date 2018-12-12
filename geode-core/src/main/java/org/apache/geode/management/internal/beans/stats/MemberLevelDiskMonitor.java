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
 * This class acts as a monitor and listen for Disk statistics updates on behalf of MemberMBean.
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
public class MemberLevelDiskMonitor extends MBeanStatsMonitor {
  private volatile long flushes = 0;
  private volatile int queueSize = 0;
  private volatile long flushTime = 0;
  private volatile long flushedBytes = 0;
  private volatile long diskReadBytes = 0;
  private volatile int backupsCompleted = 0;
  private volatile long diskWrittenBytes = 0;
  private volatile int backupsInProgress = 0;
  private final Map<Statistics, ValueMonitor> monitors;
  private final Map<Statistics, MemberLevelDiskStatisticsListener> listeners;

  public long getFlushes() {
    return flushes;
  }

  public int getQueueSize() {
    return queueSize;
  }

  long getFlushTime() {
    return flushTime;
  }

  long getFlushedBytes() {
    return flushedBytes;
  }

  long getDiskReadBytes() {
    return diskReadBytes;
  }

  public int getBackupsCompleted() {
    return backupsCompleted;
  }

  long getDiskWrittenBytes() {
    return diskWrittenBytes;
  }

  public int getBackupsInProgress() {
    return backupsInProgress;
  }

  Map<Statistics, ValueMonitor> getMonitors() {
    return monitors;
  }

  Map<Statistics, MemberLevelDiskStatisticsListener> getListeners() {
    return listeners;
  }

  public MemberLevelDiskMonitor(String name) {
    super(name);
    monitors = new HashMap<>();
    listeners = new HashMap<>();
  }

  Number computeDelta(Map<String, Number> statsMap, String name, Number currentValue) {
    if (name.equals(StatsKey.DISK_READ_BYTES)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.DISK_READ_BYTES, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_RECOVERED_BYTES)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.DISK_RECOVERED_BYTES, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.DISK_WRITEN_BYTES, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) {
      // A negative value is also OK. previous backup_in_progress = 5 curr_backup_in_progress = 2
      // delta = -3 delta should be added to aggregate backup in progress
      Number prevValue = statsMap.getOrDefault(StatsKey.BACKUPS_IN_PROGRESS, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.BACKUPS_COMPLETED, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.FLUSHED_BYTES, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.NUM_FLUSHES)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.NUM_FLUSHES, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.TOTAL_FLUSH_TIME, 0);
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      Number prevValue = statsMap.getOrDefault(StatsKey.DISK_QUEUE_SIZE, 0);
      return currentValue.intValue() - prevValue.intValue();
    }

    return 0;
  }

  void increaseStats(String name, Number value) {
    if ((name.equals(StatsKey.DISK_READ_BYTES) || name.equals(StatsKey.DISK_RECOVERED_BYTES))) {
      diskReadBytes = diskReadBytes + value.longValue();
      return;
    }

    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      diskWrittenBytes = diskWrittenBytes + value.longValue();
      return;
    }

    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) {
      backupsInProgress = backupsInProgress + value.intValue();
      return;
    }

    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      backupsCompleted = backupsCompleted + value.intValue();
      return;
    }

    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      flushedBytes = flushedBytes + value.longValue();
      return;
    }

    if (name.equals(StatsKey.NUM_FLUSHES)) {
      flushes = flushes + value.longValue();
      return;
    }

    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      flushTime = flushTime + value.longValue();
      return;
    }

    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      queueSize = queueSize + value.intValue();
      return;
    }
  }

  @Override
  public Number getStatistic(String name) {
    if (name.equals(StatsKey.DISK_READ_BYTES)) {
      return getDiskReadBytes();
    }

    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      return getDiskWrittenBytes();
    }

    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) {
      return getBackupsInProgress();
    }

    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      return getBackupsCompleted();
    }

    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      return getFlushedBytes();
    }

    if (name.equals(StatsKey.NUM_FLUSHES)) {
      return getFlushes();
    }

    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      return getFlushTime();
    }

    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      return getQueueSize();
    }

    return 0;
  }

  @Override
  public void addStatisticsToMonitor(Statistics stats) {
    ValueMonitor diskMonitor = new ValueMonitor();
    MemberLevelDiskStatisticsListener listener = new MemberLevelDiskStatisticsListener();
    diskMonitor.addListener(listener);
    diskMonitor.addStatistics(stats);

    monitors.put(stats, diskMonitor);
    listeners.put(stats, listener);
  }

  @Override
  public void stopListener() {
    for (Statistics stat : listeners.keySet()) {
      ValueMonitor monitor = monitors.get(stat);
      monitor.removeListener(listeners.get(stat));
      monitor.removeStatistics(stat);
    }

    monitors.clear();
    listeners.clear();
  }

  @Override
  public void removeStatisticsFromMonitor(Statistics stats) {
    ValueMonitor monitor = monitors.remove(stats);
    if (monitor != null) {
      monitor.removeStatistics(stats);
    }

    MemberLevelDiskStatisticsListener listener = listeners.remove(stats);
    if (listener != null) {
      if (monitor != null) {
        monitor.removeListener(listener);
      }

      listener.decreaseDiskStoreStats();
    }
  }

  class MemberLevelDiskStatisticsListener implements StatisticsListener {
    Map<String, Number> statsMap = new HashMap<>();
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
    void decreaseDiskStoreStats() {
      synchronized (statsMap) {
        queueSize -= statsMap.getOrDefault(StatsKey.DISK_QUEUE_SIZE, 0).intValue();
        backupsInProgress -= statsMap.getOrDefault(StatsKey.BACKUPS_IN_PROGRESS, 0).intValue();;
        removed = true;
      }
    }
  }
}
