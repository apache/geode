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

public class MemberLevelDiskMonitor extends MBeanStatsMonitor {
  private Map<Statistics, ValueMonitor> monitors;
  private Map<Statistics, MemberLevelDiskStatisticsListener> listeners;
  private AtomicLong flushes = new AtomicLong(0);
  private AtomicInteger queueSize = new AtomicInteger(0);
  private AtomicLong flushTime = new AtomicLong(0);
  private AtomicLong flushedBytes = new AtomicLong(0);
  private AtomicLong diskReadBytes = new AtomicLong(0);
  private AtomicInteger backupsCompleted = new AtomicInteger(0);
  private AtomicLong diskWrittenBytes = new AtomicLong(0);
  private AtomicInteger backupsInProgress = new AtomicInteger(0);

  public long getFlushes() {
    return flushes.get();
  }

  public int getQueueSize() {
    return queueSize.get();
  }

  public long getFlushTime() {
    return flushTime.get();
  }

  public long getFlushedBytes() {
    return flushedBytes.get();
  }

  public long getDiskReadBytes() {
    return diskReadBytes.get();
  }

  public int getBackupsCompleted() {
    return backupsCompleted.get();
  }

  public long getDiskWrittenBytes() {
    return diskWrittenBytes.get();
  }

  public int getBackupsInProgress() {
    return backupsInProgress.get();
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

  Number computeDelta(DefaultHashMap statsMap, String name, Number currentValue) {
    if (name.equals(StatsKey.DISK_READ_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_READ_BYTES).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_RECOVERED_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_RECOVERED_BYTES).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_WRITEN_BYTES).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) {
      // A negative value is also OK. previous backup_in_progress = 5 curr_backup_in_progress = 2
      // delta = -3 delta should be added to aggregate backup in progress
      Number prevValue = statsMap.get(StatsKey.BACKUPS_IN_PROGRESS).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      Number prevValue = statsMap.get(StatsKey.BACKUPS_COMPLETED).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.FLUSHED_BYTES).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.NUM_FLUSHES)) {
      Number prevValue = statsMap.get(StatsKey.NUM_FLUSHES).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      Number prevValue = statsMap.get(StatsKey.TOTAL_FLUSH_TIME).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      Number prevValue = statsMap.get(StatsKey.DISK_QUEUE_SIZE).longValue();
      return currentValue.longValue() - prevValue.longValue();
    }

    return 0;
  }

  void increaseStats(String name, Number value) {
    if ((name.equals(StatsKey.DISK_READ_BYTES) || name.equals(StatsKey.DISK_RECOVERED_BYTES))) {
      diskReadBytes.set(diskReadBytes.get() + value.longValue());
      return;
    }

    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      diskWrittenBytes.set(diskWrittenBytes.get() + value.longValue());
      return;
    }

    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) {
      backupsInProgress.set(backupsInProgress.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      backupsCompleted.set(backupsCompleted.get() + value.intValue());
      return;
    }

    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      flushedBytes.set(flushedBytes.get() + value.longValue());
      return;
    }

    if (name.equals(StatsKey.NUM_FLUSHES)) {
      flushes.set(flushes.get() + value.longValue());
      return;
    }

    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      flushTime.set(flushTime.get() + value.longValue());
      return;
    }

    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      queueSize.set(queueSize.get() + value.intValue());
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
    void decreaseDiskStoreStats() {
      synchronized (statsMap) {
        queueSize.set(queueSize.get() - statsMap.get(StatsKey.DISK_QUEUE_SIZE).intValue());
        backupsInProgress
            .set(backupsInProgress.get() - statsMap.get(StatsKey.BACKUPS_IN_PROGRESS).intValue());
        removed = true;
      }
    }
  }
}
