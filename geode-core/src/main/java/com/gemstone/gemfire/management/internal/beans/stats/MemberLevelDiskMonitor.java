/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.beans.stats;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.statistics.StatisticId;
import com.gemstone.gemfire.internal.statistics.StatisticNotFoundException;
import com.gemstone.gemfire.internal.statistics.StatisticsListener;
import com.gemstone.gemfire.internal.statistics.StatisticsNotification;
import com.gemstone.gemfire.internal.statistics.ValueMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor.DefaultHashMap;

/**
 * 
 *
 */
public class MemberLevelDiskMonitor extends MBeanStatsMonitor{


  private volatile long diskReadBytes = 0;

  private volatile long diskWrittenBytes = 0;

  private volatile int backupsInProgress = 0;

  private volatile int backupsCompleted = 0;

  private volatile long flushedBytes = 0;

  private volatile long flushes = 0;

  private volatile long flushTime = 0;

  private volatile int queueSize = 0;

  
  private Map<Statistics, ValueMonitor> monitors;
  
  private Map<Statistics ,MemberLevelDiskStatisticsListener > listeners;

  public MemberLevelDiskMonitor(String name) {
    super(name);
    monitors = new HashMap<Statistics, ValueMonitor>();
    listeners = new HashMap<Statistics, MemberLevelDiskStatisticsListener>();
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
  public void removeStatisticsFromMonitor(Statistics stats) {
    ValueMonitor monitor = monitors.remove(stats);
    if (monitor != null) {
      monitor.removeStatistics(stats);
    }
    MemberLevelDiskStatisticsListener listener = listeners.remove(stats);
    if (listener != null) {
      monitor.removeListener(listener);
    }
    listener.decreaseDiskStoreStats(stats);
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
  public Number getStatistic(String name) {
    if (name.equals(StatsKey.DISK_READ_BYTES)) {
      return getDiskReads();
    }
    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      return getDiskWrites();
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

  private class MemberLevelDiskStatisticsListener implements StatisticsListener {

    DefaultHashMap statsMap = new DefaultHashMap();
    
    private boolean removed = false;

    @Override
    public void handleNotification(StatisticsNotification notification) {
      synchronized (statsMap) {
        if(removed){
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
          log(name,value);

          Number deltaValue = computeDelta(statsMap, name, value);
          statsMap.put(name, value);
          increaseStats(name, deltaValue);

        }

      }
    }

    /**
     * Only decrease those values which can both increase and decrease and not
     * values which can only increase like read/writes
     * 
     * Remove last sample value from the aggregate. Last Sampled value can be
     * obtained from the DefaultHashMap for the disk
     * 
     * @param stats
     */

    public void decreaseDiskStoreStats(Statistics stats) {
      synchronized (statsMap) {
        queueSize -= statsMap.get(StatsKey.DISK_QUEUE_SIZE).intValue();
        backupsInProgress -= statsMap.get(StatsKey.BACKUPS_IN_PROGRESS)
            .intValue();
        ;
        removed = true;

      }

    }

  };
  
  private Number computeDelta(DefaultHashMap statsMap, String name,
      Number currentValue) {
    if (name.equals(StatsKey.DISK_READ_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_READ_BYTES).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.DISK_RECOVERED_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_RECOVERED_BYTES).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    
    if (name.equals(StatsKey.DISK_WRITEN_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.DISK_WRITEN_BYTES).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.BACKUPS_IN_PROGRESS)) { 
      /**
       *  A negative value is also OK. 
       * previous backup_in_progress = 5
       * curr_backup_in_progress = 2
       * delta = -3
       * delta should be added to aggregate backup in progress
       */
      Number prevValue = statsMap.get(StatsKey.BACKUPS_IN_PROGRESS).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.BACKUPS_COMPLETED)) {
      Number prevValue = statsMap.get(StatsKey.BACKUPS_COMPLETED).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.FLUSHED_BYTES)) {
      Number prevValue = statsMap.get(StatsKey.FLUSHED_BYTES).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.NUM_FLUSHES)) {
      Number prevValue = statsMap.get(StatsKey.NUM_FLUSHES).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.TOTAL_FLUSH_TIME)) {
      Number prevValue = statsMap.get(StatsKey.TOTAL_FLUSH_TIME).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    if (name.equals(StatsKey.DISK_QUEUE_SIZE)) {
      Number prevValue = statsMap.get(StatsKey.DISK_QUEUE_SIZE).longValue();
      Number deltaValue = currentValue.longValue() - prevValue.longValue();
      return deltaValue;
    }
    return 0;
  }

  private void increaseStats(String name, Number value) {
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


  public long getDiskReads() {
    return diskReadBytes;
  }

  public long getDiskWrites() {
    return diskWrittenBytes;
  }

  public int getBackupsInProgress() {
    return backupsInProgress;
  }

  public int getBackupsCompleted() {
    return backupsCompleted;
  }

  public long getFlushedBytes() {
    return flushedBytes;
  }

  public long getFlushes() {
    return flushes;
  }

  public long getFlushTime() {
    return flushTime;
  }

  public int getQueueSize() {
    return queueSize;
  }
}
