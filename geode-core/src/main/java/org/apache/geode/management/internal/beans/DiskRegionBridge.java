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
package org.apache.geode.management.internal.beans;

import org.apache.geode.internal.cache.DiskDirectoryStats;
import org.apache.geode.internal.cache.DiskRegionStats;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.stats.AggregateRegionStatsMonitor;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class DiskRegionBridge {

  private final DiskRegionStats diskRegionStats;

  private final AggregateRegionStatsMonitor aggregateRegionMonitor;

  private final MBeanStatsMonitor diskRegionMonitor;

  private StatsAverageLatency diskRegionReadsAverageLatency;

  private StatsAverageLatency diskRegionWritesAverageLatency;

  private StatsRate diskRegionReadsRate;

  private StatsRate diskRegionWritesRate;

  public static final String DISK_REGION_MONITOR = "DiskRegionMonitor";



  private static final String REGION_MONITOR = "MemberLevelRegionMonitor";

  public DiskRegionBridge(DiskRegionStats stats) {
    diskRegionStats = stats;
    aggregateRegionMonitor = new AggregateRegionStatsMonitor(REGION_MONITOR);

    diskRegionMonitor = new MBeanStatsMonitor(DISK_REGION_MONITOR);

    addDiskRegionStats(diskRegionStats);

    configureDiskRegionMetrics();
  }

  private Number getDiskRegionStatistic(String statName) {
    if (diskRegionStats != null) {
      return diskRegionStats.getStats().get(statName);
    } else {
      return ManagementConstants.ZERO;
    }
  }

  public void stopMonitor() {
    diskRegionMonitor.stopListener();
    aggregateRegionMonitor.stopListener();
  }

  public void addDirectoryStats(DiskDirectoryStats diskDirStats) {
    aggregateRegionMonitor.addStatisticsToMonitor(diskDirStats.getStats());
  }

  public void addDiskRegionStats(DiskRegionStats diskRegionStats) {
    diskRegionMonitor.addStatisticsToMonitor(diskRegionStats.getStats());
  }

  private void configureDiskRegionMetrics() {

    diskRegionReadsRate = new StatsRate(StatsKey.DISK_READS, StatType.LONG_TYPE, diskRegionMonitor);

    diskRegionWritesRate =
        new StatsRate(StatsKey.DISK_WRITES, StatType.LONG_TYPE, diskRegionMonitor);

    diskRegionReadsAverageLatency = new StatsAverageLatency(StatsKey.DISK_READS, StatType.LONG_TYPE,
        StatsKey.DISK_REGION_READ_TIME, diskRegionMonitor);

    diskRegionWritesAverageLatency = new StatsAverageLatency(StatsKey.DISK_WRITES,
        StatType.LONG_TYPE, StatsKey.DISK_REGION_WRITE_TIMES, diskRegionMonitor);
  }

  public float getDiskReadsRate() {
    return diskRegionReadsRate.getRate();
  }

  public float getDiskWritesRate() {
    return diskRegionWritesRate.getRate();
  }

  public long getDiskReadsAverageLatency() {
    return diskRegionReadsAverageLatency.getAverageLatency();
  }

  public long getDiskWritesAverageLatency() {
    return diskRegionWritesAverageLatency.getAverageLatency();
  }

  public long getTotalDiskWritesProgress() {
    return getDiskRegionStatistic(StatsKey.DISK_REGION_WRITE_IN_PROGRESS).longValue();
  }

  public long getTotalDiskEntriesInVM() {
    return getDiskRegionStatistic(StatsKey.DISK_REGION_ENTRIES_IN_VM).longValue();
  }

  public long getTotalEntriesOnlyOnDisk() {
    return getDiskRegionStatistic(StatsKey.DISK_REGION_ENTRIES_IN_DISK).longValue();
  }

  public long getDiskUsage() {
    long diskSpaceUsage = aggregateRegionMonitor.getDiskSpace();
    return diskSpaceUsage;
  }

  public long getDiskTaskWaiting() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

}
