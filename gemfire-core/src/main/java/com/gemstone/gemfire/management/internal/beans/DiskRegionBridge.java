/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import com.gemstone.gemfire.internal.cache.DiskDirectoryStats;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.AggregateRegionStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAverageLatency;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

/**
 * 
 * @author rishim
 * 
 */
public class DiskRegionBridge {

  private DiskRegionStats diskRegionStats;

  private AggregateRegionStatsMonitor aggregateRegionMonitor;

  private MBeanStatsMonitor diskRegionMonitor;

  private StatsAverageLatency diskRegionReadsAverageLatency;

  private StatsAverageLatency diskRegionWritesAverageLatency;

  private StatsRate diskRegionReadsRate;

  private StatsRate diskRegionWritesRate;

  public static final String DISK_REGION_MONITOR = "DiskRegionMonitor";
  
  
  
  private static final String REGION_MONITOR = "MemberLevelRegionMonitor";

  public DiskRegionBridge(DiskRegionStats stats) {
    this.diskRegionStats = stats;
    this.aggregateRegionMonitor = new AggregateRegionStatsMonitor(REGION_MONITOR);

    this.diskRegionMonitor = new MBeanStatsMonitor(DISK_REGION_MONITOR);

    addDiskRegionStats(diskRegionStats);

    this.configureDiskRegionMetrics();
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

    diskRegionWritesRate = new StatsRate(StatsKey.DISK_WRITES, StatType.LONG_TYPE, diskRegionMonitor);

    diskRegionReadsAverageLatency = new StatsAverageLatency(StatsKey.DISK_READS, StatType.LONG_TYPE,
        StatsKey.DISK_REGION_READ_TIME, diskRegionMonitor);

    diskRegionWritesAverageLatency = new StatsAverageLatency(StatsKey.DISK_WRITES, StatType.LONG_TYPE,
        StatsKey.DISK_REGION_WRITE_TIMES, diskRegionMonitor);
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