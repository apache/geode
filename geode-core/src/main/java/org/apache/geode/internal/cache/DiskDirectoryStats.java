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
package org.apache.geode.internal.cache;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * GemFire statistics about Disk Directories
 *
 *
 * @since GemFire 3.2
 */
public class DiskDirectoryStats {

  @Immutable
  private static final StatisticsType type;

  //////////////////// Statistic "Id" Fields ////////////////////

  private static final int diskSpaceId;
  private static final int maxSpaceId;
  private static final int volumeSizeId;
  private static final int volumeFreeSpaceId;
  private static final int volumeFreeSpaceChecksId;
  private static final int volumeFreeSpaceTimeId;

  static {
    String statName = "DiskDirStatistics";
    String statDescription = "Statistics about a single disk directory for a region";

    final String diskSpaceDesc =
        "The total number of bytes currently being used on disk in this directory for oplog files.";
    final String maxSpaceDesc =
        "The configured maximum number of bytes allowed in this directory for oplog files. Note that some product configurations allow this maximum to be exceeded.";
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription,
        new StatisticDescriptor[] {f.createLongGauge("diskSpace", diskSpaceDesc, "bytes"),
            f.createLongGauge("maximumSpace", maxSpaceDesc, "bytes"),
            f.createLongGauge("volumeSize", "The total size in bytes of the disk volume", "bytes"),
            f.createLongGauge("volumeFreeSpace", "The total free space in bytes on the disk volume",
                "bytes"),
            f.createLongCounter("volumeFreeSpaceChecks", "The total number of disk space checks",
                "checks"),
            f.createLongCounter("volumeFreeSpaceTime", "The total time spent checking disk usage",
                "nanoseconds")});

    // Initialize id fields
    diskSpaceId = type.nameToId("diskSpace");
    maxSpaceId = type.nameToId("maximumSpace");
    volumeSizeId = type.nameToId("volumeSize");
    volumeFreeSpaceId = type.nameToId("volumeFreeSpace");
    volumeFreeSpaceChecksId = type.nameToId("volumeFreeSpaceChecks");
    volumeFreeSpaceTimeId = type.nameToId("volumeFreeSpaceTime");
  }

  ////////////////////// Instance Fields //////////////////////

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DiskRegionStatistics</code> for the given region.
   */
  public DiskDirectoryStats(StatisticsFactory f, String name) {
    stats = f.createStatistics(type, name);
  }

  ///////////////////// Instance Methods /////////////////////

  public void close() {
    stats.close();
  }

  /**
   * Returns the current value of the "diskSpace" stat.
   */
  public long getDiskSpace() {
    return stats.getLong(diskSpaceId);
  }

  public void incDiskSpace(long delta) {
    stats.incLong(diskSpaceId, delta);
  }

  public void setMaxSpace(long v) {
    stats.setLong(maxSpaceId, v);
  }

  public void addVolumeCheck(long total, long free, long time) {
    stats.setLong(volumeSizeId, total);
    stats.setLong(volumeFreeSpaceId, free);
    stats.incLong(volumeFreeSpaceChecksId, 1);
    stats.incLong(volumeFreeSpaceTimeId, time);
  }

  public Statistics getStats() {
    return stats;
  }

}
