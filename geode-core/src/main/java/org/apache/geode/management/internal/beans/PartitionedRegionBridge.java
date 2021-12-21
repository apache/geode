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

import java.util.Set;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.management.FixedPartitionAttributesData;
import org.apache.geode.management.PartitionAttributesData;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsLatency;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class PartitionedRegionBridge<K, V> extends RegionMBeanBridge<K, V> {

  private final PartitionedRegionStats prStats;

  private PartitionedRegion parRegion;

  private PartitionAttributesData partitionAttributesData;

  private FixedPartitionAttributesData[] fixedPartitionAttributesTable;

  private int configuredRedundancy = -1;

  private final MBeanStatsMonitor parRegionMonitor;

  private StatsRate putAllRate;

  private StatsRate putRequestRate;

  private StatsRate getRequestRate;

  private StatsRate createsRate;

  private StatsRate destroysRate;

  private StatsRate putLocalRate;

  private StatsRate putRemoteRate;

  private StatsLatency putRemoteLatency;

  private StatsRate averageWritesRate;

  private StatsRate averageReadsRate;

  private StatsAverageLatency remotePutAvgLatency;

  public static final String PAR_REGION_MONITOR = "PartitionedRegionMonitor";


  public static <K, V> PartitionedRegionBridge<K, V> getInstance(Region<K, V> region) {
    return new PartitionedRegionBridge<K, V>(region);
  }



  protected PartitionedRegionBridge(Region<K, V> region) {
    super(region);
    parRegion = (PartitionedRegion) region;
    prStats = parRegion.getPrStats();

    PartitionAttributes<K, V> partAttrs = parRegion.getPartitionAttributes();

    parRegionMonitor = new MBeanStatsMonitor(PAR_REGION_MONITOR);

    configurePartitionRegionMetrics();

    configuredRedundancy = partAttrs.getRedundantCopies();
    partitionAttributesData =
        RegionMBeanCompositeDataFactory.getPartitionAttributesData(partAttrs);
    if (partAttrs.getFixedPartitionAttributes() != null) {
      fixedPartitionAttributesTable =
          RegionMBeanCompositeDataFactory.getFixedPartitionAttributesData(partAttrs);
    }
    parRegionMonitor.addStatisticsToMonitor(prStats.getStats());
  }

  // Dummy constructor for testing purpose only
  public PartitionedRegionBridge(PartitionedRegionStats prStats) {
    this.prStats = prStats;

    parRegionMonitor = new MBeanStatsMonitor(PAR_REGION_MONITOR);
    parRegionMonitor.addStatisticsToMonitor(prStats.getStats());
    configurePartitionRegionMetrics();
  }

  private Number getPrStatistic(String statName) {
    if (prStats != null) {
      return prStats.getStats().get(statName);
    } else {
      return ManagementConstants.ZERO;
    }
  }

  @Override
  public void stopMonitor() {
    super.stopMonitor();
    parRegionMonitor.stopListener();
  }

  private void configurePartitionRegionMetrics() {
    putAllRate = new StatsRate(StatsKey.PUTALL_COMPLETED, StatType.INT_TYPE, parRegionMonitor);
    putRequestRate = new StatsRate(StatsKey.PUTS_COMPLETED, StatType.INT_TYPE, parRegionMonitor);
    getRequestRate = new StatsRate(StatsKey.GETS_COMPLETED, StatType.INT_TYPE, parRegionMonitor);
    destroysRate = new StatsRate(StatsKey.DESTROYS_COMPLETED, StatType.INT_TYPE, parRegionMonitor);
    createsRate = new StatsRate(StatsKey.CREATES_COMPLETED, StatType.INT_TYPE, parRegionMonitor);

    // Remote Reads Only in case of partitioned Region
    putRemoteRate = new StatsRate(StatsKey.REMOTE_PUTS, StatType.INT_TYPE, parRegionMonitor);

    putLocalRate = new StatsRate(StatsKey.PUT_LOCAL, StatType.INT_TYPE, parRegionMonitor);

    remotePutAvgLatency = new StatsAverageLatency(StatsKey.REMOTE_PUTS, StatType.INT_TYPE,
        StatsKey.REMOTE_PUT_TIME, parRegionMonitor);

    putRemoteLatency = new StatsLatency(StatsKey.REMOTE_PUTS, StatType.INT_TYPE,
        StatsKey.REMOTE_PUT_TIME, parRegionMonitor);

    String[] writesRates = new String[] {StatsKey.PUTALL_COMPLETED, StatsKey.PUTS_COMPLETED,
        StatsKey.CREATES_COMPLETED};
    averageWritesRate = new StatsRate(writesRates, StatType.INT_TYPE, parRegionMonitor);
    averageReadsRate = new StatsRate(StatsKey.GETS_COMPLETED, StatType.INT_TYPE, parRegionMonitor);
  }

  @Override
  public float getAverageReads() {
    return averageReadsRate.getRate();
  }

  @Override
  public float getAverageWrites() {
    return averageWritesRate.getRate();
  }

  @Override
  public float getCreatesRate() {
    return createsRate.getRate();
  }

  @Override
  public float getPutAllRate() {
    return putAllRate.getRate();
  }

  @Override
  public float getPutsRate() {
    return putRequestRate.getRate();
  }

  @Override
  public float getDestroyRate() {
    return destroysRate.getRate();
  }

  @Override
  public float getGetsRate() {
    return getRequestRate.getRate();
  }


  @Override
  public int getActualRedundancy() {
    return getPrStatistic(StatsKey.ACTUAL_REDUNDANT_COPIES).intValue();
  }

  @Override
  public int getAvgBucketSize() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  @Override
  public int getBucketCount() {
    return getPrStatistic(StatsKey.BUCKET_COUNT).intValue();
  }

  @Override
  public int getConfiguredRedundancy() {
    return configuredRedundancy;
  }

  @Override
  public int getNumBucketsWithoutRedundancy() {
    return getPrStatistic(StatsKey.LOW_REDUNDANCYBUCKET_COUNT).intValue();
  }

  @Override
  public int getPrimaryBucketCount() {
    return getPrStatistic(StatsKey.PRIMARY_BUCKET_COUNT).intValue();
  }

  @Override
  public int getTotalBucketSize() {
    return getPrStatistic(StatsKey.TOTAL_BUCKET_SIZE).intValue();
  }

  @Override
  public PartitionAttributesData listPartitionAttributes() {
    return partitionAttributesData;
  }

  @Override
  public FixedPartitionAttributesData[] listFixedPartitionAttributes() {
    return fixedPartitionAttributesTable;
  }

  @Override
  public long getEntrySize() {
    if (parRegion.isDataStore()) {
      return getPrStatistic(StatsKey.DATA_STORE_BYTES_IN_USE).longValue();
    } else {
      return ManagementConstants.ZERO;
    }
  }

  @Override
  public long getHitCount() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  @Override
  public float getHitRatio() {
    return ManagementConstants.NOT_AVAILABLE_FLOAT;
  }

  @Override
  public long getLastAccessedTime() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  @Override
  public long getLastModifiedTime() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }


  @Override
  public long getPutRemoteAvgLatency() {
    return remotePutAvgLatency.getAverageLatency();
  }

  @Override
  public long getPutRemoteLatency() {
    return putRemoteLatency.getLatency();
  }

  @Override
  public float getPutLocalRate() {
    return putLocalRate.getRate();
  }

  @Override
  public float getPutRemoteRate() {
    return putRemoteRate.getRate();
  }

  /**
   * partition region entry count is taken from all primary bucket entry count. Ideally it should
   * come from stats. to be done in 8.0
   *
   */
  @Override
  public long getEntryCount() {
    if (parRegion.isDataStore()) {
      int numLocalEntries = 0;
      Set<BucketRegion> localPrimaryBucketRegions =
          parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
      if (localPrimaryBucketRegions != null && localPrimaryBucketRegions.size() > 0) {
        for (BucketRegion br : localPrimaryBucketRegions) {
          // TODO soplog, fix this for griddb regions
          numLocalEntries += br.getRegionMap().sizeInVM() - br.getTombstoneCount();

        }
      }
      return numLocalEntries;
    } else {
      return ManagementConstants.ZERO;
    }
  }

  @Override
  public int getLocalMaxMemory() {
    return partitionAttributesData.getLocalMaxMemory();
  }
}
