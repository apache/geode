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
package com.gemstone.gemfire.management.internal.beans;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.SizeEntry;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

/**
 * 
 * @author rishim
 * 
 * MBean Bridge for HDFS region which is a type of Partitioned Region
 */
public class HDFSRegionBridge<K, V> extends PartitionedRegionBridge<K, V> {

  private SortedOplogStatistics soplogStats;

  private MBeanStatsMonitor hdfsRegionMonitor;

  private static final String WRITTEN_BYTES = "writeBytes";

  private static final String READ_BYTES = "readBytes";

  private static final String SCANNED_BYTES = "scanBytes";

  public static final String HDFS_REGION_MONITOR = "HDFSRegionMonitor";

  private StatsRate diskWritesRate;

  private StatsRate diskReadsRate;
  
  private PartitionedRegion parRegion;

  public HDFSRegionBridge(Region<K, V> region) {
    super(region);

    HDFSRegionDirector director = HDFSRegionDirector.getInstance();

    String regionFullPath = region.getFullPath();
    this.soplogStats = director.getHdfsRegionStats(regionFullPath);
    this.hdfsRegionMonitor = new MBeanStatsMonitor(HDFS_REGION_MONITOR + "_" + regionFullPath);
    hdfsRegionMonitor.addStatisticsToMonitor(soplogStats.getStats());
    this.parRegion = (PartitionedRegion)region;
    configureHDFSRegionMetrics();
  }

  private void configureHDFSRegionMetrics() {

    diskWritesRate = new StatsRate(WRITTEN_BYTES, StatType.INT_TYPE, hdfsRegionMonitor);

    String[] readsRates = new String[] { READ_BYTES, SCANNED_BYTES };

    diskReadsRate = new StatsRate(readsRates, StatType.INT_TYPE, hdfsRegionMonitor);
  }

  
  private long estimatedEntryCount = 0;
  

  /**
   * Initialized skipCount to 10 as for the first time we want to compute size
   * of HDFS region.
   */
  private int skipCount = 10;

  /**
   * 
   * An estimated entry count for HDFS region.This may not be accurate but acts
   * as an indicative value.
   * 
   * 
   * Even for estimating size we need to iterate over all BucketRegions and call
   * BucketRegion.size(). This is expensive as compared to reading directly from
   * a statistics value. Hence we are skipping 10 samples.
   * 
   */
  public long getEstimatedSizeForHDFSRegion() {
    if(parRegion.isHDFSReadWriteRegion()){
      if(skipCount % 10 == 0) {
        computeEntryCount();
        skipCount = 1;
      } else {
        skipCount++;
      }
      return estimatedEntryCount;
    }else{
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
    
  }
  
  private void computeEntryCount() {

    if (parRegion.isDataStore()) { //if not a DataStore do nothing and keep the entryCount as 0;
      int numLocalEntries = 0;
      Map<Integer, SizeEntry> localPrimaryBucketRegions = parRegion.getDataStore()
          .getSizeEstimateForLocalPrimaryBuckets();
      if (localPrimaryBucketRegions != null && localPrimaryBucketRegions.size() > 0) {
        for (Map.Entry<Integer, SizeEntry> me : localPrimaryBucketRegions.entrySet()) {
          numLocalEntries += me.getValue().getSize();

        }
      }
      this.estimatedEntryCount = numLocalEntries;
    }
  }
  
  @Override
  public long getEntryCount() {
    if (parRegion.isDataStore()) {
      int numLocalEntries = 0;
      Set<BucketRegion> localPrimaryBucketRegions = parRegion.getDataStore().getAllLocalPrimaryBucketRegions();
      if (localPrimaryBucketRegions != null && localPrimaryBucketRegions.size() > 0) {
        for (BucketRegion br : localPrimaryBucketRegions) {
          // TODO soplog, fix this for griddb regions
          numLocalEntries += br.getRegionMap().sizeInVM() - br.getTombstoneCount();

        }
      }
      return numLocalEntries;
    } else {
      return  ManagementConstants.ZERO;
    }
  }


  @Override
  public long getEntrySize() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  @Override
  public long getDiskUsage() {
    if (soplogStats != null) {
      return soplogStats.getStoreUsageBytes();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  @Override
  public float getDiskReadsRate() {
    return diskReadsRate.getRate();
  }

  @Override
  public float getDiskWritesRate() {
    return diskWritesRate.getRate();
  }
}
