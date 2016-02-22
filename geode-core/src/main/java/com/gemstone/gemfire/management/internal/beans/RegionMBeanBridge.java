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

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.DirectoryHolder;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.FixedPartitionAttributesData;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.ManagementStrings;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAverageLatency;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

/**
 * This class acts as a bridge between a Region and RegionMBean This also
 * listens for statistics changes and update its value accordingly
 * 
 * 
 * @param <K>
 * @param <V>
 */
public class RegionMBeanBridge<K, V> {

  private EvictionAttributesData evictionAttributesData;

  private MembershipAttributesData membershipAttributesData;

  private RegionAttributesData regionAttributesData;

  private Region<K, V> region;

  private RegionAttributes<K, V> regAttrs;

  private boolean isStatisticsEnabled = false;
  
  private MBeanStatsMonitor regionMonitor;
  
  private StatsRate getRequestRate;

  private StatsRate putRequestRate;

  private StatsRate putAllRate;

  private StatsRate createsRate;

  private StatsAverageLatency listenerCallsAvgLatency;

  private StatsAverageLatency writerCallsAvgLatency;

  private StatsRate destroysRate;
  
  private StatsRate lruDestroyRate;
  
  private StatsRate lruEvictionRate;
  
  private boolean isGatewayEnabled = false;
  
  private boolean persistentEnabled = false;
  
  private String member;
  
  private LRUStatistics lruMemoryStats;
  
  private CachePerfStats regionStats;
  
  private DiskRegionBridge diskRegionBridge;
  
  private StatsRate averageWritesRate;
  
  private StatsRate averageReadsRate;
  
  

  public static <K, V> RegionMBeanBridge<K, V> getInstance(Region<K, V> region) {

    if (region.getAttributes().getPartitionAttributes() != null) {
      RegionMBeanBridge<K, V> bridge = PartitionedRegionBridge.getInstance(region);
      PartitionedRegion parRegion = ((PartitionedRegion) region);
      DiskStoreImpl dsi = parRegion.getDiskStore();
      if (dsi != null) {
        DiskRegionStats stats = parRegion.getDiskRegionStats();

        DiskRegionBridge diskRegionBridge = new DiskRegionBridge(stats);
        bridge.addDiskRegionBridge(diskRegionBridge);

        for (DirectoryHolder dh : dsi.getDirectoryHolders()) {
          diskRegionBridge.addDirectoryStats(dh.getDiskDirectoryStats());
        }

        bridge.addDiskRegionBridge(diskRegionBridge);

      }

      return bridge;

    } else {
      RegionMBeanBridge<K, V> bridge = new RegionMBeanBridge<K, V>(region);

      LocalRegion localRegion = ((LocalRegion) region);
      DiskStoreImpl dsi = localRegion.getDiskStore();
      if (dsi != null) {
        DiskRegionBridge diskRegionBridge = new DiskRegionBridge(localRegion.getDiskRegion().getStats());
        bridge.addDiskRegionBridge(diskRegionBridge);

        for (DirectoryHolder dh : dsi.getDirectoryHolders()) {
          diskRegionBridge.addDirectoryStats(dh.getDiskDirectoryStats());
        }
      }
      return bridge;

    }
  }
  
  
  protected void addDiskRegionBridge(DiskRegionBridge diskRegionBridge){
   this.diskRegionBridge = diskRegionBridge; 
  }
  
  protected RegionMBeanBridge(Region<K, V> region) {
    this.region = region;
    this.regAttrs = region.getAttributes();
    
    this.isStatisticsEnabled = regAttrs.getStatisticsEnabled();
    
    this.regionAttributesData = RegionMBeanCompositeDataFactory.getRegionAttributesData(regAttrs);
    this.membershipAttributesData = RegionMBeanCompositeDataFactory.getMembershipAttributesData(regAttrs);
    this.evictionAttributesData = RegionMBeanCompositeDataFactory.getEvictionAttributesData(regAttrs);   
  
    this.regionMonitor = new MBeanStatsMonitor(ManagementStrings.REGION_MONITOR
        .toLocalizedString());
    
    configureRegionMetrics();

    this.persistentEnabled = region.getAttributes().getDataPolicy()
        .withPersistence();
    
    
   
    
    this.regionStats = ((LocalRegion)region).getRegionPerfStats();
      if (regionStats != null) {
        regionMonitor.addStatisticsToMonitor(regionStats.getStats()); // fixes 46692
      }

    LocalRegion l = (LocalRegion) region;
    if(l.getEvictionController() != null){
      LRUStatistics stats = l.getEvictionController().getLRUHelper().getStats();
      if (stats != null) {
        regionMonitor.addStatisticsToMonitor(stats.getStats());
        EvictionAttributes ea = region.getAttributes().getEvictionAttributes();
        if (ea != null && ea.getAlgorithm().isLRUMemory()) {
          this.lruMemoryStats = stats;
        }

      }
    }

    if(regAttrs.getGatewaySenderIds() != null &&  regAttrs.getGatewaySenderIds().size() > 0){
      this.isGatewayEnabled = true;
    }
    
    this.member = GemFireCacheImpl.getInstance().getDistributedSystem()
        .getMemberId();
  }


  
  public String getRegionType() {
    return region.getAttributes().getDataPolicy().toString();
  }

  public String getFullPath() {   
    return region.getFullPath();
  }

  public String getName() {
    return region.getName();
  }

  public EvictionAttributesData listEvictionAttributes() {
    return evictionAttributesData;
  }

  public MembershipAttributesData listMembershipAttributes() {
    return membershipAttributesData;
  }


  public RegionAttributesData listRegionAttributes() {
    return regionAttributesData;
  }

  public String getParentRegion() {
    if (region.getParentRegion() != null) {
      return region.getParentRegion().getName();
    }
    return null;
  }
  
  public String[] listSubRegionPaths(boolean recursive) {
    SortedSet<String> subregionPaths = new TreeSet<String>();
    Set<Region<?, ?>> subregions = region.subregions(recursive);

    for (Region<?, ?> region : subregions) {
      subregionPaths.add(region.getFullPath());
    }
    return subregionPaths.toArray(new String[subregionPaths.size()]);
  }

  /** Statistic related Methods **/
  
  
  
  // Dummy constructor for testing purpose only
  public RegionMBeanBridge(CachePerfStats cachePerfStats) {
    this.regionStats = cachePerfStats;

    this.regionMonitor = new MBeanStatsMonitor(ManagementStrings.REGION_MONITOR
        .toLocalizedString());
    regionMonitor.addStatisticsToMonitor(cachePerfStats.getStats());
    configureRegionMetrics();
  }
  
  // Dummy constructor for testing purpose only
  public RegionMBeanBridge() {
  }
  


  public void stopMonitor(){
    regionMonitor.stopListener();
    if(diskRegionBridge != null){
      diskRegionBridge.stopMonitor();
    }
  }
  

  private void configureRegionMetrics() {

    putAllRate = new StatsRate(StatsKey.PUT_ALLS, StatType.INT_TYPE, regionMonitor);
    getRequestRate = new StatsRate(StatsKey.GETS, StatType.INT_TYPE, regionMonitor);

    putRequestRate = new StatsRate(StatsKey.PUTS, StatType.INT_TYPE, regionMonitor);

    destroysRate = new StatsRate(StatsKey.DESTROYS, StatType.INT_TYPE, regionMonitor);

    createsRate = new StatsRate(StatsKey.CREATES, StatType.INT_TYPE, regionMonitor);

    listenerCallsAvgLatency = new StatsAverageLatency(StatsKey.CACHE_LISTENER_CALLS_COMPLETED, StatType.INT_TYPE,
        StatsKey.CACHE_LISTENR_CALL_TIME, regionMonitor);

    writerCallsAvgLatency = new StatsAverageLatency(StatsKey.CACHE_WRITER_CALLS_COMPLETED, StatType.INT_TYPE,
        StatsKey.CACHE_WRITER_CALL_TIME, regionMonitor);


    lruDestroyRate = new StatsRate(StatsKey.LRU_DESTROYS, StatType.LONG_TYPE, regionMonitor);

    lruEvictionRate = new StatsRate(StatsKey.LRU_EVICTIONS, StatType.LONG_TYPE, regionMonitor);
    
    String[] writesRates = new String[] { StatsKey.PUT_ALLS, StatsKey.PUTS, StatsKey.CREATES };
    averageWritesRate = new StatsRate(writesRates, StatType.INT_TYPE, regionMonitor);
    averageReadsRate = new StatsRate(StatsKey.GETS, StatType.INT_TYPE, regionMonitor);

  }

  private Number getRegionStatistic(String statName) {
    if (regionStats != null) {
      return regionStats.getStats().get(statName);
    } else {
      return 0;
    }
  }
  
  public long getEntryCount() {
    return getRegionStatistic(StatsKey.ENTRIES).longValue();
  }

  public long getCacheListenerCallsAvgLatency() {
    return listenerCallsAvgLatency.getAverageLatency();
  }

  public long getCacheWriterCallsAvgLatency() {
    return writerCallsAvgLatency.getAverageLatency();
  }

  public float getCreatesRate() {
    return createsRate.getRate();
  }

  public float getPutAllRate() {
    return putAllRate.getRate();
  }


  public float getPutsRate() {
    return putRequestRate.getRate();
  }

  public float getDestroyRate() {
    return destroysRate.getRate();
  }

  public float getGetsRate() {
    return  getRequestRate.getRate();
  }

  public long getHitCount() {
    if (isStatisticsEnabled) {
      return region.getStatistics().getHitCount();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;

  }

  public float getHitRatio() {
    if (isStatisticsEnabled) {
      return region.getStatistics().getHitRatio();
    }
    return ManagementConstants.NOT_AVAILABLE_FLOAT;
  }

  public long getLastAccessedTime() {
    if (isStatisticsEnabled) {
      return region.getStatistics().getLastAccessedTime();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  public long getLastModifiedTime() {
    if (isStatisticsEnabled) {
      return region.getStatistics().getLastModifiedTime();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  public long getMissCount() {
    if (isStatisticsEnabled) {
      return region.getStatistics().getMissCount();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }


  public float getLruDestroyRate() {
    return lruDestroyRate.getRate();
  }

  public float getLruEvictionRate() {
    return lruEvictionRate.getRate();
  }

  public float getAverageReads() {
    return averageReadsRate.getRate();
  }

  public float getAverageWrites() {
    return averageWritesRate.getRate();
  }

  public long getEntrySize() {
    if (lruMemoryStats != null) {
      return lruMemoryStats.getCounter();
    }
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }


  public boolean isGatewayEnabled() {
    return isGatewayEnabled;
  }
  
  public boolean isPersistenceEnabled(){
    return this.persistentEnabled;
  }

  public String getMember() {
    return member;
  }

  
  /**
   * Only applicable for PRs
   * 
   * @return float
   */
  public float getPutLocalRate() {
    return ManagementConstants.NOT_AVAILABLE_FLOAT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return float
   */
  public float getPutRemoteRate() {
    return ManagementConstants.NOT_AVAILABLE_FLOAT;
  }
  
  /**
   * Only applicable for PRs
   * 
   * @return long
   */
  public long getPutRemoteAvgLatency() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }

  /**
   * Only applicable for PRs
   * 
   * @return long
   */
  public long getPutRemoteLatency() {
    return ManagementConstants.NOT_AVAILABLE_LONG;
  }
  
  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getActualRedundancy() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getAvgBucketSize() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getBucketCount() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  public int getConfiguredRedundancy() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getNumBucketsWithoutRedundancy() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getPrimaryBucketCount() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return int
   */
  public int getTotalBucketSize() {
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  /**
   * Only applicable for PRs
   * 
   * @return list of fixed PR attributes
   */
  public FixedPartitionAttributesData[] listFixedPartitionAttributes() {
    return null;
  }

  /**
   * Only applicable for PRs
   * 
   * @return list of PR attributes
   */
  public PartitionAttributesData listPartitionAttributes() {
    return null;
  }


  public long getDiskReadsAverageLatency() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskReadsAverageLatency();
    }
    return ManagementConstants.ZERO;
  }

  public float getDiskReadsRate() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskReadsRate();
    }
    return ManagementConstants.ZERO;
  }

  public long getDiskUsage() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskUsage();
    }
    return ManagementConstants.ZERO;
  }

  public long getDiskWritesAverageLatency() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskWritesAverageLatency();
    }
    return ManagementConstants.ZERO;
  }

  public float getDiskWritesRate() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskWritesRate();
    }
    return ManagementConstants.ZERO;
  }

  public long getTotalDiskEntriesInVM() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getTotalDiskEntriesInVM();
    }
    return ManagementConstants.ZERO;
  }

  public long getTotalDiskWritesProgress() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getTotalDiskWritesProgress();
    }
    return ManagementConstants.ZERO;
  }

  public long getTotalEntriesOnlyOnDisk() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getTotalEntriesOnlyOnDisk();
    }
    return ManagementConstants.ZERO;
  }
  
  public long getDiskTaskWaiting() {
    if (this.diskRegionBridge != null) {
      return diskRegionBridge.getDiskTaskWaiting();
    }
    return ManagementConstants.ZERO;
  }
  
  public int getLocalMaxMemory() {
    return -1;
  }

  
  public long getEstimatedSizeForHDFSRegion() {
    return -1;
  }
}
