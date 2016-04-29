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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.FixedPartitionAttributesData;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.RegionClusterStatsMonitor;

/**
 * Bridge to collect data from all the proxies from a named region.
 * MBeanAggregator will create this bridge and inject it into a
 * DistributedRegionMBean
 * 
 * 
 */
public class DistributedRegionBridge {

  /**
   * Map of RegionMXBean proxies
   */
  private Map<ObjectName, RegionMXBean> mapOfProxy;

  /**
   * set size of this proxy set
   */
  private volatile int setSize;

  /**
   * Eviction attributes
   */
  private EvictionAttributesData evictionAttributesData;

  /**
   * Membership attributes
   */
  private MembershipAttributesData membershipAttributesData;

  /**
   * Partition Attributes
   */
  private PartitionAttributesData partitionAttributesData;

  /**
   * Region Attributes
   */
  private RegionAttributesData regionAttributesData;

  /**
   * Array of fixed partition attributes
   */
  private FixedPartitionAttributesData[] fixedPartitionAttributesTable;
  
  private RegionClusterStatsMonitor monitor;
  
  private boolean isPartitionedRegion = false;


  /**
   * Public constructor.
   * 
   * @param objectName
   *          Name of the MBean
   * @param proxy
   *          reference to the actual proxy
   */
  public DistributedRegionBridge(ObjectName objectName, RegionMXBean proxy,
      FederationComponent newState) {
    this.mapOfProxy = new ConcurrentHashMap<ObjectName, RegionMXBean>();
    this.monitor = new RegionClusterStatsMonitor();
    addProxyToMap(objectName, proxy, newState);

  }

  /**
   * Adds a proxy to the Proxy set
   * 
   * @param objectName
   *          name of the MBean
   * @param proxy
   *          reference to the actual proxy
   */
  public void addProxyToMap(ObjectName objectName, RegionMXBean proxy,  FederationComponent newState) {
    mapOfProxy.put(objectName, proxy);
    setSize = mapOfProxy.keySet().size();
    updateRegion(newState, null);
  }

  /**
   * Removes proxy from the proxy set
   * 
   * @param objectName
   *          name of the MBean
   * @param proxy
   *          reference to the actual proxy
   * @return true if this DistributedRegion's proxy set is empty
   */
  public boolean removeProxyFromMap(ObjectName objectName, RegionMXBean proxy, FederationComponent oldState) {
    mapOfProxy.remove(objectName);
    setSize = mapOfProxy.keySet().size();
    if (setSize == 0) {
      return true;
    }
    updateRegion(null, oldState);
    return false;
  }
  
  public void updateRegion(FederationComponent newState,
      FederationComponent oldState) {
    monitor.aggregate(newState, oldState);
  }

  /**
   * 
   * @return Eviction Attributes of the Region
   */
  public EvictionAttributesData getEvictionAttributes() {

    if (this.evictionAttributesData == null) {
      Iterator<RegionMXBean> it = mapOfProxy.values().iterator();
      if (it != null) {
        while (it.hasNext()) {
          try{
            this.evictionAttributesData = it.next().listEvictionAttributes();
          }catch(Exception e){
            this.evictionAttributesData = null;
          }
          if(evictionAttributesData != null){
            break;
          }
          
        }
      }
    }
    return evictionAttributesData;
  }

  /**
   * 
   * @return fixed partition attributes of a partition region if its fixed
   *         partitioned
   */
  public FixedPartitionAttributesData[] getFixedPartitionAttributesData() {

    if (this.fixedPartitionAttributesTable == null) {
      Iterator<RegionMXBean> it = mapOfProxy.values().iterator();
      if (it != null) {
        while (it.hasNext()) {
          try {
            this.fixedPartitionAttributesTable = it.next()
                .listFixedPartitionAttributes();
          } catch (Exception e) {
            this.fixedPartitionAttributesTable = null;
          }
          if (fixedPartitionAttributesTable != null) {
            break;
          }
        }
      }
    }
    return fixedPartitionAttributesTable;
  }

  /**
   * 
   * @return number of members where this region is present
   */
  public int getMemberCount() {
    return setSize;
  }

  /**
   * @return set of member ids on which this region is present.
   */
  public String[] getMembers() {
    Iterator<ObjectName> it = mapOfProxy.keySet().iterator();    
    if (it != null) {
      List<String> memberList = new ArrayList<String>();
      while (it.hasNext()) {
        ObjectName tempObjName = it.next();
        String formatedMemberId = (String) tempObjName.getKeyProperty("member");
        memberList.add(formatedMemberId);
      }
     String[] members = new String[memberList.size()];
     return memberList.toArray(members);
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * 
   * @return membership attributes
   */
  public MembershipAttributesData getMembershipAttributes() {
    if (this.membershipAttributesData == null) {
      Iterator<RegionMXBean> it = mapOfProxy.values().iterator();
      if (it != null) {
        while (it.hasNext()) {
          try{
            this.membershipAttributesData = it.next().listMembershipAttributes();
          }catch(Exception e){
            this.membershipAttributesData = null;
          }
          if(membershipAttributesData != null){
            break;
          }
          
        }
      }
    }
    return membershipAttributesData;
  }

  /**
   * 
   * @return name of the Region
   */
  public String getName() {
    return monitor.getName();
  }

  /**
   * 
   * @return parent region name
   */
  public String getParentRegion() {
    return monitor.getParentRegion();
  }

  /**
   * Lists the sub regions of the GemFire {@link Region} represented by this
   * {@link DistributedRegionMXBean}. If <code>recursive</code> is
   * <code>true</code>, will return sub-regions by traversing recursively.
   * 
   * @param recursive
   *          if <code>true</code>, recursively traverses to find sub regions.
   * @return String array of sub region paths
   */
  public String[] listSubRegionPaths(boolean recursive) {
    String[] subRegionPathsArr = new String[0];
    
    Collection<RegionMXBean> proxies = mapOfProxy.values();
    // Need to go through all proxies as different proxies could have different sub-regions
    if (proxies != null && !proxies.isEmpty()) {
      SortedSet<String> subRegionPaths = new TreeSet<String>();
      for (RegionMXBean regionMXBean : proxies) {
        String[] listSubRegionPaths = regionMXBean.listSubregionPaths(recursive);
        subRegionPaths.addAll(Arrays.asList(listSubRegionPaths)); // Little cosly, but how can it be avoided?
      }

      subRegionPathsArr = subRegionPaths.toArray(subRegionPathsArr);
    }

    return subRegionPathsArr;
  }

  /**
   * 
   * @return partitioned attributes
   */
  public PartitionAttributesData getPartitionAttributes() {

    if (this.partitionAttributesData == null) {
      Iterator<RegionMXBean> it = mapOfProxy.values().iterator();
      if (it != null) {
        while (it.hasNext()) {
          try{
            this.partitionAttributesData = it.next().listPartitionAttributes();
          }catch(Exception e){
            this.partitionAttributesData = null;
          }
          if(partitionAttributesData != null){
            break;
          }
          
        }
      }
    }
    return partitionAttributesData;
  }

  /**
   * Region attributes
   * 
   * @return region attributes
   */
  public RegionAttributesData getRegionAttributes() {
    if (this.regionAttributesData == null) {
      Iterator<RegionMXBean> it = mapOfProxy.values().iterator();
      if (it != null) {
        while (it.hasNext()) {
          try{
            this.regionAttributesData = it.next().listRegionAttributes();
          }catch(Exception e){
            this.regionAttributesData = null;
          }
          if(regionAttributesData != null){
            break;
          }
          
        }
      }
    }
    return regionAttributesData;
  }

  /**
   * 
   * @return type of the region
   */
  public String getRegionType() {
    return monitor.getRegionType();
  }

  /**
   * 
   * @return full path of the region
   */
  public String getFullPath() {
    return monitor.getFullPath();
  }

  /**
   * 
   * @return Avg Latency of cache listener call
   */
  public long getCacheListenerCallsAvgLatency() {
    return MetricsCalculator.getAverage(monitor
        .getCacheListenerCallsAvgLatency(), setSize);

  }

  /**
   * 
   * @return Avg Latency of cache writer call
   */
  public long getCacheWriterCallsAvgLatency() {
    return MetricsCalculator.getAverage(monitor
        .getCacheWriterCallsAvgLatency(), setSize);
  }

  /**
   * 
   * @return creates per second for the Regions
   */
  public float getCreatesRate() {
    return monitor.getCreatesRate();
  }

 
  /**
   * 
   * @return destroy per second for the Regions
   */
  public float getDestroyRate() {
    return monitor.getDestroyRate();
  }

  /**
   * 
   * @return disk reads rate across disks belonging to Region
   */
  public float getDiskReadsRate() {
    return monitor.getDiskReadsRate();
  }

  /**
   * 
   * @return disk writes rate across region
   */
  public float getDiskWritesRate() {
    return monitor.getDiskWritesRate();
  }


  /**
   * 
   * @return gets per second for the Regions
   */
  public float getGetsRate() {
    return monitor.getGetsRate();
  }

  /**
   * 
   * @return total hit count
   */
  public long getHitCount() {
    if(isPartionedRegion()){
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
    return monitor.getHitCount();
  }

  /**
   * 
   * @return hit to miss ratio
   */
  public float getHitRatio() {
    if(isPartionedRegion()){
      return ManagementConstants.NOT_AVAILABLE_FLOAT;
    }
    return monitor.getHitRatio();
  }

  /**
   * 
   * @return returns the last time the region was accessed
   */
  public long getLastAccessedTime() {
    if(isPartionedRegion()){
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
    return monitor.getLastAccessedTime();

  }

  /**
   * 
   * @return last update time of the region
   */
  public long getLastModifiedTime() {
    if(isPartionedRegion()){
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
    return monitor.getLastModifiedTime();
  }

  /**
   * 
   * @return entries destroyed rate in the region through both destroy cache
   *         operations and eviction.
   */
  public float getLruDestroyRate() {
    return monitor.getLruDestroyRate();
  }

  /**
   * 
   * @return entry evictions rate triggered by LRU.
   */
  public float getLruEvictionRate() {
    return monitor.getLruEvictionRate();
  }

  /**
   * 
   * @return number of times cache missed on the local region
   */
  public long getMissCount() {
    if(isPartionedRegion()){
      return ManagementConstants.NOT_AVAILABLE_LONG;
    }
    return monitor.getMissCount();
  }

  /**
   * 
   * @return putAll per second for the Regions
   */
  public float getPutAllRate() {
    return monitor.getPutAllRate();
  }

  /**
   * 
   * @return Partition Region local put rate
   */
  public float getPutLocalRate() {
    return monitor.getPutLocalRate();
  }

  /**
   * 
   * @return Average Latency for remote put
   */
  public long getPutRemoteAvgLatency() {
    return MetricsCalculator.getAverage(monitor
        .getPutRemoteAvgLatency(), setSize);
  }

  /**
   * 
   * @return Latency for last remote put
   */
  public long getPutRemoteLatency() {
    return MetricsCalculator.getAverage(monitor
        .getPutRemoteLatency(), setSize);
  }

  /**
   * 
   * @return Partition Region remote put rate
   */
  public float getPutRemoteRate() {
    return monitor.getPutRemoteRate();
  }

  /**
   * 
   * @return puts per second for the Regions
   */
  public float getPutsRate() {
    return monitor.getPutsRate();
  }

 
  /**
   * 
   * @return number of entries
   */
  public long getSystemRegionEntryCount() {
    if (isPartionedRegion()) {      
      return monitor.getSystemRegionEntryCount();
    }
    return monitor.getEntryCount();

  }


  /**
   * 
   * @return The current number of backups in progress on this disk store
   */
  public long getTotalDiskWritesProgress() {
    return monitor.getTotalDiskWritesProgress();
  }

  /**
   * 
   * @return consolidated count of bytes on each disk of the region
   */
  public long getTotalBytesOnDisk() {
    return monitor.getTotalBytesOnDisk();
  }

  /**
   * 
   * @return The current number of entries whose value resides in the VM. The
   *         value may also have been written to disk.
   */
  public long getTotalDiskEntriesInVM() {
    return monitor.getTotalDiskEntriesInVM();
  }

  /**
   * 
   * @return The current number of entries whose value is on disk and is not in
   *         memory. This is true of overflowed entries. It is also true of
   *         recovered entries that have not yet been faulted in.
   */
  public long getTotalEntriesOnlyOnDisk() {
    return monitor.getTotalEntriesOnlyOnDisk();
  }
  

  public int getAvgBucketSize() {
    if (!isPartionedRegion()) {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
    return MetricsCalculator.getAverage(monitor.getAvgBucketSize(), setSize);
  }

  public int getBucketCount() {
    if (!isPartionedRegion()) {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
    return monitor.getBucketCount();
  }

  public int getNumBucketsWithoutRedundancy() {
    if (!isPartionedRegion()) {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
    return monitor.getNumBucketsWithoutRedundancy();
  }

  public int getPrimaryBucketCount() {
    if (!isPartionedRegion()) {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
    return monitor.getPrimaryBucketCount();
  }
  public int getTotalBucketSize() {
    if (!isPartionedRegion()) {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
    return monitor.getTotalBucketSize();
  }

  public long getDiskTaskWaiting() {
    return monitor.getDiskTaskWaiting();
  }

  public long getDiskUsage() {
    return monitor.getDiskUsage();
  }
  public float getAverageReads() {
    return monitor.getAverageReads();
  }

  public float getAverageWrites() {
    return monitor.getAverageWrites();
  }

  public boolean isGatewayEnabled() {
    return monitor.isGatewayEnabled();
  }
  
  public boolean isPersistentEnabled() {
    return monitor.isPersistentEnabled();
  }
  
  public int getEmptyNodes() {
    Iterator<RegionMXBean> it = mapOfProxy.values().iterator();

    int emptyNodes = 0;
    if (it != null) {
      while (it.hasNext()) {
        RegionMXBean bean = it.next();
        if (bean.getRegionType().equals(DataPolicy.EMPTY.toString())) {
          emptyNodes++;
        }
        if (bean.getLocalMaxMemory() == 0) {
          emptyNodes++;
        }
      }
    }
    return emptyNodes;
  }

  public long getEntrySize() {
    return monitor.getEntrySize();
  }

  private boolean isPartionedRegion(){
    if(getPartitionAttributes() != null){
      return true;
    }else{
      return false;
    }
  }
}
