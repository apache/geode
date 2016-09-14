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
package org.apache.geode.management.internal.beans;

import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.EvictionAttributesData;
import org.apache.geode.management.FixedPartitionAttributesData;
import org.apache.geode.management.MembershipAttributesData;
import org.apache.geode.management.PartitionAttributesData;
import org.apache.geode.management.RegionAttributesData;

/**
 * System-wide aggregate of a named Region. Provides high-level view of a Region
 * for all members hosting and/or using it.
 * 
 * 
 */
public class DistributedRegionMBean implements DistributedRegionMXBean {

  DistributedRegionBridge bridge;

  public DistributedRegionMBean(DistributedRegionBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public long getCacheListenerCallsAvgLatency() {

    return bridge.getCacheListenerCallsAvgLatency();
  }

  @Override
  public long getCacheWriterCallsAvgLatency() {

    return bridge.getCacheWriterCallsAvgLatency();
  }

  @Override
  public float getCreatesRate() {

    return bridge.getCreatesRate();
  }

 
  @Override
  public float getDestroyRate() {

    return bridge.getDestroyRate();
  }

  @Override
  public float getDiskReadsRate() {

    return bridge.getDiskReadsRate();
  }

  @Override
  public float getDiskWritesRate() {

    return bridge.getDiskWritesRate();
  }

  @Override
  public EvictionAttributesData listEvictionAttributes() {

    return bridge.getEvictionAttributes();
  }

  @Override
  public FixedPartitionAttributesData[] listFixedPartitionAttributesData() {

    return bridge.getFixedPartitionAttributesData();
  }


  @Override
  public String getFullPath() {

    return bridge.getFullPath();
  }


  @Override
  public float getGetsRate() {

    return bridge.getGetsRate();
  }

  @Override
  public long getHitCount() {

    return bridge.getHitCount();
  }

  @Override
  public float getHitRatio() {

    return bridge.getHitRatio();
  }

  @Override
  public long getLastAccessedTime() {

    return bridge.getLastAccessedTime();
  }

  @Override
  public long getLastModifiedTime() {

    return bridge.getLastModifiedTime();
  }

  @Override
  public float getLruDestroyRate() {

    return bridge.getLruDestroyRate();
  }

  @Override
  public float getLruEvictionRate() {

    return bridge.getLruEvictionRate();
  }

  @Override
  public int getMemberCount() {

    return bridge.getMemberCount();
  }

  @Override
  public String[] getMembers() {

    return bridge.getMembers();
  }

  @Override
  public MembershipAttributesData listMembershipAttributes() {

    return bridge.getMembershipAttributes();
  }

  @Override
  public long getMissCount() {

    return bridge.getMissCount();
  }

  @Override
  public String getName() {

    return bridge.getName();
  }

  @Override
  public String getParentRegion() {

    return bridge.getParentRegion();
  }

  public String[] listSubRegionPaths(boolean recursive) {
    return bridge.listSubRegionPaths(recursive);
  }

  @Override
  public PartitionAttributesData listPartitionAttributes() {

    return bridge.getPartitionAttributes();
  }

  @Override
  public float getPutAllRate() {

    return bridge.getPutAllRate();
  }

  @Override
  public float getPutLocalRate() {

    return bridge.getPutLocalRate();
  }

  @Override
  public long getPutRemoteAvgLatency() {

    return bridge.getPutRemoteAvgLatency();
  }

  @Override
  public long getPutRemoteLatency() {

    return bridge.getPutRemoteLatency();
  }

  @Override
  public float getPutRemoteRate() {

    return bridge.getPutRemoteRate();
  }

  @Override
  public float getPutsRate() {

    return bridge.getPutsRate();
  }

  @Override
  public RegionAttributesData listRegionAttributes() {

    return bridge.getRegionAttributes();
  }

  @Override
  public String getRegionType() {

    return bridge.getRegionType();
  }

  
  @Override
  public long getSystemRegionEntryCount() {

    return bridge.getSystemRegionEntryCount();
  }

  @Override
  public long getTotalDiskWritesProgress() {

    return bridge.getTotalDiskWritesProgress();
  }

  @Override
  public long getTotalDiskEntriesInVM() {

    return bridge.getTotalDiskEntriesInVM();
  }


  @Override
  public long getTotalEntriesOnlyOnDisk() {

    return bridge.getTotalEntriesOnlyOnDisk();
  }

  @Override
  public int getAvgBucketSize() {
    return bridge.getAvgBucketSize();
  }

  @Override
  public int getBucketCount() {
    return bridge.getBucketCount();
  }

  @Override
  public int getNumBucketsWithoutRedundancy() {
    return bridge.getNumBucketsWithoutRedundancy();
  }

  @Override
  public int getPrimaryBucketCount() {
    return bridge.getPrimaryBucketCount();
  }

  @Override
  public int getTotalBucketSize() {
    return bridge.getTotalBucketSize();
  }

  @Override
  public long getDiskTaskWaiting() {
    return bridge.getDiskTaskWaiting();
  }

  @Override
  public long getDiskUsage() {
    return bridge.getDiskUsage();
  }

  @Override
  public float getAverageReads() {
    return bridge.getAverageReads();
  }

  @Override
  public float getAverageWrites() {
    return bridge.getAverageWrites();
  }

  @Override
  public boolean isGatewayEnabled() {
    return bridge.isGatewayEnabled();
  }

  @Override
  public boolean isPersistentEnabled() {
    return bridge.isPersistentEnabled();
  }

  @Override
  public int getEmptyNodes() {
    return bridge.getEmptyNodes();
  }

  @Override
  public long getEntrySize() {
    return bridge.getEntrySize();
  }

}
