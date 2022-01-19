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

import javax.management.NotificationBroadcasterSupport;

import org.apache.geode.management.EvictionAttributesData;
import org.apache.geode.management.FixedPartitionAttributesData;
import org.apache.geode.management.MembershipAttributesData;
import org.apache.geode.management.PartitionAttributesData;
import org.apache.geode.management.RegionAttributesData;
import org.apache.geode.management.RegionMXBean;

/**
 * Concrete implementation of RegionMXBean which exposes various configuration and runtime
 * statistics about the region instance.
 *
 * It extends NotificationBroadcasterSupport for any future implementation of notification.
 *
 *
 */
public class RegionMBean<K, V> extends NotificationBroadcasterSupport implements RegionMXBean {

  /** Bridge is responsible for extracting data from GemFire Layer **/
  private final RegionMBeanBridge<K, V> bridge;

  public RegionMBean(RegionMBeanBridge<K, V> bridge) {
    this.bridge = bridge;
  }

  @Override
  public long getEntryCount() {
    return bridge.getEntryCount();
  }

  @Override
  public String getFullPath() {
    return bridge.getFullPath();
  }

  @Override
  public String getName() {
    return bridge.getName();
  }

  @Override
  public String getRegionType() {
    return bridge.getRegionType();
  }

  @Override
  public String getParentRegion() {
    return bridge.getParentRegion();
  }

  @Override
  public String[] listSubregionPaths(boolean recursive) {
    return bridge.listSubRegionPaths(recursive);
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
    return bridge.listEvictionAttributes();
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
  public MembershipAttributesData listMembershipAttributes() {
    return bridge.listMembershipAttributes();
  }

  @Override
  public long getMissCount() {
    return bridge.getMissCount();
  }

  @Override
  public PartitionAttributesData listPartitionAttributes() {
    return bridge.listPartitionAttributes();
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
    return bridge.listRegionAttributes();
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
  public FixedPartitionAttributesData[] listFixedPartitionAttributes() {
    return bridge.listFixedPartitionAttributes();
  }

  @Override
  public long getDiskReadsAverageLatency() {
    return bridge.getDiskReadsAverageLatency();
  }

  @Override
  public long getDiskWritesAverageLatency() {
    return bridge.getDiskWritesAverageLatency();
  }

  @Override
  public int getActualRedundancy() {
    return bridge.getActualRedundancy();
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
  public int getConfiguredRedundancy() {
    return bridge.getConfiguredRedundancy();
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

  public RegionMBeanBridge<K, V> getBridge() {
    return bridge;
  }

  public void stopMonitor() {
    bridge.stopMonitor();
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
  public long getEntrySize() {
    return bridge.getEntrySize();
  }

  @Override
  public boolean isGatewayEnabled() {
    return bridge.isGatewayEnabled();
  }

  @Override
  public boolean isPersistentEnabled() {
    return bridge.isPersistenceEnabled();
  }

  @Override
  public String getMember() {
    return bridge.getMember();
  }

  @Override
  public int getLocalMaxMemory() {
    return bridge.getLocalMaxMemory();
  }

}
