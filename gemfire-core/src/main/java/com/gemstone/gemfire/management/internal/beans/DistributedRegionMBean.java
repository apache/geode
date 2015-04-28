/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.FixedPartitionAttributesData;
import com.gemstone.gemfire.management.MembershipAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.RegionAttributesData;

/**
 * System-wide aggregate of a named Region. Provides high-level view of a Region
 * for all members hosting and/or using it.
 * 
 * @author rishim
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
