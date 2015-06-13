/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Holds the rebalancing details for a single partitioned
 * region.
 * 
 * Serializable form is used to allow JMX 
 * MBeans to use this as a remotable return type.
 * 
 * @author dsmith
 */
public class PartitionRebalanceDetailsImpl implements PartitionRebalanceInfo, 
Serializable, Comparable<PartitionRebalanceDetailsImpl> {
  private static final long serialVersionUID = 5880667005758250156L;
  private long bucketCreateBytes;
  private long bucketCreateTime;
  private int bucketCreatesCompleted;
  private long bucketRemoveBytes;
  private long bucketRemoveTime;
  private int bucketRemovesCompleted;
  private long bucketTransferBytes;
  private long bucketTransferTime;
  private int bucketTransfersCompleted;
  private Set<PartitionMemberInfo> partitionMemberDetailsAfter;
  private Set<PartitionMemberInfo> partitionMemberDetailsBefore;
  private long primaryTransferTime;
  private int primaryTransfersCompleted;
  transient private final PartitionedRegion region;
  private long time;
  
  public PartitionRebalanceDetailsImpl(
      PartitionedRegion region) {
    this.region = region;
  }

  public synchronized void incCreates(long bytes, long time) {
    bucketCreateBytes+= bytes;
    bucketCreateTime += time;
    bucketCreatesCompleted++;
  }
  
  public synchronized void incRemoves(long bytes, long time) {
    bucketRemoveBytes+= bytes;
    bucketRemoveTime += time;
    bucketRemovesCompleted++; 
    
  }
  
  public synchronized void incTransfers(long bytes, long time) {
    bucketTransferBytes+= bytes;
    bucketTransferTime += time;
    bucketTransfersCompleted++;
  }
  
  public synchronized void incPrimaryTransfers(long time) {
    primaryTransfersCompleted++;
    primaryTransferTime += time;
  }

  public void setPartitionMemberDetailsAfter(Set<PartitionMemberInfo> after) {
    this.partitionMemberDetailsAfter = after;
  }
  
  public void setPartitionMemberDetailsBefore(Set<PartitionMemberInfo> before) {
    this.partitionMemberDetailsBefore = before;
  }
  
  public void setTime(long time) {
    this.time = time;
  }
  
  public long getBucketCreateBytes() {
    return this.bucketCreateBytes;
  }
  public long getBucketCreateTime() {
    return TimeUnit.NANOSECONDS.toMillis(this.bucketCreateTime);
  }
  public int getBucketCreatesCompleted() {
    return this.bucketCreatesCompleted;
  }
  public long getBucketRemoveBytes() {
    return this.bucketRemoveBytes;
  }
  public long getBucketRemoveTime() {
    return TimeUnit.NANOSECONDS.toMillis(this.bucketRemoveTime);
  }
  public int getBucketRemovesCompleted() {
    return this.bucketRemovesCompleted;
  }
  public long getBucketTransferBytes() {
    return this.bucketTransferBytes;
  }
  public long getBucketTransferTime() {
    return TimeUnit.NANOSECONDS.toMillis(this.bucketTransferTime);
  }
  public int getBucketTransfersCompleted() {
    return this.bucketTransfersCompleted;
  }
  public Set<PartitionMemberInfo> getPartitionMemberDetailsAfter() {
    return this.partitionMemberDetailsAfter;
  }
  public Set<PartitionMemberInfo> getPartitionMemberDetailsBefore() {
    return this.partitionMemberDetailsBefore;
  }
  public long getPrimaryTransferTime() {
    return TimeUnit.NANOSECONDS.toMillis(this.primaryTransferTime);
  }
  public int getPrimaryTransfersCompleted() {
    return this.primaryTransfersCompleted;
  }
  public String getRegionPath() {
    return this.region.getFullPath();
  }
  public PartitionedRegion getRegion() {
    return this.region;
  }
  public long getTime() {
    return TimeUnit.NANOSECONDS.toMillis(this.time);
  }
  public int compareTo(PartitionRebalanceDetailsImpl other) {
    return this.region.getFullPath().compareTo(other.region.getFullPath());
  }
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionRebalanceDetailsImpl)) {
      return false;
    }
    PartitionRebalanceDetailsImpl o = (PartitionRebalanceDetailsImpl)other;
    return this.region.getFullPath().equals(o.region.getFullPath());
  }
  @Override
  public int hashCode() {
    return this.region.getFullPath().hashCode();
  }
}
