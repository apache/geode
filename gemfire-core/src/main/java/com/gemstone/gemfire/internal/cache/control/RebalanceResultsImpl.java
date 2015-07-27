/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;

public class RebalanceResultsImpl implements RebalanceResults, Serializable {
  private Set<PartitionRebalanceInfo> detailSet = new TreeSet<PartitionRebalanceInfo>();
  private long totalBucketCreateBytes;
  private long totalBucketCreateTime;
  private int totalBucketCreatesCompleted;
  private long totalBucketTransferBytes;
  private long totalBucketTransferTime;
  private int totalBucketTransfersCompleted;
  private long totalPrimaryTransferTime;
  private int totalPrimaryTransfersCompleted;
  private long totalTime;
  
  public void addDetails(PartitionRebalanceInfo details) {
    this.detailSet.add(details);
    totalBucketCreateBytes += details.getBucketCreateBytes();
    totalBucketCreateTime += details.getBucketCreateTime();
    totalBucketCreatesCompleted += details.getBucketCreatesCompleted();
    totalBucketTransferBytes += details.getBucketTransferBytes();
    totalBucketTransferTime += details.getBucketTransferTime();
    totalBucketTransfersCompleted += details.getBucketTransfersCompleted();
    totalPrimaryTransferTime += details.getPrimaryTransferTime();
    totalPrimaryTransfersCompleted += details.getPrimaryTransfersCompleted();
    totalTime += details.getTime();
  }

  public void addDetails(RebalanceResultsImpl details) {
    this.detailSet.addAll(details.detailSet);
    totalBucketCreateBytes += details.totalBucketCreateBytes;
    totalBucketCreateTime += details.totalBucketCreateTime;
    totalBucketCreatesCompleted += details.totalBucketCreatesCompleted;
    totalBucketTransferBytes += details.totalBucketTransferBytes;
    totalBucketTransferTime += details.totalBucketTransferTime;
    totalBucketTransfersCompleted += details.totalBucketTransfersCompleted;
    totalPrimaryTransferTime += details.totalPrimaryTransferTime;
    totalPrimaryTransfersCompleted += details.totalPrimaryTransfersCompleted;
    if(details.totalTime > totalTime)
    totalTime = details.totalTime;
  }

  public Set<PartitionRebalanceInfo> getPartitionRebalanceDetails() {
    return detailSet;
  }

  public long getTotalBucketCreateBytes() {
    return this.totalBucketCreateBytes;
  }

  public long getTotalBucketCreateTime() {
    return this.totalBucketCreateTime;
  }

  public int getTotalBucketCreatesCompleted() {
    return this.totalBucketCreatesCompleted;
  }

  public long getTotalBucketTransferBytes() {
    return this.totalBucketTransferBytes;
  }

  public long getTotalBucketTransferTime() {
    return this.totalBucketTransferTime;
  }

  public int getTotalBucketTransfersCompleted() {
    return this.totalBucketTransfersCompleted;
  }

  public long getTotalPrimaryTransferTime() {
    return this.totalPrimaryTransferTime;
  }

  public int getTotalPrimaryTransfersCompleted() {
    return this.totalPrimaryTransfersCompleted;
  }

  public long getTotalTime() {
    return this.totalTime;
  }
}
