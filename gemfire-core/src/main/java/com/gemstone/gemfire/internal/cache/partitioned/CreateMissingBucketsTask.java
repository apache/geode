/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionStats;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RecoveryLock;

/**
 * A task for creating buckets in a child colocated region that
 * are present in the leader region.
 *
 */
public final class CreateMissingBucketsTask extends RecoveryRunnable {
  public CreateMissingBucketsTask(
      PRHARedundancyProvider prhaRedundancyProvider) {
    super(prhaRedundancyProvider);
  }

  @Override
  public void run2() {
    PartitionedRegion leaderRegion = ColocationHelper
        .getLeaderRegion(redundancyProvider.prRegion);
    RecoveryLock lock = leaderRegion.getRecoveryLock();
    lock.lock();
    try {
      createMissingBuckets(redundancyProvider.prRegion);
    }
    finally {
      lock.unlock();
    }
  }

  protected void createMissingBuckets(PartitionedRegion region) {
    PartitionedRegion parentRegion = ColocationHelper.getColocatedRegion(region);
    if(parentRegion == null) {
      return;
    }
    // Fix for 48954 - Make sure the parent region has created missing buckets
    // before we create missing buckets for this child region.
    createMissingBuckets(parentRegion);
    
    for (int i = 0; i < region
        .getTotalNumberOfBuckets(); i++) {
      
      if(parentRegion.getRegionAdvisor().getBucketAdvisor(i)
      .getBucketRedundancy() != region.getRegionAdvisor().getBucketAdvisor(i).getBucketRedundancy()){
      /*if (leaderRegion.getRegionAdvisor().isStorageAssignedForBucket(i)) {*/
        final long startTime = PartitionedRegionStats.startTime();
        region.getRedundancyProvider().createBucketAtomically(i, 0, startTime, true, null);
      }
    }
  }
}