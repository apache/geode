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
package org.apache.geode.internal.cache.partitioned;

import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.PartitionedRegion.RecoveryLock;

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
