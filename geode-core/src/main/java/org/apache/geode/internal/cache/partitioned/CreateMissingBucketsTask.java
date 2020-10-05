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
package org.apache.geode.internal.cache.partitioned;

import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.RecoveryLock;
import org.apache.geode.internal.cache.PartitionedRegionHelper;

/**
 * A task for creating buckets in a child colocated region that are present in the leader region.
 *
 */
public class CreateMissingBucketsTask extends RecoveryRunnable {
  public CreateMissingBucketsTask(PRHARedundancyProvider prhaRedundancyProvider) {
    super(prhaRedundancyProvider);
  }

  @Override
  public void run2() {
    PartitionedRegion partitionedRegion = redundancyProvider.getPartitionedRegion();
    int count = 0;

    while (!ColocationHelper.isColocationComplete(partitionedRegion)) {
      // if in 5 retries region is still not colocated
      // abort task
      if (count > 4) {
        return;
      }
      // Didn't time out. Sleep a bit and then continue
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      count++;
    }

    PartitionedRegion leaderRegion =
        ColocationHelper.getLeaderRegion(redundancyProvider.getPartitionedRegion());
    RecoveryLock lock = leaderRegion.getRecoveryLock();
    lock.lock();
    try {
      createMissingBuckets(redundancyProvider.getPartitionedRegion());
    } finally {
      lock.unlock();
    }
  }

  protected void createMissingBuckets(PartitionedRegion region) {
    PartitionedRegion parentRegion = ColocationHelper.getColocatedRegion(region);
    if (parentRegion == null) {
      return;
    }
    // Make sure the parent region has created missing buckets
    // before we create missing buckets for this child region.
    createMissingBuckets(parentRegion);

    for (int i = 0; i < region.getTotalNumberOfBuckets(); i++) {

      if (parentRegion.getRegionAdvisor().getBucketAdvisor(i).getBucketRedundancy() != region
          .getRegionAdvisor().getBucketAdvisor(i).getBucketRedundancy()) {
        region.getRedundancyProvider().createBucketAtomically(i, 0, true, null);
      }
    }
  }
}
