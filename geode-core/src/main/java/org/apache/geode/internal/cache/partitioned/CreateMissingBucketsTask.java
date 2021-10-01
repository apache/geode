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
  private static final int MAX_NUMBER_INTERVALS = 31;

  private static final int SMALL_INTERVALS = 5;
  private static final int MEDIUM_INTERVALS = 10;
  private static final int LARGE_INTERVALS = 15;

  public CreateMissingBucketsTask(PRHARedundancyProvider prhaRedundancyProvider) {
    super(prhaRedundancyProvider);
  }

  @Override
  public void run2() {
    if (!waitForColocationCompleted(redundancyProvider.getPartitionedRegion())) {
      // if after all the time, colocation is still not completed, do nothing
      return;
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


  /**
   * Wait for Colocation to complete. Wait all nodes to Register this PartitionedRegion.
   */
  private boolean waitForColocationCompleted(PartitionedRegion partitionedRegion) {
    int retryCount = 0;
    int sleepInterval = PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;

    while (!ColocationHelper.isColocationComplete(partitionedRegion)
        && (retryCount < MAX_NUMBER_INTERVALS)) {

      // Didn't time out. Sleep a bit and then continue
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      retryCount++;
      if (retryCount == SMALL_INTERVALS) {
        sleepInterval = 2 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == MEDIUM_INTERVALS) {
        sleepInterval = 5 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == LARGE_INTERVALS) {
        sleepInterval = 10 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      }
    }
    return ColocationHelper.isColocationComplete(partitionedRegion);

  }
}
