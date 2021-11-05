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

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.RecoveryLock;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A task for creating buckets in a child colocated region that are present in the leader region.
 *
 */
public class CreateMissingBucketsTask extends RecoveryRunnable {
  protected static final Logger logger = LogService.getLogger();

  static final int MAX_NUMBER_INTERVALS = 60;

  private static final int SMALL_200MS_INTERVALS = 5;
  private static final int SMALL_500MS_INTERVALS = 10;

  private static final int MEDIUM_1SEC_INTERVALS = 15;
  private static final int MEDIUM_2SEC_INTERVALS = 30;

  private static final int LARGE_5SEC_INTERVALS = 45;
  private int retryCount;

  public CreateMissingBucketsTask(PRHARedundancyProvider prhaRedundancyProvider) {
    super(prhaRedundancyProvider);
    retryCount = 0;
  }

  @Override
  public void run2() {
    if (!waitForColocationCompleted(redundancyProvider.getPartitionedRegion())) {
      // if after all the time, colocation is still not completed, do nothing
      return;
    }

    if (redundancyProvider.getPartitionedRegion().isLocallyDestroyed
        || redundancyProvider.getPartitionedRegion().isClosed)
      return;

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
      if (region.isClosed || region.isLocallyDestroyed) {
        return;
      }

      if (parentRegion.getRegionAdvisor().getBucketAdvisor(i).getBucketRedundancy() != region
          .getRegionAdvisor().getBucketAdvisor(i).getBucketRedundancy()) {
        region.getRedundancyProvider().createBucketAtomically(i, 0, true, null);
      }
    }
  }


  /**
   * Wait for Colocation to complete. Wait all nodes to Register this PartitionedRegion.
   */
  protected boolean waitForColocationCompleted(PartitionedRegion partitionedRegion) {
    int sleepInterval = PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;

    while (!ColocationHelper.isColocationComplete(partitionedRegion)
        && (retryCount < MAX_NUMBER_INTERVALS)) {

      // Didn't time out. Sleep a bit and then continue
      boolean interrupted = Thread.interrupted();
      try {
        logger.info("Waiting for collocation to complete, retry number {}", retryCount);
        Thread.sleep(sleepInterval);
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }

      if (partitionedRegion.isLocallyDestroyed || partitionedRegion.isClosed) {
        return false;
      }

      retryCount++;
      if (retryCount == SMALL_200MS_INTERVALS) {
        sleepInterval = 2 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == SMALL_500MS_INTERVALS) {
        sleepInterval = 5 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == MEDIUM_1SEC_INTERVALS) {
        sleepInterval = 10 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == MEDIUM_2SEC_INTERVALS) {
        sleepInterval = 20 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      } else if (retryCount == LARGE_5SEC_INTERVALS) {
        sleepInterval = 50 * PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION;
      }

    }
    return ColocationHelper.isColocationComplete(partitionedRegion);

  }

  int getRetryCount() {
    return retryCount;
  }


}
