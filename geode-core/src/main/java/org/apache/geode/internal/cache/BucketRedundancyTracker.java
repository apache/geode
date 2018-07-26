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
package org.apache.geode.internal.cache;

/**
 * Keeps track of redundancy status for a bucket in a PartitionedRegion and update the region's
 * {@link PartitionedRegionRedundancyTracker} of the bucket's status for the region.
 */
class BucketRedundancyTracker {
  private boolean redundancySatisfied = false;
  private boolean hasAnyCopies = false;
  private boolean redundancyEverSatisfied = false;
  private boolean hasEverHadCopies = false;
  private volatile int currentRedundancy = -1;
  private final int targetRedundancy;
  private final PartitionedRegionRedundancyTracker regionRedundancyTracker;

  /**
   * Creates a new BucketRedundancyTracker
   *
   * @param redundantCopies the number of redundant copies specified for the
   *        {@link PartitionedRegion} of this bucket
   * @param regionRedundancyTracker the redundancy tracker for the {@link PartitionedRegion} of this
   *        bucket
   */
  BucketRedundancyTracker(int redundantCopies,
      PartitionedRegionRedundancyTracker regionRedundancyTracker) {
    this.targetRedundancy = redundantCopies;
    this.regionRedundancyTracker = regionRedundancyTracker;
  }

  /**
   * Adjust statistics based on closing a bucket
   */
  synchronized void closeBucket() {
    if (!redundancySatisfied) {
      regionRedundancyTracker.decrementLowRedundancyBucketCount();
      redundancySatisfied = true;
    }
    if (hasEverHadCopies && !hasAnyCopies) {
      regionRedundancyTracker.decrementNoCopiesBucketCount();
      hasAnyCopies = true;
    }
  }

  /**
   * Determines if there has been a change in current redundancy and updates statistics on
   * redundancy for the region of the bucket for this tracker
   *
   * @param currentBucketHosts number of current hosts for the bucket
   */
  synchronized void updateStatistics(int currentBucketHosts) {
    updateRedundancyStatistics(currentBucketHosts);
    updateNoCopiesStatistics(currentBucketHosts);
  }

  /**
   * Provides the current redundancy of the bucket for this tracker
   *
   * @return number of redundant copies of the bucket for this tracker
   */
  int getCurrentRedundancy() {
    return currentRedundancy;
  }

  private void updateNoCopiesStatistics(int currentBucketHosts) {
    if (hasAnyCopies && currentBucketHosts == 0) {
      hasAnyCopies = false;
      regionRedundancyTracker.incrementNoCopiesBucketCount();
    } else if (!hasAnyCopies && currentBucketHosts > 0) {
      if (hasEverHadCopies) {
        regionRedundancyTracker.decrementNoCopiesBucketCount();
      }
      hasEverHadCopies = true;
      hasAnyCopies = true;
    }
  }

  private void updateRedundancyStatistics(int updatedBucketHosts) {
    int updatedRedundancy = updatedBucketHosts - 1;
    updateCurrentRedundancy(updatedRedundancy);

    if (updatedRedundancy < targetRedundancy) {
      reportUpdatedBucketCount(updatedBucketHosts);
      if (redundancySatisfied) {
        regionRedundancyTracker.incrementLowRedundancyBucketCount();
        redundancySatisfied = false;
      } else if (!hasAnyCopies && !hasEverHadCopies && updatedRedundancy >= 0) {
        regionRedundancyTracker.incrementLowRedundancyBucketCount();
      }
    } else if (!redundancySatisfied && updatedRedundancy == targetRedundancy) {
      regionRedundancyTracker.decrementLowRedundancyBucketCount();
      redundancySatisfied = true;
      redundancyEverSatisfied = true;
    }
  }

  private void updateCurrentRedundancy(int updatedRedundancy) {
    if (updatedRedundancy != currentRedundancy) {
      regionRedundancyTracker.setActualRedundancy(updatedRedundancy);
      currentRedundancy = updatedRedundancy;
    }
  }

  private void reportUpdatedBucketCount(int updatedBucketHosts) {
    if (redundancyEverSatisfied) {
      regionRedundancyTracker.reportBucketCount(updatedBucketHosts);
    }
  }
}
