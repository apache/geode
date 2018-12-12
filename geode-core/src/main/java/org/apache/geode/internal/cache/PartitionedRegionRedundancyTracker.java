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

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Keeps track redundancy statistics across the buckets of a given {@link PartitionedRegion}
 */
class PartitionedRegionRedundancyTracker {
  private static final Logger logger = LogService.getLogger();

  private final PartitionedRegionStats stats;
  private final String regionPath;
  private final int totalBuckets;
  private final int targetRedundancy;

  private int lowRedundancyBuckets;
  private int noCopiesBuckets;
  private int lowestBucketCopies;

  /**
   * Creates a new PartitionedRegionRedundancyTracker
   *
   * @param totalBuckets number of buckets in the region to track
   * @param redundantCopies number of redundant copies specified on the region to track
   * @param stats the statistics container for the region to track
   * @param regionPath the full path of the region to track
   */
  PartitionedRegionRedundancyTracker(int totalBuckets, int redundantCopies,
      PartitionedRegionStats stats, String regionPath) {
    this.stats = stats;
    this.regionPath = regionPath;
    this.totalBuckets = totalBuckets;
    this.targetRedundancy = redundantCopies;
    this.lowestBucketCopies = redundantCopies + 1;
  }

  /**
   * Since consistency was last reached, provides the lowest number of copies of a bucket that have
   * been remaining across all the buckets in the region
   *
   * @return the number of copies of the bucket with the least copies available
   */
  int getLowestBucketCopies() {
    return lowestBucketCopies;
  }

  /**
   * Increments the count of buckets that do not meet redundancy
   */
  synchronized void incrementLowRedundancyBucketCount() {
    if (lowRedundancyBuckets == totalBuckets) {
      return;
    }
    lowRedundancyBuckets++;
    stats.incLowRedundancyBucketCount(1);
  }

  /**
   * Updates the count of copies for the bucket with the least copies if a new low has been reached
   *
   * @param bucketCopies number of copies of a bucket remaining
   */
  synchronized void reportBucketCount(int bucketCopies) {
    if (bucketCopies < lowestBucketCopies) {
      lowestBucketCopies = bucketCopies;
      logger.warn("Redundancy has dropped below {} configured copies to {} actual copies for {}",
          new Object[] {targetRedundancy + 1, bucketCopies, regionPath});
    }
  }

  /**
   * Increments the count of buckets that no longer have any copies remaining
   */
  synchronized void incrementNoCopiesBucketCount() {
    if (noCopiesBuckets == totalBuckets) {
      return;
    }
    noCopiesBuckets++;
    stats.incNoCopiesBucketCount(1);
    if (noCopiesBuckets == 1) {
      logger.warn("All in memory copies of some data have been lost for " + regionPath);
    }
  }

  /**
   * Decrements the count of buckets that do not meet redundancy
   */
  synchronized void decrementLowRedundancyBucketCount() {
    if (lowRedundancyBuckets == 0) {
      return;
    }
    lowRedundancyBuckets--;
    stats.incLowRedundancyBucketCount(-1);
    if (lowRedundancyBuckets == 0) {
      lowestBucketCopies = targetRedundancy + 1;
      logger.info("Configured redundancy of " + (targetRedundancy + 1)
          + " copies has been restored to " + regionPath);
    }
  }

  /**
   * Decrements the count of buckets that no longer have any copies remaining
   */
  synchronized void decrementNoCopiesBucketCount() {
    if (noCopiesBuckets == 0) {
      return;
    }
    noCopiesBuckets--;
    stats.incNoCopiesBucketCount(-1);
    // if the last bucket with no copies has gained a copy, the bucket with the lowest
    // number of copies (that bucket) has one copy
    if (noCopiesBuckets == 0) {
      lowestBucketCopies = 1;
    }
  }

  void setActualRedundancy(int actualRedundancy) {
    stats.setActualRedundantCopies(Math.max(actualRedundancy, 0));
  }
}
