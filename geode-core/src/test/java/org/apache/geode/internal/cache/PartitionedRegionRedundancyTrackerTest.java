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

import static org.junit.Assert.assertEquals;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PartitionedRegionRedundancyTrackerTest {
  private static final int TARGET_COPIES = 2;
  private static final int TOTAL_BUCKETS = 3;

  private PartitionedRegion region;
  private PartitionedRegionStats stats;
  private PartitionedRegionRedundancyTracker redundancyTracker;

  @Before
  public void setup() {
    PartitionAttributes<Object, Object> regionAttributes = new PartitionAttributesFactory<>()
        .setTotalNumBuckets(TOTAL_BUCKETS).setRedundantCopies(TARGET_COPIES - 1).create();
    region = (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("testRegion",
        regionAttributes);
    stats = region.getPrStats();
    redundancyTracker = new PartitionedRegionRedundancyTracker(region.getTotalNumberOfBuckets(),
        region.getRedundantCopies(), stats, region.getFullPath());
  }

  @After
  public void teardown() {
    region.destroyRegion();
  }

  @Test
  public void incrementsAndDecrementsLowRedundancyBucketCount() {
    redundancyTracker.incrementLowRedundancyBucketCount();
    assertEquals(1, stats.getLowRedundancyBucketCount());
    redundancyTracker.decrementLowRedundancyBucketCount();
    assertEquals(0, stats.getLowRedundancyBucketCount());
  }

  @Test
  public void willNotIncrementLowRedundancyBucketCountBeyondTotalsBuckets() {

    for (int i = 0; i < TOTAL_BUCKETS; i++) {
      redundancyTracker.incrementLowRedundancyBucketCount();
    }
    assertEquals(TOTAL_BUCKETS, stats.getLowRedundancyBucketCount());
    redundancyTracker.incrementLowRedundancyBucketCount();
    assertEquals(TOTAL_BUCKETS, stats.getLowRedundancyBucketCount());
  }

  @Test
  public void willNotDecrementLowRedundancyBucketCountBelowZero() {
    redundancyTracker.decrementLowRedundancyBucketCount();
    assertEquals(0, stats.getLowRedundancyBucketCount());
  }

  @Test
  public void incrementsAndDecrementsNoCopiesBucketCount() {
    redundancyTracker.incrementNoCopiesBucketCount();
    assertEquals(1, stats.getNoCopiesBucketCount());
    redundancyTracker.decrementNoCopiesBucketCount();
    assertEquals(0, stats.getNoCopiesBucketCount());
  }

  @Test
  public void willNotIncrementNoCopiesBucketCountBeyondTotalsBuckets() {

    for (int i = 0; i < TOTAL_BUCKETS; i++) {
      redundancyTracker.incrementNoCopiesBucketCount();
    }
    assertEquals(TOTAL_BUCKETS, stats.getNoCopiesBucketCount());
    redundancyTracker.incrementNoCopiesBucketCount();
    assertEquals(TOTAL_BUCKETS, stats.getNoCopiesBucketCount());
  }

  @Test
  public void willNotDecrementNoCopiesBucketCountBelowZero() {
    redundancyTracker.decrementNoCopiesBucketCount();
    assertEquals(0, stats.getNoCopiesBucketCount());
  }

  @Test
  public void reportsCorrectLowestBucketCopies() {
    redundancyTracker.reportBucketCount(1);
    assertEquals(1, redundancyTracker.getLowestBucketCopies());
    redundancyTracker.reportBucketCount(0);
    assertEquals(0, redundancyTracker.getLowestBucketCopies());
    redundancyTracker.reportBucketCount(1);
    assertEquals(0, redundancyTracker.getLowestBucketCopies());
  }

  @Test
  public void lowestBucketCopiesResetsOnRedundancyRegained() {
    redundancyTracker.incrementLowRedundancyBucketCount();
    redundancyTracker.reportBucketCount(1);
    redundancyTracker.decrementLowRedundancyBucketCount();
    assertEquals(2, redundancyTracker.getLowestBucketCopies());
  }

  @Test
  public void lowestBucketCopiesSetToOneOnHavingABucketAgain() {
    redundancyTracker.incrementNoCopiesBucketCount();
    redundancyTracker.reportBucketCount(0);
    redundancyTracker.decrementNoCopiesBucketCount();
    assertEquals(1, redundancyTracker.getLowestBucketCopies());
  }
}
