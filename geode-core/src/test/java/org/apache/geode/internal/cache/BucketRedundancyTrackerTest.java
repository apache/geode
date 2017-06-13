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

public class BucketRedundancyTrackerTest {
  private static final int TARGET_COPIES = 2;

  private PartitionedRegion region;
  private PartitionedRegionStats stats;
  private BucketRedundancyTracker redundancyTracker;

  @Before
  public void setup() {
    PartitionAttributes<Object, Object> regionAttributes = new PartitionAttributesFactory<>()
        .setTotalNumBuckets(10).setRedundantCopies(TARGET_COPIES - 1).create();
    region = (PartitionedRegion) PartitionedRegionTestHelper.createPartionedRegion("testRegion",
        regionAttributes);
    stats = region.getPrStats();
    redundancyTracker =
        new BucketRedundancyTracker(region.getRedundantCopies(), region.getRedundancyTracker());
  }

  @After
  public void teardown() {
    region.destroyRegion();
  }

  @Test
  public void whenRedundancyNeverMetDoesNotWarnOnLowRedundancy() {
    redundancyTracker.updateStatistics(TARGET_COPIES - 1);
    assertEquals(0, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void incrementsBucketCountOnLowRedundancyForBucket() {
    redundancyTracker.updateStatistics(TARGET_COPIES);
    redundancyTracker.updateStatistics(TARGET_COPIES - 1);
    assertEquals(1, stats.getLowRedundancyBucketCount());
    assertEquals(0, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnRegainingRedundancyForBucket() {
    redundancyTracker.updateStatistics(TARGET_COPIES);
    redundancyTracker.updateStatistics(TARGET_COPIES - 1);
    redundancyTracker.updateStatistics(TARGET_COPIES);
    assertEquals(0, stats.getLowRedundancyBucketCount());
    assertEquals(TARGET_COPIES - 1, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnClosingBucketBelowRedundancy() {
    redundancyTracker.updateStatistics(TARGET_COPIES);
    redundancyTracker.updateStatistics(TARGET_COPIES - 1);
    redundancyTracker.closeBucket();
    assertEquals(0, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnClosingABucketWithNoCopies() {
    redundancyTracker.updateStatistics(TARGET_COPIES);
    redundancyTracker.updateStatistics(TARGET_COPIES - 1);
    redundancyTracker.updateStatistics(0);
    redundancyTracker.closeBucket();
    assertEquals(-1, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void doesNotWarnWhenNeverHadAnyCopies() {
    redundancyTracker.updateStatistics(0);
    assertEquals(-1, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void incrementsBucketCountOnHavingNoCopiesForBucket() {
    redundancyTracker.updateStatistics(1);
    redundancyTracker.updateStatistics(0);
    assertEquals(-1, redundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnHavingAtLeastOneCopyOfBucket() {
    redundancyTracker.updateStatistics(1);
    redundancyTracker.updateStatistics(0);
    redundancyTracker.updateStatistics(1);
    assertEquals(0, redundancyTracker.getCurrentRedundancy());
  }
}
