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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

public class BucketRedundancyTrackerTest {
  private static final int TARGET_COPIES = 2;

  private PartitionedRegionRedundancyTracker regionRedundancyTracker;
  private BucketRedundancyTracker bucketRedundancyTracker;

  @Before
  public void setup() {
    regionRedundancyTracker = mock(PartitionedRegionRedundancyTracker.class);
    bucketRedundancyTracker =
        new BucketRedundancyTracker(TARGET_COPIES - 1, regionRedundancyTracker);
  }

  @Test
  public void whenRedundancyNeverMetDoesNotWarnOnLowRedundancy() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    verify(regionRedundancyTracker, never()).reportBucketCount(anyInt());
  }

  @Test
  public void incrementsBucketCountOnLowRedundancyForBucket() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    verify(regionRedundancyTracker, times(1)).incrementLowRedundancyBucketCount();
    assertEquals(0, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnRegainingRedundancyForBucket() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    verify(regionRedundancyTracker, times(1)).decrementLowRedundancyBucketCount();
    assertEquals(TARGET_COPIES - 1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnClosingBucketBelowRedundancy() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    bucketRedundancyTracker.closeBucket();
    verify(regionRedundancyTracker, times(1)).decrementLowRedundancyBucketCount();
    assertEquals(0, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnClosingABucketWithNoCopies() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    bucketRedundancyTracker.updateStatistics(0);
    bucketRedundancyTracker.closeBucket();
    verify(regionRedundancyTracker, times(1)).decrementLowRedundancyBucketCount();
    assertEquals(-1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnIncrementBeforeNoCopies() {
    bucketRedundancyTracker =
        new BucketRedundancyTracker(2, regionRedundancyTracker);
    bucketRedundancyTracker.updateStatistics(3);
    bucketRedundancyTracker.updateStatistics(2);
    // Verify incrementLowRedundancyBucketCount is invoked.
    verify(regionRedundancyTracker, times(1)).incrementLowRedundancyBucketCount();
    bucketRedundancyTracker.updateStatistics(1);
    bucketRedundancyTracker.updateStatistics(2);
    // Verify incrementLowRedundancyBucketCount is not invoked again when the count goes 2.
    verify(regionRedundancyTracker, times(1)).incrementLowRedundancyBucketCount();
    assertEquals(1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void bucketCountNotDecrementedOnClosingBucketThatNeverHadCopies() {
    verify(regionRedundancyTracker, never()).decrementLowRedundancyBucketCount();
    assertEquals(-1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void doesNotWarnWhenNeverHadAnyCopies() {
    bucketRedundancyTracker.updateStatistics(0);
    verify(regionRedundancyTracker, never()).reportBucketCount(anyInt());
    assertEquals(-1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void doesNotIncrementNoCopiesWhenNeverHadAnyCopies() {
    bucketRedundancyTracker.updateStatistics(0);
    verify(regionRedundancyTracker, never()).incrementNoCopiesBucketCount();
    assertEquals(-1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void incrementsBucketCountOnHavingNoCopiesForBucket() {
    bucketRedundancyTracker.updateStatistics(1);
    bucketRedundancyTracker.updateStatistics(0);
    verify(regionRedundancyTracker, times(1)).incrementNoCopiesBucketCount();
    assertEquals(-1, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void decrementsBucketCountOnHavingAtLeastOneCopyOfBucket() {
    bucketRedundancyTracker.updateStatistics(1);
    // Verify incrementLowRedundancyBucketCount is invoked.
    verify(regionRedundancyTracker, times(1)).incrementLowRedundancyBucketCount();
    bucketRedundancyTracker.updateStatistics(0);
    bucketRedundancyTracker.updateStatistics(1);
    // Verify incrementLowRedundancyBucketCount is not invoked again when the count goes to 1.
    verify(regionRedundancyTracker, times(1)).incrementLowRedundancyBucketCount();
    // Verify decrementLowRedundancyBucketCount is not invoked.
    verify(regionRedundancyTracker, never()).decrementLowRedundancyBucketCount();
    verify(regionRedundancyTracker, times(1)).decrementNoCopiesBucketCount();
    assertEquals(0, bucketRedundancyTracker.getCurrentRedundancy());
  }

  @Test
  public void updatesRedundancyOnlyIfChanged() {
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES - 1);
    verify(regionRedundancyTracker, times(1)).setActualRedundancy(TARGET_COPIES - 2);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    verify(regionRedundancyTracker, times(1)).setActualRedundancy(TARGET_COPIES - 1);
    bucketRedundancyTracker.updateStatistics(TARGET_COPIES);
    verify(regionRedundancyTracker, times(2)).setActualRedundancy(anyInt());
  }
}
