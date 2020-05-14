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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.Before;
import org.junit.Test;


public class PartitionedRegionRedundancyTrackerTest {
  private static final int TARGET_COPIES = 2;
  private static final int TOTAL_BUCKETS = 3;

  private PartitionedRegionStats stats;
  private PartitionedRegionRedundancyTracker redundancyTracker;

  @Before
  public void setup() {
    stats = mock(PartitionedRegionStats.class);
    redundancyTracker = new PartitionedRegionRedundancyTracker(TOTAL_BUCKETS, TARGET_COPIES - 1,
        stats, "testRegion");
  }

  @Test
  public void incrementsAndDecrementsLowRedundancyBucketCount() {
    redundancyTracker.incrementLowRedundancyBucketCount();
    verify(stats, times(1)).incLowRedundancyBucketCount(1);
    redundancyTracker.decrementLowRedundancyBucketCount();
    verify(stats, times(1)).incLowRedundancyBucketCount(-1);
  }

  @Test
  public void willNotIncrementLowRedundancyBucketCountBeyondTotalsBuckets() {

    for (int i = 0; i < TOTAL_BUCKETS; i++) {
      redundancyTracker.incrementLowRedundancyBucketCount();
    }
    verify(stats, times(TOTAL_BUCKETS)).incLowRedundancyBucketCount(1);
    redundancyTracker.incrementLowRedundancyBucketCount();
    verifyNoMoreInteractions(stats);
  }

  @Test
  public void willNotDecrementLowRedundancyBucketCountBelowZero() {
    redundancyTracker.decrementLowRedundancyBucketCount();
    verifyZeroInteractions(stats);
  }

  @Test
  public void incrementsAndDecrementsNoCopiesBucketCount() {
    redundancyTracker.incrementNoCopiesBucketCount();
    verify(stats, times(1)).incNoCopiesBucketCount(1);
    redundancyTracker.decrementNoCopiesBucketCount();
    verify(stats, times(1)).incNoCopiesBucketCount(-1);
  }

  @Test
  public void willNotIncrementNoCopiesBucketCountBeyondTotalsBuckets() {

    for (int i = 0; i < TOTAL_BUCKETS; i++) {
      redundancyTracker.incrementNoCopiesBucketCount();
    }
    verify(stats, times(TOTAL_BUCKETS)).incNoCopiesBucketCount(1);
    redundancyTracker.incrementNoCopiesBucketCount();
    verifyNoMoreInteractions(stats);
  }

  @Test
  public void willNotDecrementNoCopiesBucketCountBelowZero() {
    redundancyTracker.decrementNoCopiesBucketCount();
    verify(stats, times(0)).incNoCopiesBucketCount(-1);
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

  @Test
  public void willSetActualRedundancyAndStoreStat() {
    redundancyTracker.setActualRedundancy(7);
    assertThat(redundancyTracker.getActualRedundancy()).isEqualTo(7);
    verify(stats).setActualRedundantCopies(7);
  }

  @Test
  public void willNotSetActualRedundantCopiesStatBelowZero() {
    redundancyTracker.setActualRedundancy(-1);
    assertThat(redundancyTracker.getActualRedundancy()).isEqualTo(0);
    verify(stats).setActualRedundantCopies(0);
  }
}
