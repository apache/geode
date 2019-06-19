/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless rired y applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;

public class DistributionStatsTest {

  private Statistics mockStats;
  private DistributionStats distributionStats;

  @Before
  public void setup() {
    mockStats = mock(Statistics.class);
    distributionStats = new DistributionStats(mockStats);
    DistributionStats.enableClockStats = true;
  }

  @After
  public void cleanup() {
    DistributionStats.enableClockStats = false;
  }

  @Test
  public void recordMaxReplyWaitTime_singleRecord() {
    distributionStats.recordMaxReplyWaitTime(12);

    verify(mockStats).incLong(45, 12L);
  }

  @Test
  public void recordMaxReplyWaitTime_multipleRecords() {
    distributionStats.recordMaxReplyWaitTime(12);
    distributionStats.recordMaxReplyWaitTime(13);

    verify(mockStats).incLong(45, 12L);
    verify(mockStats).incLong(45, 1L);
  }

  @Test
  public void recordMaxReplyWaitTime_recordNothing_ifMaxTimeIsNotExceeded() {
    distributionStats.recordMaxReplyWaitTime(12);
    distributionStats.recordMaxReplyWaitTime(3);

    verify(mockStats, times(1)).incLong(45, 12);
    verifyNoMoreInteractions(mockStats);
  }

  @Test
  public void recordMaxReplyWaitTime_ignoresNegativesAndZero() {
    distributionStats.recordMaxReplyWaitTime(-12);
    distributionStats.recordMaxReplyWaitTime(0);

    verifyZeroInteractions(mockStats);
  }

  @Test
  public void incSentMessagesTime_oneMessage() {
    distributionStats.incSentMessagesTime(50000000L);

    verify(mockStats).incLong(3, 50000000L);
    verify(mockStats).incLong(4, 50L);
  }

  @Test
  public void incSentMessagesTime_twoMessages() {
    distributionStats.incSentMessagesTime(50000000L);
    distributionStats.incSentMessagesTime(80000000L);

    verify(mockStats).incLong(3, 50000000L);
    verify(mockStats).incLong(4, 50L);
    verify(mockStats).incLong(3, 80000000L);
    verify(mockStats).incLong(4, 30L);
  }

  @Test
  public void incSentMessagesTime_noChangeIfMaxTimeIsNotExceeded() {
    distributionStats.incSentMessagesTime(50000000L);
    distributionStats.incSentMessagesTime(20000000L);

    verify(mockStats).incLong(3, 50000000L);
    verify(mockStats).incLong(4, 50L);
    verify(mockStats).incLong(3, 20000000L);
    verifyNoMoreInteractions(mockStats);
  }

  @Test
  public void incSentMessagesTime_ignoresNegativesAndZeroForMaxValue() {
    distributionStats.incSentMessagesTime(-50000000L);
    distributionStats.incSentMessagesTime(0);

    verify(mockStats).incLong(3, -50000000L);
    verify(mockStats).incLong(3, 0L);
    verifyNoMoreInteractions(mockStats);
  }
}
