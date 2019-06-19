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
package org.apache.geode.distributed.internal;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import org.apache.geode.Statistics;

public class DistributionStatsTest {

  @Test
  public void recordMaxReplyWaitTime_singleRecord() {
    Statistics mockStats = mock(Statistics.class);
    DistributionStats distributionStats = new DistributionStats(mockStats);

    distributionStats.recordMaxReplyWaitTime(5, 17);
    verify(mockStats).incLong(anyInt(), eq(12L));
  }

  @Test
  public void recordMaxReplyWaitTime_multipleRecords() {
    Statistics mockStats = mock(Statistics.class);
    DistributionStats distributionStats = new DistributionStats(mockStats);

    distributionStats.recordMaxReplyWaitTime(5, 17);
    verify(mockStats).incLong(anyInt(), eq(12L));

    distributionStats.recordMaxReplyWaitTime(12, 25);
    verify(mockStats).incLong(anyInt(), eq(1L));
  }

  @Test
  public void recordMaxReplyWaitTime_recordNothing_ifMaxTimeIsNotExceeded() {
    Statistics mockStats = mock(Statistics.class);
    DistributionStats distributionStats = new DistributionStats(mockStats);

    distributionStats.recordMaxReplyWaitTime(5, 17);
    verify(mockStats).incLong(anyInt(), eq(12L));

    distributionStats.recordMaxReplyWaitTime(12, 24);
    verify(mockStats, times(1)).incLong(anyInt(), anyLong());
  }

  @Test
  public void recordMaxReplyWaitTime_recordNothing_ifInitTimeIsZero() {
    Statistics mockStats = mock(Statistics.class);
    DistributionStats distributionStats = new DistributionStats(mockStats);

    distributionStats.recordMaxReplyWaitTime(5, 17);
    verify(mockStats).incLong(anyInt(), eq(12L));

    distributionStats.recordMaxReplyWaitTime(0, 42);
    verify(mockStats, times(1)).incLong(anyInt(), anyLong());
  }

}
