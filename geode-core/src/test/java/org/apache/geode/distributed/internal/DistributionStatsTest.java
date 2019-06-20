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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
  public void recordMaxReplyWaitTime() {
    distributionStats.endReplyWait(12000000, 12);

    verify(mockStats).incLong(eq(44), Mockito.anyLong());
    verify(mockStats).incLong(eq(45), Mockito.anyLong());
  }

  @Test
  public void incSentMessagesTime() {
    distributionStats.incSentMessagesTime(50000000L);

    verify(mockStats).incLong(3, 50000000L);
    verify(mockStats).incLong(4, 50L);
  }
}
