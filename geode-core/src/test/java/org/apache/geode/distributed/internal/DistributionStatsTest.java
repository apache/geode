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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
  public void endReplyWait() {
    distributionStats.endReplyWait(12000000, 12);

    verify(mockStats).incLong(eq(DistributionStats.replyWaitTimeId), anyLong());
    verify(mockStats).incLong(eq(DistributionStats.replyWaitMaxTimeId), anyLong());
  }

  @Test
  public void incSentMessagesTime() {
    distributionStats.incSentMessagesTime(50000000L);

    verify(mockStats).incLong(DistributionStats.sentMessagesTimeId, 50000000L);
    verify(mockStats).incLong(DistributionStats.sentMessagesMaxTimeId, 50L);
  }

  @Test
  public void incSerialQueueBytes() {
    distributionStats.incSerialQueueBytes(50000000);
    distributionStats.incSerialQueueBytes(20000000);
    assertThat(distributionStats.getInternalSerialQueueBytes()).isEqualTo(70000000);
    verify(mockStats).incLong(DistributionStats.serialQueueBytesId, 50000000);
    verify(mockStats).incLong(DistributionStats.serialQueueBytesId, 20000000);
  }

  @Test
  public void startSenderCreate() {
    long startTime = distributionStats.startSenderCreate();
    verify(mockStats).incLong(DistributionStats.senderCreatesInProgressId, 1);
    assertThat(startTime).isNotEqualTo(0);
    distributionStats.incSenders(false, true, startTime);
    verify(mockStats).incLong(DistributionStats.senderCreatesInProgressId, -1);
    verify(mockStats).incLong(eq(DistributionStats.senderCreateTimeId), anyLong());
  }
}
