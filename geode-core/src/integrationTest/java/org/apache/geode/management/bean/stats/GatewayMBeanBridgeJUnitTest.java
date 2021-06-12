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
package org.apache.geode.management.bean.stats;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.management.internal.beans.GatewaySenderMBeanBridge;
import org.apache.geode.test.junit.categories.JMXTest;

@Category({JMXTest.class})
public class GatewayMBeanBridgeJUnitTest extends MBeanStatsTestCase {

  private GatewaySenderMBeanBridge bridge;

  private GatewaySenderStats senderStats;

  private static long testStartTime = NanoTimer.getTime();

  private AbstractGatewaySender sender;

  @Override
  public void init() {
    senderStats = new GatewaySenderStats(system, "gatewaySenderStats-", "test", disabledClock());

    sender = Mockito.mock(AbstractGatewaySender.class);
    Mockito.when(sender.getStatistics()).thenReturn(senderStats);
    bridge = new GatewaySenderMBeanBridge(sender);
    bridge.addGatewaySenderStats(senderStats);
  }

  @Test
  public void testSenderStats() throws InterruptedException {
    senderStats.incBatchesWithIncompleteTransactions();
    senderStats.incBatchesWithIncompleteTransactions();
    senderStats.incBatchesWithIncompleteTransactions();
    senderStats.incBatchesRedistributed();
    senderStats.incBatchesRedistributed();
    senderStats.incEventsReceived();
    Mockito.when(sender.getEventQueueSize()).thenReturn(10);
    senderStats.endPut(testStartTime);
    senderStats.endBatch(testStartTime, 100);
    senderStats.incEventsNotQueuedConflated();
    senderStats.incEventsExceedingAlertThreshold();

    sample();

    assertEquals(1, getTotalBatchesDistributed());
    assertEquals(2, getTotalBatchesRedistributed());
    assertEquals(3, getTotalBatchesWithIncompleteTransactions());
    assertEquals(1, getTotalEventsConflated());
    assertEquals(10, getEventQueueSize());
    assertTrue(getEventsQueuedRate() > 0);
    assertTrue(getEventsReceivedRate() > 0);
    assertTrue(getBatchesDispatchedRate() > 0);
    assertTrue(getAverageDistributionTimePerBatch() > 0);
    assertTrue(getEventsExceedingAlertThreshold() > 0);
  }

  private int getTotalBatchesDistributed() {
    return bridge.getTotalBatchesDistributed();
  }

  private int getTotalBatchesRedistributed() {
    return bridge.getTotalBatchesRedistributed();
  }

  private int getTotalBatchesWithIncompleteTransactions() {
    return bridge.getTotalBatchesWithIncompleteTransactions();
  }

  private int getTotalEventsConflated() {
    return bridge.getTotalEventsConflated();
  }

  private int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

  private float getEventsQueuedRate() {
    return bridge.getEventsQueuedRate();
  }

  private float getEventsReceivedRate() {
    return bridge.getEventsReceivedRate();
  }

  private float getBatchesDispatchedRate() {
    return bridge.getBatchesDispatchedRate();
  }

  private long getAverageDistributionTimePerBatch() {
    return bridge.getAverageDistributionTimePerBatch();
  }

  private long getEventsExceedingAlertThreshold() {
    return bridge.getEventsExceedingAlertThreshold();
  }
}
