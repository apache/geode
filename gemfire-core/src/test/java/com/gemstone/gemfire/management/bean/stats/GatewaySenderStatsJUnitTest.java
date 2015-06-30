/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.management.internal.beans.GatewaySenderMBeanBridge;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author rishim
 */
@Category(IntegrationTest.class)
public class GatewaySenderStatsJUnitTest extends MBeanStatsTestCase {
  
  private GatewaySenderMBeanBridge bridge;

  private GatewaySenderStats senderStats;

  private static long testStartTime = NanoTimer.getTime();

  public void init() {
    senderStats = new GatewaySenderStats(system, "test");

    bridge = new GatewaySenderMBeanBridge();
    bridge.addGatewaySenderStats(senderStats);
  }
  
  @Test
  public void testSenderStats() throws InterruptedException{
    senderStats.incBatchesRedistributed();
    senderStats.incEventsReceived();
    senderStats.setQueueSize(10);
    senderStats.endPut(testStartTime);
    senderStats.endBatch(testStartTime, 100);
    senderStats.incEventsNotQueuedConflated();
    senderStats.incEventsExceedingAlertThreshold();
    
    sample();
    
    assertEquals(1, getTotalBatchesRedistributed());
    assertEquals(1, getTotalEventsConflated());
    assertEquals(10, getEventQueueSize());
    assertTrue(getEventsQueuedRate() >0);
    assertTrue(getEventsReceivedRate() >0);
    assertTrue(getBatchesDispatchedRate() >0);
    assertTrue(getAverageDistributionTimePerBatch() >0);
    assertTrue(getEventsExceedingAlertThreshold() >0);
  }

  private int getTotalBatchesRedistributed() {
    return bridge.getTotalBatchesRedistributed();
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
