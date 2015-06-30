/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.management.internal.beans.AsyncEventQueueMBeanBridge;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author rishim
 *
 */
@Category(IntegrationTest.class)
public class AsyncEventQueueStatsJUnitTest extends MBeanStatsTestCase {

  private AsyncEventQueueMBeanBridge bridge;

  private AsyncEventQueueStats asyncEventQueueStats;

  public void init() {
    asyncEventQueueStats = new AsyncEventQueueStats(system, "test");

    bridge = new AsyncEventQueueMBeanBridge();
    bridge.addAsyncEventQueueStats(asyncEventQueueStats);
  }

  @Test
  public void testSenderStats() throws InterruptedException {
    asyncEventQueueStats.setQueueSize(10);

    sample();
    assertEquals(10, getEventQueueSize());
    
    asyncEventQueueStats.setQueueSize(0);
    sample();
    
    assertEquals(0, getEventQueueSize());
    
    
  }

  private int getEventQueueSize() {
    return bridge.getEventQueueSize();
  }

}
