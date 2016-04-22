/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
