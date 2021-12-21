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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueStats;
import org.apache.geode.management.internal.beans.AsyncEventQueueMBeanBridge;
import org.apache.geode.test.junit.categories.JMXTest;

@Category({JMXTest.class})
public class AsyncEventQueueStatsJUnitTest extends MBeanStatsTestCase {

  private AsyncEventQueueMBeanBridge bridge;

  private AsyncEventQueueStats asyncEventQueueStats;

  @Override
  public void init() {
    asyncEventQueueStats = new AsyncEventQueueStats(system, "test", disabledClock());

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

  @Test
  public void testStatDescriptors() {
    StatisticDescriptor[] sds = AsyncEventQueueStats.type.getStatistics();
    int notQueueEvents = 0;
    int notQueueToPrimary = 0;
    int eventsProcessedByPQRM = 0;
    for (StatisticDescriptor s : sds) {
      if (s.getName().equals("notQueuedEvent")) {
        notQueueEvents++;
      }
      if (s.getName().equals("eventsDroppedDueToPrimarySenderNotRunning")) {
        notQueueToPrimary++;
      }
      if (s.getName().equals("eventsProcessedByPQRM")) {
        eventsProcessedByPQRM++;
      }
    }
    assertEquals(1, notQueueEvents);
    assertEquals(1, notQueueToPrimary);
    assertEquals(1, eventsProcessedByPQRM);
  }

}
