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
package org.apache.geode.cache.asyncqueue.internal;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.test.junit.categories.AEQTest;

@Category({AEQTest.class})
public class SerialAsyncEventQueueImplJUnitTest {

  private InternalCache cache;

  @Before
  public void setUp() {
    try {
      cache = (InternalCache) CacheFactory.getAnyInstance();
    } catch (Exception e) {
      // ignore
    }
    if (null == cache) {
      cache = (InternalCache) new CacheFactory().set(MCAST_PORT, "0").create();
    }
  }

  @After
  public void tearDown() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
  }

  @Test
  public void testStopClearsStats() {

    GatewaySenderAttributes attrs = new GatewaySenderAttributes();
    String tempId = AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX + "id";
    attrs.setId(tempId);
    SerialAsyncEventQueueImpl queue = new SerialAsyncEventQueueImpl(cache,
        cache.getInternalDistributedSystem().getStatisticsManager(), cache.getStatisticsClock(),
        attrs);
    queue.getStatistics().incQueueSize(5);
    queue.getStatistics().incSecondaryQueueSize(6);
    queue.getStatistics().incTempQueueSize(10);
    queue.getStatistics().incEventsProcessedByPQRM(3);

    assertEquals(5, queue.getStatistics().getEventQueueSize());
    assertEquals(6, queue.getStatistics().getSecondaryEventQueueSize());
    assertEquals(10, queue.getStatistics().getTempEventQueueSize());
    assertEquals(3, queue.getStatistics().getEventsProcessedByPQRM());

    queue.stop();

    assertEquals(0, queue.getStatistics().getEventQueueSize());
    assertEquals(0, queue.getStatistics().getSecondaryEventQueueSize());
    assertEquals(0, queue.getStatistics().getTempEventQueueSize());
    assertEquals(0, queue.getStatistics().getEventsProcessedByPQRM());
  }

  @Test
  public void testStopStart() {
    GatewaySenderAttributes attrs = new GatewaySenderAttributes();
    String tempId = AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX + "id";
    attrs.setId(tempId);
    SerialAsyncEventQueueImpl queue = new SerialAsyncEventQueueImpl(cache,
        cache.getInternalDistributedSystem().getStatisticsManager(), cache.getStatisticsClock(),
        attrs);
    queue.getStatistics().incQueueSize(5);
    queue.getStatistics().incSecondaryQueueSize(6);
    queue.getStatistics().incTempQueueSize(10);
    queue.getStatistics().incEventsProcessedByPQRM(3);

    assertEquals(5, queue.getStatistics().getEventQueueSize());
    assertEquals(6, queue.getStatistics().getSecondaryEventQueueSize());
    assertEquals(10, queue.getStatistics().getTempEventQueueSize());
    assertEquals(3, queue.getStatistics().getEventsProcessedByPQRM());

    queue.start();
    queue.stop();
    queue.start();

    assertEquals(0, queue.getStatistics().getEventQueueSize());
    assertEquals(0, queue.getStatistics().getSecondaryEventQueueSize());
    assertEquals(0, queue.getStatistics().getTempEventQueueSize());
    assertEquals(0, queue.getStatistics().getEventsProcessedByPQRM());
  }

}
