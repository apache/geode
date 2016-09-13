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
package org.apache.geode.cache.asyncqueue.internal;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class SerialAsyncEventQueueImplJUnitTest {

  private Cache cache;
  @Before
  public void setUp() {
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");
    cache = cf.create();
  }
  
  @After
  public void tearDown() {
    cache.close();
  }
  @Test
  public void testStopClearsStats() {
    GatewaySenderAttributes attrs = new GatewaySenderAttributes();
    attrs.id = AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX + "id";
    SerialAsyncEventQueueImpl queue = new SerialAsyncEventQueueImpl(cache, attrs);
    queue.getStatistics().incQueueSize(5);
    queue.getStatistics().incTempQueueSize(10);
    
    assertEquals(5, queue.getStatistics().getEventQueueSize());
    assertEquals(10, queue.getStatistics().getTempEventQueueSize());
   
    queue.stop();
    
    assertEquals(0, queue.getStatistics().getEventQueueSize());
    assertEquals(0, queue.getStatistics().getTempEventQueueSize());
  }

}
