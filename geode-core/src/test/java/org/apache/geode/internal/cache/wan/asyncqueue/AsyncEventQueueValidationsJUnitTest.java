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
/**
 *
 */
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static junitparams.JUnitParamsRunner.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueConfigurationException;
import com.gemstone.gemfire.internal.cache.wan.MyGatewayEventFilter;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;

@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class AsyncEventQueueValidationsJUnitTest {

  private Cache cache;

  @After
  public void closeCache() {
    if(this.cache != null) {
      this.cache.close();
    }
  }

  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesWrongDispatcherThreads() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(-5);
      fact.setOrderPolicy(OrderPolicy.KEY);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
      assertTrue(e.getMessage()
          .contains(" can not be created with dispatcher threads less than 1"));
    }
  }

  @Test
  public void testConcurrentParallelAsyncEventQueueAttributesOrderPolicyThread() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    try {
      AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory();
      fact.setParallel(true);
      fact.setDispatcherThreads(5);
      fact.setOrderPolicy(OrderPolicy.THREAD);
      fact.create("id", new com.gemstone.gemfire.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
      assertTrue(e.getMessage()
          .contains("can not be created with OrderPolicy"));
    }
  }

  @Test
  @Parameters(method = "getCacheXmlFileBaseNames")
  public void testAsyncEventQueueConfiguredFromXmlUsesFilter(String cacheXmlFileBaseName) {
    // Create cache with xml
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(), getClass().getSimpleName() + "." + cacheXmlFileBaseName + ".cache.xml");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();

    // Get region and do puts
    Region region = cache.getRegion(cacheXmlFileBaseName);
    int numPuts = 10;
    for (int i=0; i<numPuts; i++) {
      region.put(i,i);
    }

    // Get AsyncEventQueue and GatewayEventFilter
    AsyncEventQueue aeq = cache.getAsyncEventQueue(cacheXmlFileBaseName);
    List<GatewayEventFilter> filters = aeq.getGatewayEventFilters();
    assertTrue(filters.size() == 1);
    MyGatewayEventFilter filter = (MyGatewayEventFilter) filters.get(0);

    // Validate filter callbacks were invoked
    Awaitility.waitAtMost(60, TimeUnit.SECONDS).until(() -> filter.getBeforeEnqueueInvocations() == numPuts);
    Awaitility.waitAtMost(60, TimeUnit.SECONDS).until(() -> filter.getBeforeTransmitInvocations() == numPuts);
    Awaitility.waitAtMost(60, TimeUnit.SECONDS).until(() -> filter.getAfterAcknowledgementInvocations() == numPuts);
  }

  private final Object[] getCacheXmlFileBaseNames() {
    return $(
        new Object[] { "testSerialAsyncEventQueueConfiguredFromXmlUsesFilter" },
        new Object[] { "testParallelAsyncEventQueueConfiguredFromXmlUsesFilter" }
    );
  }

}
