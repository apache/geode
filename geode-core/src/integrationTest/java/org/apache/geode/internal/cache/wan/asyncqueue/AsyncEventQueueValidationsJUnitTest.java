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
package org.apache.geode.internal.cache.wan.asyncqueue;

import static junitparams.JUnitParamsRunner.$;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.MyGatewayEventFilter;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.util.test.TestUtil;

@Category({AEQTest.class})
@RunWith(JUnitParamsRunner.class)
public class AsyncEventQueueValidationsJUnitTest {

  private Cache cache;

  @After
  public void closeCache() {
    if (this.cache != null) {
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
      fact.create("id", new org.apache.geode.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
      assertTrue(
          e.getMessage().contains(" can not be created with dispatcher threads less than 1"));
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
      fact.create("id", new org.apache.geode.internal.cache.wan.MyAsyncEventListener());
      fail("Expected AsyncEventQueueConfigurationException.");
    } catch (AsyncEventQueueConfigurationException e) {
      assertTrue(e.getMessage().contains("can not be created with OrderPolicy"));
    }
  }

  @Test
  @Parameters(method = "getCacheXmlFileBaseNames")
  public void testAsyncEventQueueConfiguredFromXmlUsesFilter(String cacheXmlFileBaseName) {
    // Create cache with xml
    String cacheXmlFileName = TestUtil.getResourcePath(getClass(),
        getClass().getSimpleName() + "." + cacheXmlFileBaseName + ".cache.xml");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();

    // Get region and do puts
    Region region = cache.getRegion(cacheXmlFileBaseName);
    int numPuts = 10;
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }

    // Get AsyncEventQueue and GatewayEventFilter
    AsyncEventQueue aeq = cache.getAsyncEventQueue(cacheXmlFileBaseName);
    List<GatewayEventFilter> filters = aeq.getGatewayEventFilters();
    assertTrue(filters.size() == 1);
    MyGatewayEventFilter filter = (MyGatewayEventFilter) filters.get(0);

    // Validate filter callbacks were invoked
    await()
        .until(() -> filter.getBeforeEnqueueInvocations() == numPuts);
    await()
        .until(() -> filter.getBeforeTransmitInvocations() == numPuts);
    await()
        .until(() -> filter.getAfterAcknowledgementInvocations() == numPuts);
  }

  private Object[] getCacheXmlFileBaseNames() {
    return $(new Object[] {"testSerialAsyncEventQueueConfiguredFromXmlUsesFilter"},
        new Object[] {"testParallelAsyncEventQueueConfiguredFromXmlUsesFilter"});
  }

}
