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

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.TimeUnit;

import junitparams.Parameters;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.internal.cache.wan.MyGatewayEventFilter;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({AEQTest.class})
@RunWith(GeodeParamsRunner.class)
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
  @Parameters({"true", "false"})
  public void whenAEQCreatedInPausedStateThenSenderIsStartedInPausedState(boolean isParallel) {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory()
        .setParallel(isParallel)
        .pauseEventDispatching()
        .setDispatcherThreads(5);
    AsyncEventQueue aeq =
        fact.create("aeqID", new org.apache.geode.internal.cache.wan.MyAsyncEventListener());
    assertTrue(aeq.isDispatchingPaused());
    assertTrue(((AsyncEventQueueImpl) aeq).getSender().isPaused());
  }

  @Test
  @Parameters({"true", "false"})
  public void whenAEQCreatedInPausedStateIsUnPausedThenSenderIsResumed(boolean isParallel) {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    AsyncEventQueueFactory fact = cache.createAsyncEventQueueFactory()
        .setParallel(isParallel)
        .pauseEventDispatching()
        .setDispatcherThreads(5);
    AsyncEventQueue aeq =
        fact.create("aeqID", new org.apache.geode.internal.cache.wan.MyAsyncEventListener());

    aeq.resumeEventDispatching();

    assertFalse(aeq.isDispatchingPaused());
    assertFalse(((AsyncEventQueueImpl) aeq).getSender().isPaused());

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
    String cacheXmlFileName =
        createTempFileFromResource(getClass(),
            getClass().getSimpleName() + "." + cacheXmlFileBaseName + ".cache.xml")
                .getAbsolutePath();
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

  @Test
  @Parameters(method = "getCacheXmlFileBaseNamesForPauseTests")
  public void whenAsyncQueuesAreStartedInPausedStateShouldNotDispatchEventsTillItIsUnpaused(
      String cacheXmlFileBaseName) {
    // Create cache with xml
    String cacheXmlFileName =
        createTempFileFromResource(getClass(),
            getClass().getSimpleName() + "." + cacheXmlFileBaseName + ".cache.xml")
                .getAbsolutePath();
    cache = new CacheFactory().set(MCAST_PORT, "0").set(CACHE_XML_FILE, cacheXmlFileName).create();

    // Get AsyncEventQueue and GatewayEventFilter
    AsyncEventQueue aeq = cache.getAsyncEventQueue(cacheXmlFileBaseName);

    // Get region and do puts
    Region region = cache.getRegion(cacheXmlFileBaseName);
    int numPuts = 1000;
    for (int i = 0; i < numPuts; i++) {
      region.put(i, i);
    }

    MyAsyncEventListener listener = (MyAsyncEventListener) aeq.getAsyncEventListener();

    // Ensure that no listeners are being invoked
    try {
      await().atMost(10, TimeUnit.SECONDS).until(() -> listener.getEventsMap().size() > 0);
    } catch (ConditionTimeoutException ex) {
      // Expected Exception
    }

    // Ensure that the queue is filling up
    await().atMost(60, TimeUnit.SECONDS).until(() -> ((AsyncEventQueueImpl) aeq).getSender()
        .getQueues().stream().mapToInt(i -> i.size()).sum() == 1000);

    // Unpause the sender
    aeq.resumeEventDispatching();

    // Ensure that listener is being invoke after unpause
    await().atMost(60, TimeUnit.SECONDS).until(() -> listener.getEventsMap().size() == 1000);


  }

  private Object[] getCacheXmlFileBaseNames() {
    return new Object[] {
        new Object[] {"testSerialAsyncEventQueueConfiguredFromXmlUsesFilter"},
        new Object[] {"testParallelAsyncEventQueueConfiguredFromXmlUsesFilter"}
    };
  }

  private Object[] getCacheXmlFileBaseNamesForPauseTests() {
    return new Object[] {
        new Object[] {"testSerialAsyncEventQueueConfiguredFromXmlStartsPaused"},
        new Object[] {"testParallelAsyncEventQueueConfiguredFromXmlStartsPaused"}
    };
  }

}
