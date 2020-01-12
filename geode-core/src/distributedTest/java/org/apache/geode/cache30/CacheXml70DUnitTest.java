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
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.util.GatewayConflictHelper;
import org.apache.geode.cache.util.GatewayConflictResolver;
import org.apache.geode.cache.util.TimestampedEntryEvent;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.xmlcache.AsyncEventQueueCreation;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;


public class CacheXml70DUnitTest extends CacheXml66DUnitTest {

  @Override
  protected String getGemFireVersion() {
    return CacheXml.VERSION_7_0;
  }

  /** make sure we can create regions with concurrencyChecksEnabled=true */
  @Test
  public void testConcurrencyChecksEnabled() throws Exception {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("replicated", attrs);

    attrs = new RegionAttributesCreation(cache);
    attrs.setDataPolicy(DataPolicy.PARTITION);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("partitioned", attrs);

    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.EMPTY);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("empty", attrs);

    attrs = new RegionAttributesCreation(cache);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setConcurrencyChecksEnabled(true);
    cache.createRegion("normal", attrs);

    testXml(cache);

    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("replicated");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled",
        region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("partitioned");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled",
        region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("empty");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled",
        region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();

    region = c.getRegion("normal");
    assertNotNull(region);
    assertTrue("expected concurrency checks to be enabled",
        region.getAttributes().getConcurrencyChecksEnabled());
    region.localDestroyRegion();
  }

  @Test
  public void testGatewayConflictResolver() throws Exception {
    CacheCreation cache = new CacheCreation();
    cache.setGatewayConflictResolver(new MyGatewayConflictResolver());
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    GatewayConflictResolver gatewayConflictResolver = c.getGatewayConflictResolver();
    assertNotNull(gatewayConflictResolver);
    assertTrue(gatewayConflictResolver instanceof MyGatewayConflictResolver);
  }

  @Test
  public void testAsyncEventQueue() throws Exception {
    getSystem();
    CacheCreation cache = new CacheCreation();

    String id = "WBCLChannel";
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(100);
    factory.setBatchTimeInterval(500);
    factory.setBatchConflationEnabled(true);
    factory.setMaximumQueueMemory(200);
    factory.setDiskSynchronous(true);
    factory.setParallel(false);
    factory.setDispatcherThreads(19);

    AsyncEventListener eventListener = new CacheXml70DUnitTestHelper.MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);

    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);

    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0",
        asyncEventQueuesOnCache.size() > 0);

    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      CacheXml70DUnitTestHelper.validateAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }

  @Test
  public void testConcurrentAsyncEventQueue() throws Exception {
    getSystem();
    CacheCreation cache = new CacheCreation();

    String id = "WBCLChannel";
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(100);
    factory.setBatchTimeInterval(500);
    factory.setBatchConflationEnabled(true);
    factory.setMaximumQueueMemory(200);
    factory.setDiskSynchronous(true);
    factory.setDispatcherThreads(5);
    factory.setOrderPolicy(OrderPolicy.THREAD);

    AsyncEventListener eventListener = new CacheXml70DUnitTestHelper.MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);

    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);

    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0",
        asyncEventQueuesOnCache.size() > 0);

    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      validateConcurrentAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }

  /**
   * Added to test the scenario of defect #50600.
   */
  @Test
  public void testAsyncEventQueueWithGatewayEventFilter() throws Exception {
    getSystem();
    CacheCreation cache = new CacheCreation();

    String id = "WBCLChannel";
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(100);
    factory.setBatchTimeInterval(500);
    factory.setBatchConflationEnabled(true);
    factory.setMaximumQueueMemory(200);
    factory.setDiskSynchronous(true);
    factory.setParallel(false);
    factory.setDispatcherThreads(33);

    AsyncEventListener eventListener = new CacheXml70DUnitTestHelper.MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);

    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);

    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0",
        asyncEventQueuesOnCache.size() > 0);

    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      CacheXml70DUnitTestHelper.validateAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }

  public static class MyGatewayConflictResolver implements GatewayConflictResolver, Declarable {

    @Override
    public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper) {}

    @Override
    public void init(Properties p) {}
  }

  private void validateConcurrentAsyncEventQueue(AsyncEventQueue eventChannelFromXml,
      AsyncEventQueue channel) {
    assertEquals("AsyncEventQueue id doesn't match", eventChannelFromXml.getId(), channel.getId());
    assertEquals("AsyncEventQueue batchSize doesn't match", eventChannelFromXml.getBatchSize(),
        channel.getBatchSize());
    assertEquals("AsyncEventQueue batchTimeInterval doesn't match",
        eventChannelFromXml.getBatchTimeInterval(), channel.getBatchTimeInterval());
    assertEquals("AsyncEventQueue batchConflationEnabled doesn't match",
        eventChannelFromXml.isBatchConflationEnabled(), channel.isBatchConflationEnabled());
    assertEquals("AsyncEventQueue persistent doesn't match", eventChannelFromXml.isPersistent(),
        channel.isPersistent());
    assertEquals("AsyncEventQueue diskStoreName doesn't match",
        eventChannelFromXml.getDiskStoreName(), channel.getDiskStoreName());
    assertEquals("AsyncEventQueue isDiskSynchronous doesn't match",
        eventChannelFromXml.isDiskSynchronous(), channel.isDiskSynchronous());
    assertEquals("AsyncEventQueue maximumQueueMemory doesn't match",
        eventChannelFromXml.getMaximumQueueMemory(), channel.getMaximumQueueMemory());
    assertEquals("AsyncEventQueue Parallel doesn't match", eventChannelFromXml.isParallel(),
        channel.isParallel());
    assertEquals("AsyncEventQueue dispatcherThreads doesn't match",
        eventChannelFromXml.getDispatcherThreads(), channel.getDispatcherThreads());
    assertEquals("AsyncEventQueue orderPolicy doesn't match", eventChannelFromXml.getOrderPolicy(),
        channel.getOrderPolicy());
    assertTrue("AsyncEventQueue should be instanceof Creation",
        eventChannelFromXml instanceof AsyncEventQueueCreation);
    assertTrue("AsyncEventQueue should be instanceof Impl", channel instanceof AsyncEventQueueImpl);
  }

  // test bug 47197
  @Test
  public void testPartitionedRegionAttributesForCoLocation3() throws Exception {
    closeCache();
    setXmlFile(findFile("coLocation3.xml"));
    Cache c = getCache();
    assertNotNull(c);
    Region cust = c.getRegion(Region.SEPARATOR + "Customer");
    assertNotNull(cust);
    Region order = c.getRegion(Region.SEPARATOR + "Order");
    assertNotNull(order);

    assertTrue(cust.getAttributes().getPartitionAttributes().getColocatedWith() == null);
    assertTrue(
        order.getAttributes().getPartitionAttributes().getColocatedWith().equals("Customer"));
  }

  @Test
  public void testBug44710() throws Exception {
    closeCache();
    setXmlFile(findFile("bug44710.xml"));
    Cache c = getCache();
    assertNotNull(c);
    Region r1 = c.getRegion(Region.SEPARATOR + "r1");
    assertNotNull(r1);
    assertTrue(r1.getAttributes().getDataPolicy().withPersistence());
    assertTrue(r1.getAttributes().getDataPolicy().withPartitioning());
    Region r2 = c.getRegion(Region.SEPARATOR + "r2");
    assertNotNull(r2);
    assertTrue(r2.getAttributes().getDataPolicy().withPersistence());
    assertTrue(r2.getAttributes().getDataPolicy().withPartitioning());
  }
}
