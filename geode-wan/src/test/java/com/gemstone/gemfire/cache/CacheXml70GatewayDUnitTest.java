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
package com.gemstone.gemfire.cache;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.CacheXml70DUnitTest;
import com.gemstone.gemfire.cache30.CacheXmlTestCase;
import com.gemstone.gemfire.cache30.MyGatewayEventFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.ParallelGatewaySenderCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.SerialGatewaySenderCreation;

public class CacheXml70GatewayDUnitTest extends CacheXmlTestCase {

  public CacheXml70GatewayDUnitTest(String name) {
    super(name);
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_7_0;
  }
  
  /**
   * Added to test the scenario of defect #50600.
   */
  public void testAsyncEventQueueWithGatewayEventFilter() {
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
    factory.addGatewayEventFilter(new MyGatewayEventFilter());
            
    AsyncEventListener eventListener = new CacheXml70DUnitTest.MyAsyncEventListener();
    AsyncEventQueue asyncEventQueue = factory.create(id, eventListener);
            
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addAsyncEventQueueId(asyncEventQueue.getId());
    cache.createRegion("UserRegion", attrs);
            
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    
    Set<AsyncEventQueue> asyncEventQueuesOnCache = c.getAsyncEventQueues();
    assertTrue("Size of asyncEventQueues should be greater than 0", asyncEventQueuesOnCache.size() > 0);
            
    for (AsyncEventQueue asyncEventQueueOnCache : asyncEventQueuesOnCache) {
      CacheXml70DUnitTest.validateAsyncEventQueue(asyncEventQueue, asyncEventQueueOnCache);
    }
  }
  
  public void testGatewayReceiver() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(54321);
    gatewayReceiverFactory.setEndPort(54331);
    gatewayReceiverFactory.setMaximumTimeBetweenPings(2000);
    gatewayReceiverFactory.setSocketBufferSize(1500);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewayReceiver receiver1 = gatewayReceiverFactory.create();
    try {
      receiver1.start();
    }
    catch (IOException e) {
      fail("Could not start GatewayReceiver");
    }
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewayReceiver> receivers = c.getGatewayReceivers();
    for(GatewayReceiver receiver : receivers){
      validateGatewayReceiver(receiver1, receiver);
    }
  }
  
  public void testParallelGatewaySender() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewaySenderFactory gatewaySenderFactory = cache.createGatewaySenderFactory();
    gatewaySenderFactory.setParallel(true);
    gatewaySenderFactory.setDispatcherThreads(13);
    gatewaySenderFactory.setManualStart(true);
    gatewaySenderFactory.setSocketBufferSize(1234);
    gatewaySenderFactory.setSocketReadTimeout(1050);          
    gatewaySenderFactory.setBatchConflationEnabled(false);
    gatewaySenderFactory.setBatchSize(88);
    gatewaySenderFactory.setBatchTimeInterval(9);           
    gatewaySenderFactory.setPersistenceEnabled(true);          
    gatewaySenderFactory.setDiskStoreName("LNSender");  
    gatewaySenderFactory.setDiskSynchronous(true);
    gatewaySenderFactory.setMaximumQueueMemory(211);           
    gatewaySenderFactory.setAlertThreshold(35);
    
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    gatewaySenderFactory.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender parallelGatewaySender = gatewaySenderFactory.create("LN", 2);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewaySender> sendersOnCache = c.getGatewaySenders();
    for(GatewaySender sender : sendersOnCache){
      assertEquals(true, sender.isParallel());
      validateGatewaySender(parallelGatewaySender, sender);
    }
  }
  
  public void testSerialGatewaySender() throws CacheException{
    getSystem();
    CacheCreation cache = new CacheCreation();
    GatewaySenderFactory gatewaySenderFactory = cache.createGatewaySenderFactory();
    gatewaySenderFactory.setParallel(false);
    gatewaySenderFactory.setManualStart(true);
    gatewaySenderFactory.setSocketBufferSize(124);
    gatewaySenderFactory.setSocketReadTimeout(1000);          
    gatewaySenderFactory.setBatchConflationEnabled(false);
    gatewaySenderFactory.setBatchSize(100);
    gatewaySenderFactory.setBatchTimeInterval(10);           
    gatewaySenderFactory.setPersistenceEnabled(true);          
    gatewaySenderFactory.setDiskStoreName("LNSender"); 
    gatewaySenderFactory.setDiskSynchronous(true);
    gatewaySenderFactory.setMaximumQueueMemory(200);           
    gatewaySenderFactory.setAlertThreshold(30);
    
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    gatewaySenderFactory.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewaySenderFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender serialGatewaySender = gatewaySenderFactory.create("LN", 2);
    
    RegionAttributesCreation attrs = new RegionAttributesCreation();
    attrs.addGatewaySenderId(serialGatewaySender.getId());
    cache.createRegion("UserRegion", attrs);
    
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewaySender> sendersOnCache = c.getGatewaySenders();
    for(GatewaySender sender : sendersOnCache){
      assertEquals(false, sender.isParallel());
      validateGatewaySender(serialGatewaySender, sender);
    }
  }
  
  public static class MyGatewayEventFilter implements GatewayEventFilter, Declarable {
    public void afterAcknowledgement(GatewayQueueEvent event) {
    }
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }
    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }
    public void close() {
    }
    public void init(Properties properties) {
    }
  }

  static void validateGatewayReceiver(GatewayReceiver receiver1, GatewayReceiver gatewayReceiver) {
    assertEquals(receiver1.getHost(), gatewayReceiver.getHost());
    assertEquals(receiver1.getStartPort(), gatewayReceiver.getStartPort());
    assertEquals(receiver1.getEndPort(), gatewayReceiver.getEndPort());
    assertEquals(receiver1.getMaximumTimeBetweenPings(), gatewayReceiver.getMaximumTimeBetweenPings());
    assertEquals(receiver1.getSocketBufferSize(), gatewayReceiver.getSocketBufferSize());
    assertEquals(receiver1.getGatewayTransportFilters().size(), gatewayReceiver.getGatewayTransportFilters().size());
  } 

  static void validateGatewaySender(GatewaySender sender1, GatewaySender gatewaySender) {
    assertEquals(sender1.getId(), gatewaySender.getId());
    assertEquals(sender1.getRemoteDSId(), gatewaySender.getRemoteDSId());
    assertEquals(sender1.isParallel(), gatewaySender.isParallel());
    assertEquals(sender1.isBatchConflationEnabled(), gatewaySender.isBatchConflationEnabled());
    assertEquals(sender1.getBatchSize(), gatewaySender.getBatchSize());
    assertEquals(sender1.getBatchTimeInterval(), gatewaySender.getBatchTimeInterval());
    assertEquals(sender1.isPersistenceEnabled(), gatewaySender.isPersistenceEnabled());
    assertEquals(sender1.getDiskStoreName(),gatewaySender.getDiskStoreName());
    assertEquals(sender1.isDiskSynchronous(),gatewaySender.isDiskSynchronous());
    assertEquals(sender1.getMaximumQueueMemory(), gatewaySender.getMaximumQueueMemory());
    assertEquals(sender1.getAlertThreshold(), gatewaySender.getAlertThreshold());
    assertEquals(sender1.getGatewayEventFilters().size(), gatewaySender.getGatewayEventFilters().size());
    assertEquals(sender1.getGatewayTransportFilters().size(), gatewaySender.getGatewayTransportFilters().size());
    
    boolean isParallel = sender1.isParallel();
    if (isParallel) {
      assertTrue("sender should be instanceof Creation", sender1 instanceof ParallelGatewaySenderCreation);
    } else {
      assertTrue("sender should be instanceof Creation", sender1 instanceof SerialGatewaySenderCreation);
    }
  }
}
