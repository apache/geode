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
package com.gemstone.gemfire.internal.cache.wan.misc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.MyGatewayEventFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.wan.GatewayReceiverException;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.InternalGatewaySenderFactory;
import com.gemstone.gemfire.internal.cache.wan.MyGatewaySenderEventListener;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class WANConfigurationJUnitTest {

  private Cache cache;

  /**
   * Test to validate that the sender can not be started without configuring
   * locator
   * @throws IOException 
   * 
   * @throws IOException
   */
  @Test
  public void test_GatewaySender_without_Locator() throws IOException {
    try {
      cache = new CacheFactory().set("mcast-port", "0").create();
      GatewaySenderFactory fact = cache.createGatewaySenderFactory();
      fact.setParallel(true);
      GatewaySender sender1 = fact.create("NYSender", 2);
      sender1.start();
      fail("Expectd IllegalStateException but not thrown");
    }
    catch (Exception e) {
      if ((e instanceof IllegalStateException && e
          .getMessage()
          .startsWith(
              LocalizedStrings.AbstractGatewaySender_LOCATOR_SHOULD_BE_CONFIGURED_BEFORE_STARTING_GATEWAY_SENDER
                  .toLocalizedString()))) {
      }
      else {
        fail("Expectd IllegalStateException but received :" + e);
      }
    }
  }

  /**
   * Test to validate that sender with same Id can not be added to cache.
   */
  @Test
  public void test_SameGatewaySenderCreatedTwice() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    try {
      GatewaySenderFactory fact = cache.createGatewaySenderFactory();
      fact.setParallel(true);
      fact.setManualStart(true);
      fact.create("NYSender", 2);
      fact.create("NYSender", 2);
      fail("Expectd IllegalStateException but not thrown");
    }
    catch (Exception e) {
      if (e instanceof IllegalStateException
          && e.getMessage().contains("A GatewaySender with id")) {

      }
      else {
        fail("Expectd IllegalStateException but received :" + e);
      }
    }
  }
  
  /**
   * Test to validate that same gatewaySender Id can not be added to the region attributes.
   */
  @Test
  public void test_SameGatewaySenderIdAddedTwice() {
    try {
      cache = new CacheFactory().set("mcast-port", "0").create();
      GatewaySenderFactory fact = cache.createGatewaySenderFactory();
      fact.setParallel(true);
      fact.setManualStart(true);
      GatewaySender sender1 = fact.create("NYSender", 2);
      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId(sender1.getId());
      factory.addGatewaySenderId(sender1.getId());
      fail("Expectd IllegalArgumentException but not thrown");
    }
    catch (Exception e) {
      if (e instanceof IllegalArgumentException
          && e.getMessage().contains("is already added")) {

      }
      else {
        fail("Expectd IllegalStateException but received :" + e);
      }
    }
  }
  
  @Test
  public void test_GatewaySenderIdAndAsyncEventId() {
      cache = new CacheFactory().set("mcast-port", "0").create();
      AttributesFactory factory = new AttributesFactory();
      factory.addGatewaySenderId("ln");
      factory.addGatewaySenderId("ny");
      factory.addAsyncEventQueueId("Async_LN");
      RegionAttributes attrs = factory.create();
      
      Set<String> senderIds = new HashSet<String>();
      senderIds.add("ln");
      senderIds.add("ny");
      Set<String> attrsSenderIds = attrs.getGatewaySenderIds();
      assertEquals(senderIds, attrsSenderIds);
      Region r = cache.createRegion("Customer", attrs);
      assertEquals(senderIds, ((LocalRegion)r).getGatewaySenderIds());
  }

  /**
   * Test to validate that distributed region can not have the gateway sender
   * with parallel distribution policy
   * 
   */
  public void DIABLEB_DUE_TO_BUG51491_test_GatewaySender_Parallel_DistributedRegion() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);
    fact.setManualStart(true);
    GatewaySender sender1 = fact.create("NYSender", 2);
    AttributesFactory factory = new AttributesFactory();
    factory.addGatewaySenderId(sender1.getId());
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      RegionFactory regionFactory = cache.createRegionFactory(factory.create());
      Region region = regionFactory
          .create("test_GatewaySender_Parallel_DistributedRegion");
    }
    catch (Exception e) {
      fail("Unexpectd Exception :" + e);
    }
  }
  
  @Test
  public void test_GatewaySender_Parallel_MultipleDispatherThread() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);
    fact.setManualStart(true);
    fact.setDispatcherThreads(4);
    try {
      GatewaySender sender1 = fact.create("NYSender", 2);
    }
    catch (GatewaySenderException e) {
       fail("UnExpectd Exception " + e);
    }
  }
  
  @Test
  public void test_GatewaySender_Serial_ZERO_DispatherThread() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setManualStart(true);
    fact.setDispatcherThreads(0);
    try {
      GatewaySender sender1 = fact.create("NYSender", 2);
      fail("Expectd GatewaySenderException but not thrown");
    }
    catch (GatewaySenderException e) {
      if (e.getMessage().contains("can not be created with dispatcher threads less than 1")) {
      }
      else {
        fail("Expectd IllegalStateException but received :" + e);
      }
    }
  }

  /**
   * Test to validate the gateway receiver attributes are correctly set
   */
  @Test
  public void test_ValidateGatewayReceiverAttributes() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    if(port1 < port2){
      fact.setStartPort(port1);
      fact.setEndPort(port2);  
    }else{
      fact.setStartPort(port2);
      fact.setEndPort(port1);
    }
    
    fact.setMaximumTimeBetweenPings(2000);
    fact.setSocketBufferSize(200);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayReceiver receiver1 = fact.create();
    

    Region region = cache.createRegionFactory().create(
        "test_ValidateGatewayReceiverAttributes");
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    GatewayReceiver rec = receivers.iterator().next();
    assertEquals(receiver1.getHost(), rec.getHost());
    assertEquals(receiver1.getStartPort(), rec.getStartPort());
    assertEquals(receiver1.getEndPort(), rec.getEndPort());
    assertEquals(receiver1.getBindAddress(), rec.getBindAddress());
    assertEquals(receiver1.getMaximumTimeBetweenPings(), rec
        .getMaximumTimeBetweenPings());
    assertEquals(receiver1.getSocketBufferSize(), rec
        .getSocketBufferSize());
    assertEquals(receiver1.getGatewayTransportFilters().size(), rec
        .getGatewayTransportFilters().size());

  }

  @Test
  public void test_ValidateGatewayReceiverStatus() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    int port1 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    int port2 = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    if(port1 < port2){
      fact.setStartPort(port1);
      fact.setEndPort(port2);  
    }else{
      fact.setStartPort(port2);
      fact.setEndPort(port1);
    }
    
    fact.setMaximumTimeBetweenPings(2000);
    fact.setSocketBufferSize(200);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayReceiver receiver1 = fact.create();
    assertTrue(receiver1.isRunning());
  }
  
  /**
   * Test to validate that serial gateway sender attributes are correctly set
   * 
   * @throws IOException
   */
  @Test
  public void test_ValidateSerialGatewaySenderAttributes() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setManualStart(true);
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(false);
    fact.setDiskStoreName("FORNY");
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender sender1 = fact.create("TKSender", 2);
    

    AttributesFactory factory = new AttributesFactory();
    factory.addGatewaySenderId(sender1.getId());
    factory.setDataPolicy(DataPolicy.PARTITION);
    Region region = cache.createRegionFactory(factory.create()).create(
        "test_ValidateGatewaySenderAttributes");
    Set<GatewaySender> senders = cache.getGatewaySenders();
    assertEquals(senders.size(), 1);
    GatewaySender gatewaySender = senders.iterator().next();
    assertEquals(sender1.getRemoteDSId(), gatewaySender
        .getRemoteDSId());
    assertEquals(sender1.isManualStart(), gatewaySender.isManualStart());
    assertEquals(sender1.isBatchConflationEnabled(), gatewaySender
        .isBatchConflationEnabled());
    assertEquals(sender1.getBatchSize(), gatewaySender.getBatchSize());
    assertEquals(sender1.getBatchTimeInterval(), gatewaySender
        .getBatchTimeInterval());
    assertEquals(sender1.isPersistenceEnabled(), gatewaySender
        .isPersistenceEnabled());
    assertEquals(sender1.getDiskStoreName(), gatewaySender.getDiskStoreName());
    assertEquals(sender1.getMaximumQueueMemory(), gatewaySender
        .getMaximumQueueMemory());
    assertEquals(sender1.getAlertThreshold(), gatewaySender
        .getAlertThreshold());
    assertEquals(sender1.getGatewayEventFilters().size(), gatewaySender
        .getGatewayEventFilters().size());
    assertEquals(sender1.getGatewayTransportFilters().size(), gatewaySender
        .getGatewayTransportFilters().size());

  }
  
  /**
   * Test to validate that parallel gateway sender attributes are correctly set
   * 
   * @throws IOException
   */
  @Test
  public void test_ValidateParallelGatewaySenderAttributes() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);
    fact.setManualStart(true);
    fact.setBatchConflationEnabled(true);
    fact.setBatchSize(200);
    fact.setBatchTimeInterval(300);
    fact.setPersistenceEnabled(false);
    fact.setDiskStoreName("FORNY");
    fact.setMaximumQueueMemory(200);
    fact.setAlertThreshold(1200);
    GatewayEventFilter myeventfilter1 = new MyGatewayEventFilter1();
    fact.addGatewayEventFilter(myeventfilter1);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    fact.addGatewayTransportFilter(myStreamfilter2);
    GatewaySender sender1 = fact.create("TKSender", 2);
    

    AttributesFactory factory = new AttributesFactory();
    factory.addGatewaySenderId(sender1.getId());
    factory.setDataPolicy(DataPolicy.PARTITION);
    Region region = cache.createRegionFactory(factory.create()).create(
        "test_ValidateGatewaySenderAttributes");
    Set<GatewaySender> senders = cache.getGatewaySenders();
    assertEquals(1, senders.size());
    GatewaySender gatewaySender = senders.iterator().next();
    assertEquals(sender1.getRemoteDSId(), gatewaySender
        .getRemoteDSId());
    assertEquals(sender1.isManualStart(), gatewaySender.isManualStart());
    assertEquals(sender1.isBatchConflationEnabled(), gatewaySender
        .isBatchConflationEnabled());
    assertEquals(sender1.getBatchSize(), gatewaySender.getBatchSize());
    assertEquals(sender1.getBatchTimeInterval(), gatewaySender
        .getBatchTimeInterval());
    assertEquals(sender1.isPersistenceEnabled(), gatewaySender
        .isPersistenceEnabled());
    assertEquals(sender1.getDiskStoreName(), gatewaySender.getDiskStoreName());
    assertEquals(sender1.getMaximumQueueMemory(), gatewaySender
        .getMaximumQueueMemory());
    assertEquals(sender1.getAlertThreshold(), gatewaySender
        .getAlertThreshold());
    assertEquals(sender1.getGatewayEventFilters().size(), gatewaySender
        .getGatewayEventFilters().size());
    assertEquals(sender1.getGatewayTransportFilters().size(), gatewaySender
        .getGatewayTransportFilters().size());

  }
  
  @Test
  public void test_GatewaySenderWithGatewaySenderEventListener1() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    InternalGatewaySenderFactory fact = (InternalGatewaySenderFactory)cache.createGatewaySenderFactory();
    AsyncEventListener listener = new MyGatewaySenderEventListener();
    ((InternalGatewaySenderFactory)fact).addAsyncEventListener(listener);
    try {
      fact.create("ln", 2);
      fail("Expected GatewaySenderException. When a sender is added , remoteDSId should not be provided.");
    } catch (Exception e) {
      if (e instanceof GatewaySenderException
          && e.getMessage()
              .contains(
                  "cannot define a remote site because at least AsyncEventListener is already added.")) {

      } else {
        fail("Expected GatewaySenderException but received :" + e);
      }
    }
  }  
  
  @Test
  public void test_GatewaySenderWithGatewaySenderEventListener2() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    AsyncEventListener listener = new MyGatewaySenderEventListener();
    ((InternalGatewaySenderFactory)fact).addAsyncEventListener(listener);
    try {
      ((InternalGatewaySenderFactory)fact).create("ln");
    } catch (Exception e) {
      fail("Received Exception :" + e);
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverAttributes() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setStartPort(50504);
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setEndPort(70707);
    fact.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      fail("The test failed with IOException");
    }

    assertEquals(50504, receiver.getStartPort());
    assertEquals(1000, receiver.getMaximumTimeBetweenPings());
    assertEquals(4000,receiver.getSocketBufferSize());
    assertEquals(70707, receiver.getEndPort());
  }

  /**
   * This test takes a minimum of 120s to execute. It is known to hang on Mac OS
   * X Yosemite do to changes in the the message string checked in
   * GatewayRecieverImpl around line 167. Expects
   * "Cannot assign requested address" but gets
   * "Can't assign requested address". Timout after 150s to safeguard against
   * hanging on other platforms that may differ.
   * 
   * @see <a
   *      href="https://svn.gemstone.com/trac/gemfire/ticket/51536">https://svn
   *      .gemstone.com/trac/gemfire/ticket/51536<a>
   */
  @Test(timeout = 150000)
  public void test_ValidateGatwayReceiverAttributes_WrongBindAddress() {
    if (System.getProperty("os.name").equals("Mac OS X")) {
     fail("Failing to avoid known hang on Mac OS X.");
    }
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setStartPort(50504);
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setEndPort(70707);
    fact.setManualStart(true);
    fact.setBindAddress("200.112.204.10");
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
      fail("Expected GAtewayReceiverException");
    }
    catch (GatewayReceiverException gRE){
      assertTrue(gRE.getMessage().contains("Failed to create server socket on"));
    }
    catch (IOException e) {
      e.printStackTrace();
      fail("The test failed with IOException");
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverDefaultStartPortAndDefaultEndPort() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      fail("The test failed with IOException");
    }
    int port = receiver.getPort();
    System.out.println("SKSKSK The port of receiver is " + port);
    if((port < 5000) || (port > 5500)) {
      fail("GatewayReceiver started on out of range port");
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverDefaultStartPortAndEndPortProvided() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setEndPort(50707);
    fact.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      fail("The test failed with IOException");
    }
    int port = receiver.getPort();
    if((port < GatewayReceiver.DEFAULT_START_PORT) || (port > 50707)) {
      fail("GatewayReceiver started on out of range port");
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverWithManualStartFALSE() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setStartPort(5303);
    fact.setManualStart(false);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);
    GatewayReceiver receiver = fact.create();
    int port = receiver.getPort();
    if ((port < 5303) || (port > GatewayReceiver.DEFAULT_END_PORT)) {
      fail("GatewayReceiver started on out of range port");
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverWithStartPortAndDefaultEndPort() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setMaximumTimeBetweenPings(1000);
    fact.setSocketBufferSize(4000);
    fact.setStartPort(5303);
    fact.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    fact.addGatewayTransportFilter(myStreamfilter1);

    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      fail("The test failed with IOException");
    }
    int port = receiver.getPort();
    if ((port < 5303) || (port > GatewayReceiver.DEFAULT_END_PORT)) {
      fail("GatewayReceiver started on out of range port");
    }
  }
  
  @Test
  public void test_ValidateGatwayReceiverWithWrongEndPortProvided() {
    cache = new CacheFactory().set("mcast-port", "0").create();
    try {
      GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
      fact.setMaximumTimeBetweenPings(1000);
      fact.setSocketBufferSize(4000);
      fact.setEndPort(4999);
      GatewayReceiver receiver = fact.create();  
      fail("wrong end port set in the GatewayReceiver");
    } catch (IllegalStateException expected) {
      if(!expected.getMessage().contains("Please specify either start port a value which is less than end port.")){
        fail("Caught IllegalStateException");
        expected.printStackTrace();
      }
    }
  }
  
  @After
  public void tearDown() throws Exception {
    if (this.cache != null) {
      this.cache.close();
    }
  }
}
