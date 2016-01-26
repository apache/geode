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
package com.gemstone.gemfire.internal.cache.wan.serial;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.MyGatewaySenderEventListener;
import com.gemstone.gemfire.internal.cache.wan.MyGatewaySenderEventListener2;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

/**
 * @author skumar
 *
 */
public class SerialGatewaySenderEventListenerDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SerialGatewaySenderEventListenerDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test validates whether the listener attached receives all the events.
   * this test hangs after the Darrel's checkin 36685. Need to work with Darrel.Commenting it out so that test suit will not hang 
   */
  public void __testGatewaySenderEventListenerInvocationWithoutLocator() {
    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm5.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm6.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm7.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });

    vm4.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, true});
    vm5.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, true});

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    final Map keyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", keyValues.size() });
    
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", keyValues.size() });
    
    vm4.invoke(WANTestBase.class, "printEventListenerMap", new Object[]{});
    vm5.invoke(WANTestBase.class, "printEventListenerMap", new Object[]{});
    
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener1", new Object[] {"ln", keyValues });
  }
  
  /**
   * Test validates whether the listener attached receives all the events.
   */
  public void testGatewaySenderEventListenerInvocation() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, true});
    vm5.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, true});

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    final Map keyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", keyValues.size() });
    
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 0 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 0 });
    
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener1", new Object[] {"ln", keyValues });
  }

  /**
   * Test validates whether the listener attached receives all the events. 
   * When there are 2 listeners attcahed to the GatewaySender.
   */
  public void testGatewaySender2EventListenerInvocation() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, true});
    vm5.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, true});

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    final Map keyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }

    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 0 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 0 });
    
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener2", new Object[] {"ln", keyValues });
  }
  
  /**
   * Test vaildates whether the PoolImpl is created. Ideally when a listener is attached
   * pool should not be created.
   */
  public void testGatewaySenderEventListenerPoolImpl() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] {1});
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] {2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver",
        new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver",
        new Object[] {nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, false });
    
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateNoPoolCreation", new Object[] {"ln" });
  }
  
  // Test start/stop/resume on listener invocation
  //this test hangs after the Darrel's checkin 36685. Need to work with Darrel.Commenting it out so that test suit will not hang
  public void __testGatewaySenderEventListener_GatewayOperations() {

    int mPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    vm4.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm5.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm6.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });
    vm7.invoke(WANTestBase.class, "createCacheWithoutLocator", new Object[] {mPort });

    vm4.invoke(WANTestBase.class, "createSenderWithListener", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, false, true});

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });

    final Map initialKeyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      initialKeyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      initialKeyValues });

    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener1", new Object[] {"ln", initialKeyValues });
    
    vm4.invoke(WANTestBase.class, "stopSender", new Object[] { "ln" });
    
    final Map keyValues = new HashMap();
    for(int i=1000; i< 2000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener1", new Object[] {"ln", initialKeyValues });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    final Map finalKeyValues = new HashMap();
    for(int i=2000; i< 3000; i++) {
      finalKeyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      finalKeyValues });
    
    finalKeyValues.putAll(initialKeyValues);
    vm4.invoke(SerialGatewaySenderEventListenerDUnitTest.class, "validateReceivedEventsMapSizeListener1", new Object[] {"ln", finalKeyValues });
    
  }
    
  public static void validateNoPoolCreation(final String siteId) {
    Set<GatewaySender> senders = cache.getGatewaySenders();
    for(GatewaySender sender: senders) {
      if (sender.getId().equals(siteId)) {
        AbstractGatewaySender sImpl = (AbstractGatewaySender)sender;
        assertNull(sImpl.getProxy());
      }
    }
  }
  
  public static void validateReceivedEventsMapSizeListener1(final String senderId, final HashMap map) {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    
    final List<AsyncEventListener> listeners = ((AbstractGatewaySender)sender).getAsyncEventListeners();
    if(listeners.size() == 1) {
      final AsyncEventListener l = listeners.get(0);

      WaitCriterion wc = new WaitCriterion() {
        Map listenerMap;
        public boolean done() {
          listenerMap = ((MyGatewaySenderEventListener)l)
              .getEventsMap();
          boolean sizeCorrect = map.size() == listenerMap.size();
          boolean keySetCorrect = listenerMap.keySet().containsAll(map.keySet());
          boolean valuesCorrect = listenerMap.values().containsAll(map.values());
          return sizeCorrect && keySetCorrect && valuesCorrect;
        }

        public String description() {
          return "Waiting for all sites to get updated, the sizes are " + listenerMap.size() + " and " + map.size();
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60000, 500, true); 
    }
  }
  
  public static void validateReceivedEventsMapSizeListener2(final String senderId, final HashMap map) {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    GatewaySender sender = null;
    for(GatewaySender s : senders){
      if(s.getId().equals(senderId)){
        sender = s;
        break;
      }
    }
    
    final List<AsyncEventListener> listeners = ((AbstractGatewaySender)sender).getAsyncEventListeners();
    if(listeners.size() == 2) {
      final AsyncEventListener l1 = listeners.get(0);
      final AsyncEventListener l2 = listeners.get(1);

      WaitCriterion wc = new WaitCriterion() {
        Map listenerMap1, listenerMap2;
        public boolean done() {
          listenerMap1 = ((MyGatewaySenderEventListener)l1)
              .getEventsMap();
          
          listenerMap2 = ((MyGatewaySenderEventListener2)l2)
          .getEventsMap();
          
          boolean sizeCorrect = map.size() == listenerMap1.size();
          boolean keySetCorrect = listenerMap1.keySet().containsAll(map.keySet());
          boolean valuesCorrect = listenerMap1.values().containsAll(map.values());
          
          boolean sizeCorrect2 = map.size() == listenerMap2.size();
          boolean keySetCorrect2 = listenerMap2.keySet().containsAll(map.keySet());
          boolean valuesCorrect2 = listenerMap2.values().containsAll(map.values());
          
          
          return sizeCorrect && keySetCorrect && valuesCorrect && sizeCorrect2 && keySetCorrect2 && valuesCorrect2;
        }

        public String description() {
          return "Waiting for all sites to get updated, the sizes are "
              + listenerMap1.size() + " and listenerMap2 "
              + listenerMap2.size() + " expected map size " + map.size();
        }
      };
      DistributedTestCase.waitForCriterion(wc, 60000, 500, true); 
    }
  }
}

