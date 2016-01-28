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
package com.gemstone.gemfire.internal.cache.wan.concurrent;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase.MyGatewayEventFilter;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

/**
 * All the test cases are similar to SerialWANPropogationDUnitTest except that
 * the we create concurrent serial GatewaySender with concurrency of 4
 * @author skumar
 *
 */
public class ConcurrentWANPropogation_2_DUnitTest extends WANTestBase {

  /**
   * @param name
   */
  public ConcurrentWANPropogation_2_DUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1L;
  
  public void testSerialReplicatedWanWithOverflow() {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver",
        new Object[] {nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver",
        new Object[] {nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    //keep the maxQueueMemory low enough to trigger eviction
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 50, 5, false, false, null, true, 5, OrderPolicy.KEY });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 50, 5, false, false, null, true, 5, OrderPolicy.KEY });

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

    vm4.invoke(WANTestBase.class, "doHeavyPuts", new Object[] {
        testName + "_RR", 150 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 150 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 150 });
  }

  public void Bug46921_testSerialReplicatedWanWithPersistence() {

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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, true, null, true, 5, OrderPolicy.THREAD });

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

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });

  }

  public void testReplicatedSerialPropagationToTwoWanSites() throws Exception {

    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "lnSerial1",
        2, false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "lnSerial1",
        2, false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "lnSerial2",
        3, false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "lnSerial2",
        3, false, 100, 10, false, false, null, true, 5 , OrderPolicy.THREAD});

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "lnSerial2" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "lnSerial1,lnSerial2", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
  }

  public void testReplicatedSerialPropagationHA() throws Exception {
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");

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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });

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

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_RR", 10000 });
    pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    
    inv1.join();
    inv2.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 10000 });
  }

  public void testReplicatedSerialPropagationWithConflation() throws Exception {

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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 1000, true, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 1000, true, false, null, true, 5, OrderPolicy.THREAD });

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

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
  }

  public void testReplicatedSerialPropagationWithParallelThreads()
      throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] {1});
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] {2,lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 4, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 4, OrderPolicy.THREAD });

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

    vm4.invoke(WANTestBase.class, "doMultiThreadedPuts", new Object[] {
        testName + "_RR", 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1000 });
  }

  public void testSerialPropogationWithFilter() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] {1});
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] {2,lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
  }

  public void testReplicatedSerialPropagationWithFilter() throws Exception {

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

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false,
        new MyGatewayEventFilter(), true, 5, OrderPolicy.THREAD });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, null, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, "ln", isOffHeap() });
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName, "ln", isOffHeap() });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
  }
  
  public void testNormalRegionSerialPropagation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });
    vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true, 5, OrderPolicy.THREAD });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", null, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    pause(500);
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "createNormalRegion", new Object[] {
        testName + "_RR", "ln" });
    vm5.invoke(WANTestBase.class, "createNormalRegion", new Object[] {
        testName + "_RR", "ln" });

    vm5.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(WANTestBase.class, "checkQueueStats", new Object[] { "ln", 0,
        0, 0, 0});

    vm5.invoke(WANTestBase.class, "checkQueueStats", new Object[] { "ln", 0,
        1000, 0, 0 });
    
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 1000});

    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 0});
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 0 });
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {0, 0, 0});
    
  }

}
