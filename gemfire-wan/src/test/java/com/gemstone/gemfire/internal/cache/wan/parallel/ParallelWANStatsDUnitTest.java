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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase.MyGatewayEventFilter;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

public class ParallelWANStatsDUnitTest extends WANTestBase{
  
  private static final long serialVersionUID = 1L;

  public ParallelWANStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testPartitionedRegionParallelPropagation_BeforeDispatch() throws Exception {
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

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    pause(3000);

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    pause(2000);
    
    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, keyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() });

    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 1000 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 1000 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 1000 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 1000 });
    
    assertEquals(1000, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(1000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(1000, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(0, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertEquals(0, v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4)); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
  }
  
  public void testPartitionedRegionParallelPropagation_AfterDispatch_NoRedundacny() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
   

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 0, 100, isOffHeap() });
   
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 0, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(1000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(1000, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(1000, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 100); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
  }
  
  public void testPartitionedRegionParallelPropagation_AfterDispatch_Redundancy_3() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 0, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(4000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(4000, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(1000, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 100); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
  }
  
  public void testWANStatsTwoWanSites_Bug44331() throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln1",
        2, true, 100, 10, false, false, null, true });
  
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln2",
        3, true, 100, 10, false, false, null, true });
  
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 0, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 0, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln1,ln2", 0, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
        1000 });

    pause(10000);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 1000 });
    
    ArrayList<Integer> v4Sender1List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln1", 0 });
    ArrayList<Integer> v4Sender2List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln2", 0 });
    
    assertEquals(0, v4Sender1List.get(0).intValue()); //queue size
    assertEquals(1000, v4Sender1List.get(1).intValue()); //eventsReceived
    assertEquals(1000, v4Sender1List.get(2).intValue()); //events queued
    assertEquals(1000, v4Sender1List.get(3).intValue()); //events distributed
    assertTrue(v4Sender1List.get(4).intValue()>=100); //batches distributed
    assertEquals(0, v4Sender1List.get(5).intValue()); //batches redistributed
    
    assertEquals(0, v4Sender2List.get(0).intValue()); //queue size
    assertEquals(1000, v4Sender2List.get(1).intValue()); //eventsReceived
    assertEquals(1000, v4Sender2List.get(2).intValue()); //events queued
    assertEquals(1000, v4Sender2List.get(3).intValue()); //events distributed
    assertTrue(v4Sender2List.get(4).intValue()>=100); //batches distributed
    assertEquals(0, v4Sender2List.get(5).intValue()); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
    vm3.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {100, 1000, 1000 });
  }
  
  public void testParallelPropagationHA() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", null, 0, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 3, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName + "_PR", 10000 });
    pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    inv1.join();
    inv2.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 10000 });
    
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    
    assertEquals(0, v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    int receivedEvents = v5List.get(1) + v6List.get(1) + v7List.get(1);
    //We may see a single retried event on all members due to the kill
    assertTrue("Received " + receivedEvents, 30000 <= receivedEvents && 30003 >= receivedEvents); //eventsReceived
    int queuedEvents = v5List.get(2) + v6List.get(2) + v7List.get(2);
    assertTrue("Queued " + queuedEvents, 30000 <= queuedEvents && 30003 >= queuedEvents); //eventsQueued
    //assertTrue(10000 <= v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed : its quite possible that vm4 has distributed some of the events
    //assertTrue(v5List.get(4) + v6List.get(4) + v7List.get(4) > 1000); //batches distributed : its quite possible that vm4 has distributed some of the batches.
    assertEquals(0, v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
  
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStatsHA", new Object[] {1000, 10000, 10000 });
  }
  
  /**
   * 1 region and sender configured on local site and 1 region and a 
   * receiver configured on remote site. Puts to the local region are in progress.
   * Remote region is destroyed in the middle.
   * 
   * @throws Exception
   */
  public void  testParallePropagationWithRemoteRegionDestroy() throws Exception {
    addExpectedException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    //these are part of remote site
    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    //these are part of local site
    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    //senders are created on local site
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
    true, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
    true, 100, 10, false, false, null, true });

  vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
    testName + "_PR", null, 0, 100, isOffHeap() });
  
  vm2.invoke(WANTestBase.class, "addCacheListenerAndDestroyRegion", new Object[] {
    testName + "_PR"});
  
  vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
    testName + "_PR", "ln", 0, 100, isOffHeap() });
  vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
    testName + "_PR", "ln", 0, 100, isOffHeap() });
  vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
    testName + "_PR", "ln", 0, 100, isOffHeap() });
  vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
    testName + "_PR", "ln", 0, 100, isOffHeap() });
  
  vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
  vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
  vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
  vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    //start puts in RR_1 in another thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 20000 });
    
    //verify that all is well in local site. All the events should be present in local region
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_PR", 20000 });
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", -1});
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", -1});
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", -1});
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", -1});
    
    
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 1); //batches distributed : its quite possible that vm4 has distributed some of the batches.
    assertTrue(v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5) >= 1); //batches redistributed
  }
  
  public void testParallelPropogationWithFilter() throws Exception {

    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class, "createFirstLocatorWithDSId",
        new Object[] {1});
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] {2,lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false,
        new MyGatewayEventFilter(), true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false,
      new MyGatewayEventFilter(), true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 10, false, false,
      new MyGatewayEventFilter(), true });
  
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 1000 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 800 });
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(1000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(900, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(800, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 80); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    assertEquals(200, v4List.get(6) + v5List.get(6) + v6List.get(6) + v7List.get(6)); //events filtered
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {80, 800, 800});
  }
  
  public void testParallelPropagationConflation() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, true, false, null, true });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 0, 100, isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    pause(3000);

    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    pause(2000);
    
    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, keyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() });
    for(int i=0;i<500;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() /*creates aren't conflated*/ });
    
    // Do the puts again. Since these are updates, the previous updates will be conflated.
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() /*creates aren't conflated*/ });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });

    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {0, 0, 0});
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {0, 1500, 1000});
    
    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", 0 });
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(2000, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(2000, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(1500, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 100); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    assertEquals(500, v4List.get(7) + v5List.get(7) + v6List.get(7) + v7List.get(7)); //events conflated 
    
  }
}
