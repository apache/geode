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

import static com.gemstone.gemfire.test.dunit.Wait.*;
import static com.gemstone.gemfire.test.dunit.IgnoredException.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.VM;

public class ParallelWANStatsDUnitTest extends WANTestBase{
  
  private static final int NUM_PUTS = 100;
  private static final long serialVersionUID = 1L;
  
  private String testName;
  
  public ParallelWANStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    this.testName = getTestMethodName();
  }
  
  public void testPartitionedRegionParallelPropagation_BeforeDispatch() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createReceiver(vm2, nyPort);
    createReceiver(vm3, nyPort);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();
    
    createReceiverPR(vm2, 1);
    createReceiverPR(vm3, 1);

    putKeyValues();

    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", NUM_PUTS });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", NUM_PUTS });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", NUM_PUTS });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", NUM_PUTS });
    
    assertEquals(NUM_PUTS, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(NUM_PUTS, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(NUM_PUTS, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(0, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertEquals(0, v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4)); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
  }

  public void testPartitionedRegionParallelPropagation_AfterDispatch_NoRedundacny() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createReceiver(vm2, nyPort);
   

    createSenders(lnPort);

    createReceiverPR(vm2, 0);
   
    createSenderPRs(0);
    
    startSenders();

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName,
        NUM_PUTS });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, NUM_PUTS });
    
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(NUM_PUTS, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(NUM_PUTS, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {10, NUM_PUTS, NUM_PUTS });
  }
  
  public void testPartitionedRegionParallelPropagation_AfterDispatch_Redundancy_3() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createReceiver(vm2, nyPort);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    createSenderPRs(3);
    
    startSenders();

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName,
        NUM_PUTS });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, NUM_PUTS });
    
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    assertEquals(0, v4List.get(0) + v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    assertEquals(400, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(400, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(NUM_PUTS, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {10, NUM_PUTS, NUM_PUTS });
  }
  
  public void testWANStatsTwoWanSites_Bug44331() throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);
    Integer nyPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    createReceiver(vm2, nyPort);
    createReceiver(vm3, tkPort);

    vm4.invoke(WANTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln1",
        2, true, 100, 10, false, false, null, true });
  
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln2",
        3, true, 100, 10, false, false, null, true });
  
    createReceiverPR(vm2, 0);
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName, null, 0, 10, isOffHeap() });

    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName, "ln1,ln2", 0, 10, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });

    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName,
        NUM_PUTS });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, NUM_PUTS });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, NUM_PUTS });
    
    ArrayList<Integer> v4Sender1List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln1", 0 });
    ArrayList<Integer> v4Sender2List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln2", 0 });
    
    assertEquals(0, v4Sender1List.get(0).intValue()); //queue size
    assertEquals(NUM_PUTS, v4Sender1List.get(1).intValue()); //eventsReceived
    assertEquals(NUM_PUTS, v4Sender1List.get(2).intValue()); //events queued
    assertEquals(NUM_PUTS, v4Sender1List.get(3).intValue()); //events distributed
    assertTrue(v4Sender1List.get(4).intValue()>=10); //batches distributed
    assertEquals(0, v4Sender1List.get(5).intValue()); //batches redistributed
    
    assertEquals(0, v4Sender2List.get(0).intValue()); //queue size
    assertEquals(NUM_PUTS, v4Sender2List.get(1).intValue()); //eventsReceived
    assertEquals(NUM_PUTS, v4Sender2List.get(2).intValue()); //events queued
    assertEquals(NUM_PUTS, v4Sender2List.get(3).intValue()); //events distributed
    assertTrue(v4Sender2List.get(4).intValue()>=10); //batches distributed
    assertEquals(0, v4Sender2List.get(5).intValue()); //batches redistributed
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {10, NUM_PUTS, NUM_PUTS });
    vm3.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {10, NUM_PUTS, NUM_PUTS });
  }
  
  public void testParallelPropagationHA() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createReceiver(vm2, nyPort);
    

    createSenders(lnPort);
    
    createReceiverPR(vm2, 0);
    
    createSenderPRs(3);
    
    startSenders();

    AsyncInvocation inv1 = vm5.invokeAsync(WANTestBase.class, "doPuts",
        new Object[] { testName, 1000 });
    pause(200);
    AsyncInvocation inv2 = vm4.invokeAsync(WANTestBase.class, "killSender");
    inv1.join();
    inv2.join();
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 1000 });
    
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0});
    
    assertEquals(0, v5List.get(0) + v6List.get(0) + v7List.get(0) ); //queue size
    int receivedEvents = v5List.get(1) + v6List.get(1) + v7List.get(1);
    //We may see a single retried event on all members due to the kill
    assertTrue("Received " + receivedEvents, 3000 <= receivedEvents && 3003 >= receivedEvents); //eventsReceived
    int queuedEvents = v5List.get(2) + v6List.get(2) + v7List.get(2);
    assertTrue("Queued " + queuedEvents, 3000 <= queuedEvents && 3003 >= queuedEvents); //eventsQueued
    //assertTrue(10000 <= v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed : its quite possible that vm4 has distributed some of the events
    //assertTrue(v5List.get(4) + v6List.get(4) + v7List.get(4) > 1000); //batches distributed : its quite possible that vm4 has distributed some of the batches.
    assertEquals(0, v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
  
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStatsHA", new Object[] {NUM_PUTS, 1000, 1000 });
  }

  /**
   * 1 region and sender configured on local site and 1 region and a 
   * receiver configured on remote site. Puts to the local region are in progress.
   * Remote region is destroyed in the middle.
   * 
   * @throws Exception
   */
  public void  testParallePropagationWithRemoteRegionDestroy() throws Exception {
    addIgnoredException("RegionDestroyedException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    createReceiver(vm2, nyPort);

    createSenders(lnPort);

    createReceiverPR(vm2, 0);

    vm2.invoke(WANTestBase.class, "addCacheListenerAndDestroyRegion", new Object[] {
        testName});

    createSenderPRs(0);

    startSenders();

    //start puts in RR_1 in another thread
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName, 2000 });
    
    //verify that all is well in local site. All the events should be present in local region
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 2000 });
    
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

    createReceiver(vm2, nyPort);

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
  
    createSenderPRs(0);

    startSenders();

    createReceiverPR(vm2, 1);
    
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

    createReceiver(vm2, nyPort);

    createSendersWithConflation(lnPort);

    createSenderPRs(0);

    startPausedSenders();

    createReceiverPR(vm2, 1);

    Map keyValues = putKeyValues();
    final Map updateKeyValues = new HashMap();
    for(int i=0;i<50;i++) {
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
    
    vm2.invoke(WANTestBase.class, "checkGatewayReceiverStats", new Object[] {0, 150, NUM_PUTS});
    
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
    assertEquals(200, v4List.get(1) + v5List.get(1) + v6List.get(1) + v7List.get(1)); //eventsReceived
    assertEquals(200, v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)); //events queued
    assertEquals(150, v4List.get(3) + v5List.get(3) + v6List.get(3) + v7List.get(3)); //events distributed
    assertTrue(v4List.get(4) + v5List.get(4) + v6List.get(4) + v7List.get(4) >= 10); //batches distributed
    assertEquals(0, v4List.get(5) + v5List.get(5) + v6List.get(5) + v7List.get(5)); //batches redistributed
    assertEquals(50, v4List.get(7) + v5List.get(7) + v6List.get(7) + v7List.get(7)); //events conflated 
    
  }
  
  protected Map putKeyValues() {
    final Map keyValues = new HashMap();
    for(int i=0; i< NUM_PUTS; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, keyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() });
    
    return keyValues;
  }

  protected void createReceiverPR(VM vm, int redundancy) {
    vm.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, redundancy, 10, isOffHeap() });
  }
  
  protected void createSenderPRs(int redundancy) {
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", redundancy, 10, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", redundancy, 10, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", redundancy, 10, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", redundancy, 10, isOffHeap() });
  }

  protected void startPausedSenders() {
    startSenders();
    
    vm4.invoke(() ->pauseSender( "ln" ));
    vm5.invoke(() ->pauseSender( "ln" ));
    vm6.invoke(() ->pauseSender( "ln" ));
    vm7.invoke(() ->pauseSender( "ln" ));
  }

  protected void createReceiver(VM vm, Integer nyPort) {
    vm.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
  }
  
  protected void startSenders() {
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
  }

  protected void createSendersWithConflation(Integer lnPort) {
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
  }

  protected void createSenders(Integer lnPort) {
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
  }
}
