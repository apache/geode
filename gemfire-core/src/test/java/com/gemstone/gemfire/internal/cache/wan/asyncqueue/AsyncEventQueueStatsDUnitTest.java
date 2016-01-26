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
package com.gemstone.gemfire.internal.cache.wan.asyncqueue;

import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.AsyncEventQueueTestBase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;

public class AsyncEventQueueStatsDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;
  
  public AsyncEventQueueStatsDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Normal replication scenario
   */
  public void testReplicatedSerialPropagation() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });

    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });// primary sender
    pause(2000);//give some time for system to become stable
    
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln", 0, 1000, 1000, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln", 10 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln", 0, 1000, 0, 0 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln", 0 });
  }
  
  /**
   * Two listeners added to the same RR.
   */
  public void testAsyncStatsTwoListeners() throws Exception {
    Integer lnPort = createFirstLocatorWithDSId(1);

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln1",
      false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln1",
      false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln1",
      false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln1",
      false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln2",
      false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln2",
      false, 100, 100, false, false, null, false });
    vm6.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln2",
      false, 100, 100, false, false, null, false });
    vm7.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln2",
      false, 100, 100, false, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln1,ln2", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln1,ln2", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln1,ln2", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln1,ln2", isOffHeap() });

    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { testName + "_RR",
        1000 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln1", 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln2", 1000 });
    pause(2000);//give some time for system to become stable

    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln1", 0, 1000, 1000, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln1", 10 });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln2", 0, 1000, 1000, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln2", 10 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln1", 0, 1000, 0, 0 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln1", 0 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln2", 0, 1000, 0, 0 });
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueBatchStats",
        new Object[] { "ln2", 0 });
  }
  
  /**
   * HA scenario: kill one vm when puts are in progress on the other vm.
   */
  public void testReplicatedSerialPropagationHA() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] {lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
      false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
      false, 100, 100, false, false, null, false });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    
    AsyncInvocation inv1 = vm5.invokeAsync(AsyncEventQueueTestBase.class, "doPuts",
        new Object[] { testName + "_RR", 10000 });
    pause(2000);
    AsyncInvocation inv2 = vm4.invokeAsync(AsyncEventQueueTestBase.class, "killAsyncEventQueue", new Object[] { "ln" });
    Boolean isKilled = Boolean.FALSE;
    try {
      isKilled = (Boolean)inv2.getResult();
    }
    catch (Throwable e) {
      fail("Unexpected exception while killing a AsyncEventQueue");
    }
    AsyncInvocation inv3 = null; 
    if(!isKilled){
      inv3 = vm5.invokeAsync(AsyncEventQueueTestBase.class, "killSender", new Object[] { "ln" });
      inv3.join();
    }
    inv1.join();
    inv2.join();
    pause(2000);//give some time for system to become stable
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats_Failover", new Object[] {"ln", 10000});
  }

  /**
   * Two regions attached to same AsyncEventQueue
   */
  public void testReplicatedSerialPropagationUNPorcessedEvents() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
      false, 100, 100, false, false, null, false });
    vm5.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
      false, 100, 100, false, false, null, false });

    //create one RR (RR_1) on local site
    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_1", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_1", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_1", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_1", "ln", isOffHeap() });

    //create another RR (RR_2) on local site
    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_2", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_2", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_2", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue", new Object[] {
        testName + "_RR_2", "ln", isOffHeap() });
    
    //start puts in RR_1 in another thread
    vm4.invoke(AsyncEventQueueTestBase.class, "doPuts", new Object[] { testName + "_RR_1", 1000 });
    //do puts in RR_2 in main thread
    vm4.invoke(AsyncEventQueueTestBase.class, "doPutsFrom", new Object[] { testName + "_RR_2", 1000, 1500 });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1500 });
        
    pause(2000);//give some time for system to become stable
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {"ln",
      0, 1500, 1500, 1500});
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueUnprocessedStats", new Object[] {"ln", 0});
    
    
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {"ln",
      0, 1500, 0, 0});
    vm5.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueUnprocessedStats", new Object[] {"ln", 1500});
  }
  
  /**
   * Test with conflation enabled
   */
  public void testSerialPropagationConflation() {
    Integer lnPort = (Integer)vm0.invoke(AsyncEventQueueTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    vm4.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(AsyncEventQueueTestBase.class, "createCache", new Object[] { lnPort });

    vm4.invoke(AsyncEventQueueTestBase.class, "createAsyncEventQueue", new Object[] { "ln",
        false, 100, 100, true, false, null, false });

    vm4.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm6.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(AsyncEventQueueTestBase.class, "createReplicatedRegionWithAsyncEventQueue",
        new Object[] { testName + "_RR", "ln", isOffHeap() });
    
    vm4
        .invoke(AsyncEventQueueTestBase.class, "pauseAsyncEventQueue",
            new Object[] { "ln" });
    //pause at least for the batchTimeInterval to make sure that the AsyncEventQueue is actually paused
    pause(2000);

    final Map keyValues = new HashMap();
    final Map updateKeyValues = new HashMap();
    for(int i=0; i< 1000; i++) {
      keyValues.put(i, i);
    }
    
    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR", keyValues });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] { "ln", keyValues.size() });
    
    for(int i=0;i<500;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    // Put the update events and check the queue size.
    // There should be no conflation with the previous create events.
    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR", updateKeyValues });    
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() });
    
    // Put the update events again and check the queue size.
    // There should be conflation with the previous update events.
    vm4.invoke(AsyncEventQueueTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR", updateKeyValues });    
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() });
    
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 0 });
  
    vm4.invoke(AsyncEventQueueTestBase.class, "resumeAsyncEventQueue", new Object[] { "ln" });
    vm4.invoke(AsyncEventQueueTestBase.class, "validateAsyncEventListener",
        new Object[] { "ln", 1000 });
    
    pause(2000);// give some time for system to become stable
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueStats", new Object[] {
        "ln", 0, 2000, 2000, 1000 });
    vm4.invoke(AsyncEventQueueTestBase.class, "checkAsyncEventQueueConflatedStats",
        new Object[] { "ln", 500 });
  }
}
