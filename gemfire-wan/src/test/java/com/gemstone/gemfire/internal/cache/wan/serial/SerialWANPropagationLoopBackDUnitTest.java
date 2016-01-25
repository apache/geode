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

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;


public class SerialWANPropagationLoopBackDUnitTest extends WANTestBase {
  
  private static final long serialVersionUID = 1L;
  
  public SerialWANPropagationLoopBackDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testReplicatedSerialPropagationLoopBack() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm5.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });

    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 1,
        false, 100, 10, false, false, null, true });

    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny", isOffHeap() });

    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    
    vm4.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln",
        false });
    vm6.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ny",
        false });
    
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()});
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny", isOffHeap()});

    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm6.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 2 });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 2 });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 2 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 2 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 2 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_RR", 2 });
  
  
    pause(5000);
    vm4.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln", 0 });
    vm6.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny", 0 });
    
    Map queueMap1 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap2 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});

    List createList1 = (List)queueMap1.get("Create");
    List updateList1 = (List)queueMap1.get("Update");
    List createList2 = (List)queueMap2.get("Create");
    List updateList2 = (List)queueMap2.get("Update");
    
    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    
    assertEquals(1, createList1.size());
    assertEquals(1, createList2.size());
  }
  
  
  public void testReplicatedSerialPropagationLoopBack3SitesLoop() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    // using vm5 for sender in ds 3. cache is already created.
    
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        false, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny", 3,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk", 1,
      false, 100, 10, false, false, null, true });

    
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap()});
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny", isOffHeap()});
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "tk", isOffHeap()});

    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk" });
    
    vm6.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln",
      false });
    vm7.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ny",
      false });
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "tk",
      false });
  
    
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny", isOffHeap() });


    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
        
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    
    
    pause(5000);
    vm6.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln", 0 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "tk", 0 });
    
    
    
    Map queueMap1 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap2 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap3 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    
    List createList1 = (List)queueMap1.get("Create");
    List updateList1 = (List)queueMap1.get("Update");
    List createList2 = (List)queueMap2.get("Create");
    List updateList2 = (List)queueMap2.get("Update");
    List createList3 = (List)queueMap3.get("Create");
    List updateList3 = (List)queueMap3.get("Update");
    
    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());
    
    assertEquals(2, createList1.size());
    assertEquals(2, createList2.size());
    assertEquals(2, createList3.size());
  }
  
  
  public void testReplicatedSerialPropagationLoopBack3SitesNtoNPutInAllDS() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    vm6.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });
    vm7.invoke(WANTestBase.class, "createCache", new Object[] { nyPort });
    // using vm5 for sender in ds 3. cache is already created.
    
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
        false, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 3,
        false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      false, 100, 10, false, false, null, true });

    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      false, 100, 10, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 1,
      false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
    false, 100, 10, false, false, null, true });

    
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln1,ln2", isOffHeap()});
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny1,ny2", isOffHeap()});
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "tk1,tk2", isOffHeap() });

    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    
    vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });
    
    
    vm6.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln1",
      false });
    vm7.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ny1",
      false });
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "tk1",
      false });
    vm6.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "ln2",
      false });
    vm7.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "ny2",
      false });
    vm5.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "tk2",
      false });
  
    
    vm6.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln1,ln2", isOffHeap() });
    vm7.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny1,ny2", isOffHeap() });
    
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    keyValues.clear();
    for(int i=1; i< 2; i++) {
      keyValues.put(i, i);
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    keyValues.clear();
    for(int i=2; i< 3; i++) {
      
      keyValues.put(i, i);
    }
    vm5.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    pause(2000);
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 3 });
    
    pause(5000);
    vm6.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln1", 0 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny1", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "tk1", 0 });
    vm6.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln2", 0 });
    vm7.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny2", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "tk2", 0 });
    
    
    Map queueMap1 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap2 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap3 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap4 = (HashMap)vm6.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    Map queueMap5 = (HashMap)vm7.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    Map queueMap6 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    
    List createList1 = (List)queueMap1.get("Create");
    List updateList1 = (List)queueMap1.get("Update");
    List createList2 = (List)queueMap2.get("Create");
    List updateList2 = (List)queueMap2.get("Update");
    List createList3 = (List)queueMap3.get("Create");
    List updateList3 = (List)queueMap3.get("Update");
    
    List createList4 = (List)queueMap4.get("Create");
    List updateList4 = (List)queueMap4.get("Update");
    
    List createList5 = (List)queueMap5.get("Create");
    List updateList5 = (List)queueMap5.get("Update");
    
    List createList6 = (List)queueMap6.get("Create");
    List updateList6 = (List)queueMap6.get("Update");
    
    
    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());
    assertEquals(0, updateList4.size());
    assertEquals(0, updateList5.size());
    assertEquals(0, updateList6.size());
    
    assertEquals(1, createList1.size());
    assertEquals(1, createList2.size());
    assertEquals(1, createList3.size());
    assertEquals(1, createList4.size());
    assertEquals(1, createList5.size());
    assertEquals(1, createList6.size());
  }

  
  public void testReplicatedSerialPropagationLoopBack3SitesNtoNPutFromOneDS() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });

    
    vm3.invoke(WANTestBase.class, "createReceiver", new Object[] { lnPort });
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln1", 2,
        false, 100, 10, false, false, null, true });
    vm3.invoke(WANTestBase.class, "createSender", new Object[] { "ln2", 3,
      false, 100, 10, false, false, null, true });
    
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny1", 3,
        false, 100, 10, false, false, null, true });
    vm4.invoke(WANTestBase.class, "createSender", new Object[] { "ny2", 1,
      false, 100, 10, false, false, null, true });
    
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk1", 1,
      false, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "tk2", 2,
    false, 100, 10, false, false, null, true });

    
    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ln1,ln2", isOffHeap() });
    vm4.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "ny1,ny2", isOffHeap() });
    vm5.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        testName + "_RR", "tk1,tk2", isOffHeap() });

    
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny1" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk1" });
    
    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ny2" });
    vm5.invoke(WANTestBase.class, "startSender", new Object[] { "tk2" });
    
    
    vm3.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ln1",
      false });
    vm4.invoke(WANTestBase.class, "addQueueListener", new Object[] { "ny1",
      false });
    vm5.invoke(WANTestBase.class, "addQueueListener", new Object[] { "tk1",
      false });
    vm3.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "ln2",
      false });
    vm4.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "ny2",
      false });
    vm5.invoke(WANTestBase.class, "addSecondQueueListener", new Object[] { "tk2",
      false });
  
    final Map keyValues = new HashMap();
    for(int i=0; i< 1; i++) {
      keyValues.put(i, i);
    }
    vm3.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName + "_RR",
      keyValues });
    
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1 });
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1 });
    vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName + "_RR", 1 });
    
    pause(5000);
    vm3.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln1", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny1", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "tk1", 0 });
    vm3.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ln2", 0 });
    vm4.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "ny2", 0 });
    vm5.invoke(WANTestBase.class, "verifyQueueSize",
        new Object[] { "tk2", 0 });
    
    
    Map queueMap1 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap2 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap3 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue",
        new Object[] {});
    Map queueMap4 = (HashMap)vm3.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    Map queueMap5 = (HashMap)vm4.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    Map queueMap6 = (HashMap)vm5.invoke(WANTestBase.class, "checkQueue2",
        new Object[] {});
    
    List createList1 = (List)queueMap1.get("Create");
    List updateList1 = (List)queueMap1.get("Update");
    List createList2 = (List)queueMap2.get("Create");
    List updateList2 = (List)queueMap2.get("Update");
    List createList3 = (List)queueMap3.get("Create");
    List updateList3 = (List)queueMap3.get("Update");
    
    List createList4 = (List)queueMap4.get("Create");
    List updateList4 = (List)queueMap4.get("Update");
    
    List createList5 = (List)queueMap5.get("Create");
    List updateList5 = (List)queueMap5.get("Update");
    
    List createList6 = (List)queueMap6.get("Create");
    List updateList6 = (List)queueMap6.get("Update");
    
    
    assertEquals(0, updateList1.size());
    assertEquals(0, updateList2.size());
    assertEquals(0, updateList3.size());
    assertEquals(0, updateList4.size());
    assertEquals(0, updateList5.size());
    assertEquals(0, updateList6.size());
    
    assertEquals(1, createList1.size());
    assertEquals(0, createList2.size());
    assertEquals(0, createList3.size());
    assertEquals(1, createList4.size());
    assertEquals(0, createList5.size());
    assertEquals(0, createList6.size());
  }

}
