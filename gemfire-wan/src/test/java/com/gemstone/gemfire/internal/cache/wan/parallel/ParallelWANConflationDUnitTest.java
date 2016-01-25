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
import java.util.Random;

import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

/**
 * @author skumar
 * 
 */
public class ParallelWANConflationDUnitTest extends WANTestBase {
  private static final long serialVersionUID = 1L;

  public ParallelWANConflationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("java.net.ConnectException");
  }

  public void testParallelPropagationConflationDisabled() throws Exception {
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
        true, 100, 10, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true  });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true  });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
        true, 100, 10, false, false, null, true  });

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
    for(int i=0;i<500;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", (keyValues.size() + updateKeyValues.size()) });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    
  }

  /**
   * This test is desabled as it is not guaranteed to pass it everytime. This
   * test is related to the conflation in batch. yet did find any way to
   * ascertain that the vents in the batch will always be conflated.
   * 
   * @throws Exception
   */
  public void testParallelPropagationBatchConflation() throws Exception {
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
        true, 100, 50, false, false, null, true });
    vm5.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 50, false, false, null, true });
    vm6.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
      true, 100, 50, false, false, null, true });
    vm7.invoke(WANTestBase.class, "createSender", new Object[] { "ln", 2,
    true, 100, 50, false, false, null, true });
  
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
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, null, 1, 100, isOffHeap() });

    final Map keyValues = new HashMap();
    
    for (int i = 1; i <= 100; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i) ;
      }
      vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] {
        testName, keyValues });
    }
    
    vm4.invoke(WANTestBase.class, "enableConflation", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "enableConflation", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "enableConflation", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "enableConflation", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    pause(2000);
    ArrayList<Integer> v4List = (ArrayList<Integer>)vm4.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v5List = (ArrayList<Integer>)vm5.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v6List = (ArrayList<Integer>)vm6.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    ArrayList<Integer> v7List = (ArrayList<Integer>)vm7.invoke(
        WANTestBase.class, "getSenderStats", new Object[] { "ln", 0 });
    
    getLogWriter().info("KBKBKB: batch conflated events : vm4 : " + v4List.get(8));
    getLogWriter().info("KBKBKB: batch conflated events : vm5 : " + v5List.get(8));
    getLogWriter().info("KBKBKB: batch conflated events : vm6 : " + v6List.get(8));
    getLogWriter().info("KBKBKB: batch conflated events : vm7 : " + v7List.get(8));
    getLogWriter().info("KBKBKB: batch conflated events : " + (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)));
    assertTrue("No events conflated in batch", (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)) > 0);
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, 10 });

  }
  
  public void testParallelPropagationConflation() throws Exception {
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
    for(int i=0;i<500;i++) {
      updateKeyValues.put(i, i+"_updated");
    }
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() }); // creates aren't conflated
    
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() }); // creates aren't conflated

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
  }
  

  /**
   * Reproduce the bug #47213.
   * The test is same as above test, with the only difference that 
   * redundancy is set to 1.
   * @throws Exception
   */
  public void testParallelPropagationConflation_Bug47213() throws Exception {
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
        testName, "ln", 2, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 2, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 2, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName, "ln", 2, 100, isOffHeap() });

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

    pause(2000);//give some time for all the senders to pause
    
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

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() }); // creates aren't conflated

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, keyValues.size() });
    
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        testName, keyValues });
  }
  
  public void testParallelPropagationConflationOfRandomKeys() throws Exception {
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
    
    while(updateKeyValues.size()!=500) {
      int key = (new Random()).nextInt(keyValues.size());
      updateKeyValues.put(key, key+"_updated");
    }
    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() });

    vm4.invoke(WANTestBase.class, "putGivenKeyValue", new Object[] { testName, updateKeyValues });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", keyValues.size() + updateKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    
    keyValues.putAll(updateKeyValues);
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        testName, keyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName, keyValues.size() });
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
      testName, keyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
      testName, keyValues });
    
  }
  
  public void testParallelPropagationColocatedRegionConflation()
      throws Exception {
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

    vm4.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
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

    vm2.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, null, 1, 100, isOffHeap() });

    pause(2000);

    Map custKeyValues = (Map)vm4.invoke(WANTestBase.class, "putCustomerPartitionedRegion",
        new Object[] { 1000 });
    Map orderKeyValues = (Map)vm4.invoke(WANTestBase.class, "putOrderPartitionedRegion",
        new Object[] { 1000 });
    Map shipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "putShipmentPartitionedRegion",
        new Object[] { 1000 });

    vm4.invoke(
        WANTestBase.class,
        "checkQueueSize",
        new Object[] {
            "ln",
            (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
                .size()) });

    Map updatedCustKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateCustomerPartitionedRegion",
        new Object[] { 500 });
    Map updatedOrderKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateOrderPartitionedRegion",
        new Object[] { 500 });
    Map updatedShipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateShipmentPartitionedRegion",
        new Object[] { 500 });

    vm4.invoke(
        WANTestBase.class,
        "checkQueueSize",
        new Object[] {
            "ln",
            (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
                .size())
                + updatedCustKeyValues.size()
                + updatedOrderKeyValues.size()
                + updatedShipmentKeyValues.size() });

    updatedCustKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateCustomerPartitionedRegion",
        new Object[] { 500 });
    updatedOrderKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateOrderPartitionedRegion",
        new Object[] { 500 });
    updatedShipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateShipmentPartitionedRegion",
        new Object[] { 500 });

    vm4.invoke(
        WANTestBase.class,
        "checkQueueSize",
        new Object[] {
            "ln",
            (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
                .size())
                + updatedCustKeyValues.size()
                + updatedOrderKeyValues.size()
                + updatedShipmentKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, custKeyValues.size() });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues.size() });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.customerRegionName, custKeyValues });
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues });
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues });
    
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, custKeyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() });

    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.customerRegionName, custKeyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues });
    
  }
  
  public void testParallelPropagationColoatedRegionConflationSameKey()
      throws Exception {
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

    vm4.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm5.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm6.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, "ln", 0, 100, isOffHeap() });
    vm7.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
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

    vm2.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, null, 1, 100, isOffHeap() });
    vm3.invoke(WANTestBase.class,
        "createCustomerOrderShipmentPartitionedRegion", new Object[] {
            testName, null, 1, 100, isOffHeap() });

    pause(2000);

    Map custKeyValues = (Map)vm4.invoke(WANTestBase.class, "putCustomerPartitionedRegion",
        new Object[] { 1000 });
    Map orderKeyValues = (Map)vm4.invoke(WANTestBase.class, "putOrderPartitionedRegionUsingCustId",
        new Object[] { 1000 });
    Map shipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "putShipmentPartitionedRegionUsingCustId",
        new Object[] { 1000 });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
      .size()) });

    Map updatedCustKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateCustomerPartitionedRegion",
        new Object[] { 500 });
    Map updatedOrderKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateOrderPartitionedRegionUsingCustId",
        new Object[] { 500 });
    Map updatedShipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateShipmentPartitionedRegionUsingCustId",
        new Object[] { 500 });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size()) + updatedCustKeyValues.size() + updatedOrderKeyValues.size() + updatedShipmentKeyValues.size() });

    updatedCustKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateCustomerPartitionedRegion",
        new Object[] { 500 });
    updatedOrderKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateOrderPartitionedRegionUsingCustId",
        new Object[] { 500 });
    updatedShipmentKeyValues = (Map)vm4.invoke(WANTestBase.class, "updateShipmentPartitionedRegionUsingCustId",
        new Object[] { 500 });

    vm4.invoke(WANTestBase.class, "checkQueueSize", new Object[] { "ln", (custKeyValues.size() + orderKeyValues.size() + shipmentKeyValues
        .size()) + updatedCustKeyValues.size() + updatedOrderKeyValues.size() + updatedShipmentKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, 0 });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, 0 });

    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    vm7.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

    custKeyValues.putAll(updatedCustKeyValues);
    orderKeyValues.putAll(updatedOrderKeyValues);
    shipmentKeyValues.putAll(updatedShipmentKeyValues);
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, custKeyValues.size() });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues.size() });
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() });

    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.customerRegionName, custKeyValues });
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues });
    vm2.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues });

    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.customerRegionName, custKeyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues.size() });
    vm3.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues.size() });

    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.customerRegionName, custKeyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.orderRegionName, orderKeyValues });
    vm3.invoke(WANTestBase.class, "validateRegionContents", new Object[] {
        WANTestBase.shipmentRegionName, shipmentKeyValues });
  }
  
}
