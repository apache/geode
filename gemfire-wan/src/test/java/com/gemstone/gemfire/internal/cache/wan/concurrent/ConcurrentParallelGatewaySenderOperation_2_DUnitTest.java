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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
/**
 * @author skumar
 *
 */
public class ConcurrentParallelGatewaySenderOperation_2_DUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;
  
  public ConcurrentParallelGatewaySenderOperation_2_DUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    addExpectedException("RegionDestroyedException");
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");
  }
  
  // to test that when userPR is locally destroyed, shadow Pr is also locally
  // destroyed and on recreation usrePr , shadow Pr is also recreated.
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    
    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since sender is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      // since shodowPR is locally destroyed, so no data to dispatch
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 4, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "destroyRegion", new Object[] { testName
          + "_PR" });

      // before destoy, there is wait for queue to drain, so data will be
      // dispatched
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
        "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  
  public void testParallelGatewaySender_SingleNode_UserPR_Close_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class, "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME", new Object[] { lnPort });
    
    getLogWriter().info("Created cache on local site");
    
    getLogWriter().info("Created senders on local site");
    
    vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] { "ln", 2,
      true, 100, 10, false, false, null, false, 7, OrderPolicy.KEY });
  
    vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 1, 10, isOffHeap()});
    
    getLogWriter().info("Created PRs on local site");
    
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        testName + "_PR", null, 1, 10, isOffHeap() });
   
    vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR", 10 });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10 });
    
    //since resume is paused, no dispatching
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 0 });
    
    vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class, "closeRegion", new Object[] { testName + "_PR" });
    
    vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
    
    pause(500); //paused if there is any element which is received on remote site
    
    //before close, there is wait for queue to drain, so data will be dispatched
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 0 });
    
    vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
      testName + "_PR", "ln", 1, 10, isOffHeap() });
    
    vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] { testName + "_PR", 10, 20 });
    
    vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10 });
    
    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
      testName + "_PR", 10 });
    
    vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
        "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
  }
  
  //to test that while localDestroy is in progress, put operation does not successed
  public void testParallelGatewaySender_SingleNode_UserPR_localDestroy_SimultenuousPut_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      getLogWriter().info("Created senders on local site");

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 100, 2000 });
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(WANTestBase.class,
          "localDestroyRegion", new Object[] { testName + "_PR" });
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      if (localDestroyAsync.getException() != null) {
        fail("Not Expected Exception got", putAsync.getException());
      }

      if (putAsync.getException() != null
          && !(putAsync.getException() instanceof RegionDestroyedException)) {
        fail("Expected RegionDestroyedException but got",
            putAsync.getException());
      }

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      pause(500); // paused if there is any element which is received on remote
                  // site

      // since shodowPR is locally destroyed, so no data to dispatch
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_SimultenuousPut_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 6, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });
      vm4.invoke(WANTestBase.class, "addCacheListenerAndDestroyRegion", new Object[] {
        testName + "_PR"});
      
      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 10, 101 });
      try {
        putAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      if (putAsync.getException() != null
          && !(putAsync.getException() instanceof RegionDestroyedException)) {
        fail("Expected RegionDestroyedException but got",
            putAsync.getException());
      }

      // before destroy, there is wait for queue to drain, so data will be
      // dispatched
      vm2.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "validateRegionSizeWithinRange", new Object[] { testName + "_PR", 10,
              101 }); // possible size is more than 10

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "validateRegionSizeWithinRange", new Object[] { testName + "_PR", 20,
              101 });// possible size is more than 20
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
    
  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Destroy_NodeDown()
      throws Exception {
    addExpectedException("Broken pipe");
    addExpectedException("Connection reset");
    addExpectedException("Unexpected IOException");
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm6.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });
      vm6.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });
      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });
      vm6.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      pause(200);
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(WANTestBase.class,
          "destroyRegion", new Object[] { testName + "_PR" });

      AsyncInvocation closeAsync = vm4.invokeAsync(WANTestBase.class,
          "closeCache");
      try {
        localDestroyAsync.join();
        closeAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10000 });// possible size is more than 20
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm6.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }

  }
  
  public void testParallelGatewaySender_SingleNode_UserPR_Close_SimultenuousPut_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      getLogWriter().info("Created senders on local site");

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, false, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "pauseSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      // since resume is paused, no dispatching
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      AsyncInvocation putAsync = vm4.invokeAsync(WANTestBase.class,
          "doPutsFrom", new Object[] { testName + "_PR", 10, 2000 });
      AsyncInvocation localDestroyAsync = vm4.invokeAsync(
          ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "closeRegion", new Object[] { testName + "_PR" });
      try {
        putAsync.join();
        localDestroyAsync.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 0 });

      vm4.invoke(WANTestBase.class, "resumeSender", new Object[] { "ln" });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 10, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySenders_SingleNode_UserPR_localDestroy_RecreateRegion() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    Integer pnPort = (Integer)vm3.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 4, lnPort });
    
    vm4.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm5.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { pnPort });

    try {
      vm7.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      getLogWriter().info("Created cache on local site");

      vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln1", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

      vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln2", 3, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

      vm7.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln3", 4, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
      vm7.invoke(WANTestBase.class, "startSender", new Object[] { "ln3" });

      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2,ln3", 1, 10, isOffHeap() });

      getLogWriter().info("Created PRs on local site");

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });
      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 10, isOffHeap() });

      vm7.invoke(WANTestBase.class, "doPuts", new Object[] { testName + "_PR",
          10 });
      pause(1000);
      vm7.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2,ln3", 1, 10, isOffHeap() });

      vm7.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 10, 20 });

      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 10 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
      vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 20 });
    } finally {
      vm7.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_MultipleNode_UserPR_localDestroy_Recreate() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { testName + "_PR", 1000 });
      pause(1000);
      vm5.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 1000, 2000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });

      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySenders_MultiplNode_UserPR_localDestroy_Recreate() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });
    Integer tkPort = (Integer)vm2.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 3, lnPort });
    
    vm6.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });
    vm7.invoke(WANTestBase.class, "createReceiver", new Object[] { tkPort });
    
    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln1", 2, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln2", 3, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln1", 2, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln2", 3, true, 100, 10, false, false, null, true, 4, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln1" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln2" });

      vm6.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });
      vm7.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class, "doPuts",
          new Object[] { testName + "_PR", 1000 });
      pause(1000);
      vm5.invoke(WANTestBase.class, "localDestroyRegion",
          new Object[] { testName + "_PR" });

      try {
        inv1.join();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
        fail("Interrupted the async invocation.");
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });
      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 1000 });

      vm5.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
          testName + "_PR", "ln1,ln2", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "doPutsFrom", new Object[] {
          testName + "_PR", 1000, 2000 });

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });

      vm6.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
      vm7.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          testName + "_PR", 2000 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public void testParallelGatewaySender_ColocatedPartitionedRegions_localDestroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 5, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
          "putcolocatedPartitionedRegion", new Object[] { 2000 });
      pause(1000);

      try {
        vm5.invoke(WANTestBase.class, "localDestroyRegion",
            new Object[] { customerRegionName });
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof UnsupportedOperationException);
      }

      try {
        inv1.join();
      } catch (Exception e) {
        fail("Unexpected exception", e);
      }

      vm4.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
      vm5.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
      vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
          customerRegionName, 2000 });
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
    
  }
  
  public void testParallelGatewaySender_ColocatedPartitionedRegions_destroy() throws Exception {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });
    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(WANTestBase.class, "createReceiver", new Object[] { nyPort });

    try {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME",
          new Object[] { lnPort });

      vm4.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });
      vm5.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              "ln", 1, 100, isOffHeap() });

      vm4.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });
      vm5.invoke(WANTestBase.class, "createConcurrentSender", new Object[] {
          "ln", 2, true, 100, 10, false, false, null, true, 6, OrderPolicy.KEY });

      vm4.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });
      vm5.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

      getLogWriter().info("Created PRs on local site");

      vm2.invoke(WANTestBase.class,
          "createCustomerOrderShipmentPartitionedRegion", new Object[] { null,
              null, 1, 100, isOffHeap() });

      AsyncInvocation inv1 = vm4.invokeAsync(WANTestBase.class,
          "putcolocatedPartitionedRegion", new Object[] { 2000 });
      pause(1000);

      try {
        vm5.invoke(WANTestBase.class, "destroyRegion",
            new Object[] { customerRegionName });
      } catch (Exception ex) {
        assertTrue(ex.getCause() instanceof IllegalStateException);
        return;
      }
      fail("Excpeted UnsupportedOperationException");
    } finally {
      vm4.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
      vm5.invoke(ConcurrentParallelGatewaySenderOperation_2_DUnitTest.class,
          "clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME");
    }
  }
  
  public static void createCache_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME(
      Integer locPort) {
    createCache(false, locPort);
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = -1;
  }

  public static void clear_INFINITE_MAXIMUM_SHUTDOWN_WAIT_TIME() {
    AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME = 0;
  }
  
  public static void closeRegion(String regionName) {
    Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    r.close();
  }

  public static void validateRegionSizeWithinRange(String regionName,
      final int min, final int max) {
    final Region r = cache.getRegion(Region.SEPARATOR + regionName);
    assertNotNull(r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        if (r.keySet().size() > min && r.keySet().size() <= max) {
          return true;
        }
        return false;
      }

      public String description() {
        return "Expected region entries to be within range : " + min + " "
            + max + " but actual entries: " + r.keySet().size();
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120000, 500, true);
  }
}
